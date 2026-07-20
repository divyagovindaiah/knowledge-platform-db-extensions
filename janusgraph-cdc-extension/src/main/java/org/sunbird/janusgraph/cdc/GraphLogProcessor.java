package org.sunbird.janusgraph.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.log.Change;
import org.janusgraph.core.log.ChangeProcessor;
import org.janusgraph.core.log.ChangeState;
import org.janusgraph.core.log.LogProcessorFramework;
import org.janusgraph.core.log.TransactionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * GraphLogProcessor running inside JanusGraph Server.
 * Listens to "learning_graph_events" user log and publishes changes to
 * configured sinks.
 */
public class GraphLogProcessor {

    private static final Logger logger = LoggerFactory.getLogger(GraphLogProcessor.class);
    private static final String LOG_IDENTIFIER = "learning_graph_events";
    private static final ObjectMapper mapper = new ObjectMapper();

    // Singleton instance
    private static volatile GraphLogProcessor instance;

    // Instance State
    private List<EventSink> sinks = new ArrayList<>();
    private MessageConverter converter;
    private boolean isStarted = false;

    // Event buffering removed

    private GraphLogProcessor() {
        // Prevent direct instantiation
    }

    public static synchronized void start(JanusGraph graph, Map<String, Object> config) {
        if (instance == null) {
            instance = new GraphLogProcessor();
        }
        instance.init(graph, config);
    }

    public static synchronized void shutdown() {
        if (instance != null) {
            instance.stop();
            instance = null;
        }
    }

    private void init(JanusGraph graph, Map<String, Object> config) {
        if (isStarted) {
            logger.info("GraphLogProcessor is already running.");
            return;
        }

        // Configuration passed from script
        boolean enableProcessor = Boolean
                .parseBoolean(String.valueOf(config.getOrDefault("graph.txn.log_processor.enable", "false")));
        if (!enableProcessor) {
            logger.info("GraphLogProcessor is disabled by config.");
            return;
        }

        logger.info("Starting GraphLogProcessor...");

        // INitialize Converter
        String converterType = (String) config.getOrDefault("graph.txn.log_processor.converter", "DEFAULT");
        if ("TELEMETRY".equalsIgnoreCase(converterType)) {
            converter = new TelemetryMessageConverter();
            logger.info("Using TelemetryMessageConverter");
        } else if ("SUNBIRD_LEGACY".equalsIgnoreCase(converterType)) {
            converter = new SunbirdLegacyMessageConverter();
            logger.info("Using SunbirdLegacyMessageConverter");
        } else {
            converter = new SimpleMessageConverter();
            logger.info("Using SimpleMessageConverter");
        }

        // Initialize Sinks
        sinks.clear();
        String sinksConfig = (String) config.getOrDefault("graph.txn.log_processor.sinks", "LOG"); // Default to LOG
        String[] sinkTypes = sinksConfig.split(",");

        for (String sinkType : sinkTypes) {
            if ("LOG".equalsIgnoreCase(sinkType.trim())) {
                sinks.add(new LogFileEventSink());
                logger.info("Added Log File Event Sink");
            }
        }

        if (sinks.isEmpty()) {
            logger.warn("No sinks configured. Processor will consume logs but output nowhere.");
        }

        try {
            LogProcessorFramework framework = JanusGraphFactory.openTransactionLog(graph);
            // No setProcessorIdentifier(): an identified processor persists its read
            // marker and resumes it on every restart, ignoring setStartTime() after the
            // first run. That forces replay of old/backlogged transaction-log entries on
            // every JanusGraph restart, and vertex property lookups on those replayed
            // entries come back empty (JanusGraph/storage limitation), which makes
            // SunbirdLegacyMessageConverter treat every backlogged vertex as
            // objectType="vertex" and filter it out — i.e. CDC silently stops emitting
            // events after any stop/start. Staying unidentified means we always start
            // fresh from setStartTime() below, trading "replay the outage window" for
            // "CDC actually works after a restart."
            framework.addLogProcessor(LOG_IDENTIFIER)
                    .setStartTime(Instant.now().minus(1, ChronoUnit.MINUTES))
                    .addProcessor(new ChangeProcessor() {
                        @Override
                        public void process(JanusGraphTransaction tx, TransactionId txId, ChangeState changeState) {
                            try {
                                processChanges(txId, changeState);
                            } catch (Exception e) {
                                logger.error("Error processing transaction logs", e);
                            }
                        }
                    })
                    .build();

            // Event buffering removed

            isStarted = true;
            logger.info("GraphLogProcessor started successfully with {} sinks.", sinks.size());

        } catch (Exception e) {
            logger.error("Failed to start GraphLogProcessor", e);
        }
    }

    private void stop() {
        // Buffer scheduler removed

        for (EventSink sink : sinks) {
            try {
                sink.close();
            } catch (Exception e) {
                logger.warn("Error closing sink", e);
            }
        }
        sinks.clear();
        isStarted = false;
        logger.info("GraphLogProcessor stopped.");
    }

    private void processChanges(TransactionId txId, ChangeState changeState) {
        Set<Object> processedIds = new HashSet<>();

        // 1. Process Added Vertices (CREATE)
        for (JanusGraphVertex vertex : changeState.getVertices(Change.ADDED)) {
            processVertexChange(vertex, changeState, "CREATE", txId);
            processedIds.add(vertex.id());
        }

        // 2. Process Removed Vertices (DELETE)
        for (JanusGraphVertex vertex : changeState.getVertices(Change.REMOVED)) {
            processVertexChange(vertex, changeState, "DELETE", txId);
            processedIds.add(vertex.id());
        }

        // 3 & 4. Process Property Updates AND Edge Changes (UPDATE)
        // getVertices(Change.ANY) returns the union of ADDED/REMOVED vertices
        // plus endpoint vertices extracted from all changed relations (edges).
        // This ensures edge-only changes are also discovered.
        Set<JanusGraphVertex> changedVertices = changeState.getVertices(Change.ANY);

        // Count all relations in this ChangeState (edges + properties combined)
        int addedRelCount = 0;
        int removedRelCount = 0;
        for (JanusGraphRelation r : changeState.getRelations(Change.ADDED)) {
            addedRelCount++;
            if (r.isEdge()) {
                logger.info("  ADDED relation (edge): type={}, vertices={}", r.getType().name(), r);
            }
        }
        for (JanusGraphRelation r : changeState.getRelations(Change.REMOVED)) {
            removedRelCount++;
            if (r.isEdge()) {
                logger.info("  REMOVED relation (edge): type={}, vertices={}", r.getType().name(), r);
            }
        }
        logger.info("ChangeState — vertices ADDED: {}, REMOVED: {}, ANY: {} | relations ADDED: {}, REMOVED: {}",
                changeState.getVertices(Change.ADDED).size(),
                changeState.getVertices(Change.REMOVED).size(),
                changedVertices.size(),
                addedRelCount, removedRelCount);
        for (JanusGraphVertex vertex : changedVertices) {
            // If it's a new vertex, we already processed it as CREATE
            if (changeState.getVertices(Change.ADDED).contains(vertex)) {
                continue;
            }
            // If it's a removed vertex, we already processed it as DELETE
            if (changeState.getVertices(Change.REMOVED).contains(vertex)) {
                continue;
            }

            if (!processedIds.contains(vertex.id())) {
                logger.info("Processing vertex {} as UPDATE", vertex.id());
                processVertexChange(vertex, changeState, "UPDATE", txId);
            }
        }
    }

    private void processVertexChange(JanusGraphVertex vertex, ChangeState changeState, String operationType,
            TransactionId txId) {
        try {

            // 2. Convert message using the Strategy Pattern
            // We now delegate all operations (including UPDATE) to the converter.
            // SunbirdLegacyMessageConverter logic has been updated to handle UPDATEs with
            // full snapshots.
            Map<String, Object> event = converter.convert(vertex, changeState, operationType, txId);

            if (event == null) {
                logger.debug("Event filtered by converter for node {}", vertex.id());
                return;
            }

            // 4. Send event immediately (No buffering)
            sendEventToSinks(vertex.id().toString(), event);

        } catch (Exception e) {
            logger.error("Error converting/processing vertex change event", e);
        }
    }

    /**
     * Send event to all configured sinks
     */
    private void sendEventToSinks(String key, Map<String, Object> event) {
        try {
            String json = mapper.writeValueAsString(event);
            for (EventSink sink : sinks) {
                try {
                    sink.send(key, json);
                } catch (Exception e) {
                    logger.error("Error sending event to sink: {}", sink.getClass().getSimpleName(), e);
                }
            }
            logger.info("Sent event: {}", json);
        } catch (Exception e) {
            logger.error("Error serializing event", e);
        }
    }

    private boolean hasStatusAttribute(Map<String, Object> event) {
        try {
            if (event.containsKey("transactionData")) {
                Map<String, Object> txData = (Map<String, Object>) event.get("transactionData");
                if (txData != null && txData.containsKey("properties")) {
                    Map<String, Object> props = (Map<String, Object>) txData.get("properties");
                    return props != null && props.containsKey("status");
                }
            }
            // Check for flat properties (SimpleMessageConverter)
            if (event.containsKey("properties")) {
                Map<String, Object> props = (Map<String, Object>) event.get("properties");
                return props != null && props.containsKey("status");
            }
        } catch (Exception e) {
            // ignore
        }
        return false;
    }

    private String getNodeUniqueId(Map<String, Object> event) {
        if (event.containsKey("nodeUniqueId")) {
            return (String) event.get("nodeUniqueId");
        }
        return null;
    }

    private Long getLastUpdatedOn(Map<String, Object> event) {
        try {
            // Check nested structure first
            // (TelemetryMessageConverter/SunbirdLegacyMessageConverter)
            if (event.containsKey("transactionData")) {
                Map<String, Object> txData = (Map<String, Object>) event.get("transactionData");
                if (txData != null && txData.containsKey("properties")) {
                    Map<String, Object> props = (Map<String, Object>) txData.get("properties");
                    if (props != null && props.containsKey("lastUpdatedOn")) {
                        Object lastUpdatedOnObj = props.get("lastUpdatedOn");
                        return parseLastUpdatedOn(lastUpdatedOnObj);
                    }
                }
            }

            // Check flat properties if not found (SimpleMessageConverter)
            if (event.containsKey("properties")) {
                Map<String, Object> props = (Map<String, Object>) event.get("properties");
                if (props != null && props.containsKey("lastUpdatedOn")) {
                    Object lastUpdatedOnObj = props.get("lastUpdatedOn");
                    return parseLastUpdatedOn(lastUpdatedOnObj);
                }
            }
        } catch (Exception e) {
            logger.warn("Error extracting lastUpdatedOn from event", e);
        }
        return null;
    }

    private Long parseLastUpdatedOn(Object lastUpdatedOnObj) {
        if (lastUpdatedOnObj == null) {
            return null;
        }

        if (lastUpdatedOnObj instanceof Map) {
            Map<String, Object> valMap = (Map<String, Object>) lastUpdatedOnObj;
            if (valMap.containsKey("nv")) {
                Object nv = valMap.get("nv");
                return parseTimestamp(nv);
            }
        } else {
            return parseTimestamp(lastUpdatedOnObj);
        }
        return null;
    }

    private Long parseTimestamp(Object ts) {
        if (ts == null)
            return null;
        if (ts instanceof Long) {
            return (Long) ts;
        }
        if (ts instanceof String) {
            try {
                // ISO-8601 format: 2026-02-11T05:34:06.476+0000
                // We can use DateTimeFormatter to parse it, or Instant.parse if it's standard.
                // The example shows: 2026-02-11T05:34:06.476+0000
                // This corresponds to DateTimeFormatter.ISO_OFFSET_DATE_TIME
                return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
                        .parse((String) ts, Instant::from)
                        .toEpochMilli();
            } catch (Exception e) {
                logger.warn("Failed to parse timestamp string: {}", ts);
            }
        }
        return null;
    }
}
