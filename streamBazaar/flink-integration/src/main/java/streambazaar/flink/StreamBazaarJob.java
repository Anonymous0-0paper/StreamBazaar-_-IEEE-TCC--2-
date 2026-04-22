package streambazaar.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * StreamBazaar Flink Job: Orchestrates auction-driven resource allocation natively in Flink.
 * 
 * Pipeline:
 * 1. Consume tenant input events from sources (Kafka, socket, etc.)
 * 2. Apply pricing logic and collect bids
 * 3. Execute auction and compute allocations
 * 4. Emit allocations to sinks
 * 5. Emit per-tenant outputs
 * 
 * This replaces the REST-based stream-coordinator with native Flink streaming.
 */
public class StreamBazaarJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamBazaarJob.class);

    public static void main(String[] args) throws Exception {
        RecordingMetadata.initialize();
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10000); // 10s checkpointing for fault tolerance
        
        LOG.info("Starting StreamBazaar Flink Job (Kafka source/sink enabled)");

        final String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        final String topicPattern = System.getenv().getOrDefault("KAFKA_INPUT_TOPIC_PATTERN", "tenant\\\\..*\\\\.input");
        final String allocTopic = System.getenv().getOrDefault("ALLOC_TOPIC", "streamBazaar.allocations");

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopicPattern(java.util.regex.Pattern.compile(topicPattern))
            .setGroupId(System.getenv().getOrDefault("KAFKA_GROUP_ID", "streambazaar-flink-job"))
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> inputEvents = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Kafka Tenant Input");

        // Parse JSON and apply StreamBazaar decision logic
        DataStream<TenantEvent> events = inputEvents
                .map(json -> {
                    try {
                        JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
                        TenantEvent event = new TenantEvent();
                        JsonElement tenantElem = obj.has("tenantId") ? obj.get("tenantId") : obj.get("tenant_id");
                        JsonElement recordElem = obj.has("recordId") ? obj.get("recordId") : obj.get("record_id");
                        JsonElement tsElem = obj.get("timestamp");
                        JsonElement workloadElem = obj.has("workload") ? obj.get("workload") : obj.get("dataset");
                        JsonElement slaElem = obj.get("slaTarget");
                        JsonElement prioElem = obj.get("priority");

                        event.tenantId = (tenantElem != null && !tenantElem.isJsonNull()) ? tenantElem.getAsString() : "unknown";
                        event.recordId = (recordElem != null && !recordElem.isJsonNull()) ? recordElem.getAsString() : "rec-unknown";
                        event.timestamp = (tsElem != null && !tsElem.isJsonNull()) ? tsElem.getAsLong() : System.currentTimeMillis();
                        event.workload = (workloadElem != null && !workloadElem.isJsonNull()) ? workloadElem.getAsString() : "unknown";
                        event.slaTarget = (slaElem != null && !slaElem.isJsonNull()) ? slaElem.getAsLong() : 200L;
                        event.priority = (prioElem != null && !prioElem.isJsonNull()) ? prioElem.getAsDouble() : 1.0;
                        event.originalJson = json;
                        return event;
                    } catch (Exception e) {
                        LOG.error("Failed to parse event: {}", json, e);
                        return null;
                    }
                })
                .filter(event -> event != null);

        // Batch events into windows and apply auction logic
        DataStream<AllocationDecision> decisions = events
                .keyBy(e -> e.tenantId)
                .process(new AuctionOrchestrator());

        KafkaSink<String> allocationSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(allocTopic)
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build())
            .build();

        decisions
            .map(AllocationDecision::toJson)
            .sinkTo(allocationSink);

        decisions
            .map(d -> {
                LOG.info("Allocation for tenant {}: {} resources allocated, preempted={}",
                    d.tenantId, String.format("%.2f", d.allocatedResources), d.preempted);
                return d.toJson();
            })
            .print()
            .name("Allocation Logger");

        env.execute("StreamBazaar Auction Job");
    }

    /**
     * Represents an input event from a tenant workload
     */
    public static class TenantEvent {
        public String tenantId;
        public String recordId;
        public long timestamp;
        public String workload;
        public long slaTarget;
        public double priority;
        public String originalJson;
    }

    /**
     * Represents an allocation decision for a tenant
     */
    public static class AllocationDecision {
        public String tenantId;
        public long timestamp;
        public double allocatedResources;
        public double cpuShare;
        public double memoryShare;
        public boolean preempted;
        
        public String toJson() {
            JsonObject obj = new JsonObject();
            obj.addProperty("tenantId", tenantId);
            obj.addProperty("timestamp", timestamp);
            obj.addProperty("allocatedResources", allocatedResources);
            obj.addProperty("cpuShare", cpuShare);
            obj.addProperty("memoryShare", memoryShare);
            obj.addProperty("preempted", preempted);
            return obj.toString();
        }
    }
}
