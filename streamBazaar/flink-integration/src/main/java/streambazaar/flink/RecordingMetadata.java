package streambazaar.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Tracks per-operator latencies for all records passing through the StreamBazaar job.
 * Integrates with the LatencyTracker to emit metrics for paper evaluation.
 */
public class RecordingMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(RecordingMetadata.class);
    
    private static final Map<String, OperatorLatencies> operatorMetrics = new ConcurrentHashMap<>();
    
    public static void initialize() {
        LOG.info("Initializing RecordingMetadata for StreamBazaar latency tracking");
    }
    
    public static void recordOperatorLatency(String operatorId, String recordId, long latencyMs) {
        operatorMetrics.putIfAbsent(operatorId, new OperatorLatencies());
        OperatorLatencies metrics = operatorMetrics.get(operatorId);
        metrics.recordLatency(latencyMs);
    }
    
    public static Map<String, OperatorLatencies> getMetrics() {
        return operatorMetrics;
    }
    
    public static void printMetrics() {
        LOG.info("=== StreamBazaar Operator Latencies ===");
        for (Map.Entry<String, OperatorLatencies> entry : operatorMetrics.entrySet()) {
            LOG.info("Operator: {}", entry.getKey());
            LOG.info("  Count: {}", entry.getValue().count);
            LOG.info("  Mean: {:.2f}ms", entry.getValue().getMean());
            LOG.info("  P50: {:.2f}ms", entry.getValue().getPercentile(50));
            LOG.info("  P99: {:.2f}ms", entry.getValue().getPercentile(99));
        }
    }
    
    /**
     * Per-operator latency statistics
     */
    public static class OperatorLatencies {
        private final Map<Integer, Long> latencies = new ConcurrentHashMap<>();
        private int count = 0;
        
        public synchronized void recordLatency(long latencyMs) {
            latencies.put(count++, latencyMs);
        }
        
        public double getMean() {
            if (latencies.isEmpty()) return 0;
            return latencies.values().stream()
                    .mapToLong(Long::longValue)
                    .average()
                    .orElse(0);
        }
        
        public double getPercentile(int percentile) {
            if (latencies.isEmpty()) return 0;
            int index = (int) Math.ceil((percentile / 100.0) * latencies.size()) - 1;
            index = Math.max(0, Math.min(index, latencies.size() - 1));
            
            return latencies.values().stream()
                    .mapToLong(Long::longValue)
                    .sorted()
                    .skip(index)
                    .findFirst()
                    .orElse(0);
        }
    }
}
