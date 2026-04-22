package streambazaar.scheduler;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Placeholder implementation aligned with the paper's per-operator latency tracking.
 * Integrate this class into a Flink scheduler plugin when the custom scheduler build is ready.
 */
public class LatencyTracker {
    private final Map<String, Long> recordTimestamps = new ConcurrentHashMap<>();

    public void trackRecordStart(String recordId, String operatorId) {
        recordTimestamps.put(recordId + ":" + operatorId, System.nanoTime());
    }

    public LatencySample trackRecordComplete(String recordId, String operatorId, String tenantId) {
        String key = recordId + ":" + operatorId;
        Long startTime = recordTimestamps.remove(key);
        if (startTime == null) {
            return null;
        }

        long latencyNs = System.nanoTime() - startTime;
        return new LatencySample(tenantId, operatorId, recordId, latencyNs / 1_000_000.0, Instant.now().toEpochMilli());
    }

    public static class LatencySample {
        public final String tenantId;
        public final String operatorId;
        public final String recordId;
        public final double latencyMs;
        public final long timestampMs;

        public LatencySample(String tenantId, String operatorId, String recordId, double latencyMs, long timestampMs) {
            this.tenantId = tenantId;
            this.operatorId = operatorId;
            this.recordId = recordId;
            this.latencyMs = latencyMs;
            this.timestampMs = timestampMs;
        }
    }
}
