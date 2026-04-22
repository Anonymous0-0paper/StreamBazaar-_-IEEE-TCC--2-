# Flink Integration Quick Start

## ⏱️ 5-Minute Setup

### 1. Build the Flink Cluster
```bash
cd /home/user/Downloads/StreamBazaar\ _\ IEEE\ TCC\ \(2\)/streamBazaar
docker compose build --no-cache flink-jobmanager flink-taskmanager
```

### 2. Start All Services
```bash
docker compose up -d
```

Wait ~30 seconds for services to initialize.

### 3. Verify Flink Cluster is Healthy
```bash
curl -s http://localhost:18088/v1/overview | jq .
```

Expected output:
```json
{
  "flink-version": "1.18.0",
  "taskmanagers": 1,
  "slots-total": 4,
  "slots-available": 4
}
```

### 4. Check Job Status
```bash
curl -s http://localhost:18088/v1/jobs | jq '.jobs[] | {id, status, name}'
```

Expected output (after job is submitted):
```json
{
  "id": "xxxxxxxx...",
  "status": "RUNNING",
  "name": "StreamBazaar Auction Job"
}
```

### 5. Monitor Job Logs
```bash
docker logs -f $(docker ps --filter "name=flink-jobmanager" -q)
```

Expected output (every ~100ms):
```
2026-03-18 15:50:00.123 [pool-2-thread-1] INFO StreamBazaarJob - Allocation decision: {"tenantId":"fraud","timestamp":1710773400123,"allocatedResources":...}
2026-03-18 15:50:00.223 [pool-2-thread-1] INFO AuctionOrchestrator - Allocation for tenant fraud: 1.23 resources allocated, preempted=false
```

## 🔍 Access Flink UI

1. Open browser: `http://localhost:18088`
2. Click on running job to see:
   - operator graph and parallelism
   - task distribution across taskmanagers
   - backpressure and latency metrics
   - checkpoint history and state size

## 📊 Metrics Inspection

### View Prometheus Metrics
```bash
curl -s http://localhost:19249/metrics | grep flink | head -20
```

### Check Job Statistics
```bash
curl -s http://localhost:18088/v1/jobs/<JOB_ID> | jq '.status, .start_time'
```

## ❌ Troubleshooting

### Problem: No Flink containers running
```bash
# Check logs
docker compose logs | grep -i error

# Rebuild from scratch
docker compose down -v
docker system prune -a
docker compose build --no-cache
docker compose up -d
```

### Problem: Job not in RUNNING state
```bash
# Check job logs for startup errors
docker logs $(docker ps -f "name=flink-jobmanager" -q) --tail 50

# Manually submit job
./scripts/submit-flink-job.sh flink-jobmanager 8081
```

### Problem: No allocation decisions in logs
```bash
# Verify job is consuming events (check task manager)
curl -s http://localhost:18088/v1/jobs/<JOB_ID>/stats | jq '.subtasks' 

# Check RecordsIn, RecordsOut metrics
```

## 🔌 Next: Connect to Kafka

To consume real tenant events from Kafka:

1. **Update StreamBazaarJob.java** - Replace SimpleAuctionEventSource:
   ```java
   // Comment out:
   // DataStream<String> inputEvents = env.addSource(new SimpleAuctionEventSource());
   
   // Replace with:
   KafkaSource<String> source = KafkaSource.<String>getBuilder()
       .setBootstrapServers("kafka:9092")
       .setTopicPattern("tenant\\..*\\.input")
       .setValueOnlyDeserializer(new SimpleStringSchema())
       .setStartingOffsets(OffsetsInitializer.latest())
       .build();
   
   DataStream<String> inputEvents = env.fromSource(source,
       WatermarkStrategy.noWatermarks(),
       "Kafka Tenant Input");
   ```

2. **Add Kafka sink** - Write allocations to Kafka:
   ```java
   decisions
       .map(d -> d.toJson())
       .sinkTo(KafkaSink.<String>builder()
           .setBootstrapServers("kafka:9092")
           .setRecordSerializationSchema(
               KafkaRecordSerializationSchema.builder()
               .setTopic("streamBazaar.allocations")
               .setValueSerializationSchema(new SimpleStringSchema())
               .build())
           .build());
   ```

3. **Rebuild and restart**:
   ```bash
   docker compose build flink-jobmanager flink-taskmanager --no-cache
   docker compose down && docker compose up -d
   ```

4. **Generate events**:
   ```bash
   python3 scripts/run_workloads.py --duration-sec 30 --records-per-tenant 100
   ```

5. **Verify allocations appear**:
   ```bash
   docker exec $(docker ps -f "name=^kafka" -q) \
     kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic streamBazaar.allocations \
     --from-beginning \
     --max-messages 10
   ```

## 📈 Performance Benchmarking

### Measure Event Throughput
```bash
# Monitor job metrics
watch -n 1 'curl -s http://localhost:18088/v1/jobs/$(curl -s http://localhost:18088/v1/jobs | jq -r .jobs[0].id)/stats | jq .subtasks'

# Look for "records_in_rate" and "records_out_rate"
```

### Measure Allocation Latency
```bash
# Add timestamp tracking in AuctionOrchestrator.java:
// Start:
long startTime = System.nanoTime();

// End:
long latencyMs = (System.nanoTime() - startTime) / 1_000_000;
LOG.info("Allocation latency: {}ms", latencyMs);
```

### Measure Checkpoint Duration
```bash
curl -s http://localhost:18088/v1/jobs/<JOB_ID>/checkpoints | jq '.latest | {id, trigger_timestamp, completion_timestamp}'
```

## 🧪 Test Scenarios

### Test 1: Basic Functionality
```bash
# Start job, generate 100 events, verify 100 allocations
python3 scripts/run_workloads.py --duration-sec 10 --records-per-tenant 30
docker compose logs flink-jobmanager 2>&1 | grep "Allocation decision" | wc -l
# Expected: ~90 allocations (3 tenants × 30)
```

### Test 2: SLA Violation Handling
```bash
# Modify TenantState in AuctionOrchestrator to simulate high latencies
# Expected: preemption flag should be TRUE after 10% SLA breaches
```

### Test 3: Migration Cooldown
```bash
# Trigger multiple migrations, verify 60s cooldown prevents threshold
# Expected: Only 1 migration per 60s per tenant
```

### Test 4: Kafka Integration
```bash
# Switch to KafkaSource, run workloads, verify allocations on topic
# Expected: streamBazaar.allocations receives all allocation decisions
```

## 📚 Documentation References

- **FLINK_INTEGRATION.md**: Complete architecture and design
- **FLINK_NEXT_STEPS.md**: Integration roadmap and validation
- **FLINK_FILE_MANIFEST.md**: File-by-file code inventory
- **README.md Section 5.2**: Flink deployment and usage

## 🎯 Key Files to Understand

| File | Purpose | Key Classes |
|------|---------|------------|
| `flink-integration/src/main/java/streambazaar/flink/StreamBazaarJob.java` | Main job entry point | `SimpleAuctionEventSource`, `TenantEvent`, `AllocationDecision` |
| `flink-integration/src/main/java/streambazaar/flink/AuctionOrchestrator.java` | Auction logic | `AuctionOrchestrator`, `TenantState` |
| `docker-compose.yml` | Container orchestration | Services: flink-jobmanager, flink-taskmanager, flink-job-submitter |
| `scripts/submit-flink-job.sh` | Job deployment | REST API calls to Flink jobmanager |

## 🚀 Next Comprehensive Steps

1. ✅ Build Flink integration (done)
2. ⬜ Test with simple event source (start here)
3. ⬜ Connect to Kafka input/output topics
4. ⬜ Run paper experiments with Flink active
5. ⬜ Generate comparative charts vs REST-based coordinator
6. ⬜ Benchmark against YARN/Kubernetes baselines

---

**Estimated time to first allocation**: 2 minutes after `docker compose up -d`
