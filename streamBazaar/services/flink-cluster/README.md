# Flink Cluster (Optional)

## Aim
Optional Flink runtime for future data-plane integration.

## Note
Main workload/monitoring flow in this project currently uses Python services and Kafka without requiring Flink.

## Run
```bash
docker compose up -d flink-jobmanager flink-taskmanager
```
