# Configs

## Aim
Static configuration files for cluster behavior, tenants, Prometheus, and Grafana provisioning.

## What You Can Change
- Prometheus scrape targets/interval: `prometheus.yml`.
- Tenant defaults: `tenant-configs/`.
- Cluster-level options: `cluster-config.yml` and `benchmark-configs/`.
- Grafana data source/dashboards: `grafana/provisioning/`.

## Run Impact
Config edits immediately change observability coverage and how experiments are parameterized.

## Validation
```bash
docker compose restart prometheus grafana
curl -fsS http://localhost:19090/-/ready
```
