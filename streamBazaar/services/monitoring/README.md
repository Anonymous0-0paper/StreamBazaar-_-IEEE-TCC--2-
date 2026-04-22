# Monitoring Service

## Aim
Runs Prometheus scraping stack-level metrics.

## What You Can Change
- Scrape jobs/targets and intervals in `prometheus.yml`.
- Add new services by adding target host:port and metrics path.

## Run
```bash
docker compose up -d prometheus grafana
curl -fsS http://localhost:19090/api/v1/targets
```

## Impact
Controls which metrics are visible for alerting, dashboarding, and CSV export.
