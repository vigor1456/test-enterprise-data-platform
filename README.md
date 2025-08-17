# Enterprise Data Platform (Starter)

A test project to demonstrate **enterprise-grade data ops**:
- Orchestrated pipelines (Prefect)
- Ingestion → transform → load → validate
- Postgres as the system of record
- Observability via Prometheus Pushgateway (success/failure, latency)
- Tests, retries, idempotency, and documentation

## Components
- **Python 3.10+**
- **Prefect 2.x** for orchestration
- **PostgreSQL** for storage
- **Prometheus Pushgateway** for metrics (optional; easy to replace with Grafana Cloud or Alertmanager later)
- **pytest** for tests

## Quickstart (Local)
1. Create and activate a virtual env
   ```bash
   python -m venv .venv && source .venv/bin/activate
   ```
2. Install deps
   ```bash
   pip install -r requirements.txt
   ```
3. Start Postgres (Docker example)
   ```bash
   docker compose up -d postgres
   ```
4. (Optional) Start Pushgateway & Prometheus stack
   ```bash
   docker compose up -d pushgateway prometheus grafana
   ```
5. Run the pipeline locally (one-off run)
   ```bash
   python -m src.pipeline
   ```
6. Start Prefect UI (optional, local)
   ```bash
   prefect server start
   ```

## What it does
- **Ingest:** Fetches data from a source (placeholder API or CSV).
- **Transform:** Validates schema, deduplicates, type-casts.
- **Load:** Upserts into Postgres (idempotent).
- **Validate:** Row counts, null checks; pushes metrics to Prometheus.
- **Alert hooks:** Failures push a metric; you can wire alerts in Prometheus/Grafana.

## SLA / SLO (example)
- **SLA:** Daily pipeline by 06:00 CET, success rate ≥ 99.5% month-to-date.
- **SLOs:** Freshness < 24h; end-to-end latency < 10 min; data completeness ≥ 99%.
- **Error budget:** ≤ 3 failed runs per 1,000.

## Repository structure
```
enterprise_data_platform_skeleton/
  ├─ docker-compose.yml
  ├─ requirements.txt
  ├─ Makefile
  ├─ src/
  │   ├─ pipeline.py
  │   ├─ ingest.py
  │   ├─ transform.py
  │   ├─ load.py
  │   ├─ monitoring.py
  │   └─ config.py
  ├─ tests/
  │   ├─ test_transform.py
  │   └─ test_pipeline_smoke.py
  └─ README.md
```

## Next steps (optional upgrades)
- Replace Pushgateway with **Prometheus + Alertmanager** rules and Grafana dashboard.
- Add **dbt** for transformations and data modeling.
- Introduce **Great Expectations** for data validation suites.
- Switch orchestration to **Airflow** and deploy on **Kubernetes** with a proper CI/CD.
- Add secrets manager (Vault/GCP Secret Manager), and role-based access (users/roles in Postgres).

```

---

## Health Surveillance Pipeline (portfolio-ready domain example)

### Run it
1. Start infra (Postgres + Prometheus/Pushgateway + Grafana):  
   ```bash
   docker compose up -d postgres pushgateway prometheus grafana
   ```
2. Install Python deps and run the domain pipeline:  
   ```bash
   source .venv/bin/activate  # or create it first
   python -m src.domains.health_pipeline
   ```

### What it does
- **Ingest**: `data/health_surveillance_daily.csv` (synthetic).
- **Transform**: schema/type validation, dedupe, compute `positivity_rate` and `case_fatality_rate`, clamp outliers.
- **Load**: upsert to `fact_health_surveillance` (Postgres).
- **Observe**: pushes metrics to Pushgateway with job=`health_pipeline` including extras (`facilities`, `null_rows`).

### Grafana
- Auto‑provisioned **Prometheus** and **Postgres** datasources.
- Preloaded dashboard: *Health Surveillance Pipeline* with status, rows, duration, and a table of top facilities by positivity rate.

Login Grafana: `admin / admin` (password set to `admin` in docker‑compose; change in real use).

### Extend
- Replace CSV with a secure object store or API.
- Add alert rules (e.g., pipeline_status==0 for >5m; duration p95 > SLA; positivity_rate spikes).
- Wire Great Expectations for formal data‑quality suites.
