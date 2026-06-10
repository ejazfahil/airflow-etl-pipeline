# airflow-etl-pipeline

> Production-style Apache Airflow ETL — CSV and paginated-API ingestion into PostgreSQL, a dbt orchestration DAG, Slack failure alerting, and DAG-integrity tests.

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7-017CEE?logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-warehouse-4169E1?logo=postgresql&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-orchestrated-FF694B?logo=dbt&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-2.1-150458?logo=pandas&logoColor=white)
![Status](https://img.shields.io/badge/status-working%20core-success)

---

## Overview / Aim

This repository orchestrates the **ingest → load → transform** lifecycle of a data
warehouse using **Apache Airflow**. It covers the three canonical ingestion shapes
a data engineer faces — **batch files**, **paginated REST APIs**, and **triggering
the downstream transform layer (dbt)** — each as an independent, scheduled,
retry-aware DAG, with **Slack alerting** on failure and **DAG-integrity tests** in
CI.

## Architecture / How It Works

```
        ┌─────────────────────────────────────────────────────────┐
        │                     Apache Airflow                       │
        │                                                         │
  CSV ──▶ csv_ingest   (@daily)  ─┐                                │
         retries=2, ON CONFLICT  │                                │
                                  ├──▶  PostgreSQL (raw_*) ──┐     │
  API ──▶ api_ingest  (@hourly)  ─┘    warehouse            │     │
         retries=3, pagination                              │     │
                                                            ▼     │
                                   dbt_run (0 6 * * *):  dbt run → dbt test
        │                                                         │
        │   any task fails ──▶ slack_fail_alert (plugins)         │
        └─────────────────────────────────────────────────────────┘
```

- **csv_ingest** — reads a CSV with pandas, stamps `loaded_at`, and upserts into a
  Postgres raw table using `ON CONFLICT DO NOTHING` (idempotent re-runs).
- **api_ingest** — paginates a REST endpoint (page/size) until exhausted, with
  3 retries and a short backoff for transient upstream errors.
- **dbt_run** — a `BashOperator` chain (`dbt run >> dbt test`) against the prod
  target, decoupling orchestration from transformation logic.
- **slack_fail_alert** — a reusable failure callback posting DAG/task/date to a
  Slack webhook.

## Tech Stack & Tools

| Tool | Role |
|------|------|
| **Apache Airflow 2.7.3** | Orchestration (DAGs, scheduling, retries) |
| **apache-airflow-providers-postgres** | Postgres connectivity |
| **pandas 2.1** | CSV parsing / shaping |
| **psycopg2-binary** | Postgres driver for loads |
| **requests** | Paginated API client |
| **dbt** (via BashOperator) | Downstream transformation layer |
| **Slack webhooks** | Failure alerting |
| **pytest + DagBag** | DAG import / structure tests |

## Project Structure

```
airflow-etl-pipeline/
├── dags/
│   ├── csv_ingest_dag.py    # @daily CSV → Postgres, ON CONFLICT DO NOTHING, retries=2
│   ├── api_ingest_dag.py    # @hourly paginated API pull, retries=3 + backoff
│   └── dbt_trigger_dag.py   # 06:00 cron: dbt run >> dbt test (prod target)
├── plugins/
│   └── slack_alert.py       # slack_fail_alert: failure callback → Slack webhook
├── tests/
│   └── test_dags.py         # no import errors, ≥3 DAGs, expected tasks present
├── docs/
│   └── runbook.md           # per-DAG triage (mounts, connections, rate limits)
└── requirements.txt
```

## Key Features / Highlights

- **Three ingestion patterns** — batch file, paginated API, and transform-trigger,
  each scheduled independently (`@daily`, `@hourly`, `0 6 * * *`).
- **Idempotent loads** — `ON CONFLICT DO NOTHING` plus a `loaded_at` audit column
  make re-runs and backfills safe.
- **Retry policies tuned per source** — CSV (2 retries / 5 min), API (3 retries /
  2 min) reflecting each source's failure profile.
- **dbt orchestration** — transformation runs and tests are chained so a failing
  test surfaces in the same DAG run.
- **Failure alerting** — a single reusable Slack callback for all DAGs.
- **CI-grade DAG tests** — `DagBag` checks assert zero import errors, a minimum DAG
  count, and presence of expected tasks — catching broken DAGs before deploy.
- **Operational runbook** — `docs/runbook.md` maps failures (missing mount, dead
  connection, 429/503) to concrete recovery steps.

## Challenges

- **Pipeline idempotency** — designing loads that tolerate retries and catch-up
  runs without duplicating data.
- **Resilient API ingestion** — paginating to exhaustion while surviving rate
  limits and transient upstream outages via retries/backoff.
- **Catching breakage early** — DAG-integrity tests prevent a syntax or import
  error from reaching the scheduler.

## Future Work

- Replace inline credentials with Airflow Connections / Secrets backend.
- Swap row-by-row inserts for `COPY` / `execute_values` bulk loads.
- TaskFlow API + datasets for data-aware scheduling.
- Astronomer/Cosmos-style dbt integration with per-model task mapping.
- Containerised local stack (Airflow + Postgres) via Docker Compose.

## Getting Started / Usage

```bash
pip install -r requirements.txt

# Point Airflow at the dags/ folder, then:
airflow db init
airflow dags list            # csv_ingest, api_ingest, dbt_run

# Trigger a run
airflow dags trigger csv_ingest

# Validate DAGs (CI)
pytest tests/ -v
```

## Conclusion

Demonstrates **orchestration engineering** with Airflow: multiple ingestion
patterns, idempotent Postgres loads, source-tuned retries, dbt orchestration,
Slack alerting, and CI DAG-integrity tests — the operational backbone of a
batch data platform.
