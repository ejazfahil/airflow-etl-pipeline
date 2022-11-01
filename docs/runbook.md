# DAG Runbook 2022-11-01

## csv_ingest failure
1. Check `/data/` mount is available
2. Check Postgres connection: `psql -h localhost -U analyst warehouse`
3. Re-trigger from Airflow UI → Graph → Clear task

## api_ingest failure
- 429 rate-limit: increase `retry_delay` in DEFAULT_ARGS
- 503 upstream: wait 10 min, re-trigger
