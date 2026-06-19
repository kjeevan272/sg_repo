# Softgames Payments — Data Engineer Assignment

Production-grade ETL pipeline for payment provider data. Ingests JSON from a REST API,
lands it in S3, transforms with PySpark into a Delta Lake lakehouse following the
**Medallion architecture** (bronze → silver → gold), and exposes analyst-ready KPI marts.
Orchestrated with Airflow.

## Architecture

```
Payment API
    │  (pydantic contract + retry/backoff)
    ▼
S3 bronze/raw/dt=YYYY-MM-DD/run_id=<airflow_run>/payments.json.gz   (immutable)
    │  PySpark + Great Expectations  (schema enforcement, DQ gates)
    ▼
S3 silver/payments/   Delta table  (MERGE upserts, CDC enabled, SCD2 dims)
    │  Spark SQL
    ▼
S3 gold/
  ├─ fact_payment_transaction    (grain: 1 row per txn, status-change CDC)
  ├─ dim_game (SCD2) · dim_currency · dim_date · fx_rate_daily
  └─ agg_game_daily_revenue      (analyst mart: revenue, success_rate, ARPPU…)
    ▼
Athena / Redshift Spectrum  →  BI
```

## What's in the repo

| Path | Purpose |
|---|---|
| `dags/payments_etl.py` | Airflow DAG — TaskFlow, dynamic mapping, idempotent backfill |
| `src/sg_payments/contracts.py` | Pydantic v2 data contract — fail-fast on schema drift |
| `src/sg_payments/ingest.py` | API → S3 bronze. Retries, backoff, manifest + checksum |
| `src/sg_payments/bronze_to_silver.py` | PySpark JSON → Delta. AQE, skew handling, MERGE |
| `src/sg_payments/silver_to_gold.py` | Star-schema fact + dims + KPI mart |
| `src/sg_payments/dq.py` | Great Expectations suite — not_null, unique, ranges, enums |
| `sql/ddl/` | Table DDL (fact, dims, marts) |
| `tests/` | pytest + chispa unit tests, DAG integrity test |
| `infra/terraform/` | S3 bucket, Glue DB, IAM least-privilege roles |

## Data model (star schema)

- **fact_payment_transaction** — `transaction_id` PK, `game_key`, `date_key`,
  `currency_key`, `price`, `price_eur`, `status`, `ingest_ts`, `valid_from/to`, `is_current`
- **dim_game** — SCD2 (genre, studio can change)
- **dim_currency** + **fx_rate_daily** — multi-currency normalization to EUR
- **dim_date** — conformed
- **agg_game_daily_revenue** — gold mart: gross_revenue_eur, success_rate, txn counts,
  7d/28d rolling, WoW %, currency mix

## Business KPIs surfaced

- Daily gross revenue (EUR-normalized) per game
- Payment success rate = success / (success + error)
- 7d / 28d rolling revenue, WoW % change
- Top-N games by revenue, currency mix
- Anomaly z-score on daily revenue (freshness + outlier alerts)
- ARPPU (roadmap — requires user_id from provider)

## Engineering choices that matter

- **Contract-first ingestion** (`pydantic`) — drift fails the DAG, not the analyst
- **Schema enforcement** in Spark via explicit `StructType` (never `inferSchema`)
- **Delta Lake** — `MERGE INTO` for upserts → free CDC + SCD2 + time travel
- **Idempotency** — S3 keys include `run_id`; Delta MERGE on `transaction_id`
- **Same DAG = full + incremental** via Airflow dynamic task mapping over date range
- **Performance** — AQE, coalescePartitions, skewJoin, salting for whale games,
  partition by `payment_date`, Z-order by `game`, broadcast small dims, `OPTIMIZE` + `VACUUM`
- **Observability** — OpenLineage events, freshness SLA, row-count anomaly checks
- **Governance** — Secrets Manager, IAM per-task roles, S3 lifecycle bronze→Glacier 90d

## Local dev

```bash
pip install -e ".[dev]"
pre-commit install
make local-up && make seed     # LocalStack S3 + Secrets Manager
make test                       # ruff + black + mypy + pytest
```

Open http://localhost:8080 for the Airflow UI; trigger `payments_etl`.

## Extras (v0.2.0)

- **Exactly-once watermark** (`_ingestion_state` Delta table) — skip re-fetch on same checksum
- **Quarantine** for contract failures → S3 prefix with the validation error attached
- **Pydantic → Spark schema generator** — single source of truth (contract, Spark, Glue)
- **Salted join** helper for whale-game skew
- **Bloom filter + deletion vectors** on silver (GDPR erasure + faster MERGE)
- **KPI anomaly z-score view** with `OK / WARN / ALERT` severity
- **Slack `on_failure_callback`** + 7-day restatement task
- **ADRs** in `docs/decisions/` documenting Delta, MERGE, Airflow choices
- **CI**: ruff, black, mypy, pytest, terraform validate, checkov

## Reliability & analytics layer (v0.3.0)

- **Declarative DQ suite** (`dq/expectations.py`) with `fail / quarantine / warn` severities
- **Referential integrity + reconciliation** (`dq/referential.py`) — FK orphans, silver↔gold totals
- **Pipeline metrics** (`metrics.py`) — per-task `in/out/rejected/quarantined`; detects
  "succeeded but rows not processed" via a balance assertion
- **Schema evolution gate** (`schema_evolution.py`) — additive-nullable allowed; rename/retype blocked
- **Row-level retry queue** (`retry_queue.py`) — exponential backoff, dead-letter after N attempts
- **Structured JSON logging + OpenLineage** (`logging_setup.py`) — CloudWatch Insights + Marquez
- **SLO views** (`sql/marts/slo_views.sql`) — freshness, completeness, accuracy
- **CloudWatch alarms** (`infra/terraform/monitoring.tf`) on all three SLOs → SNS
- **Security** (`infra/terraform/security.tf`) — KMS-CMK, Lake Formation masking, SSM, VPC endpoints
- **Two cost tiers** — Spark/Delta vs Lambda+DuckDB (`serverless/`), routed by volume
  (`volume_router.py`); see ADR 0004. < $5/mo at brief volume.
- **Streaming seam** (`streaming.py`) — `Trigger.AvailableNow`, same MERGE logic
- **Pushdown discipline** — ADR 0005 + `.sqlfluff` guard against partition-column casts

## Trade-offs

- **Delta vs Iceberg** — chose Delta for tighter Spark integration and simpler CDC.
  Iceberg would be the pick if multi-engine (Trino/Flink) is a near-term need.
- **Star vs snowflake** — star; dim cardinality doesn't justify normalization.
- **EMR vs Glue** — Glue jobs for serverless ops; EMR if job runtime > 30 min.
