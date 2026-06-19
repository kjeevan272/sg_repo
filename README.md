# Softgames Payments — Data Engineer Assignment

ETL pipeline that processes payment transactions and delivers revenue analytics.

## What

Fetch payment data from API → store raw in S3 → transform to Delta tables → expose KPIs via Athena.

**Stack:** Airflow · PySpark · Delta Lake · S3

## Architecture

```
API → bronze (raw) → silver (cleaned) → gold (analytics) → BI
```

- **Bronze**: immutable raw records, partitioned by date + run_id
- **Silver**: deduplicated Delta tables with MERGE upserts, tracks dimension changes (SCD2)
- **Gold**: business tables (transactions, dimensions, aggregates for analysts)

## Why This Approach

- **Medallion pattern** = clear separation of concerns (capture, clean, analyze)
- **Delta Lake MERGE** = no rebuilding pipelines, natural CDC + time travel
- **Idempotent design** = safe to retry/backfill using `run_id` keys
- **Schema contracts** (Pydantic) = catch drift before warehouse is affected

## Outcomes / KPIs

- Daily revenue per game (EUR-normalized, multi-currency)
- Success rate (% of successful transactions)
- Rolling windows: 7d, 28d revenue + week-over-week %
- Revenue anomaly alerts (z-score)
- Audit trail on dimension changes (valid_from/to timestamps)

## Repo Layout

```
dags/                      Airflow orchestration
src/sg_payments/
  ├─ contracts.py          Pydantic schemas (single source of truth)
  ├─ ingest.py             API fetch → S3 bronze
  ├─ bronze_to_silver.py   PySpark + Delta MERGE
  ├─ silver_to_gold.py     Star schema facts & dims
  └─ dq.py                 Data quality checks
sql/ddl/                   Table definitions
tests/                     Unit & integration tests
infra/terraform/           S3, Glue catalog, IAM
```

## Local Setup

```bash
pip install -e ".[dev]"
make local-up    # LocalStack (S3 + Secrets)
make test        # Run tests
```

Open http://localhost:8080 → trigger `payments_etl` DAG

## Design Choices

| Choice | Rationale |
|--------|-----------|
| Delta over Iceberg | Better Spark integration; CDC + SCD2 built-in |
| Star schema | Simpler queries for analysts; dims don't justify snowflake |
| AWS Glue | Serverless, cost-effective for short-running jobs |
| Pydantic contracts | Single source of truth; fail fast on schema drift |

## Roadmap

- Watermark table (skip re-fetching same data)
- Quarantine for failed contracts
- GDPR deletion with Bloom filters
- Slack alerts on anomalies
