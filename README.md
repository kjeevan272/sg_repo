# Softgames Payments Data Engineering Solution

<<<<<<< HEAD
## Overview
=======
ETL pipeline that processes payment transactions and delivers revenue analytics.

## What

Fetch payment data from API → store raw in S3 → transform to Delta tables → expose KPIs via Athena.

**Stack:** Airflow · PySpark · Delta Lake · S3
>>>>>>> a33de2034293aca1312f3a98283d471247838911

This solution builds a scalable payment data platform that ingests payment transactions from a provider API, processes them through a medallion architecture, and delivers analytics-ready datasets for business users.

<<<<<<< HEAD
---

# Solution Architecture

```text
                +--------------------+
                |  Payment Provider  |
                |       API          |
                +---------+----------+
                          |
                          v
                +--------------------+
                | Python Ingestion   |
                | Validation Layer   |
                +---------+----------+
                          |
                          v
                +--------------------+
                | S3 Bronze Layer    |
                | Raw JSON Files     |
                +---------+----------+
                          |
                          v
                +--------------------+
                | PySpark Transform  |
                | Deduplication      |
                | Data Quality       |
                +---------+----------+
                          |
                          v
                +--------------------+
                | Silver Layer       |
                | Delta Tables       |
                +---------+----------+
                          |
                          v
                +--------------------+
                | Gold Layer         |
                | Fact & Dimensions  |
                | KPI Aggregations   |
                +---------+----------+
                          |
                          v
                +--------------------+
                | Athena / BI Tools  |
                | Business Users     |
                +--------------------+
```

---

# End-to-End Data Flow

1. Payment data is retrieved from the provider API.
2. Raw data is stored in S3 Bronze layer.
3. Data validation checks are applied.
4. Invalid records are moved to quarantine.
5. PySpark transforms and cleans the data.
6. Duplicate transactions are removed.
7. Delta Lake MERGE updates changed records.
8. Silver tables store cleansed transaction data.
9. Gold tables create business-ready datasets.
10. Athena and BI tools query the final data.

---

# Key Components

## Ingestion

### What
Fetch payment data from the provider API and store raw files in S3.

### Why
Preserve original source data for auditing and replay purposes.

### Outcome
Reliable and traceable raw data storage.

---

## Data Validation

### What
Validate incoming records using Pydantic models.

### Why
Ensure only valid data enters the processing pipeline.

### Outcome
Improved data quality and reduced downstream issues.

---

## Quarantine Process

### What
Store invalid records separately.

### Why
Prevent bad data from impacting reporting.

### Outcome
Easy investigation and recovery of problematic records.

---

## Bronze to Silver Processing

### What
Transform raw JSON into Delta tables using PySpark.

### Why
Create a clean and standardized transaction dataset.

### Outcome
Reliable and analytics-ready transaction records.

---

## Deduplication

### What
Remove duplicate transactions using transaction identifiers.

### Why
Prevent revenue inflation and inaccurate reporting.

### Outcome
Accurate transaction counts and revenue calculations.

---

## Delta Lake Merge Strategy

### What
Use MERGE operations based on transaction_id.

### Why
Handle late-arriving records and status updates.

### Outcome
Accurate and up-to-date transaction history.

---

## Gold Layer

### What
Create fact tables, dimensions, and aggregated KPI tables.

### Why
Support business reporting and analytics.

### Outcome
Fast and easy access to revenue metrics.

---

## Currency Conversion

### What
Convert transactions into EUR using daily exchange rates.

### Why
Standardize reporting across currencies.

### Outcome
Consistent financial reporting.

---

## Data Quality Framework

### What
Implement reusable validation and business rules.

### Why
Maintain trust in analytical datasets.

### Outcome
Consistent quality monitoring.

---

## Audit and Metadata Tables

### What
Track pipeline executions, row counts, and processing status.

### Why
Support monitoring and troubleshooting.

### Outcome
Improved operational visibility.

---

## Exactly-Once Processing

### What
Use watermarks and ingestion tracking.

### Why
Prevent duplicate processing.

### Outcome
Reliable and repeatable data loads.

---

## Late Arriving Data Handling

### What
Reprocess recent historical data.

### Why
Capture transaction updates and corrections.

### Outcome
Accurate reporting over time.

---

## Monitoring and Alerting

### What
CloudWatch alarms and Slack notifications.

### Why
Detect failures quickly.

### Outcome
Reduced incident response time.

---

## Security and Governance

### What
Encryption, access controls, and data masking.

### Why
Protect sensitive payment information.

### Outcome
Secure and compliant platform.

---

## Airflow Orchestration

### What
Manage workflow scheduling and dependencies.

### Why
Automate end-to-end processing.

### Outcome
Reliable and maintainable ETL execution.

---

# Data Model

## Fact Table

### fact_payment_transaction
- Transaction ID
- Game ID
- Currency
- Amount
- EUR Amount
- Transaction Status
- Transaction Date

## Dimension Tables

### dim_game
Stores game attributes.

### dim_currency
Stores currency details.

### dim_date
Stores calendar information.

### fx_rate_daily
Stores daily exchange rates.

---

# Technologies Used

- Python
- PySpark
- Apache Airflow
- AWS S3
- AWS Glue Catalog
- AWS Athena
- Delta Lake
- CloudWatch
- Slack Alerts
- Terraform

---

# Business Benefits

- Automated payment data ingestion
- High-quality and trusted reporting
- Scalable architecture for future growth
- Cost-optimized processing
- Improved operational monitoring
- Secure and compliant data platform
=======
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
>>>>>>> a33de2034293aca1312f3a98283d471247838911
