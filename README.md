

## Solution Overview

This solution builds a scalable payment analytics platform that ingests payment transactions from a provider API, processes them through a Medallion Architecture (Bronze, Silver, Gold), and delivers business-ready datasets for reporting and analytics.

The goal is to provide accurate daily revenue reporting, transaction monitoring, financial reconciliation, and operational visibility while maintaining scalability, reliability, security, and cost efficiency.

---

# High Level Architecture

```text
Payment API
    |
    v
Python Ingestion
    |
    v
S3 Bronze Layer (Raw JSON)
    |
    v
PySpark Transformations
    |
    v
Silver Layer (Clean Delta Tables)
    |
    v
Gold Layer (Facts, Dimensions, KPIs)
    |
    v
Athena / BI Dashboard / Analysts
```

---

# Design Approach

## Why Medallion Architecture?

The Medallion Architecture separates data into multiple layers.

### Bronze Layer
Stores raw source data exactly as received.

Benefits:
- Supports auditing
- Allows replaying historical data
- Preserves source records

### Silver Layer
Stores cleaned and validated data.

Benefits:
- Removes duplicates
- Standardizes formats
- Applies business rules

### Gold Layer
Stores business-ready reporting tables.

Benefits:
- Faster reporting
- Simplified analytics
- Self-service access for business users

---

# Solution Components

| Functionality | What | Why | Impact | Outcome |
|------------|------|------|---------|---------|
| Ingestion | Pull payment data from API and store in S3 | Capture source data reliably | Creates a single source of truth | Auditable raw data storage |
| Data Validation | Validate records using Pydantic contracts | Prevent bad data entering pipeline | Reduces downstream failures | Higher data quality |
| Schema Evolution | Detect schema changes automatically | APIs can add or modify fields | Prevents pipeline breakage | Safe schema management |
| Quarantine | Store invalid records separately | Avoid stopping entire pipeline | Faster debugging and recovery | Fault-tolerant processing |
| Bronze Layer | Store raw JSON data | Preserve original records | Supports auditing and replay | Immutable raw data layer |
| Silver Layer | Clean, standardize and transform data | Raw data is not analytics-ready | Improves consistency | Trusted transaction dataset |
| Deduplication | Remove duplicate transactions | Avoid double counting revenue | Improves reporting accuracy | Correct metrics |
| Delta MERGE | Upsert records using transaction_id | Handle late arrivals and updates | Maintains current transaction state | Accurate historical records |
| Currency Conversion | Convert amounts to EUR | Standardize multiple currencies | Enables financial reporting | Consistent revenue metrics |
| Gold Layer | Create facts, dimensions and KPIs | Support business reporting | Simplifies analytics | Business-ready datasets |
| Data Quality Checks | Apply validation and business rules | Detect issues early | Prevents bad data propagation | Trusted reports |
| Referential Integrity | Validate fact-to-dimension relationships | Ensure data consistency | Prevents missing references | Accurate reporting |
| Audit Tables | Track pipeline executions and row counts | Improve observability | Simplifies troubleshooting | Full operational visibility |
| Watermarking | Track processed files and dates | Prevent duplicate processing | Supports re-runs safely | Exactly-once processing |
| Late Data Handling | Reprocess recent days | Capture refunds and updates | Keeps reports current | Accurate KPIs |
| Retry Mechanism | Retry failed records automatically | Handle temporary failures | Improves reliability | Higher success rate |
| Dead Letter Queue | Store permanently failed records | Enable manual investigation | Reduces data loss | Recoverable failures |
| Delta Lake | Use ACID-compliant storage | Support updates and versioning | Improves reliability | Enterprise-grade data lake |
| Airflow | Orchestrate ETL workflow | Manage scheduling and dependencies | Automates operations | Reliable execution |
| Monitoring | CloudWatch metrics and alerts | Detect issues quickly | Reduces downtime | Proactive monitoring |
| Slack Alerts | Send failure notifications | Improve incident response | Faster issue resolution | Better operational support |
| Security | Encryption and access controls | Protect sensitive data | Reduces compliance risk | Secure platform |
| Governance | Metadata, lineage and auditing | Improve transparency | Easier compliance reporting | Better governance |
| Cost Optimization | Lambda/DuckDB for small volumes and Spark for large volumes | Avoid overprovisioning | Reduces infrastructure costs | Cost-efficient processing |
| Multi-Tenant Design | Support multiple games/providers | Enable future growth | Improves scalability | Reusable platform |
| KPI Aggregations | Generate daily revenue metrics | Support business decisions | Faster analytics | Actionable insights |
| Athena Reporting | Query gold datasets | Enable self-service analytics | Reduces engineering dependency | Faster reporting |
| Data Lineage | Track data movement across layers | Improve traceability | Easier root-cause analysis | Better observability |

---

# Understanding Key Concepts

## What is Delta Lake?

Delta Lake is a storage layer built on top of S3.

It provides:

- ACID transactions
- Data versioning
- Time travel
- Efficient MERGE operations
- Schema enforcement

Why important?

Without Delta Lake, updating transactions and handling late-arriving records becomes difficult and unreliable.

---

## What is MERGE?

MERGE updates existing records and inserts new records in a single operation.

Example:

Transaction 123 arrives with status = Pending.

Later the same transaction arrives with status = Success.

MERGE updates the existing record instead of creating duplicates.

---

## What is Deduplication?

Duplicate records can occur due to:

- API retries
- Pipeline reruns
- Source system issues

Deduplication ensures only one version of a transaction exists.

---

## What is Watermarking?

Watermarks track processed files and dates.

Purpose:

- Prevent duplicate processing
- Support backfills safely
- Enable exactly-once processing

---

## What is a Dead Letter Queue?

A storage location for records that repeatedly fail processing.

Benefits:

- No data loss
- Easy investigation
- Supports reprocessing later

---

## What is a Star Schema?

A reporting model consisting of:

Fact Table:
- Stores transactions

Dimension Tables:
- Game
- Currency
- Date

Benefits:

- Faster queries
- Simpler reporting
- Better performance in Athena

---

# Data Model

Fact Table

fact_payment_transaction

Dimensions

- dim_game
- dim_currency
- dim_date
- fx_rate_daily

Aggregations

- agg_game_daily_revenue

Operational Tables

- pipeline_metrics
- ingestion_state
- retry_queue

---

# Technology Stack

- Python
- PySpark
- Apache Airflow
- AWS S3
- AWS Athena
- AWS Glue Catalog
- Delta Lake
- CloudWatch
- Slack
- Terraform

---

# Business Benefits

- Automated payment reporting
- Improved data quality
- Accurate revenue calculations
- Faster analytics
- Reduced operational effort
- Secure and scalable architecture
- Lower infrastructure costs
