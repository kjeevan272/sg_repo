# Changelog

All notable changes documented per [Keep a Changelog](https://keepachangelog.com).

## [0.3.0] - 2026-06-19
### Added
- Declarative DQ suite with fail/quarantine/warn severities (`dq/expectations.py`)
- Referential integrity + silver↔gold reconciliation (`dq/referential.py`)
- Pipeline metrics table + row-balance assertion (`metrics.py`)
- Schema evolution compatibility gate (`schema_evolution.py`)
- Row-level retry queue with backoff + dead-letter (`retry_queue.py`)
- Structured JSON logging + OpenLineage emitter (`logging_setup.py`)
- SLO views: freshness/completeness/accuracy (`sql/marts/slo_views.sql`)
- CloudWatch alarms + SNS (`infra/terraform/monitoring.tf`)
- KMS-CMK, Lake Formation masking, SSM, VPC endpoints (`infra/terraform/security.tf`)
- Lean cost tier: Lambda + DuckDB (`serverless/`), volume router, ADR 0004
- Streaming seam via Trigger.AvailableNow (`streaming.py`)
- Pushdown rules ADR 0005 + `.sqlfluff` guard
- Spark tuning: skewJoin factor, advisory partition size, maxPartitionBytes
- DAG: referential_checks + replay_quarantine tasks
- Tests for schema evolution

## [0.2.0] - 2026-06-19
### Added
- LocalStack-backed `docker-compose.dev.yml` + `scripts/seed_localstack.py`
- Watermark table (`_ingestion_state`) for exactly-once ingestion
- Pydantic → Spark schema generator (`schema_gen.py`)
- Quarantine path for contract failures (`quarantine.py`)
- Salted-join helper for whale-game skew (`joins.py`)
- Bloom filter + deletion vectors on silver (`sql/ddl/silver_table_properties.sql`)
- KPI anomaly z-score view (`sql/marts/kpi_anomalies.sql`)
- Slack on_failure_callback (`alerts.py`)
- Restatement window task (last 7 days) in DAG
- ADRs 0001–0003
- GitHub Actions CI (ruff/black/mypy/pytest + terraform validate + checkov)

## [0.1.0] - 2026-06-19
### Added
- Initial medallion scaffold (bronze JSON → silver Delta MERGE → gold star schema)
- Airflow TaskFlow DAG, pydantic contract, DQ gates, Terraform S3 + Glue
