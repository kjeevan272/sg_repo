# Changelog

All notable changes documented per [Keep a Changelog](https://keepachangelog.com).

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
