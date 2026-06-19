# ADR 0003: Airflow over Dagster

**Status:** Accepted · **Date:** 2026-06-19

## Context
Need an orchestrator for daily + backfill, with retries, SLAs, and on-call alerting.

## Decision
Apache Airflow 2.9 with the TaskFlow API.

## Rationale
- Org already runs MWAA — zero new infra
- TaskFlow + dynamic task mapping covers our backfill story without code forks
- Mature ecosystem of operators (S3, Glue, EMR) we may need next
- `catchup=True` + idempotent tasks = one DAG for daily and historical replays

## Alternatives considered
- **Dagster** — better asset/lineage model, but adopting it means new infra + retraining
- **Step Functions** — strong for AWS-only, weaker for cross-team DAG visibility
- **Prefect** — small team, less battle-tested for our scale

## Trigger to revisit
If we adopt dbt + Iceberg + multi-cloud, Dagster's asset model becomes compelling.
