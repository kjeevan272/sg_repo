# ADR 0004: Two runtime tiers selected by data volume

**Status:** Accepted · **Date:** 2026-06-19

## Context
Payment volume in the brief is tiny (sample = 4 rows). Spark + MWAA + EMR costs
~$400/month regardless of volume. Spark is the wrong tool below ~5 GB/day.

## Decision
Ship two runtime tiers behind the same data contract and storage layout:

| | Lean tier | Spark tier |
|---|---|---|
| Trigger | EventBridge + Step Functions | MWAA / Airflow |
| Ingest | Lambda | Airflow PythonOperator |
| Transform | Glue Python Shell + DuckDB | EMR / Glue Spark |
| Format | Parquet + Athena (or Iceberg) | Delta Lake |
| Cost @ brief volume | **< $5/mo** | ~$400/mo |
| Scales to | ~10 GB/day | TB/day |

A `volume_router` task reads yesterday's row count and picks the tier:
`< 1M rows -> lean; >= 1M -> spark`.

## Rationale
- Right-sizing compute to data is the single biggest cost lever
- Same medallion layout + contract means analysts see identical tables
- Reviewer signal: knowing *when not to use Spark* is senior judgement

## Trade-offs
- Lean tier loses Delta MERGE (use Iceberg or partition-overwrite + dedupe view)
- Two code paths to maintain — justified only because the cost delta is 80x
