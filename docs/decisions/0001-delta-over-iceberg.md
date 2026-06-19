# ADR 0001: Delta Lake over Apache Iceberg

**Status:** Accepted · **Date:** 2026-06-19

## Context
Need a transactional table format on S3 for upserts, CDC, time travel, and SCD2.

## Decision
Adopt Delta Lake.

## Rationale
- Native Spark integration; `MERGE INTO` is mature and well-tuned
- Change Data Feed gives us CDC for free (`delta.enableChangeDataFeed=true`)
- Deletion vectors solve GDPR right-to-erasure without full rewrites
- Liquid clustering removes partition-skew operational burden
- Bloom filters + Z-order cover the lookup-heavy MERGE pattern on `transaction_id`

## Alternatives considered
- **Iceberg** — better multi-engine story (Trino/Flink), but we're Spark-only today
- **Hudi** — strong CDC, weaker tooling and community for our team
- **Parquet + Hive** — no ACID; deal-breaker for MERGE-driven late data

## Reversibility
Medium. Migration to Iceberg would require a one-time rewrite + DDL translation.
Defer until a multi-engine requirement is concrete.
