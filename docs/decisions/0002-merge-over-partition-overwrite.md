# ADR 0002: MERGE INTO over partition overwrite

**Status:** Accepted · **Date:** 2026-06-19

## Context
Payment status can change days after the transaction (pending → success/error, refunds).
Source provider can re-emit corrected rows for arbitrary historical dates.

## Decision
Use `MERGE INTO` keyed on `transaction_id` for the bronze → silver step.

## Rationale
- Handles late-arriving + status flips in one write — no per-row date logic
- Idempotent retries: re-running the same batch is a no-op
- Combined with `delta.enableChangeDataFeed=true`, downstream consumers get CDC events
- Pairs with deletion vectors for GDPR erasure without rewriting partitions

## Alternatives considered
- **`replaceWhere` partition overwrite** — fast but breaks on cross-partition restatements
- **Append-only + dedupe view** — cheap writes, expensive reads; doesn't solve status flips

## Cost
MERGE rewrites touched files. Mitigated by bloom filter on `transaction_id` and
Z-order on `game`. OPTIMIZE + VACUUM scheduled nightly.
