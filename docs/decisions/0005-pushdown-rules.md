# ADR 0005: Predicate pushdown & partition discipline

**Status:** Accepted · **Date:** 2026-06-19

## Context
The partition column is `payment_date DATE`. Wrong predicate shapes silently
disable partition pruning and predicate pushdown, causing full-table scans.

## Rules (enforced by sqlfluff + review)
1. **Filter on the typed column with a date literal:**
   `WHERE payment_date = DATE '2025-01-15'`  ✅
2. **Never cast the partition column:**
   `WHERE CAST(payment_date AS STRING) = '2025-01-15'`  ❌ (kills pruning)
   `WHERE date_format(payment_date,'yyyy-MM') = '2025-01'`  ❌
3. Range scans use half-open intervals:
   `WHERE payment_date >= DATE '2025-01-01' AND payment_date < DATE '2025-02-01'`
4. Joins use `date_key INT` (sort-merge friendly) or `payment_date DATE` — never strings.
5. Compute column stats so the CBO can push down:
   `ANALYZE TABLE silver.payments COMPUTE STATISTICS FOR COLUMNS transaction_id, game, status`

## Why
Parquet/Delta row-group skipping + partition pruning need the predicate to bind
directly to the stored column type. Any function wrapping the column defeats it.

## Enforcement
- `.sqlfluff` rule + pre-commit hook flags `CAST(payment_date` patterns
- CI runs `sqlfluff lint sql/`
