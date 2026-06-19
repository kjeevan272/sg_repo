-- Freshness SLO: is today's mart present and on time?
CREATE OR REPLACE VIEW gold.v_data_freshness AS
SELECT
    MAX(payment_date)                                   AS latest_partition,
    DATEDIFF(current_date(), MAX(payment_date))         AS days_behind,
    CASE WHEN MAX(payment_date) >= current_date() - 1
         THEN 'OK' ELSE 'STALE' END                     AS freshness_status
FROM gold.agg_game_daily_revenue;

-- Completeness SLO: rows out vs rows in, per run.
CREATE OR REPLACE VIEW gold.v_completeness AS
SELECT
    payment_date,
    run_id,
    SUM(input_rows)       AS input_rows,
    SUM(output_rows)      AS output_rows,
    SUM(rejected_rows)    AS rejected_rows,
    SUM(quarantined_rows) AS quarantined_rows,
    SUM(output_rows) / NULLIF(SUM(input_rows), 0) AS completeness_ratio,
    CASE WHEN SUM(output_rows) / NULLIF(SUM(input_rows), 0) >= 0.999
         THEN 'OK' ELSE 'BREACH' END AS completeness_status
FROM gold._pipeline_metrics
GROUP BY payment_date, run_id;

-- Accuracy SLO: silver vs gold reconciliation surface.
CREATE OR REPLACE VIEW gold.v_reconciliation AS
SELECT
    g.payment_date,
    g.gross_revenue_eur                       AS gold_revenue_eur,
    g.txn_success_count                       AS gold_success_count
FROM gold.agg_game_daily_revenue g;
