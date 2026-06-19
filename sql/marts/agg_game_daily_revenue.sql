-- Analyst-facing mart. Refreshed by silver_to_gold.run(dt).
-- KPIs: revenue (EUR-normalized), success rate, rolling windows, WoW%.
CREATE OR REPLACE VIEW gold.v_game_daily_kpis AS
WITH base AS (
    SELECT
        game_key,
        payment_date,
        gross_revenue_eur,
        txn_success_count,
        txn_error_count,
        success_rate,
        avg_txn_value_eur
    FROM gold.agg_game_daily_revenue
)
SELECT
    game_key,
    payment_date,
    gross_revenue_eur,
    success_rate,
    txn_success_count,
    avg_txn_value_eur,
    SUM(gross_revenue_eur) OVER (
        PARTITION BY game_key
        ORDER BY payment_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS revenue_7d,
    SUM(gross_revenue_eur) OVER (
        PARTITION BY game_key
        ORDER BY payment_date
        ROWS BETWEEN 27 PRECEDING AND CURRENT ROW
    ) AS revenue_28d,
    gross_revenue_eur
        / NULLIF(LAG(gross_revenue_eur, 7) OVER (PARTITION BY game_key ORDER BY payment_date), 0)
        - 1 AS wow_growth
FROM base;
