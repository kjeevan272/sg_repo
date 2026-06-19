-- Daily revenue z-score per game. Surfaces outliers + freshness gaps.
CREATE OR REPLACE VIEW gold.kpi_anomalies AS
WITH stats AS (
    SELECT
        game_key,
        payment_date,
        gross_revenue_eur,
        AVG(gross_revenue_eur) OVER w AS mu_28d,
        STDDEV(gross_revenue_eur) OVER w AS sigma_28d
    FROM gold.agg_game_daily_revenue
    WINDOW w AS (
        PARTITION BY game_key ORDER BY payment_date
        ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING
    )
)
SELECT
    game_key,
    payment_date,
    gross_revenue_eur,
    (gross_revenue_eur - mu_28d) / NULLIF(sigma_28d, 0) AS z_score,
    CASE
        WHEN ABS((gross_revenue_eur - mu_28d) / NULLIF(sigma_28d, 0)) > 3 THEN 'ALERT'
        WHEN ABS((gross_revenue_eur - mu_28d) / NULLIF(sigma_28d, 0)) > 2 THEN 'WARN'
        ELSE 'OK'
    END AS severity
FROM stats;
