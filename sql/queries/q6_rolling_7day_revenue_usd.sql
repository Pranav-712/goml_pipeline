-- Query 6 (extra): daily USD revenue with a 7-day moving average (window frame).
-- LEFT JOIN from dim_date so days without sales count as 0 instead of disappearing.
WITH daily AS (
    SELECT d.full_date, COALESCE(SUM(f.transaction_amount), 0) AS daily_revenue_usd
    FROM dim_date d
    LEFT JOIN fact_transactions f ON f.date_key = d.date_key AND f.currency = 'USD'
    GROUP BY d.full_date
)
SELECT
    full_date,
    daily_revenue_usd,
    ROUND(AVG(daily_revenue_usd) OVER (ORDER BY full_date
                                       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS moving_avg_7d_usd
FROM daily
ORDER BY full_date;
