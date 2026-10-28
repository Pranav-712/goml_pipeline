-- Query 3 (extra): month-over-month revenue trend per currency using LAG().
-- The first and the current month are partial months (data covers a rolling window).
WITH monthly AS (
    SELECT d.year, d.month, f.currency,
           SUM(f.transaction_amount) AS total_amount,
           COUNT(*)                  AS transaction_count
    FROM fact_transactions f
    JOIN dim_date d ON d.date_key = f.date_key
    GROUP BY d.year, d.month, f.currency
)
SELECT
    year, month, currency, total_amount, transaction_count,
    LAG(total_amount) OVER w AS prev_month_amount,
    ROUND(100.0 * (total_amount - LAG(total_amount) OVER w) / NULLIF(LAG(total_amount) OVER w, 0), 2)
        AS mom_growth_pct
FROM monthly
WINDOW w AS (PARTITION BY currency ORDER BY year, month)
ORDER BY currency, year, month;
