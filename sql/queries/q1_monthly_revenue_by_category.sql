-- Query 1: total transaction amount per category for the CURRENT calendar month.
-- NOTE: amounts are summed as-is across USD/EUR/GBP (the brief defines no FX rates).
--       See q1b for the per-currency breakdown, which is the analytically safe view.
SELECT
    f.category,
    SUM(f.transaction_amount) AS total_transaction_amount
FROM fact_transactions f
JOIN dim_date d ON d.date_key = f.date_key
WHERE d.year  = YEAR(CURRENT_DATE)
  AND d.month = MONTH(CURRENT_DATE)
GROUP BY f.category
ORDER BY total_transaction_amount DESC;
