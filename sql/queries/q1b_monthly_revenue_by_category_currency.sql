-- Query 1b (extra): same as Q1 but split by currency, so no mixed-currency sums.
SELECT
    f.category,
    f.currency,
    SUM(f.transaction_amount) AS total_transaction_amount
FROM fact_transactions f
JOIN dim_date d ON d.date_key = f.date_key
WHERE d.year  = YEAR(CURRENT_DATE)
  AND d.month = MONTH(CURRENT_DATE)
GROUP BY f.category, f.currency
ORDER BY f.category, f.currency;
