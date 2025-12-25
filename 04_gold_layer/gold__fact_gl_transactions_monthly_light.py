import duckdb, os

# Connect to the DuckDB file
con = duckdb.connect(
    os.path.join(os.path.dirname(__file__), "..", "..", "finance_analytics_light_raw.duckdb")
)

# Create Gold Aggregated Fact Table (Monthly)
con.execute("""
CREATE OR REPLACE TABLE gold__fact_gl_transactions_monthly_light AS
WITH base AS (
    SELECT
        DATE(STRPTIME(TransactionDate, '%d.%m.%Y')) AS transaction_date,
        StoreCode,
        AccountNumber,
        AmountLocal
    FROM gold__fact_gl_transactions_light
)
SELECT
    DATE_TRUNC('month', transaction_date) AS TransactionMonth,
    StoreCode,
    AccountNumber,
    SUM(AmountLocal) AS AmountLocal
FROM base
GROUP BY
    TransactionMonth,
    StoreCode,
    AccountNumber
ORDER BY
    TransactionMonth,
    StoreCode,
    AccountNumber;
""")

print("✅ Created gold__fact_gl_transactions_monthly_light")
con.close()
