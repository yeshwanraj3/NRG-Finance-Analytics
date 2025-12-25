import duckdb, os

# --- Connect to the DuckDB file ---
con = duckdb.connect(
    os.path.join(os.path.dirname(__file__), "..", "..", "finance_analytics_light_raw.duckdb")
)

# --- Create Gold Advanced Scenario Fact Table ---
con.execute("""
CREATE OR REPLACE TABLE gold__adv_fact_gl_scenarios_light AS

WITH base AS (
    SELECT
        DATE_TRUNC(
            'month',
            DATE(STRPTIME(TransactionDate, '%d.%m.%Y'))
        ) AS TransactionMonth,
        StoreCode,
        AccountNumber,
        SUM(AmountLocal) AS AmountLocal
    FROM gold__fact_gl_transactions_light
    GROUP BY
        TransactionMonth,
        StoreCode,
        AccountNumber
)

-- Actual values
SELECT
    TransactionMonth,
    StoreCode,
    AccountNumber,
    'Actual' AS Scenario,
    AmountLocal AS AmountLocal
FROM base

UNION ALL

-- Best-case scenario (revenue up, costs down)
SELECT
    TransactionMonth,
    StoreCode,
    AccountNumber,
    'BestCase' AS Scenario,
    CASE
        WHEN AmountLocal >= 0 THEN AmountLocal * 1.10
        ELSE AmountLocal * 0.90
    END AS AmountLocal
FROM base

UNION ALL

-- Worst-case scenario (revenue down, costs up)
SELECT
    TransactionMonth,
    StoreCode,
    AccountNumber,
    'WorstCase' AS Scenario,
    CASE
        WHEN AmountLocal >= 0 THEN AmountLocal * 0.90
        ELSE AmountLocal * 1.10
    END AS AmountLocal
FROM base

ORDER BY
    TransactionMonth,
    StoreCode,
    AccountNumber,
    Scenario;
""")

print("✅ Created gold__adv_fact_gl_scenarios_light")

con.close()
