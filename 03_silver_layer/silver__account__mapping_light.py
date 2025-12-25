import duckdb, os

con = duckdb.connect(
    os.path.join(os.path.dirname(__file__), "..", "..", "finance_analytics_light_raw.duckdb")
)

con.execute("""
CREATE OR REPLACE TABLE silver__account_mapping_light AS
SELECT
    CAST(Accountnumber AS INTEGER) AS account_number,
    TRIM(Accountname) AS account_name,
    TRIM(PLLine) AS pl_line,
    UPPER(TRIM(StatementType)) AS statement_type,
    CAST(SortOrder AS INTEGER) AS sort_order,
    TRIM(notes) AS notes
FROM bronze__account_mapping_light;
""")

print("silver__account_mapping_light created successfully.")
con.close()
