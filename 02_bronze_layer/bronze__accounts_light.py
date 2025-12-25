import duckdb,os
#connect
con = duckdb.connect(os.path.join(os.path.dirname(__file__),"..","..", "finance_analytics_light_raw.duckdb"))

#create bronze table
con.execute("""
CREATE or REPLACE TABLE bronze__accounts_light AS
SELECT
        account_number,
        account_name,
        account_type,
        currency
 FROM raw__accounts_light;
""")      

print("bronze__accounts_light created successfully")

con.close()