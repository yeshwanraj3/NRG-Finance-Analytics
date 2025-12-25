import duckdb,os
#connect
con = duckdb.connect(os.path.join(os.path.dirname(__file__),"..","..", "finance_analytics_light_raw.duckdb"))

#create bronze table
con.execute("""
CREATE or REPLACE TABLE bronze__stores_light AS
SELECT
     store_code,
     store_name,
     store_type,
FROM raw__stores_light;
""")

print("bronze__stores_light created successfully")

con.close()
