import duckdb,os
#connect
con = duckdb.connect(os.path.join(os.path.dirname(__file__),"..","..", "finance_analytics_light_raw.duckdb"))

#create bronze table
con.execute("""
CREATE or REPLACE TABLE bronze__geography_light AS
SELECT
     store_code,
     country,
     region,
 FROM raw__geography_light;
""")      

print("bronze__geography_light created successfully")

con.close()
