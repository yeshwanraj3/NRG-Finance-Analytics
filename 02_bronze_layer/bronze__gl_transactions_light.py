import duckdb,os
#connect
con = duckdb.connect(os.path.join(os.path.dirname(__file__),"..","..", "finance_analytics_light_raw.duckdb"))

#create bronze table
con.execute("""
CREATE or REPLACE TABLE bronze__gl_transactions_light AS
SELECT
     transaction_id,
     transaction_date,
     store_code,
     account_number,  
     amount_local,
     currency,
     document_number,
     description
                                    
 FROM raw__gl_transactions_light;
""")      

print("bronze__gl_transactions_light created successfully")

con.close()
