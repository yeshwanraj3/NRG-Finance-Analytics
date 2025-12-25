#silver from bronze
import duckdb,os
con=duckdb.connect(os.path.join(os.path.dirname(__file__),"..","..","finance_analytics_light_raw.duckdb"))

con.execute("""
create or replace table silver__gl_transactions_light as
select
    transaction_id,
     STRFTIME(transaction_date,'%d.%m.%Y') as transaction_date, 
    store_code,
    account_number,
    amount_local,
    upper(trim(currency))as currency,
    document_number,
    trim(description) as description,     
            
from bronze__gl_transactions_light;
""")

print("silver__gl_transactions_light created successfully.")
con.close()