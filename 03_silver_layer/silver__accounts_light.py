import duckdb,os

con=duckdb.connect(os.path.join(os.path.dirname(__file__),"..","..","finance_analytics_light_raw.duckdb"))

con.execute("""
create or replace table silver__accounts_light as
select
    cast(account_number as integer) as account_number,
    trim(account_name) as account_name,
    upper(trim(account_type)) as account_type,
    upper(trim(currency)) as currency,
from bronze__accounts_light;
""")

print("silver__accounts_light created successfully.")
con.close() 
            