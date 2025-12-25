import duckdb,os
con=duckdb.connect(os.path.join(os.path.dirname(__file__),"..","..","finance_analytics_light_raw.duckdb"))
con.execute("""
create or replace table gold__dim_accounts_light as 
            select
            account_number as AccountNumber,
            account_name as AccountName,
            account_type as AccountType,
            currency as Currency,
from silver__accounts_light
            order by AccountNumber;
""")

print("gold__dim_accounts_light materialized")
con.close() 
