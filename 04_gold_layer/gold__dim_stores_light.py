import duckdb,os
con=duckdb.connect(os.path.join(os.path.dirname(__file__),"..","..","finance_analytics_light_raw.duckdb"))
con.execute("""    
create or replace table gold__dim_stores_light as
            select
            store_code as StoreCode,
            store_name as StoreName,
            country as Country,
            region as Region,
from silver__stores_light
            order by StoreCode;
""")

print("gold__dim_stores_light materialized")
con.close() 