import duckdb,os
con=duckdb.connect(os.path.join(os.path.dirname(__file__),"..","..","finance_analytics_light_raw.duckdb"))

con.execute("""
create or replace table silver__stores_light as
select
     s.store_code,
     trim(s.store_name) as store_name,
        upper(trim(s.store_type)) as store_type,
        g.country,
        g.region
 from bronze__stores_light s
 left join bronze__geography_light g on s.store_code= g.store_code;
""")

print("silver__stores_light created successfully.")
con.close()            