import duckdb,os
con=duckdb.connect(os.path.join(os.path.dirname(__file__),"..","..","finance_analytics_light_raw.duckdb"))
con.execute("""    
create or replace table gold__dim_account_mapping_light as
            select
            account_number as AccountNumber,
            account_name as AccountName,
            pl_line as PLLine,
            statement_type as StatementType,
            sort_order as SortOrder,
             notes as Notes
from silver__account_mapping_light
            order by SortOrder, AccountNumber;
""")
print("gold__dim_account_mapping_light materialized")
con.close()
