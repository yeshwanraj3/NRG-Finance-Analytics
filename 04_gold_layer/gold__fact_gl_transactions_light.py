import duckdb,os
con=duckdb.connect(os.path.join(os.path.dirname(__file__),"..","..","finance_analytics_light_raw.duckdb"))
con.execute(""" 
    create or replace table gold__fact_gl_transactions_light as     
            select
            transaction_id as TransactionID,
            transaction_date as TransactionDate,
            store_code as StoreCode,
            account_number as AccountNumber,
            amount_local as AmountLocal,
            currency as Currency,
            document_number as DocumentNumber,
            description as Description,

from silver__gl_transactions_light
            order by TransactionID;
""")       
print("gold__fact_gl_transactions_light materialized")
con.close() 
