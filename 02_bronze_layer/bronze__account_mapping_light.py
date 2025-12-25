import duckdb,os
import pandas as pd

base_dir=os.path.dirname(__file__)
db_path=os.path.join(base_dir,"..","..","finance_analytics_light_raw.duckdb")
excel_path=os.path.join(base_dir,"..","..", "account_mapping_light.xlsx")

df_mapping_raw=pd.read_excel(excel_path)

con=duckdb.connect(db_path)

con.register("df_account_mapping__raw",df_mapping_raw)

con.execute("""
CREATE or REPLACE TABLE bronze__account_mapping_light AS
            select *
             from df_account_mapping__raw;
""")
print("bronze__account_mapping_light created successfully")
con.close()