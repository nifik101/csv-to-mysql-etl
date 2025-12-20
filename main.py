import os
from dev.config import local_directory,raw_file ,processed_file
from etl_pipeline.extract import extract_csv
from etl_pipeline.transform import transform_csv
from etl_pipeline.load import load_to_mysql
from db.mysql_connection import get_mysql_connection

file = "raw\\Raw_Superstore.csv"
full_path = os.path.join(local_directory,raw_file)
save_path = os.path.join(local_directory,processed_file)
#extract
df = extract_csv(full_path)

#transform
new_df = transform_csv(df,save_path)

#sql connection and load
conn = get_mysql_connection()
load_to_mysql(new_df,conn)
conn.close()