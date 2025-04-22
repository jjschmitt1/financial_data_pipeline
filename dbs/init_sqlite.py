import sqlite3
import pandas as pd
from dotenv import load_dotenv
import os

# get data from the dotenv file
load_dotenv()

transformed_data_csv = os.getenv("TRANSFORMED_CSV")
sqlite_db_path = os.getenv("SQLITE_DB")

conn = sqlite3.connect(sqlite_db_path)

df = pd.read_csv(transformed_data_csv)
df.to_sql('financial_data', conn, if_exists='replace', index=False)

conn.commit()
conn.close()