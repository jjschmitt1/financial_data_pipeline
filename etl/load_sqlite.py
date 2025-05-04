import sqlite3
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="/Users/johnschmitt/code/finance_data_pipeline/.env")

conn = sqlite3.connect(os.getenv("SQLITE_DB"))

df = pd.read_csv(os.getenv("DAILY_DATA_CSV"))
df.to_sql('financial_data', conn, if_exists="append", index=False)

conn.commit()
conn.close()
