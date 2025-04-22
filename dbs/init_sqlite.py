import sqlite3
import pandas as pd

transformed_data_csv = "../data_sources/historical_sources/sp100_transformed_1yr_data.csv"

conn = sqlite3.connect('storage.db')

df = pd.read_csv(transformed_data_csv)
df.to_sql('financial_data', conn, if_exists='replace', index=False)

conn.commit()
conn.close()