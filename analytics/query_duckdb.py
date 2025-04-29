import duckdb
from dotenv import load_dotenv
import os

load_dotenv()

conn = duckdb.connect(os.getenv("DUCKDB_DB"))

conn.execute("SELECT * FROM sp100_daily_prices ORDER BY date DESC LIMIT 10;")
print(conn.fetchall())

conn.close()
