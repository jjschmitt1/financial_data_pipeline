import duckdb
from dotenv import load_dotenv
import os

load_dotenv()

conn = duckdb.connect(os.getenv("DUCKDB_DB"))

conn.execute("SELECT ticker, close FROM latest_prices ORDER BY ticker LIMIT 10;")
print(conn.fetchall())

conn.close()
