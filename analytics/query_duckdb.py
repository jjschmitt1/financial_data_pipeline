import duckdb
from dotenv import load_dotenv
import os

load_dotenv()

conn = duckdb.connect(os.getenv("DUCKDB_DB"))

last_date_in_db = conn.execute("""
    SELECT
        date
    FROM
        sp100_daily_prices
    ORDER BY date DESC
    LIMIT 1;
    """).fetchone()[0]

print(last_date_in_db)

conn.close()
