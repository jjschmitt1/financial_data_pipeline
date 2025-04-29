import duckdb
from dotenv import load_dotenv
import os

load_dotenv()

conn = duckdb.connect(os.getenv("DUCKDB_DB"))

# insert the prices into the larger db
conn.execute(f"""INSERT INTO sp100_daily_prices
            SELECT 
                ticker,
                company_name,
                date,
                close,
                high,
                low,
                open,
                volume,
                daily_return_pct,
                intraday_return_pct,
                7day_ma,
                30day_ma,
                day_of_week
            FROM read_csv('{os.getenv("DAILY_DATA_CSV")}');
             """)

# update the latest_prices view with the new latest prices
conn.execute("""Create or replace view latest_prices as
             select
                ticker,
                company_name,
                date,
                close
             from (
                SELECT *,
                RANK() OVER (PARTITION BY ticker ORDER BY date DESC) as r
                FROM sp100_daily_prices
                ) AS temp_ranked
             WHERE r = 1;""")

conn.close()