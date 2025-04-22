import duckdb
from dotenv import load_dotenv
import os

load_dotenv()

transformed_csv_path = os.getenv("TRANSFORMED_CSV")
duckdb_save_path = os.getenv("DUCKDB_DB")

# establish connection to db at the given path
conn = duckdb.connect(duckdb_save_path)

# create the table
conn.execute(f"""
             CREATE OR REPLACE TABLE sp100_daily_prices AS 
                FROM read_csv('{transformed_csv_path}', HEADER=TRUE);
             """)

# create view that shows the most recent prices
conn.execute("""
             CREATE OR REPLACE VIEW latest_prices AS
             SELECT
                ticker,
                company_name,
                date,
                close
             FROM (
                SELECT *,
                RANK() OVER (PARTITION BY ticker ORDER BY date DESC) as r
                FROM sp100_daily_prices
                ) AS temp_ranked
             WHERE r = 1;
             """)


conn.close()