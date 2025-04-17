import duckdb
import os

transformed_csv_path = "../data_sources/historical_sources/sp100_transformed_1yr_data.csv"
duckdb_save_path = "../analytics/analytics.duckdb"

# establish connection to db at the given path
conn = duckdb.connect(duckdb_save_path)

# create the table
conn.execute(f"""
             CREATE OR REPLACE TABLE sp100_daily_prices AS 
             SELECT
                ticker::VARCHAR,
                company_name::VARCHAR,
                date::DATE,
                close::DOUBLE
                open::DOUBLE
                high::DOUBLE
                low::DOUBLE
                volume::BIGINT
             FROM read_csv_auto('{transformed_csv_path}', HEADERS=TRUE);
             """)

# create view that shows the most recent prices
conn.execute(f"""
             CREATE VIEW latest_prices AS
             SELECT
                ticker,
                company_name,
                date,
                close
             FROM (
                Select *,
                RANK() OVER (PARTITION BY ticker ORDER BY date DESC) as r)
                FROM sp100_daily_prices
                ) temp_ranked
             WHERE r = 1;
             """)

