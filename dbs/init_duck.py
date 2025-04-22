import duckdb

transformed_csv_path = "../data_sources/historical_sources/sp100_transformed_1yr_data.csv"
duckdb_save_path = "../analytics/analytics.duckdb"

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