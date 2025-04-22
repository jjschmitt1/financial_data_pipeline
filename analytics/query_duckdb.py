import duckdb

conn = duckdb.connect('analytics.duckdb')

conn.execute("SELECT * FROM latest_prices ORDER BY ticker LIMIT 10;")
print(conn.fetchall())

conn.close()
