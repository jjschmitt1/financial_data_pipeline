import duckdb
from dotenv import load_dotenv
import os

load_dotenv()

conn = duckdb.connect(os.getenv("DUCKDB_DB"))



conn.close()