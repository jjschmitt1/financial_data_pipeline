#!/bin/bash

# get companies from wikipedia and historical data from yfinance
python3.12 extract_historical_data.py
if [ $? -ne 0 ]; then
  echo "Error extracting historical data"
  exit 1
else
  echo "historical data extracted"
fi

# add new cols with transformed data
python3.12 transform.py
if [ $? -ne 0 ]; then
  echo "Error transforming data"
  exit 1
else
  echo "historical data transformed"
fi

# load historical data into duckdb
python3.12 ../dbs/init_duck.py
if [ $? -ne 0 ]; then
  echo "Error loading data into duckdb"
  exit 1;
else
  echo "data loaded into duckdb"
fi

#load historical data into sqlite
python3.12 ../dbs/init_sqlite.py
if [ $? -ne 0 ]; then
  echo "Error loading data into SQLite"
  exit 1
else
  echo "data loaded into sqlite"
fi

echo "Process finished"
