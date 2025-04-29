#!/bin/bash

# get daily data from yfinance
python3.12 extract_daily_data.py
if [ $? -ne 0 ]; then
  echo "Error extracting daily data"
  exit 1
else
  echo "daily data extracted"
fi

# load data into duckdb
python3.12 load_duck.py
if [ $? -ne 0 ]; then
  echo "Error loading into duckdb"
  exit 1
else
  echo "successfully loaded daily data into duckdb"
fi

# load data into sqlite
python3.12 load_sqlite.py
if [ ?$ -ne 0 ]; then
  echo "Error loading into sqlite"
  exit 1
else
  echo "successfully loaded daily data into sqlite"
fi

echo "Processed finished without error"
