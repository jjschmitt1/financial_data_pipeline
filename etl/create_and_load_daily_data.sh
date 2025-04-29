#!/bin/bash

python3.12 extract_daily_data.py

if [ $? -ne 0 ]; then
  echo "Error extracting daily data"
  exit 1
else
  echo "daily data extracted"
fi

python3.12 load_duck.py
if [ $? -ne 0 ]; then
  echo "Error loading into duckdb"
  exit 1
else
  echo "successfully loaded daily data into duckdb"
fi

echo "Processed finished without error"
