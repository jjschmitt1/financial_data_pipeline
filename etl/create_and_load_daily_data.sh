#!/bin/bash

# must be run in etl folder
log_file="/Users/johnschmitt/code/finance_data_pipeline/logs/daily_update.log"
py_path="/Library/Frameworks/Python.framework/Versions/3.12/bin/python3.12"
extract_daily_path="/Users/johnschmitt/code/finance_data_pipeline/etl/extract_daily_data.py"
load_duck_path="/Users/johnschmitt/code/finance_data_pipeline/etl/load_duck.py"
load_sqlite_path="/Users/johnschmitt/code/finance_data_pipeline/etl/load_sqlite.py"

log() {
  echo "[$(date)]: $1" >> "$log_file"
}

# get daily data from yfinance
$py_path $extract_daily_path
extract_exit_code=$?

if [ $extract_exit_code -eq 0 ]; then
  log "Daily data extracted"
elif [ $extract_exit_code -eq 70 ]; then
  log "Duplicate data detected from 'extract_daily_data'"
  exit 1
else
  log "Error extracting daily data"
  exit 1
fi

# load data into duckdb
$py_path $load_duck_path
if [ $? -ne 0 ]; then
  log "Error loading daily data into duckdb"
  exit 1
else
  log "Successfully loaded daily data into duckdb"
fi

# load data into sqlite
$py_path $load_sqlite_path
if [ $? -ne 0 ]; then
  log "Error loading into sqlite"
  exit 1
else
  log "Successfully loaded daily data into sqlite"
fi

log "'create_and_load_daily_data' completed"
