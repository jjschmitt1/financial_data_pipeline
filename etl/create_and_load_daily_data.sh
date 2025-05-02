#!/bin/bash

# must be run in etl folder
log_file="../logs/daily_update.log"

log() {
  echo "[$(date)]: $1" >> "$log_file"
}

# get daily data from yfinance
python3.12 extract_daily_data.py
extract_exit_code=$?

if [ $extract_exit_code -eq 0 ]; then
  log "Error extracting daily data"
  exit 1
elif [ $extract_exit_code -eq 2 ]; then
  log "Duplicate data detected from 'extract_daily_data'"
  exit 1
else
  log "Daily data extracted"
fi

# load data into duckdb
python3.12 load_duck.py
if [ $? -ne 0 ]; then
  log "Error loading daily data into duckdb"
  exit 1
else
  log "Successfully loaded daily data into duckdb"
fi

# load data into sqlite
python3.12 load_sqlite.py
if [ $? -ne 0 ]; then
  log "Error loading into sqlite"
  exit 1
else
  log "Successfully loaded daily data into sqlite"
fi

log "'create_and_load_daily_data' completed"
