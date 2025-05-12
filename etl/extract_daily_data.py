import duckdb
from dotenv import load_dotenv
import os
import datetime
import pandas as pd
import yfinance as yf

load_dotenv(dotenv_path="/Users/johnschmitt/code/finance_data_pipeline/.env")

conn = duckdb.connect(os.getenv("DUCKDB_DB"))

todays_date = datetime.date.today()

conn.execute("SELECT DISTINCT(ticker, company_name) FROM latest_prices ORDER BY ticker")

tickers_raw = conn.fetchall()

companies = list(map(lambda company: {'ticker': company[0][0], 'company_name': company[0][1]}, tickers_raw))

# sanity check to make sure we get all tickers
if len(companies) != 101:
    print("Error fetching tickers from 'latest_prices' view!")
    exit(1)

end_time = datetime.datetime.now()
start_time = end_time - datetime.timedelta(days=46)

stock_data = []

for company in companies:
    ticker = company['ticker']
    company_name = company['company_name']

    print(f"Downloading data for {ticker}:")
    df = yf.download(tickers=ticker, start=start_time.strftime("%Y-%m-%d"), end=end_time.strftime("%Y-%m-%d"), multi_level_index=False)

    df.reset_index(inplace=True)

    df['ticker'] = ticker
    df['company_name'] = company_name

    stock_data.append(df)


# check to make sure that the most recent date in the db is different than the data we just fetched to avoid duplication in the db
last_date_in_db = conn.execute("""
    SELECT
        date
    FROM
        sp100_daily_prices
    ORDER BY date DESC
    LIMIT 1;
    """).fetchone()[0]



# exit with code 2 if duplicate data is detected


final_df = pd.concat(stock_data, ignore_index=True)
final_df.rename(columns={"Date": "date", "Close": "close", "High": "high",
                         "Low": "low", "Open": "open", "Volume": "volume"}, inplace=True)
# perform transforms from transform.py here, to get same result in data
final_df['daily_return_pct'] = final_df.groupby('ticker')['close'].pct_change()
final_df['intraday_return_pct'] = ((final_df['close'] - final_df['open']) / final_df['open']) * 100

final_df['7day_ma'] = final_df.groupby('ticker')['close'].transform(lambda x: x.rolling(7).mean())
final_df['30day_ma'] = final_df.groupby('ticker')['close'].transform(lambda x: x.rolling(30).mean())

final_df['day_of_week'] = final_df['date'].dt.day_name()

final_df = final_df.sort_values(by='date', ascending=False)
final_df = final_df.head(101)
final_df = final_df.sort_values(by='ticker')
final_df.reset_index(inplace=True)
final_df = final_df.drop("index", axis=1)

date_in_daily = final_df['date'].iloc[0].date()

if last_date_in_db == date_in_daily: exit(70)

final_df.to_csv(os.getenv("DAILY_DATA_CSV"))

conn.close()
