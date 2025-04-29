import duckdb
from dotenv import load_dotenv
import os
import datetime
import pandas as pd
import yfinance as yf

load_dotenv()

conn = duckdb.connect(os.getenv("DUCKDB_DB"))

todays_date = datetime.date.today()

conn.execute("SELECT DISTINCT(ticker) FROM latest_prices ORDER BY ticker")

tickers_raw = conn.fetchall()

tickers = list(map(lambda tick: tick[0], tickers_raw))

# sanity check to make sure we get all tickers
if len(tickers) != 101:
    print("Error fetching tickers from 'latest_prices' view!")
    exit(1)

end_time = datetime.datetime.now()
start_time = end_time - datetime.timedelta(days=46)

stock_data = []

for ticker in tickers:
    print(f"Downloading data for {ticker}:")
    df = yf.download(tickers=ticker, start=start_time.strftime("%Y-%m-%d"), end=end_time.strftime("%Y-%m-%d"), multi_level_index=False)

    df.reset_index(inplace=True)

    df['ticker'] = ticker

    stock_data.append(df)

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

final_df.to_csv(os.getenv("DAILY_DATA_CSV"))


conn.close()