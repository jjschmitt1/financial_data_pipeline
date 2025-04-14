import pandas as pd
from datetime import datetime
import numpy as np

load_csv_path = "../data_sources/historical_sources/sp100_1yr_data.csv"
save_csv_path = "../data_sources/historical_sources/sp100_transformed_1yr_data.csv"

df = pd.read_csv(load_csv_path)

# transform data
# normalize column data types
df['date'] = pd.to_datetime(df['date'])
df[['close', 'open', 'high', 'low']] = df[['close', 'open', 'high', 'low']].astype(float)
df['volume'] = df['volume'].astype(int)

# daily return
df['daily_return_pct'] = df.groupby('ticker')['close'].pct_change()
df['intraday_return_pct'] = ((df['close'] - df['open']) / df['open']) * 100

# 7-day and 30-day moving averages
df['7day_ma'] = df.groupby('ticker')['close'].transform(lambda x: x.rolling(7).mean())
df['30day_ma'] = df.groupby('ticker')['close'].transform(lambda x: x.rolling(30).mean())

# add day_of_week for group_by_day analysis later?
df['day_of_week'] = df['date'].dt.day_name()

df = df.iloc[:, 1:]

df.to_csv(save_csv_path)
