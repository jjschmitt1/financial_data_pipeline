import yfinance as yf
from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime, timedelta

url="https://en.wikipedia.org/wiki/S%26P_100"
csv_path="./sp100_list.csv"
df_attrs = ["ticker", "company_name"]


response = requests.get(url).text

data = BeautifulSoup(response, 'html.parser')

# init dataframe
sp100_list = []

table = data.find_all("tbody")[2]
rows = table.find_all("tr")

for row in rows:
    col = row.find_all("td")
    if len(col) != 0:
        ticker = col[0].text.strip()
        if len(col[1].find_all("a")) == 0:
            company_name = ' '.join(col[1].text.split(" ")[:2])
        else:   
            company_name = col[1].find_all("a")[0]['title']

        # make temp values
        temp_dict = {"ticker": ticker, "company_name": company_name}

        sp100_list.append(temp_dict)

# have df of sp100 companies in dr

df = pd.DataFrame()

end_time = datetime.now()
start_time = end_time - timedelta(days=365)

all_data = []


for company in sp100_list:
    ticker = company['ticker']
    company_name = company['company_name']

    try:
        print(f"Fetching {ticker}")

        df = yf.download(ticker, start=start_time.strftime('%Y-%m-%d'), end=end_time.strftime('%Y-%m-%d'))

        df.reset_index(inplace=True)
        
        df = df[["Date", "Close", "High", "Low", "Open", "Volume"]]
        df["ticker"] = ticker
        df["company_name"] = company_name

        all_data.append(df)

    except Exception as e:
        print("Failed to get data")


final_df = pd.concat(all_data, ignore_index=True)
final_df.rename(columns={"Date": "date", "Close": "close", "High": "high",
                         "Low": "low", "Open": "open", "Volume": "volume"}, inplace=True)

final_df.to_csv(csv_path)

print("Data saved")