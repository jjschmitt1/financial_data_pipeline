import yfinance as yf
from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime, timedelta

url="https://en.wikipedia.org/wiki/S%26P_100"
csv_path="../data_sources/historical_sources/sp100_1yr_data.csv"
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

# have listz of sp100 companies
df = pd.DataFrame()

# get last year of data
end_time = datetime.now()
start_time = end_time - timedelta(days=365)

all_data = []


for company in sp100_list:
    # account for error when retreiving Berkshire hathway data
    ticker = company['ticker'] if company['ticker'] != "BRK.B" else "BRK-B"
    company_name = company['company_name']

    try:
        print(f"Fetching {ticker}")

        # get info from yfinance
        df = yf.download(ticker, start=start_time.strftime('%Y-%m-%d'), end=end_time.strftime('%Y-%m-%d'), multi_level_index=False)

        df.reset_index(inplace=True)

        #set ticker and company_name cols
        df['ticker'] = ticker
        df['company_name'] = company_name
            
        all_data.append(df)
        
    except Exception as e:
        print("Failed to get data")


# concat all of the data into a final df and rename cols
final_df = pd.concat(all_data, ignore_index=True)
final_df.rename(columns={"Date": "date", "Close": "close", "High": "high",
                         "Low": "low", "Open": "open", "Volume": "volume"}, inplace=True)

# rearrange columns so ticker and company name are first
last_cols = final_df.iloc[:, -2:]
final_df = final_df.iloc[:, :-2]
final_df = pd.concat([last_cols, final_df], axis=1)

final_df.to_csv(csv_path)

print("Data saved")