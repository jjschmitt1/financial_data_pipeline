My financial data pipeline!

Tracks the data from stocks in the S&P 100 as of April 2025

In this project, I use BeautifulSoup to extract the list of s&p 100 constituants. Then I use the yfinance module to get the stock trading data from the last year, format it, and load it into a csv. I admit that I could just load the data into a database at this step, but I would like to also showcase my ability to pull data from csv files, not just from html pages

Once the data is in the csv, I load it into a new datafram in a seperate script, perform transformations (intra-day and daily returns, 7 and 30-day moving averages, as well as the day of the week for analysis later), and save it to a csv with the transformations.

This csv is then loaded into SQLite for storage and DuckDB for querying.


DuckDB database:
  - 'sp100_daily_prices' table, containing the following columns
    - date
    - ticker
    - company name
    - close
    - high
    - low
    - volume
    - open
    - daily_return_pct
    - intraday_return_pct
    - 7day_ma (7-day moving average)
    - 30day_ma (30-day moving average)
  - 'latest_prices' view, holding the latest trading day information for all stocks containing the following columns
    - ticker
    - company_name
    - date
    - close
