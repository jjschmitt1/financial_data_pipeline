My financial data pipeline!

Tracks the data from stocks in the S&P 100 as of March 2025

In this project, I use BeautifulSoup to extract the list of s&p 100 constituants. Then I use the yfinance module to get the stock trading data from the last year, format it, and load it into a csv. I admit that I could just load the data into a database at this step, but I would like to also showcase my ability to pull data from csv files, not just from html pages

Once the data is in the csv, I load it into a new datafram in a seperate script, perform transformations (intra-day and daily returns, 7 and 30-day moving averages, as well as the day of the week for analysis later), and save it to a csv with the transformations.
