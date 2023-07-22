import pandas as pd
import bs4 as bs
import requests
from datetime import date, timedelta
import yfinance as yf

stock_info = pd.DataFrame(columns=["Symbol", "Sector",
                                   "Sub-Industry", "Headquarter", "Founded"])
resp = requests.get(
    'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
soup = bs.BeautifulSoup(resp.text, 'lxml')
table = soup.find('table', {'id': 'constituents'})
tickers = []

for i, row in enumerate(table.findAll('tr')):
    if i == 0:
        continue

    stock_data = row.findAll('td')
    symbol = stock_data[0].text.strip()
    sector = stock_data[2].text.strip()
    industry = stock_data[3].text.strip()
    headquarter = stock_data[4].text.strip()
    founded = stock_data[7].text.strip()[:4]

    tickers.append(symbol)
    stock_info.loc[i - 1] = [symbol, sector, industry, headquarter, founded]

yesterday = str(date.today() + timedelta(days=-1))
today = str(date.today())
stock_price = None

for ticker in tickers:
    data = yf.download(tickers=ticker, start=yesterday,
                       end=today, interval="1m", ignore_tz=True)

    data['Symbol'] = ticker
    data.reset_index(inplace=True)

    if stock_price is None:
        stock_price = data
    else:
        stock_price = pd.concat([stock_price, data])

stock_price.drop(columns=["Date"]).to_csv("data/stock_price.csv", index=False)
stock_info.to_csv("data/stock_info.csv", index=False)
