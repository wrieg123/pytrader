from pytrader.feeds.db.etfs import ETFs, ETFPrices


futures_contracts = ETFs(tickers = ['SPY', 'TLT'], start_date = '2018-01-01')
tickers = list(futures_contracts.meta.keys())

series = ETFPrices(tickers = tickers, start_date = '2018-01-01')
print(series.data)
print(series['SPY'])
