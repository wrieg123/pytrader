from pytrader.feeds.db.futures import Futures, FuturesPrices


futures_contracts = Futures(products = ['ES'], start_date = '2018-01-01')
contracts = list(futures_contracts['ES'].keys())

series = FuturesPrices(contracts = contracts, start_date = '2018-01-01')
print(series.data)
print(series['ESM19'])
