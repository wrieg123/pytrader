from pytrader.finance.assets import ETF
from pytrader.feeds.db.etfs import ETFs

from .universe import Universe

class ETFUniverse(Universe):
    def __init__(
            self,
            name,
            tickers,
            start_date = None,
            end_date = None,
            bars = ['daily'],
            ):
        super().__init__('ETF', name, start_date, end_date, bars)
        self._assets = self.__create_assets(tickers)
    
    def __create_assets(self, tickers):
        assets = {}
        for ticker, info in list(ETFs(tickers = tickers).__dict__().items()):
            assets[ticker] = ETF(ticker, self.bars, info.copy())
        return assets
