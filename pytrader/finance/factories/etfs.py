from pytrader.feeds.db.etfs import ETFPrices

from .factory import Factory

class ETFFactory(Factory):

    def __init__(self, calendar, assets, start_date = None, end_date = None, bars = ['daily']):
        super().__init__(calendar, assets, start_date, end_date, bars)
    
    def get_data_feeds(self):
        data_feeds = {}
        for bar in self.bars:
            data_feeds[bar] = ETFPrices(
                                    series = bar,
                                    tickers = self.identifiers,
                                    start_date = self.start_date,
                                    end_date = self.end_date,
                                    query_tool = 'build_tree'
                                    )
        return data_feeds
