from pytrader.feeds.db.futures import FuturesPrices

from .factory import Factory


class FuturesFactory(Factory):

    def __init__(self, calendar, universe, start_date = None, end_date = None, bars = ['daily']):
        super().__init__(calendar, universe, start_date, end_date, bars)
    
    def get_data_feeds(self):
        data_feeds = {}
        for bar in self.bars:
            data_feeds[bar] = FuturesPrices(
                                        series = bar,
                                        contracts = self.identifiers,
                                        start_date = self.start_date,
                                        end_date = self.end_date,
                                        query_tool = 'build_tree'
                                        )
        return data_feeds

    def check_workers(self, identifiers = None):
        if identifeirs is not None:
            for identifier in identifiers:
                self.workers[identifier].update()
        else:
            for _, w in list(self.workers.items()):
                w.update()
        # special override for conts
        if self.universe.include_continuations:
            self.universe.update_continuous_contracts()
