from pytrader.finance.universes import ETFUniverse
from pytrader.feeds.streams import Indicator
from pytrader import Engine, Strategy, Metrics

import matplotlib.pyplot as plt
import numpy as np

class MA(Indicator):

    def __init__(self, stream, period = 126):
        super().__init__(stream)
        self.period = period

    def calculate(self):
        return self.data.close[-self.period:].mean()


class TestStrat(Strategy):
    
    def trade(self):
        tree = self.daily_asset_indicators.get_signal_tree()
        avg = tree['SPY']['Trend']['MA'][-1]
        close = self.universes['TEST']['SPY'].daily_stream.close[-1]
        
        spy = self.portfolio['ETF','SPY']
        orig = spy.units if spy is not None else 0
        tgt_shares = self.portfolio.eod_value // close if close - avg > 0 else 0

        if (orig == 0 and tgt_shares != 0) or (orig != 0 and tgt_shares == 0):
            shares = tgt_shares - orig
            self.oms.place_order(self.universes['TEST']['SPY'], np.sign(shares), abs(shares))
       
        


if __name__ == '__main__':

    import os
    engine = Engine('daily', '1998-01-01')
    universe = ETFUniverse('TEST', ['SPY'], start_date = '2000-01-01')
    engine.add_universe(universe)
    

    indicator_mapping = {'MA': MA}

    strat = TestStrat(config_path = os.getcwd()+'/ma_config.yaml', indicator_mapping = indicator_mapping)
    engine.add_strategy(strat)
    
    engine.run()
    #engine.metrics(config = {'show_as': '%', 'rolling':252,})
    metrics = Metrics([strat], **{'show_as':'%', 'rolling':252})
    metrics.print()
    metrics.plot()
