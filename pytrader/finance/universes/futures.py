from pytrader.finance.assets import Future
from pytrader.feeds.db.futures import Futures

from .universe import Universe

import pandas as pd
import numpy as np

class ContinuousFutures():

    def __init__(
            self, 
            products,
            assets,
            daily_tradeable,
            include_continuous_contracts = False,
            continuation_periods = (1,1), 
            continuation_method = 'calendar', 
            roll_lag = 0, 
            roll_on = 'last_trade_date', 
            roll_period = 21,
            ):
        self.assets = assets
        self.tradeable = daily_tradeable
        self.include_continuous_contract = include_continuous_contracts
        self.continuation_periods = continuation_periods,
        self.continuation_method = continuation_method,
        self.roll_on = roll_on
        self.roll_period = roll_period
        self._on_the_run = {}

    @property
    def on_the_run(self):
        return self._on_the_run
    
    def roll_contracts(self):
        # pass 
        return




class FuturesUniverse(Universe):

    def __init__(
            self,
            name, 
            products, 
            start_date = None, 
            end_date = None,
            bars = ['daily'],
            continuation_config = {}
            ):
        super().__init__('FUT', name, start_date, end_date, bars)
        self.products = products
        self._assets, self._product_tree = self.__create_assets()
        self._continuous_futures = ContinuousFutures(products, self._assets, self.daily_tradeable, **continuation_config)
    
    @property
    def product_tree(self):
        return self._product_tree

    @property
    def continuations(self):
        return self._continuous_futures

    def __create_assets(self):
        assets = {}
        product_tree = {}
        for product, contract_info in list(Futures(products = self.products, start_date = self.start_date, end_date = self.end_date).__dict__().items()):
            if product not in assets:
                product_tree[product] = []
            for contract, info in list(contract_info.items()):
                product_tree[product].append(contract)
                assets[contract] = Future(contract, self.bars, info.copy())
        return assets, product_tree 
    
    def roll_calendar(self):
        """
        based on the defined calendar logic; update the otr contracts
        """
        # update them
        self._continuous_futures.roll_contracts()
