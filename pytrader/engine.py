from pytrader.finance.factories import FuturesFactory, ETFFactory
from pytrader.oms import OrderManagementSystem
from pytrader.portfolio import Portfolio
from pytrader.metrics import Metrics
from pytrader.clock import Calendar

from datetime import datetime

import time

class Engine:

    def __init__(self, min_frequency, start_date, end_date = datetime.utcnow().strftime('%Y-%m-%d'), initial_cash = 25000, silent = False, fast_forward = False):
        self.min_frequency = min_frequency
        self.start_date = start_date
        self.end_date = end_date
        self.initial_cash = initial_cash
        self.silent = silent
        self.fast_forward = fast_forward
        
        ## built-ins
        self.calendar  = Calendar(min_frequency, start_date, end_str = end_date)
        self.portfolio = Portfolio(initial_cash, min_frequency, self.calendar)
        self.oms       = OrderManagementSystem(min_frequency, self.calendar, self.portfolio)

        self.universes = {}
        self.factories = {}
        self.strategy = None
        self.last_checked_wd = -1
        self.last_checked_hour = -1
        
    def add_universe(self, universe):
        name = universe.name
        universe.connect(self.calendar)
        self.universes[name] = universe
        if not self.silent:
            print('Loading universe:',name)
        if universe.asset_type == 'FUT':
            self.factories[name] = FuturesFactory(
                    self.calendar,
                    self.universes[name],
                    start_date = self.start_date,
                    end_date = self.end_date,
                    bars = self.universes[name].bars,
                    )
        elif universe.asset_type == 'ETF':
            self.factories[name] = ETFFactory(
                    self.calendar,
                    self.universes[name],
                    start_date = self.start_date,
                    end_date = self.end_date,
                    bars = self.universes[name].bars,
                    )
         
    def add_strategy(self, strategy):
        self.strategy = strategy        
        self.strategy.connect(self.universes, self.portfolio, self.calendar, self.oms)
        self.strategy.initialize()
   

    def _daily(self):
        # if you need to update the futures universe rolls
        for universe in self.universes.values():
            if universe.asset_type == 'FUT':
                universe.roll_calendar()

        # update prices
        tradeable_assets = []
        for name, factory in self.factories.items():
            tradeable = self.universes[name].daily_tradeable
            tradeable_assets.extend(tradeable)
            factory.check_workers(identifiers = tradeable, bar = 'daily')
        
        # do daily steps
        # OMS -> check for trades
        if self.min_frequency == 'daily':
            self.oms.check_for_fills()

        # Portfolio -> Rec values
        self.portfolio.mark_positions_eod()

        # Strategy -> place trades
        self.strategy.daily_asset_indicators.refresh(assets = tradeable_assets)
        self.strategy.daily_indicators.refresh()

        if not self.fast_forward:
            self.strategy.trade()
    

    def _intraday(self):
        pass
   

    def _check_for_eod(self):
        return (self.calendar.hour != self.last_checked_hour and self.calendar.weekday != self.last_check_wd and self.calendar.hour == 21)
   

    def run(self):

        print('Running backtest...')
        print(f'Starting value ${self.initial_cash:,.0f}')
        start = time.time()
        while self.calendar.is_running:
            if self.calendar.is_market_holiday:
                self.calendar.iter()
                continue
            if self.min_frequency == 'daily' and (self.calendar.weekday == 0 or self.calendar.weekday == 6):
                self.calendar.iter()
                continue

            if self.min_frequency == 'daily' or self._check_for_eod():
                self.last_checked_hour = self.calendar.hour
                self.last_check_wd = self.calendar.weekday

                self._daily()
            else:
                # do intraday steps
                self._intraday()

            self.calendar.iter() # bump the tstamp
        print(f'Ending value ${self.portfolio.eod_value:,.0f}')
        print(f'Ran in {time.time() - start:,.0f} seconds')

    #def metrics(self, plot = True, config = {}):
    #    metrics = Metrics(**config)
    #    metrics.run(self.portfolio)

    #    if plot:
    #        metrics.plot()
