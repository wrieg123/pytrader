from pytrader.feeds.static import run_query, FuturesInfo
from pytrader.finance.assets import Future

from .universe import Universe

import pandas as pd


class CalendarRouter:

    def __init__(
            self,
            manager,
            products,
            futures_info,
            method,
            periods,
            params,
            assets,
            ):
        self.manager = manager
        self.products = products
        self.futures_info = futures_info
        self.method = method
        self.periods = periods
        self.params = params
        self.assets = assets

        self.calendars, self.calendar_indexes = self.create_calendar()
        self.tradeable = []
        self.active_list = []
        self.inactive_list = []
        self.active_products = {}
        self.inactive_products = {}
        


    def find_date(self, indexes, date, actor = 'active'):
        if actor == 'active':
            for i in indexes:
                if i > date:
                    return i
        elif actor == 'inactive':
            for n, i in enumerate(indexes):
                if i >= date:
                    if n >= 1:
                        return indexes[:(n-1)]
                    else:
                        return []
            return []
    

    def create_calendar(self):
    
        df = pd.DataFrame(self.futures_info).T
        df.index.name = 'contract'
        df.reset_index(inplace = True)

        roll_lag = self.params.get('roll_lag')
        roll_on = self.params.get('roll_on')
        if roll_on is None:
            roll_on = 'last_trade_date'

        if roll_lag is not None:
            df[roll_on] = df[roll_on].apply(lambda x: pd.to_datetime(x) - BDay(roll_lag))
        
        calendars = {}
        indexes = {}

        for product in self.products:
            sub_df = df.loc[df['Product'] == product].copy()
            sub_df = sub_df[['Contract', roll_on]].set_index(roll_on)
            sub_df.index = pd.to_datetime(sub_df.index)
            sub_df.sort_index(inplace = True)
            sub_df.columns = [f'{Product}-1']
            conts = []

            for i in range(self.periods[0], self.periods[1] + 1):
                s_factor = i - 1
                cont = f'{Product}-{i}'
                conts.append(cont)

                if s_factor != 0:
                    sub_df[cont] = sub_df[f'{Product}-1'].shift(-s_factor)
            calendars[product] = sub_df[conts].to_dict(orient = 'index')
            index = list(calendars[product].keys())
            index.sort()
            indexes[product] = index
        return calendars, indexes

class FuturesUniverse(Universe):
    
    def __init__(
            name,
            products,
            continuation_periods,
            start_date = None,
            end_date = None,
            start_time = None,
            end_time = None,
            bars = ['daily'],
            continuation_method = 'calendar',
            roll_params = {'roll_on': 'LastTradeDate', 'roll_lag': 0},
            ):
        super().__init__('FUT', name, start_date, end_date, start_time, end_time)
        self.products = products
        self.bars = bars
        self.continuation_method = continuation_method
        self.roll_params = roll_params
    
        self.product_info = self.__get_product_info()
        self.futures_info = FuturesInfo(product = self.products)
        self.continuations = []
        self.__create_assets(name, bars)

        self._router = CalendarRouter(
                            self.manager,
                            products,
                            futures_info,
                            continuation_method, 
                            continuation_periods,
                            roll_params, 
                            self.assets
                            )
        

    def __get_products_info(self):
        """returns information for product information"""
        print("Loading Universe:", self.name, str(self.products).strip('[]'))
        query = f"select * from products where product in ({str(self.products).strip('[]')})"

        return run_query(query).set_index('Product').to_dict(orient='index')
    
    def __create_assets(self, name, bars):
        for k, v in self.futures_info.items():
            self.assets[k] = Future(k, name, bars, v)
        if self.continuation_method is not None:
            for product in self.products:
                for i in range(self.continuation_periods[0], self.continuation_periods[1] + 1):
                    cont = f'{product}-{i}'
                    self.assets[cont] = Future(cont, name, bars, {}, tradeable_override = True)
                    self.continuations.append(cont)
