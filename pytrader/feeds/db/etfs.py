from pytrader.utils import Connector
from .utils import execute_pandas

import pandas as pd


SERIES_SELECT_MAP = {
        'daily': (
            "trade_date, ticker, open_px as open, high_px as high, low_px as low, close_px as close, volume", 
            "trade_date, ticker"
            ),
        'minute': (
            "trade_date, timestamp, ticker, open_px as open, high_px as high, low_px as low, close_px as close, volume",
            "trade_date, timestamp, ticker"
            ),
        }


class ETFs:

    def __init__(
            self,
            tickers = [],
            start_date = None,
            end_date = None,
            custom_query = None,
            series_to_check_for = 'daily'
            ):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.custom_query = custom_query
        self.series_to_check_for = series_to_check_for
        self.meta = self.__get_tickers()

    def __getitem__(self, val):
        return self.meta.get(val, {})

    def __dict__(self):
        return self.meta

    def __get_tickers(self):
        tickers_where_str = f"AND ticker in ({str(self.tickers).strip('[]')})" if len(self.tickers) > 0 else ''
        start_date_where_str = f"AND {self.series_to_check_for}_end_date >= '{self.start_date}'" if self.start_date is not None else ''
        end_date_where_str = f"AND {self.series_to_check_for}_end_date >= '{self.end_date}'" if self.end_date is not None else ''

        if self.custom_query is None:
            query = f"""
            SELECT *
            FROM etfs_universe 
            WHERE 1=1
                AND {self.series_to_check_for}_end_date is not NULL
                {tickers_where_str}
                {start_date_where_str}
                {end_date_where_str}
            ORDER BY ticker, daily_start_date
            """
        else:
            query = self.custom_query
        return execute_pandas(query).set_index('ticker').to_dict(orient='index')
         


def build_daily_tree(query):
    cnx = Connector().cnx()
    cursor = cnx.cursor()
    tree = {}
    for row in cnx.execute(query):
        trade_date = row[0][:10]
        ticker = row[1]
        if ticker not in tree:
            tree[ticker] = {}
        if trade_date not in tree[ticker]:
            tree[ticker][trade_date] = {}
        for i, c in enumerate(['open', 'high', 'low', 'close', 'volume']):
            tree[ticker][trade_date][c] = row[i+2]
    cursor.close()
    cnx.close()
    return tree

def build_minute_tree(query):
    cnx = Connector().cnx()
    cursor = cnx.cursor()
    tree = {}
    for row in cnx.execute(query):
        trade_date = row[0][:10]
        timestamp = int(row[1])
        ticker = row[2]
        # TODO needs to be reubuilt ticker -> tstamp
        #if trade_date not in tree:
        #    tree[trade_date] = {}
        #if timestamp not in tree[trade_date]:
        #    tree[trade_date][timestamp] = {}
        #if ticker not in tree[trade_date][timestamp]:
        #    tree[trade_date][timestamp][ticker] = {}
        #for i, c in enumerate(['open', 'high', 'low', 'close', 'volume']):
        #    tree[trade_date][timestamp][ticker][c] = row[i]
    cursor.close()
    cnx.close()
    return tree


class ETFPrices:

    def __init__(
            self,
            series = 'daily',
            tickers  = [],
            start_date = None,
            end_date   = None,
            custom_query = None,
            query_tool = 'default'
            ):
        self.series = series
        self.tickers  = tickers
        self.start_date = start_date
        self.end_date   = end_date
        self.custom_query = custom_query
        self.query_tool = query_tool
        self.data = self.get_data()
    
    def __getitem__(self, val):
        if self.query_tool != 'default':
            return self.data.get(val, {})
        else:
            return self.data.loc[self.data['ticker'] == val].copy()

    def __dict__(self):
        return data

    def get_data(self):
        tickers_where_str = f"AND ticker IN ({str(self.tickers).strip('[]')})" if len(self.tickers) > 0 else ''
        start_date_where_str = f"AND trade_date >= '{self.start_date}'" if self.start_date is not None else ''
        end_date_where_str = f"AND trade_date <= '{self.end_date}'" if self.end_date is not None else ''
        
        select, order = SERIES_SELECT_MAP[self.series]
        if self.custom_query is None:
            query = f"""
            SELECT {select}
            FROM etfs_{self.series}_bars
            WHERE 1=1
                {tickers_where_str}
                {start_date_where_str}
                {end_date_where_str}
            ORDER BY {order}
            """
        else:
            query = self.custom_query
        if self.query_tool == 'default':
            return execute_pandas(query)
        elif self.query_tool == 'build_tree':
            if self.series == 'daily':
                return build_daily_tree(query)
            elif self.series == 'minute':
                return build_minute_tree(query)
