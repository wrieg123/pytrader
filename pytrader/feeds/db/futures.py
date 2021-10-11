from pytrader.utils import Connector
from .utils import execute_pandas

import pandas as pd

SERIES_SELECT_MAP = {
        'daily': (
            "trade_date, contract, open_px as open, high_px as high, low_px as low, close_px as close, volume, open_interest", 
            "trade_date, contract"
            ),
        'minute': (
            "trade_date, timestamp, contract, open_px as open, high_px as high, low_px as low, close_px as close, volume",
            "trade_date, timestamp, contract"
            ),
        }


def build_contract_tree(query):
    tree = {}
    for row in execute_pandas(query).to_dict(orient = 'records'):
        product = row['product']
        contract = row['contract']
        if product not in tree:
            tree[product] = {}
        tree[product][contract] = row
    return tree

class Futures:

    def __init__(
            self,
            contracts = [],
            products = [],
            start_date = None,
            end_date = None,
            custom_query = None,
            series_to_check_for = 'daily',
            asset_class = None,
            ):
        self.contracts = contracts
        self.products = products
        self.start_date = start_date
        self.end_date = end_date
        self.custom_query = custom_query
        self.series_to_check_for = series_to_check_for
        self.meta = self.__get_contracts()

    def __getitem__(self, val):
        return self.meta.get(val, {})

    def __dict__(self):
        return self.meta

    def __get_contracts(self):
        contracts_where_str = f"AND contract in ({str(self.contracts).strip('[]')})" if len(self.contracts) > 0 else ''
        products_where_str = f"AND product in ({str(self.products).strip('[]')})" if len(self.products) > 0 else ''
        start_date_where_str = f"AND {self.series_to_check_for}_end_date >= '{self.start_date}'" if self.start_date is not None else ''
        end_date_where_str = f"AND {self.series_to_check_for}_end_date >= '{self.end_date}'" if self.end_date is not None else ''

        if self.custom_query is None:
            query = f"""
            SELECT *
            FROM futures_universe
            WHERE 1=1
                AND {self.series_to_check_for}_end_date is not NULL
                {contracts_where_str}
                {products_where_str}
                {start_date_where_str}
                {end_date_where_str}
            ORDER BY product, soft_expiry
            """
        else:
            query = self.custom_query
        return build_contract_tree(query) 
         


def build_daily_tree(query):
    cnx = Connector().cnx()
    cursor = cnx.cursor()
    tree = {}
    for row in cnx.execute(query):
        trade_date = row[0][:10]
        contract = row[1]
        if contract not in tree:
            tree[contract] = {}
        if contract not in tree[contract]:
            tree[contract][trade_date] = {}
        for i, c in enumerate(['open', 'high', 'low', 'close', 'volume', 'open_interest']):
            tree[contract][trade_date][c] = row[i]

    cursor.close()
    cnx.close()
    return tree

def build_minute_tree(query):
    cnx = Connector().cnx()
    cursor = cnx.cursor()
    tree = {}
    for row in cnx.execute(query):
        timestamp = int(row[1]) # is !UTC!
        contract = row[2]
        if contract not in tree:
            tree[contract] = {}
        if contract not in tree[contract]:
            tree[contract][timestmap] = {}
        for i, c in enumerate(['open', 'high', 'low', 'close', 'volume', 'open_interest']):
            tree[contract][timestamp][c] = row[i]
    cursor.close()
    cnx.close()
    return tree


class FuturesPrices:

    def __init__(
            self,
            series = 'daily',
            contracts  = [],
            start_date = None,
            end_date   = None,
            custom_query = None,
            query_tool = 'default'
            ):
        self.series = series
        self.contracts  = contracts
        self.start_date = start_date
        self.end_date   = end_date
        self.custom_query = custom_query
        self.query_tool = query_tool
        self.data = self.get_data()
    
    def __getitem__(self, val):
        if self.query_tool != 'default':
            return self.data.get(val)
        else:
            return self.data.loc[self.data['contract'] == val]

    def __dict__(self):
        return self.data

    def get_data(self):
        contracts_where_str = f"AND contract IN ({str(self.contracts).strip('[]')})" if len(self.contracts) > 0 else ''
        start_date_where_str = f"AND trade_date >= '{self.start_date}'" if self.start_date is not None else ''
        end_date_where_str = f"AND trade_date <= '{self.end_date}'" if self.end_date is not None else ''
        
        select, order = SERIES_SELECT_MAP[self.series]
        if self.custom_query is None:
            query = f"""
            SELECT {select}
            FROM futures_{self.series}_bars
            WHERE 1=1
                {contracts_where_str}
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

