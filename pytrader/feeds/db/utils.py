from pytrader.utils import Connector
import pandas as pd
def execute_pandas(query):
    return pd.read_sql_query(query, Connector().engine())
