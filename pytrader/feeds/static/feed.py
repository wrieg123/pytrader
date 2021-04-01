import pytrader.utils.svconfig as sv

import pandas as pd


def run_query(query):

    self._data = self.__run_query()
    
    query = query.replace(';', '')
    cnx = self.connector.engine().raw_connection()
    cur = cnx.cursor()

    cur.execute(query)
    columns = [i[0] for i in cur.description]
    data = cur.fetchall()

    return pd.DataFrame(data, columns = columns)
