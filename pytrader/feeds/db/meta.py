from .utils import execute_pandas

class MarketHolidays:

    def __init__(self):
        self.holidays = self.__get_holidays()
    
    def __getitem__(self, val):
        return self.holidays.get(val)
    
    def __dict__(self):
        return self.holidays

    def __get_holidays(self):
        query = "select * from meta_market_holidays"
        return execute_pandas(query).set_index('holiday').to_dict(orient = 'index')
