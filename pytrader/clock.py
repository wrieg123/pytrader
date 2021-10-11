from pytrader.utils.calendar import epoch_seconds_to_gregorian_date
from pytrader.utils.calendar import make_utc_timestamp
from pytrader.feeds.db.meta import MarketHolidays

from numba import jit
import numpy as np
import datetime


ITER_LEVELS = {
        'minute': 60,
        'daily': 86400,
        }

EST_ADJ = 14400


class Clock:
    """
    Basic Clock Iterator !!!! IN GMT !!!!
    """

    def __init__(self, iter_level, start_time, end_time):
        self.iter_level = iter_level
        self.iter_amt = ITER_LEVELS[iter_level]
        self.start_time = start_time
        self.end_time = end_time
        self._curr_epoch = start_time
        self._prev_epoch = -1
        self._weekday = None
        self._previous_weekday = -1
        self._y = None
        self._m = None
        self._d = None
        self._h = None
        self._date_str = None
        self._time_elapsed = 0
        self.__set_props()
    
    @property
    def now(self):
        return self._curr_epoch 
    
    @property
    def now_dt(self):
        return datetime.datetime.fromtimestamp(self.now)

    @property
    def date_str(self):
        return self._date_str

    @property
    def weekday(self):
        # sunday = 0
        return self._weekday
    
    @property
    def previous(self):
        return self._prev_epoch

    @property
    def previous_weekday(self):
        return self._previous_weekday
    
    @property
    def is_new_day(self):
        return self.previous_weekday != self.weekday

    @property
    def is_running(self):
        return self.now <= self.end_time

    @property
    def hour(self):
        return self._h
    
    def _calc_weekday(self, now):
        return (np.floor(now / 86400) + 4) % 7

    def __set_props(self):
        self._y, self._m , _d, self._h = epoch_seconds_to_gregorian_date(self.now)
        if _d != self._d:
            self._d = _d
            self._date_str = f'{self._y}-{self._m:02d}-{self._d:02d}'
            self._previous_weekday = self._calc_weekday(self.previous)
            self._weekday = self._calc_weekday(self.now)

    def iter(self):
        self._prev_epoch = self.now
        self._curr_epoch = self.now + self.iter_amt
        self.__set_props()
     

class Calendar(Clock):

    def __init__(self, iter_type, start_str, end_str):
        start_dt = make_utc_timestamp(start_str)
        end_dt = make_utc_timestamp(end_str)
        super().__init__(iter_type, start_dt, end_dt)
        self.holidays = MarketHolidays()

    @property
    def is_market_holiday(self):
        return self.holidays[self.date_str] is not None
    
    @property
    def market_holiday(self):
        return self.holidays.get(self.date_str)
