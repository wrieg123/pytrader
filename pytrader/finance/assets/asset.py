from pytrader.utils.calendar import make_utc_timestamp
from pytrader.feeds.streams import PriceStream

import pandas as pd

CACHE_VALUES = {
        'daily': 5000,
        'minute': 60 * 24 * 63
        }

DT_STRING_FORMATS = {
        'daily': '%Y-%m-%d',
        'minute': '%Y-%m-%d %H:%M',
        }

class Asset:

    def __init__(
            self, 
            asset_type, 
            identifier, 
            bars,
            meta, 
            tradeable_override
            ):
        self.asset_type = asset_type
        self.identifier = identifier
        self._hash = hash(f'{asset_type}|{identifier}')
        self.meta = meta
        self.bars = bars
        self.tradeable_override = tradeable_override
        self.make_timestamp = make_utc_timestamp
        self.__clean_meta(meta)
        self.calendar = None
        self.multiplier = 1
        
        for bar in bars:
            setattr(self, bar+'_stream', PriceStream(bar if bar == 'daily' else 'intraday', multiplier = meta.get('multiplier', 1), cache = CACHE_VALUES[bar]))
    
    @property
    def daily_tradeable(self):
        if self.tradeable_override:
            return True
        if self.daily_start_date is None:
            return False
        return self.daily_start_date <= self.calendar.now and self.calendar.now <= self.daily_end_date
    
    @property
    def minute_tradeable(self):
        if self.tradeable_override:
            return True
        if self.minute_start_date is None:
            return False
        return self.minute_start_date <= self.calendar.now and self.calendar.now <= self.minute_end_date

    @property
    def mkt_is_open(self):
        """
        check the pre, mkt, and post open start/ends for whether or not hte contract can actually be traded and have an inraday price update
        """
        return True

    @property
    def daily_mv(self):
        return self.daily_stream.current_market_value

    @property
    def minute_mv(self):
        return self.minute_stream.current_market_value
    
    def __hash__(self):
        return self._hash

    def __clean_meta(self, meta):
        self.daily_start_date = self.make_timestamp(meta['daily_start_date'])
        self.daily_end_date = self.make_timestamp(meta['daily_end_date'])
        self.minute_start_date = self.make_timestamp(meta.get('minute_start_date'), bar='minute')
        self.minute_end_date = self.make_timestamp(meta.get('minute_end_date'), bar='minute')
    
    def connect(self, cal):
        self.calendar = cal
