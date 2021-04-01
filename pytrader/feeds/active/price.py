from .stream import Stream

import numpy as np


ATTRIBUTES = {
        'minute': ['year', 'month', 'day', 'hour', 'minute', 'open', 'high', 'low', 'close', 'volume'],
        'daily': ['year', 'month', 'day', 'hour', 'minute', 'open', 'high', 'low', 'close', 'volume', 'open_interest'],
        }

class PriceStream:


    def __init__(
            self,
            bar_type,
            cache = 25000,
            multiplier = 1
            ):
        self.attributes = ATTRIBUTES[bar_type]

        for a in ATTRIBUTES[bar_type]:
            setattr(self, a, Stream(cache = cache))
        
    @property
    def market_value(self):
        return self.close.v * self.multiplier
    
    @property
    def ts(self):
        return {a : getattr(self, a).ts for a in self.attributes}
    
    def push(self, bar):
        for k, v in list(bar.items()):
            getattr(self, k).push(v)
