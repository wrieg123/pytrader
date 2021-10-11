from .stream import Stream

from functools import reduce
import numpy as np


ATTRIBUTES = {
        'daily': ['open', 'high', 'low', 'close', 'volume', 'open_interest'],
        'intraday': ['open', 'high', 'low', 'close', 'volume'],
        }
PRIVATE_ATTR_MAP = {
        'open': '_open',
        'high': '_high',
        'low': '_low',
        'close': '_close',
        'volume': '_volume',
        'open_interest': '_open_interest',
        }

class PriceStream:

    def __init__(
            self,
            bar_type,
            cache = 5000,
            multiplier = 1
            ):
        self.attributes = ATTRIBUTES[bar_type]
        self.multiplier = multiplier
        self._times_seen = Stream(cache = cache)
        for a in ATTRIBUTES[bar_type]:
            setattr(self, f'_{a}', Stream(cache = cache))
        
    @property
    def current_market_value(self):
        return self.close[-1] * self.multiplier
    
    @property
    def last(self):
        return {a : getattr(self, a).last for a in attributes}
    
    @property
    def times_seen(self):
        return self._times_seen[:]

    @property
    def open(self):
        return getattr(self, '_open')[:]

    @property
    def high(self):
        return getattr(self, '_high')[:]

    @property
    def low(self):
        return getattr(self, '_low')[:]

    @property
    def close(self):
        return getattr(self, '_close')[:]

    @property
    def mv(self):
        return getattr(self, '_close')[:] * self.multiplier

    @property
    def volume(self):
        return getattr(self, '_volume')[:]

    @property
    def open_interest(self):
        if 'open_interest' in self.attributes:
            return getattr(self, '_open_interest')[:]
        else:
            return None

    def __getitem__(self, val):
        return {a : getattr(self, f'_{a}')[val] for a in self.attributes}

    def push(self, timestamp, bar):
        if bar is not None:
            for k, v in list(bar.items()):
                getattr(self, PRIVATE_ATTR_MAP[k]).push(v)
            self._times_seen.push(timestamp)

    def align(self, price_streams, field='close'):
        if isinstance(price_streams, list):
            times = [self.times_seen, *[x.times_seen for x in price_streams]]
            unique_tstamps = reduce(np.intersect1d, tuple(times))
            inds = [self.in1d(t, unique_tstamps) for t in times]
            arrs = []
            for i, ind in enumerate(inds):
                if i == 0:
                    arrs.append(getattr(self, field)[ind])
                else:
                    arrs.append(getattr(price_streams[i], field)[ind])
            return np.vstack(arrs)
        else:
            _, a_ind, b_ind = np.intersect1d(self.times_seen, price_streams.times_seen, assume_unique=True, return_indices=True)
            return np.vstack([getattr(self, field)[a_ind], getattr(price_stream, field)[b_ind]])
