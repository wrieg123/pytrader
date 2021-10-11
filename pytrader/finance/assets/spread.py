from pytrader.feeds.streams import PriceStream, Stream
from numba import jit

CACHE_VALUES = {
        'daily': 5000,
        'minute': 60 * 24 * 63
        }

@jit(nopython=True, nogil=True)
def adjusted_spread(last, a, b, beta):
    #NQ / ES : (A / B)
    a_delta = a[-1] - a[-2] # dollar change in a
    b_delta = b[-1] - b[-2] # dollar change in b
    
    return (a_delta - b_delta * beta) + last

@jit(nopython=True, nogil=True)
def size_adjusted_trade(capital, size, ratio):
    a = capital / size
    b = a * ratio
    return a, b

@jit(nopython=True, nogil=True)
def size_ratio_trade(capital, size, ratio):
    r = capital / size
    return self.ratio[0] * r, self.ratio[1] * r


@jit(nopython=True, nogil=True)
def size_crush_trade(capital, size, ratio):
    return 0

class Spread:
    # spreads get added into the engine to be calced after the assets do ... this is really the only way to do it effectively

    def __init__(self, bar, asset_a, asset_b, spread_method = 'fixed', ratio = (1, -1), spread_type = 'RATIO', indicator = None):
        self.bar = bar
        self.asset_a = asset_a
        self.asset_b = asset_b
        self.spread_type = spread_type
        self.ratio = ratio
        self.indicator = indicator

        self.stream_a = getattr(asset_a, f'{bar}_stream')
        self.stream_b = getattr(asset_b, f'{bar}_stream')
        self._close = Stream(cache = CACHE_VALUES.get(bar), NULL_VAL = 0)
        self._timestamps = Stream(cache = CACHE_VALUES.get(bar))

    @property
    def tradeable(self):
        return getattr(self.asset_a, f'{self.bar}_tradeable') and getattr(self.asset_b, f'{self.bar}_tradeable')
    
    @property
    def size(self):
        """
        GMV $ size
        """
        a_mv = getattr(asset_a, f'{bar}_mv')
        b_mv = getattr(asset_b, f'{bar}_mv')

        if self.spread_type == 'RATIO':
            return a_mv * abs(self.ratio[0]) + b_mv[-1] * abs(self.ratio[1])
        elif self.spread_type == 'ADJUSTED':
            return a_mv + b_mv * self.indicator[-1]
    
    def __getitem__(self, val):
        return self._close[val]
    
    def _calc_spread(self, a, b):
        if self.spread_type == 'RATIO':
            return a[-1] * self.ratio[0] + b[-1] * self.ratio[1]
        elif self.spread_type == 'ADJUSTED':
            return adjusted_spread(self._close[-1], a, b, self.indicator[-1]) 

    def size_trade(self, capital):
        if self.spread_type == 'RATIO':
            return size_ratio_trade(capital, self.size, self.ratio)
        return size_adjusted_trade(capital, self.size, self.indicator[-1])

    def update(self):
        a_last = self.stream_a.times_seen[-1]
        b_last = self.stream_b.times_seen[-1]
        if a_last is None or b_last is None:
            return

        a = self.stream_a.close[-2:]
        b = self.stream_b.close[-2:]

        if a_last == b_last:
            self._close.push(self._calc_spread(a, b))
            self._timestamps.push(a_last)

class Crush:

    def __init__(self, bar, assets, ratio):
        self.bar = bar
        self.assets = assets
        self.ratio = ratio
        self._close = Stream(cache = CACHE_VALUES.get(bar), NULL_VAL = 0)
        self._timestamps = Stream(cache = CACHE_VALUES.get(bar))
