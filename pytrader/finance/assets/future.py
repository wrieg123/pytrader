from .asset import Asset

class Future(Asset):

    def __init__(
            self,
            contract,
            bars,
            meta,
            tradeable_override = False):
        super().__init__('FUT', contract, bars, meta, tradeable_override)

        self.soft_expiry = self.make_timestamp(meta.get('soft_expiry'))
        self.first_trade_date = self.make_timestamp(meta.get('first_trade_date'))
        self.last_trade_date = self.make_timestamp(meta.get('last_trade_date'))
        self.settlement_date = self.make_timestamp(meta.get('settlement_date'))
        self.multipler = self.meta.get('multiplier', 1)
        self.exchange = self.meta.get('exchange')
        self.product = self.meta.get('product')

class Continuous():

    def __init__(
            self,
            initial_constituent,
            bars,
            multiplier = 1,
            ):
        self._constituent = initial_constituent
        for bar in bars:
            setattr(self, bar+'_stream', PriceStream(bar if bar == 'daily' else 'intraday', multiplier = multiplier, cache = CACHE_VALUES[bar]))
    
    @property
    def daily_tradeable(self):
        return self._constituent.daily_tradeable  

    @property
    def minute_tradeable(self):
        return self._constituent.minute_tradeable
    
    @property
    def daily_mv(self):
        return self._constituent.minute_stream.current_market_value

    @property
    def minute_mv(self):
        return self._constituent.minute_stream.current_market_value

    def set_constituent(self, asset):
        self._constituent = asset
