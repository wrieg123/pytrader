from .asset import Asset

class ETF(Asset):

    def __init__(
            self,
            ticker,
            bars,
            meta,
            tradeable_override = False):
        super().__init__('ETF', ticker, bars, meta, tradeable_override = tradeable_override)
