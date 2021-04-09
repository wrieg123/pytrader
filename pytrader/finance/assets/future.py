from .asset import Asset

class Future(Asset):

    def __init__(
            self,
            contract,
            universe,
            bars,
            meta,
            tradeable_override = False):
        super().__init__('FUT', contract, universe, bars, meta, tradeable_override = tradeable_override)
