from .asset import Asset

class CCYPair(Asset):

    def __init__(
            self,
            ccy1,
            ccy2,
            bars,
            meta,
            ):
        super().__init__('CCY', (ccy1, ccy2), bars, meta, False)

