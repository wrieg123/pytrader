from pytrader.feeds.active import PriceStream

import pandas as pd

class Asset:

    def __init__(
            self,
            id_type,
            identifier,
            universe,
            meta,
            bars = ['daily'],
            meta,
            tradeable_override = False
            ):
        self.id_type = id_type
        self.identifier = identifier
        self.universe = universe
        self.bars = bars
        self.info = self.__clean_meta(meta, bars)
        self.tradeable_override = tradeable_override
        self.manager = None
        
        for bar in bars:
            multiplier = meta['multiplier'] if meta.get('multiplier') else 1
            setattr(self, bar+'_stream', PriceStream(bar, multiplier = multiplier))
    
    
    def __clean_meta(self, meta, bars):
        meta['last_trade_date'] = pd.to_datetime(meta['last_trade_date']) if meta.get('last_trade_date') else None
        for bar in bars:
            start = f'{bar}_start'
            end = f'{bar}_end'
            meta[start] = pd.to_datetime(meta[start]) if meta.get(start) else None
            meta[end] = pd.to_datetime(meta[end]) if meta.get(end) else None
        
        return meta
    
    @property
    def tradeable(self):
        if self.tradeable_override:
            return True

        return self.info['daily_start_date'] <= self.manager.now and self.manager.now <= self.info['daily_end_date']
    
    def set_manager(self, manager):
        self.manager = manager
    
