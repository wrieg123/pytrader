class Universe:
    
    def __init__(
            self,
            asset_type,
            name,
            start_date,
            end_date,
            bars,
            ):
        self.asset_type = asset_type
        self.name = name
        self.start_date = start_date
        self.end_date = end_date
        self.bars = bars
        self.calendar = None
        self._assets = {} # must be set at init
        self._spreads = {}
        self._has_spreads = False 
        self._daily_tradeable = None
        self._daily_tradeable_check = 0 
        self._minute_tradeable = None
        self._minute_tradeable_check = 0 
    
    @property
    def assets(self):
        return self._assets
    
    @property
    def spreads(self):
        return self._spreads

    @property
    def daily_tradeable(self):
        if self._daily_tradeable_check != self.calendar.now:
            self._daily_tradeable = [a for a, asset in list(self.assets.items()) if asset.daily_tradeable]
            self._daily_tradeable_check = self.calendar.now
        return self._daily_tradeable

    @property
    def minute_tradeable(self):
        if self._minute_tradeable_check != self.calendar.now:
            self._minute_tradeable = [a for a, asset in list(self.assets.items()) if asset.minute_tradeable]
            self._minute_tradeable_check = self.calendar.now
        return self._minute_tradeable

    @property
    def has_spreads(self):
        return self._has_spreads

    def __getitem__(self, val):
        return self._assets.get(val)
    
    def add_spread(self, name, spread):
        self._spreads[name] = spread
        if not self._has_spreads:
            self._has_spreads = True

    def connect(self, cal):
        self.calendar = cal
        for asset in self._assets.values():
            asset.connect(cal)
