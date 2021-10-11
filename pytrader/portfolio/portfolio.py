from pytrader.portfolio.ccy_position import CCYPosition, USDPosition
from pytrader.portfolio.position import Position
from pytrader.feeds.streams import Stream

class Portfolio:

    def __init__(self, initial_cash, granularity, calendar, primary_currency = 'USD'):
        self.initial_cash = initial_cash
        self.granularity = granularity
        self.primary_currency = primary_currency

        self.calendar = calendar
        self._positions = {
                'CCY': { self.primary_currency: USDPosition(initial_cash) }
                }
        self._value = initial_cash
        self._lmv = 0
        self._smv = 0
        self._lmv_eod = Stream()
        self._smv_eod = Stream()
        self._eod_values = Stream()
        self._eod_tstamps = Stream()
        self._values = Stream()

    def __getitem__(self, val):
        return self._positions.get(val[0], {}).get(val[1], None)

    @property
    def cash_balance(self):
        return self._positions['CCY'][self.primary_currency].units

    @property
    def lmv(self):
        return self._lmv

    @property
    def smv(self):
        return self._smv

    @property
    def lmv_eod(self):
        return self._lmv_eod[:]

    @property
    def smv_eod(self):
        return self._smv_eod[:]

    @property
    def gmv(self):
        return self._lmv - self._smv

    @property
    def nmv(self):
        return self._lmv + self._smv

    @property
    def eod_value(self):
        return self._eod_values[-1]

    @property
    def eod_values(self):
        return self._eod_values[:]

    @property
    def eod_tstamps(self):
        return self._eod_tstamps[:]

    @property
    def intraday_value(self):
        return self._lmv + self._smv + self._positions['CCY'][self.primary_currency].units

    @property
    def intraday_values(self):
        return self._values[:]

    @property
    def positions(self):
        return self._positions
    
    def process_fill(self, asset, side, units, cost_basis):
        """
        process a fill from the oms and handle the position properly
        
        """
        asset = asset
        side = side
        units = units
        cost_basis = cost_basis
        price = cost_basis / units
        identifier = asset.identifier
        asset_type = asset.asset_type
        if asset_type not in self._positions:
            self._positions[asset_type] = {}
        if asset_type != 'CCY':
            if identifier not in self._positions[asset_type]:
                self._positions[asset_type][identifier] = Position(asset, granularity = self.granularity)
        else:
            # create the ccy in portfolio
            pass

        # adjust the primary ccy
        if asset_type != 'CCY':
            self._positions['CCY'][self.primary_currency] - cost_basis
            if side == 1:
                self._positions[asset_type][identifier].buy(units, price, cost_basis)
            elif side == -1:
                self._positions[asset_type][identifier].sell(units, price, cost_basis)

            # if the trade closes the position, get rid of it
            if self._positions[asset_type][identifier].units == 0:
                del self._positions[asset_type][identifier]
        else:
            # fill the ccy
            pass
    
    def mark_positions(self):
        """
        marks positions values for intraday reporting (if applicable)
        """
        pass
    
    def mark_positions_eod(self):
        """
        at the end of trading day (around 5:00 EST) mark the book and append the value to self._eod_values
        """
        lmv = 0
        smv = 0
        for asset_type, positions in self.positions.items():
            for identifier, position in positions.items():
                if identifier == self.primary_currency and asset_type == 'CCY':
                    pass
                else:
                    mark_eod = position.mark_eod
                    if mark_eod < 0:
                        smv += mark_eod
                    else:
                        lmv += mark_eod
        self._lmv_eod.push(lmv)
        self._smv_eod.push(smv)
        self._eod_values.push(lmv + smv + self.cash_balance)
        self._eod_tstamps.push(self.calendar.now)
