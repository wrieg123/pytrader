import numpy as np

class Position:

    def __init__(self, asset, granularity = 'daily'):
        self.asset = asset
        self.granularity = granularity
        self.stream = getattr(asset, f'{granularity}_stream')
        self._longs = []
        self._shorts = []

        self._units = 0
        self._cost_basis = 0
        self._avg_px = 0

    def __getitem__(self, val):
        return getattr(self, val)

    @property
    def asset_type(self):
        return self.asset.type

    @property
    def identifier(self):
        return self.asset.identifier

    @property
    def multiplier(self):
        return self.asset.multiplier

    @property
    def ccy(self):
        return self.asset.meta['ccy']

    @property
    def units(self):
        return self._units

    @property
    def cost_basis(self):
        return self._cost_basis

    @property
    def avg_px(self):
        return self._avg_px

    @property
    def mark(self):
        return self.stream.current_market_value * self._units

    @property
    def mark_eod(self):
        return self.asset.daily_stream.current_market_value * self._units
    
    @property
    def eod_pnl(self):
        return (self.asset.daily_stream.current_market_value * self._units) - self._cost_basis

    @property
    def pnl(self):
        return (self.stream.current_market_value * self._units) - self._cost_basis

    @property
    def last(self):
        return self.stream.close[-1]

    @property
    def details(self):
        return {
                'asset_type': self.asset_type,
                'identifier': self.identifier,
                'multiplier': self.multiplier,
                'ccy': self.ccy,
                'units': self.units,
                'cost_basis': self.cost_basis,
                'avg_px' : self.avg_px,
                'pnl': self.pnl,
                'eod_pnl': self.eod_pnl,
                'value': self.mark,
                'mark_eod': self.mark_eod,
                'last': self.last,
                }

    def buy(self, units, price, cost_basis):
        self._units += units
        self._cost_basis += cost_basis
        self._avg_px = self.cost_basis/self.units if self.units != 0 else 0

    def sell(self, units, price, cost_basis):
        self._units -= units
        self._cost_basis += cost_basis
        self._avg_px = self.cost_basis/self.units if self.units != 0 else 0


    #def _update_values(self):
    #    list_to_check = self._longs if len(self._longs) > 0 else self._shorts
    #    units = 0
    #    cost_basis = 0
    #    price = 0
    #    for trade in list_to_check: 
    #        units += trade['units'] * trade['side']
    #        cost_basis += trade['cost_basis'] * trade['side']
    #        price += trade['price'] * trade['units']

    #    self._units = units
    #    self._cost_basis = cost_basis
    #    self._avg_px = price / abs(units) if units != 0 else None

    #def fill(self, side, units, price, cost_basis):
    #    """
    #    """
    #    cross_list = self._longs if side == -1 else self._shorts
    #    add_list = self._longs if side == 1 else self._shorts
    #    amt = units
    #    if len(cross_list) > 0:
    #        while amt > 0:
    #            trade = cross_list[0]
    #            delta = trade['units'] - amt 
    #            amt -= min(amt, trade['units'])
    #            if amt > 0: # the buy did not cover the recent FIFO sell
    #                trade['units'] = delta
    #                trade['cost_basis'] = delta * trade['price']
    #            else:
    #                cross_list.pop(0)
    #    if units > abs(self._units): # this trade will flip the side
    #        add_list.append({'side': side, 'units': units, 'price': price, 'cost_basis': cost_basis})
    #    self._update_values()


