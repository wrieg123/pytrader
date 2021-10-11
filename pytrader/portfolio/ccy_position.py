class CCYPosition:

    def __init__(self):
        pass


class USDPosition:

    def __init__(self, initial_value):
        self._units = initial_value
    def _update_values(self):
        pass
    
    @property
    def units(self):
        return self._units
    @property
    def mark(Self):
        return self._units
    @property
    def mark_eod(Self):
        return self._units
    @property
    def details(self):
        return {
                'asset_type': 'CCY',
                'identifier': 'USD',
                'multiplier': 1,
                'ccy': 'USD',
                'units': self._units,
                'cost_basis': self._units,
                'avg_px' : 1,
                'pnl': 0,
                'eod_pnl': 0,
                'mark': self._units,
                'mark_eod': self._units,
                'last': 1,
                }

    @property
    def pnl(self):
        return 0

    def __add__(self, val):
        self._units += val
    def __sub__(self, val):
        self._units -= val
