

class Order:

    def __init__(
            self, 
            order_num, 
            order_type, 
            asset, 
            side, 
            units, 
            entry_time, 
            start_price, 
            order_details = {},
            limit = None, 
            peg_to_open = False,
            good_till = 'CANCEL',
            days_in_force = 0,
            ):
        self._order_num = order_num
        self._order_type = order_type
        self._asset = asset
        self._side = side
        self._units = units
        self._entry_time = entry_time
        self._start_price = start_price
        self.order_details = order_details
        self._limit = limit
        self._peg_to_open = peg_to_open
        self._good_till = good_till
        self._days_in_force = days_in_force

        self._status = 'PLACED'
        self._days_on = 0
        self._fill_price = None
        self._fill_time = None
        self._fill_units = 0
    
    @property
    def order_num(self):
        return self._order_num

    @property
    def order_type(self):
        return self._order_type
    
    @property
    def asset(self):
        return self._asset

    @property
    def side(self):
        return self._side

    @property
    def units(self):
        return self._units

    @property
    def entry_time(self):
        return self._entry_time

    @property
    def start_price(self):
        return self._start_price

    @property
    def limit(self):
        return self._limit

    @property
    def peg_to_open(self):
        return self._peg_to_open

    @property
    def good_till(self):
        return self._good_till

    @property
    def days_in_force(self):
        return self._days_in_force

    @property
    def status(self):
        return self._status

    @property
    def info(self):
        return {
                'status': self._status,
                'order_num': self._order_num,
                'asset_type': self._asset.asset_type,
                'identifier': self._asset.identifier,
                'side': self._side,
                'units': self._units,
                'entry_time': self._entry_time,
                'start_price': self._start_price,
                'limit': self._limit,
                'peg_to_open': self._peg_to_open,
                'good_till': self._good_till,
                'days_in_force': self._days_in_force,
                'days_on': self._days_on,

                'fill_price': self._fill_price,
                'fill_date': self._fill_time,
                'fill_units': self._fill_units,
                }
    
    def cancel(self, date):
        self._status = 'CANCELLED'
        self._fill_time = date

    def update(self, date):
        self._status = 'UPDATED'
        self._fill_time = date

    def fill(self, date, price, partial):
        self._status = 'FILLED' if partial == self.units else 'PARTIAL'
        self._fill_time = date
        self._fill_price = price
        self._fill_units = partial
