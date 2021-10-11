from .order import Order

from numba import jit
import numpy as np

STANDARD_FEES = {
        'FUT': 3.00,
        'SEC': 0.01,
        'ETF': 0.01,
        }
ORDER_TYPES = ['LIMIT', 'TWAP', 'OPEN', 'CLOSE', 'BEST', 'HL','OC']

@jit(nopython=True,nogil=True)
def max_adv_shares(volume, adv_participation):
    return max(int(volume.mean() * adv_participation), 1)

@jit(nopython=True,nogil=True)
def max_shares(volume, adv_participation):
    return max(int(volume * adv_participation), 1)

class OrderManagementSystem:

    def __init__(
            self, 
            min_price_series, 
            calendar,
            portfolio,
            adv_participation = 0.05, 
            adv_period = 21, 
            fee_schedule = STANDARD_FEES
            ):
        self.min_price_series = min_price_series
        self.calendar = calendar
        self.portfolio = portfolio 
        self.fee_schedule = fee_schedule

        self.price_stream_key = f'{self.min_price_series}_stream'
        self.adv_participation = adv_participation
        self.adv_period = adv_period

        self._order_num = 0
        self._order_book = {}

    @property
    def order_num(self):
        return self._order_num

    def place_order(self, asset, side, units, order_type = 'CLOSE', order_details = {}):
        """
        places order, returns Order object to keep track of it in the portfolio
        """
        assert order_type in ORDER_TYPES
        self._order_book[asset] = Order(self.order_num, 
                                            order_type, 
                                            asset, 
                                            side, 
                                            units, 
                                            self.calendar.now,
                                            getattr(asset, self.price_stream_key).close[-1],
                                            order_details = order_details, 
                                            **order_details
                                            )
        order = self._order_book[asset]
        self._order_num += 1
        return order

    def _remove_from_ob(self, asset):
        del self._order_book[asset]
    
    def _fill_order(self, order_num, order, asset, fill_price, filled_units, fees):
        self._remove_from_ob(asset)
        cost_basis = order.side * fill_price * filled_units * asset.multiplier + fees
        order.fill(self.calendar.now, fill_price, filled_units)
        self.portfolio.process_fill(asset, order.side, filled_units, cost_basis)
        unfilled_units = order.units - filled_units

        if order.status == 'PARTIAL' and unfilled_units > 0:
            self.place_order(
                    asset,
                    order.side,
                    unfilled_units,
                    order_type = order.order_type,
                    order_details = order.order_details
                    )

    def process_order(self, order_num, order):
        # TODO: processing crypto and ccy orders should be different
        asset = order.asset

        if not getattr(asset, f'{self.min_price_series}_tradeable'):
            order.cancel(self.calendar.now)
            self._remove_from_ob(order_num)
            return
        if order.status == 'CANCELLED':
            self._remove_from_ob(order_num)
            return

        order_type = order.order_type
        side = order.side
        units = order.units
        limit = order.limit

        fee = self.fee_schedule[asset.asset_type]
        price_stream = getattr(asset, self.price_stream_key)
        o = price_stream.open[-1]
        h = price_stream.high[-1]
        l = price_stream.low[-1]
        c = price_stream.close[-1]
        volume = price_stream.volume

        adv_allowable = max_adv_shares(volume[-self.adv_period:], self.adv_participation)
        bar_allowable = max_shares(volume[-1], self.adv_participation)

        filled_units = min(units, max(adv_allowable, bar_allowable))
        order_fill = False
        fill_price = None

        if order_type == 'CLOSE':
            order_fill = True
            fill_price = c
        elif order_type == 'OPEN':
            order_fill = True
            fill_price = o 
        elif order_type == 'LIMIT':
            if side == 1:
                if low <= limit:
                    order_fill = True
                    fill_price = limit
            elif side == -1:
                if high >= limit:
                    order_fill = True
                    fill_price = limit
        elif order_type == 'TWAP':
            order_fill = True
            if c > o:
                ol = (o + l) / 2
                hl = (h + l) / 2
                hc = (h + c) / 2
                fill_price = (ol + hl + hc) / 3
            else:
                oh = (o + h) / 2
                hl = (h + l) / 2
                lc = (l + c) / 2
                fill_price = (oh + hl + lc) / 3
        elif order_type == 'BEST':
            order_fill = True
            if side == 1:
                fill_price = l
            elif side == -1:
                fill_price = h
        
        if order_fill:
            self._fill_order(order_num, order, asset, fill_price, filled_units, filled_units * fee)

    def check_for_fills(self):
        """
        called by engine after rotation to see if there are any fills
        """
        for order_num, order in list(self._order_book.items()):
            self.process_order(order_num, order)
    
