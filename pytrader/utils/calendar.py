from datetime import datetime
from numba import jit

import pytz

NUMBER_TO_LETTER = {
	1: 'F',
	2: 'G',
	3: 'H',
	4: 'J',
	5: 'K',
	6: 'M',
	7: 'N',
	8: 'Q',
	9: 'U',
	10: 'V',
	11: 'X',
	12: 'Z',
}


NUMBER_TO_NAME = {
	1: 'January',
	2: 'February',
	3: 'March',
	4: 'April',
	5: 'May',
	6: 'June',
	7: 'July',
	8: 'August',
	9: 'September',
	10: 'October',
	11: 'November',
	12: 'December'
}

DT_STRING_FORMATS = {
        'daily': '%Y-%m-%d',
        'minute': '%Y-%m-%d %H:%M',
        }

@jit(nopython=True, nogil = True)
def epoch_seconds_to_gregorian_date(eseconds):
    y = 4716; j = 1401; m = 2; n = 12; r = 4; p = 1461 
    v = 3; u = 5; s = 153; w = 2; B = 274277; C = -38

    J = int(0.5 + eseconds / 86400.0 + 2440587.5)

    f = J + j + (((4 * J + B) // 146097) * 3) // 4 + C
    e = r * f + v
    g = (e % p) // r
    h = u * g + w
    D = (h % s) // u + 1
    M = (h // s + m) % n + 1
    Y = (e // p) - y + (n + m - M) // n

    return Y, M, D, h % 24


"""
import pytz
est = pytz.timezone('US/Eastern')
gmt = pytz.timezone('GMT')
date_obj = datetime.datetiem # assume you have a datetime.datetime obj
utc_timestamp = est.localize(date_obj).timestamp() # in utc
"""


def make_utc_timestamp(string_repr, bar = 'daily', adjust_to_period_close = None):
    if string_repr is None:
        return None
    dt = datetime.strptime(string_repr, DT_STRING_FORMATS[bar])
    if adjust_to_period_close is not None:
        h, m = adjust_to_period_close.split(':')
        dt.replace(hour = int(hour), minute = int(minutes))
    return int(dt.timestamp())
