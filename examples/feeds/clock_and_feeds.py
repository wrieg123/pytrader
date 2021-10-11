from pytrader.clock import Calendar
from timeit import timeit


def run_clock(c):
    while c.is_running:
        #print(f'{c.date_str}|{c.weekday}:{c.now}', end='\r')
        d = c.date_str
        w = c.weekday
        n = c.now
        c.iter()

if __name__ == '__main__':

    c = Calendar('minute', '2008-01-01', '2021-01-01')
    print(timeit(lambda: run_clock(c), number = 1), 'seconds')
