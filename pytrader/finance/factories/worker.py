## Base link layer between a static feed and a stream

class Worker:

    def __init__(
            self, 
            calendar,
            stream,
            db_feed,
            bar = 'daily',
            ):
        self.calendar = calendar
        self.stream = stream
        self.db_feed = db_feed
        self.bar = bar

    def update(self):
        if self.bar == 'daily':
            self.stream.push(self.calendar.now, self.db_feed.get(self.calendar.date_str))
        elif self.bar == 'minute':
            self.stream.push(self.calendar.now, self.db_feed.get(self.calendar.now))

class WorkerGroup:

    def __init__(self, n_bars = 1):
        self.n_bars = n_bars
        self.workers = []

    def append(self, worker):
        self.workers.append(worker)

    def update(self, bar):
        for w in self.workers:
            if bar is None or w.bar == bar:
                w.update()
