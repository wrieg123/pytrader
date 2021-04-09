
class ClockManager:

    def __init__(self):
        self._calendar = None
        self._start = None
        self._end = None
        self._now = None
        self._previous = None

    @property
    def now(self):
        return self._now
    
    @property
    def previous(self):
        return self._previous

    @property
    def today(self):
        return self._now.strftime('%Y-%m-%d')

    @property
    def yesterday(self):
        return self._previous.strftime('%Y-%m-%d')
    
    @property
    def new_day(self):
        if self._previous is None or str(self.now) == 'END':
            return True

        return self.now.day != self.previous.day

    def set_calendar(self, calendar):
        self._calendar = cal
        self._end = max(cal)
        self._start = min(cal)
    
    def update(self):
        self._previous = self.now
        if len(self._calendar) > 0:
            self._now = self.calendar.pop(0)
        else:
            self._now = 'END'
    

class Worker:
    
    def __init__(
            self,
            identifier,
            feed = None,
            manager = ClockManager(),
            ):
        self.identifier = identifier
        self.feed = feed
        self.manager = manager
        self.stream = None
    
    @property
    def bar(self):
        return self.feed.get(self.manager.now)
    
    @property
    def feed_range(self):
        return list(self.feed.keys())
    
    def set_stream(self, stream):
        self.stream = stream
    
    def update(self):
        bar = self.bar

        if not self.stream is None and not bar is None:
            self.stream.push(bar)
            
            del self.feed[self.manager.now]


class WorkerGroup:

    def __init__(
            self,
            identifiers,
            ):
        self.identifiers = identifiers
        self.group = {}
        self.active = []
    
    @propery
    def feed_range(self):
        cal = []
        for i, f in list(self.group.items()):
            cal += f.feed_range
        cal = list(set(cal))
        cal.sort()
        return cal
    
    @property
    def members(self):
        return list(self.group.keys())
    
    def set_active(self, active):
        self.active = active
    
    def update(self):
        keys = self.members
        for f in self.active:
            if f in self.members:
                self.group[f].update()
