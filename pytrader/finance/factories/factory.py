from .worker import Worker, WorkerGroup


class Factory:

    def __init__(self, calendar, universe, start_date, end_date, bars):
        self.calendar = calendar
        self.universe = universe
        self.identifiers = list(universe.assets)
        self.start_date = start_date
        self.end_date = end_date
        self.bars = bars
        self.workers = self.link_streams_and_feeds(universe.assets)
    
    def get_data_feeds(self):
        raise NotImplementedError('You must implement this in the asset specific factories')

    def link_streams_and_feeds(self, assets):
        workers = {}

        data_feeds = self.get_data_feeds()
        for identifier in self.identifiers:
            for bar in self.bars:
                if identifier not in workers:
                    workers[identifier] = WorkerGroup(n_bars = len(self.bars))
                workers[identifier].append(
                        Worker(
                            self.calendar, 
                            getattr(assets[identifier], f'{bar}_stream'), 
                            data_feeds[bar][identifier].copy(), bar = bar
                            )
                        )

        del data_feeds
        return workers
    
    def check_workers(self, identifiers = None, bar = None):
        if identifiers is not None:
            for identifier in identifiers:
                self.workers[identifier].update(bar)
        else:
            for w in self.workers.values():
                w.update(bar)

        if self.universe.has_spreads:
            for s in self.universe.spreads.values():
                s.update()
