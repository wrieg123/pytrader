from .stream import Stream

TIME_FREQ_ATTRS = {
        'daily': ['tstamp', 'year', 'month', 'day'],
        'minute': ['tstamp', 'year', 'month', 'day', 'hour', 'minute'],
        }


class Indicator:

    def __init__(
            self,
            data, 
            freq = 'daily',
            normalizer = None,
            attributes = ['_indicator'],
            override = False
            ):
        self.data = data
        self.freq = freq
        self.normalizer = normalizer
        self.attributes = attributes
        self.override = override
        self.manager = None

        for t in TIME_FREQ_ATTRS[freq]:
            setattr(self, t, Stream())

        for i in attributes:
            setattr(self, i, Stream())
            if normalizer is not None:
                setattr(self, f'{i}_helper', Stream())
    
    @property
    def helper_v(self):
        if len(self.attributes) == 1:
            return getattr(self, self.attributes[0]+'_helper').v
        else:
            return {a: getattr(self, a+'_helper').v for a in self.attributes}

    @property
    def v(self):
        if len(self.attributes) == 1:
            return getattr(self, self.attributes[0]).v
        else:
            return {a: getattr(self, a).v for a in self.attributes}

    @property
    def ts(self):
        if len(self.attributes) == 1:
            return getattr(self, self.attributes[0]).ts
        else:
            return {a: getattr(self, a).ts for a in self.attributes}
    
    def _connect_manager(self, manager):
        self.manager = manager


    def _update_time_periods(self):
        # TODO: update the proper time streams
        pass

    def refresh(self):
        model_output = self.calculate()
        
        if len(self.attributes) == 1:
            model_output = {self.attributes[0] : model_output}
        
        for a, v in list(self.model_output.items()):
            if self.normalizer is None:
                getattr(self, a).push(v)
            else:
                getattr(self, a+'_helper').push(self.calculate())
                getattr(self, a).push(
                        self.normalizer.normalize(
                            getattr(self, a+'_helper').ts
                            )
                        )
        self._update_time_periods() 
