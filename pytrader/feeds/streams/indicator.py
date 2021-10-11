from .stream import Stream

import numpy as np

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
        self._freq = freq
        self.normalizer = normalizer
        self.attributes = attributes
        self.len_attrs = len(attributes)
        self.override = override

        for i in attributes:
            setattr(self, i, Stream())
            if normalizer is not None:
                setattr(self, f'{i}_helper', Stream())
    @property
    def frequency(self):
        return self._freq

    def __getitem__(self, val):
        if self.len_attrs == 1:
            return getattr(self, self.attributes[0])[val]
        out = {a : getattr(self, a)[val] for a in self.attributes}
        if self.normalizer is not None:
            out.update({f'{a}_helper' : getattr(self, f'{a}_helper')[val] for a in self.attributes})
        return {a : getattr(self, a)[val] for a in self.attributes}
    
    def calculate(self):
        raise NotImplementedError('you must implement a self.calculate value that returns a dict or int')
    
    def refresh(self):
        model_output = self.calculate()
        if model_output is None:
            return
        if self.len_attrs == 1:
            model_output = {self.attributes[0] : model_output}
        for a, v in list(model_output.items()):
            if self.normalizer is None:
                getattr(self, a).push(v)
            else:
                getattr(self, f'{a}_helper').push(v)
                getattr(self, a).push(self.normalizer.normalize(getattr(self, f'{a}_helper')[:]))
