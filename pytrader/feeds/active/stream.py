import numpy as np
import math


class Stream:

    def __init__(self, cache = 25000):
        self._stream = np.empty([cache])
        self._pointer = 0
    
    @property
    def ts(self):
        if self.pointer > 0:
            return self._stream[:(self.period)]
        else:
            return np.array([])
    
    @property
    def v(self):
        if self.pointer > 0:
            return self._stream[self.pointer-1]
        else:
            return None
    
    @property
    def pointer(self):
        return self._pointer
    
    def push(self, x):
        if str(x) != 'nan' and not x is None:
            if self._stream.size == self._pointer:
                self._stream = np.concatenate([self._stream, np.empty([self._stream.size * 2])])
            self._stream[self.pointer] = x
            self._pointer += 1
        else:
            pass
