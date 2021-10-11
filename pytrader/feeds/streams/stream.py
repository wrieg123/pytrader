import numpy as np
import math


class Stream:

    def __init__(self, cache = 25000, NULL_VAL = None):
        self._cache = cache
        self.NULL_VAL = NULL_VAL
        self._stream = np.empty([cache])
        self._pointer = 0
    

    def __getitem__(self, val):
        if self.pointer == 0:
            return self.NULL_VAL 
        return self._stream[:self.pointer][val]

    @property
    def pointer(self):
        return self._pointer
    
    @property
    def last(self):
        if self.pointer == 0:
            return self.NULL_VAL 
        return self._stream[:self.pointer][-1]

    def _push(self, x):
        if self._stream.size == self._pointer:
            self._stream = np.concatenate([self._stream, np.empty([self._stream.size * 2])])
        self._stream[self.pointer] = x
        self._pointer += 1

    def ffill(self):
        val = self.last
        if val:
            self._push(val)

    def push(self, x):
        self._push(x)
    
