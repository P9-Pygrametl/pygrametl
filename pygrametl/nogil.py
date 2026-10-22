from threading import RLock


__all__ = ["Value"]

class Value:

    def __init__(self, typecode: str, value=0):
        self.typecode = typecode
        self.value = value
        self.lock = RLock()

    def get_lock(self):
        return self.lock

    @property
    def value(self):
        with self.lock:
            return self._value

    @value.setter
    def value(self, new_value):
        with self.lock:
            self._value = new_value

    