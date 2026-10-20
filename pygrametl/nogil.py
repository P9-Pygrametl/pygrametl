from freethreading import RLock
import psycopg
from pygrametl import ConnectionWrapper

__all__ = ["Value", "PicklableConnectionWrapper", "PicklableSQLSource"]

class Pickle:
    def reconstruct(cls, state):
        """Reconstruct from pickled state."""
        obj = cls.__new__(cls)
        obj.__dict__.update(state)
        return obj

class Value:
    """Drop-in threading replacement for multiprocessing.Value."""
    def __init__(self, typecode: str, value=0):
        self.typecode = typecode  # Retained for API compatibility
        self._value = value
        self._lock = RLock()

    def get_lock(self):
        """Returns the underlying lock, matching multiprocessing.Value.get_lock()."""
        return self._lock

    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, val):
        with self._lock:
            self._value = val


class PicklableConnectionWrapper(ConnectionWrapper):
    """A ConnectionWrapper that can be pickled and recreated later."""

    def __init__(
        self,
        connection,
        stmtcachesize=1000,
        paramstyle=None,
        copyintonew=False,
    ):
        self._stmtcachesize = stmtcachesize
        self._paramstyle = paramstyle
        self._copyintonew = copyintonew
        self._conn_params = connection.info.get_parameters()
        
        super().__init__(
            self._connect(),
            stmtcachesize=stmtcachesize,
            paramstyle=paramstyle,
            copyintonew=copyintonew,
        )

    def _connect(self):
        connection = psycopg.connect(**self._conn_params)
        connection.autocommit = False
        return connection

    def __reduce__(self):
        return (
            self.__class__,
            (
                self._conn_params,  
                self._stmtcachesize,
                self._paramstyle,
                self._copyintonew,
            ),
        )
class PicklableSQLSource(object):
    """An SQLSource wrapper that delegates to SQLSource and safely handles
    pickling/unpickling by discarding active cursor references.
    """
    def __init__(self, *args, **kwargs):
        from pygrametl.datasources import SQLSource
        self._args = args
        self._kwargs = kwargs
        self._source = SQLSource(*args, **kwargs)

    def __reduce__(self):
        # Return the class and the arguments needed to recreate it from scratch.
        # This naturally discards active cursor references.
        return (
            self.__class__,
            self._args,
            self._kwargs,
        )

    def __iter__(self):
        return self._source.__iter__()

    def __next__(self):
        return self._source.__next__()

    def __getattr__(self, name):
        # Delegate any other method/attribute access directly to the SQLSource instance
        return getattr(self._source, name)