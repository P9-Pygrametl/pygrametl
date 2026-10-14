from freethreading import RLock
import psycopg
from pygrametl import ConnectionWrapper

__all__ = ["Value", "PicklableConnectionWrapper", "PicklableSQLSource"]

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
        
        # Extract and store the serializable connection parameters immediately.
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

    def __getstate__(self):
        return {
            "_stmtcachesize": self._stmtcachesize,
            "_paramstyle": self._paramstyle,
            "_copyintonew": self._copyintonew,
            "_conn_params": self._conn_params,
        }

    def __setstate__(self, state):
        self._stmtcachesize = state["_stmtcachesize"]
        self._paramstyle = state["_paramstyle"]
        self._copyintonew = state["_copyintonew"]
        self._conn_params = state["_conn_params"]
        
        ConnectionWrapper.__init__(
            self,
            self._connect(),
            stmtcachesize=self._stmtcachesize,
            paramstyle=self._paramstyle,
            copyintonew=self._copyintonew,
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

    def __getstate__(self):
        # We save the arguments needed to reconstruct the source
        # instead of serializing the live SQLSource and its active cursor.
        return {
            "_args": self._args,
            "_kwargs": self._kwargs,
        }

    def __setstate__(self, state):
        from pygrametl.datasources import SQLSource
        self._args = state["_args"]
        self._kwargs = state["_kwargs"]
        
        # Re-instantiate the fresh SQLSource.
        # This triggers PicklableConnectionWrapper to reconnect automatically.
        self._source = SQLSource(*self._args, **self._kwargs)

    def __iter__(self):
        return self._source.__iter__()

    def __next__(self):
        return self._source.__next__()

    def __getattr__(self, name):
        # Delegate any other method/attribute access directly to the SQLSource instance
        return getattr(self._source, name)