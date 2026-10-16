from contextlib import contextmanager
from freethreading import RLock, current_worker as current_thread
import psycopg
from pygrametl import ConnectionWrapper

__all__ = ["Value", "PicklableConnectionWrapper", "PicklableSQLSource"]

class Value:
    """Drop-in threading replacement for multiprocessing.Value."""
    def __init__(self, typecode: str, value=0):
        self.typecode = typecode
        self._value = value
        self._lock = RLock()

    def get_lock(self):
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
    """
    Wraps a psycopg3 connection to utilize pipeline mode for batching write operations,
    while supporting clean serialization (pickling) and restoration.
    """

    def __init__(
        self,
        connection,
        sync_threshold=10000,
        stmtcachesize=1000,
        paramstyle=None,
        copyintonew=False,
    ):
        if not hasattr(connection, "pipeline"):
            raise TypeError(
                "PicklableConnectionWrapper needs a psycopg 3 connection "
                "(pipeline mode, libpq >= 14); got %r" % type(connection)
            )

        self._sync_threshold = sync_threshold
        self._stmtcachesize = stmtcachesize
        self._paramstyle = paramstyle
        self._copyintonew = copyintonew

        # Save connection details as a serializable dict immediately
        self._conn_params = connection.info.get_parameters()

        # Identify the creator thread (the ETL pipeline main thread)
        self._creator_thread_id = current_thread().ident

        # Initialize internal variables
        self._pending = 0
        self._is_suspended = False
        self._closed = False

        # Open the raw connection
        self._raw_conn = psycopg.connect(**self._conn_params)
        self._raw_conn.autocommit = False

        # Start the pipeline context manager
        self._pipeline_context = self._raw_conn.pipeline()
        self._pipeline = self._pipeline_context.__enter__()

        # Now ConnectionWrapper.__init__ runs with pipeline active
        super().__init__(
            self._raw_conn,
            stmtcachesize=stmtcachesize,
            paramstyle=paramstyle,
            copyintonew=copyintonew,
        )
        
        # Flush any metadata-probing statements that occurred during __init__
        self._sync()

    def _is_main_etl_thread(self):
        # Fall back to synchronous execution if called from a shared connection background worker thread
        return current_thread().ident == self._creator_thread_id

    def _connect(self):
        connection = psycopg.connect(**self._conn_params)
        connection.autocommit = False
        return connection

    def _enter_pipeline(self):
        if not self._is_main_etl_thread():
            return
        self._pipeline_context = self._raw_conn.pipeline()
        self._pipeline = self._pipeline_context.__enter__()
        self._is_suspended = False
        self._pending = 0

    def _sync(self):
        """Send everything queued and receive the results."""
        if not self._is_main_etl_thread() or self._is_suspended or self._closed or self._pending == 0:
            return
        pending, self._pending = self._pending, 0
        try:
            self._pipeline.sync()
        except Exception as error:
            if hasattr(error, "add_note"):
                error.add_note(
                    "Raised while synchronizing a pipeline holding %d queued "
                    "statement(s); the failing statement is one of them, not "
                    "necessarily the most recent call." % pending
                )
            raise

    def suspend_pipeline(self):
        """Leave pipeline mode (after syncing); needed before COPY."""
        if self._is_main_etl_thread() and not self._is_suspended and not self._closed:
            self._sync()
            self._is_suspended = True
            self._pipeline_context.__exit__(None, None, None)

    def resume_pipeline(self):
        """Re-enter pipeline mode after suspend_pipeline()."""
        if self._is_main_etl_thread() and self._is_suspended and not self._closed:
            self._enter_pipeline()

    @contextmanager
    def suspended_pipeline(self):
        """`with connection.suspended_pipeline(): ...COPY...`"""
        self.suspend_pipeline()
        try:
            yield
        finally:
            self.resume_pipeline()

    def execute(self, stmt, arguments=None, namemapping=None, translate=True):
        super().execute(stmt, arguments, namemapping, translate)
        if not self._is_main_etl_thread() or self._is_suspended:
            return
        self._pending += 1
        
        normalized_stmt = stmt.lstrip().upper()
        is_config = (
            normalized_stmt.startswith("SET") or 
            normalized_stmt.startswith("SHOW") or 
            normalized_stmt.startswith("ALTER")
        )
        
        if is_config or self._pending >= self._sync_threshold:
            self._sync()

    def executemany(self, stmt, params, translate=True):
        super().executemany(stmt, params, translate)
        if not self._is_main_etl_thread() or self._is_suspended:
            return
        self._pending += 1
        if self._pending >= self._sync_threshold:
            self._sync()

    def fetchone(self, *args, **kwargs):
        self._sync()
        return super().fetchone(*args, **kwargs)

    def fetchonetuple(self):
        self._sync()
        return super().fetchonetuple()

    def fetchmanytuples(self, cnt):
        self._sync()
        return super().fetchmanytuples(cnt)

    def fetchalltuples(self):
        self._sync()
        return super().fetchalltuples()

    def rowcount(self):
        self._sync()
        return super().rowcount()

    def resultnames(self):
        self._sync()
        return super().resultnames()

    def rowfactory(self, *args, **kwargs):
        self._sync()
        return super().rowfactory(*args, **kwargs)

    def commit(self):
        self._sync()
        super().commit()

    def rollback(self):
        try:
            self._sync()
        except Exception:
            pass
        super().rollback()

    def close(self):
        try:
            if self._is_main_etl_thread() and not self._is_suspended and not self._closed:
                self._closed = True
                self._pipeline_context.__exit__(None, None, None)
        finally:
            self._closed = True
            super().close()

    def __getstate__(self):
        if self._is_main_etl_thread() and not self._is_suspended and not self._closed:
            try:
                self._sync()
                self._pipeline_context.__exit__(None, None, None)
            except Exception:
                pass

        return {
            "_sync_threshold": self._sync_threshold,
            "_stmtcachesize": self._stmtcachesize,
            "_paramstyle": self._paramstyle,
            "_copyintonew": self._copyintonew,
            "_conn_params": self._conn_params,
        }

    def __setstate__(self, state):
        self._sync_threshold = state["_sync_threshold"]
        self._stmtcachesize = state["_stmtcachesize"]
        self._paramstyle = state["_paramstyle"]
        self._copyintonew = state["_copyintonew"]
        self._conn_params = state["_conn_params"]

        # Register the new active thread as the creator
        self._creator_thread_id = current_thread().ident
        self._pending = 0
        self._is_suspended = False
        self._closed = False

        self._raw_conn = psycopg.connect(**self._conn_params)
        self._raw_conn.autocommit = False

        self._pipeline_context = self._raw_conn.pipeline()
        self._pipeline = self._pipeline_context.__enter__()

        ConnectionWrapper.__init__(
            self,
            self._raw_conn,
            stmtcachesize=self._stmtcachesize,
            paramstyle=self._paramstyle,
            copyintonew=self._copyintonew,
        )

        self._sync()


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
        return {
            "_args": self._args,
            "_kwargs": self._kwargs,
        }

    def __setstate__(self, state):
        from pygrametl.datasources import SQLSource
        self._args = state["_args"]
        self._kwargs = state["_kwargs"]
        self._source = SQLSource(*self._args, **self._kwargs)

    def __iter__(self):
        return self._source.__iter__()

    def __next__(self):
        return self._source.__next__()

    def __getattr__(self, name):
        return getattr(self._source, name)