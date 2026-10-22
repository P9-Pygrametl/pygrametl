from pygrametl import asyncawaittables
import psycopg
import pygrametl


class AsyncConnectionWrapper:
    """Async counterpart of pygrametl's ConnectionWrapper, for psycopg 3.

    Each query method does execute + fetch on its own cursor, so results are
    never stored on the connection and two coroutines cannot read each
    other's rows.
    """

    def __init__(self, connection):
        self._conn = connection
        self.nametranslator = lambda s: s

    @classmethod
    async def connect(cls, *args, **kwargs):
        return cls(await psycopg.AsyncConnection.connect(*args, **kwargs))

    def _args(self, arguments, namemapping):
        if namemapping and arguments:
            return pygrametl.copy(arguments, **namemapping)
        return arguments

    async def execute(self, stmt, arguments=None, namemapping=None):
        async with self._conn.cursor() as cur:
            await cur.execute(stmt, self._args(arguments, namemapping))

    async def executemany(self, stmt, params):
        async with self._conn.cursor() as cur:
            await cur.executemany(stmt, params)

    async def _query(self, stmt, arguments, namemapping, limit=None):
        async with self._conn.cursor() as cur:
            await cur.execute(stmt, self._args(arguments, namemapping))
            names = [d.name for d in cur.description]
            rows = await (cur.fetchall() if limit is None else cur.fetchmany(limit))
            return names, rows

    async def fetchonetuple(self, stmt, arguments=None, namemapping=None):
        names, rows = await self._query(stmt, arguments, namemapping, 1)
        return rows[0] if rows else (None,) * len(names)

    async def fetchone(self, stmt, arguments=None, namemapping=None, names=None):
        qnames, rows = await self._query(stmt, arguments, namemapping, 1)
        row = rows[0] if rows else (None,) * len(qnames)
        return dict(zip(names or qnames, row))

    async def fetchalltuples(self, stmt, arguments=None, namemapping=None):
        _, rows = await self._query(stmt, arguments, namemapping)
        return rows

    async def fetchmanytuples(self, stmt, count, arguments=None, namemapping=None):
        _, rows = await self._query(stmt, arguments, namemapping, count)
        return rows

    async def rowfactory(self, stmt, arguments=None, namemapping=None, names=None):
        qnames, rows = await self._query(stmt, arguments, namemapping)
        return [dict(zip(names or qnames, r)) for r in rows]

    async def commit(self):
        await asyncawaittables.endload_all(self)
        await self._conn.commit()

    async def rollback(self):
        asyncawaittables.discard_all(self)
        await self._conn.rollback()

    async def close(self):
        await self._conn.close()

    def getunderlyingmodule(self):
        return psycopg

    async def copyfrom(self, stmt, filehandle, chunksize=65536):
        async with self._conn.cursor() as cur:
            async with cur.copy(stmt) as copy:
                while chunk := filehandle.read(chunksize):
                    await copy.write(chunk)