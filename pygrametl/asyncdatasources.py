"""Asynchronous SQL extraction and merge joining for pygrametl.

This module uses modern Python async syntax and is imported conditionally
from pygrametl.datasources to preserve compatibility with Jython.
"""

import asyncio


class AsyncSQLSource(object):
    """Asynchronously iterate the result set of a single SQL query.

    Requires an asynchronous database connection, such as
    psycopg.AsyncConnection.

    Prefetches batches using a background asyncio task.
    Rows are yielded as dictionaries, like those from SQLSource.
    """

    def __init__(
        self,
        connection,
        query,
        names=(),
        initsql=None,
        cursorarg=None,
        parameters=None,
        fetchsize=500,
        prefetch=2,
    ):
        """Arguments:

        - connection: An asynchronous database connection.
        - query: The SQL query that generates the result.
        - names: Optional attribute names for returned dictionaries.
        - initsql: Optional SQL executed before the main query.
        - cursorarg: Optional argument passed to connection.cursor().
        - parameters: Optional query parameters.
        - fetchsize: Number of rows to fetch per batch.
          If <= 0, all rows are fetched at once.
        - prefetch: Maximum number of completed batches buffered
          ahead of the consumer. Must be a positive integer.
        """
        if (
            not isinstance(prefetch, int)
            or isinstance(prefetch, bool)
            or prefetch < 1
        ):
            raise ValueError("prefetch must be a positive integer")

        self.connection = connection
        self.query = query
        self.names = names
        self.initsql = initsql
        self.cursorarg = cursorarg
        self.parameters = parameters
        self.fetchsize = fetchsize
        self.prefetch = prefetch

    async def __aiter__(self):
        if self.cursorarg is not None:
            cursor = self.connection.cursor(self.cursorarg)
        else:
            cursor = self.connection.cursor()

        producer_task = None

        try:
            if self.initsql:
                await cursor.execute(self.initsql)

            if self.parameters is not None:
                await cursor.execute(self.query, self.parameters)
            else:
                await cursor.execute(self.query)

            names = self.names or None

            if not names and cursor.description:
                names = [
                    column[0]
                    for column in cursor.description
                ]

            def convert_batch(data):
                nonlocal names

                if not names:
                    names = [
                        column[0]
                        for column in cursor.description
                    ]

                if len(names) != len(data[0]):
                    raise ValueError(
                        "Incorrect number of names provided. "
                        + "%d given, %d needed."
                        % (len(names), len(data[0]))
                    )

                for row in data:
                    yield dict(zip(names, row))

            if self.fetchsize <= 0:
                data = await cursor.fetchall()

                if data:
                    for row in convert_batch(data):
                        yield row

            else:
                queue = asyncio.Queue(maxsize=self.prefetch)

                async def producer():
                    try:
                        while True:
                            data = await cursor.fetchmany(
                                self.fetchsize
                            )

                            if not data:
                                break

                            await queue.put(("data", data))

                    except Exception as error:
                        await queue.put(("error", error))
                    else:
                        await queue.put(("done", None))

                producer_task = asyncio.create_task(producer())

                while True:
                    message, value = await queue.get()

                    if message == "done":
                        break

                    if message == "error":
                        raise value

                    for row in convert_batch(value):
                        yield row

        finally:
            if producer_task is not None:
                if not producer_task.done():
                    producer_task.cancel()

                try:
                    await producer_task
                except asyncio.CancelledError:
                    pass

            try:
                await cursor.close()
            except Exception:
                pass


class AsyncMergeJoiningSource(object):
    """A class for asynchronously merge-joining two sorted data sources.

    Both sources must support asynchronous iteration and must
    be sorted by their respective join keys.
    """

    def __init__(self, src1, key1, src2, key2):
        """Arguments:

        - src1: An asynchronous data source.
        - key1: The join attribute in src1.
        - src2: An asynchronous data source.
        - key2: The join attribute in src2.
        """
        self.__src1 = src1
        self.__key1 = key1
        self.__src2 = src2
        self.__key2 = key2

    async def __aiter__(self):
        iter1 = aiter(self.__src1)
        iter2 = aiter(self.__src2)

        try:
            row1 = await anext(iter1, None)

            if row1 is None:
                return

            keyval1 = row1[self.__key1]

            # The pending row is the first row belonging
            # to the next group in source 2.
            pending = None

            rows2, pending = await self.__getnextrows(
                iter2, pending
            )

            if not rows2:
                return

            keyval2 = rows2[0][self.__key2]

            while True:
                if keyval1 == keyval2:
                    # Produce joined rows.
                    for part in rows2:
                        resrow = row1.copy()
                        resrow.update(part)
                        yield resrow

                    row1 = await anext(iter1, None)

                    if row1 is None:
                        break

                    keyval1 = row1[self.__key1]

                elif keyval1 < keyval2:
                    row1 = await anext(iter1, None)

                    if row1 is None:
                        break

                    keyval1 = row1[self.__key1]

                else:
                    # Advance source 2 to its next group.
                    rows2, pending = await self.__getnextrows(
                        iter2, pending
                    )

                    if not rows2:
                        break

                    keyval2 = rows2[0][self.__key2]

        finally:
            # Close underlying asynchronous generators.
            for iterator in (iter1, iter2):
                close = getattr(iterator, "aclose", None)

                if close is not None:
                    await close()

    async def __getnextrows(self, iterator, pending):
        """Retrieve the next group of rows sharing the same key.

        Returns:
            (rows, pending)

        pending contains the first row of the following group,
        or None if there is no buffered row.
        """
        rows = []

        if pending is not None:
            first = pending
        else:
            first = await anext(iterator, None)

        if first is None:
            return [], None

        keyval = first[self.__key2]
        rows.append(first)

        while True:
            row = await anext(iterator, None)

            if row is None:
                return rows, None

            if row[self.__key2] == keyval:
                rows.append(row)
            else:
                return rows, row
