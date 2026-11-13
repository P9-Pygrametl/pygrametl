"""Unit tests for asynchronous SQL and merge joining sources.

Run from the pygrametl repository with:
    python3 -m unittest discover -s tests -p 'test_asyncdatasources.py' -v

No external database or third-party packages required. Python 3.10+ only.
"""

import asyncio
import unittest

from pygrametl.datasources import AsyncMergeJoiningSource, AsyncSQLSource


class FakeCursor:
    def __init__(self, rows, columns=("id", "value"), error_at_fetch=None):
        self.rows = list(rows)
        self.description = [(name,) for name in columns]
        self.error_at_fetch = error_at_fetch
        self.fetch_calls = 0
        self.fetchall_calls = 0
        self.executions = []
        self.closed = False

    async def execute(self, query, parameters=None):
        self.executions.append((query, parameters))

    async def fetchmany(self, size):
        await asyncio.sleep(0)  # Permit producer/consumer scheduling.
        self.fetch_calls += 1
        if self.error_at_fetch == self.fetch_calls:
            raise RuntimeError("simulated fetch failure")
        result, self.rows = self.rows[:size], self.rows[size:]
        return result

    async def fetchall(self):
        self.fetchall_calls += 1
        result, self.rows = self.rows, []
        return result

    async def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self.result_cursor = cursor
        self.cursor_arguments = []

    def cursor(self, *args):
        self.cursor_arguments.append(args)
        return self.result_cursor


async def rows_as_source(rows, closed=None):
    try:
        for row in rows:
            yield dict(row)
    finally:
        if closed is not None:
            closed.append(True)


class AsyncSQLSourceTest(unittest.IsolatedAsyncioTestCase):
    async def test_prefetch_fetches_every_row_in_order(self):
        cursor = FakeCursor([(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")])
        source = AsyncSQLSource(FakeConnection(cursor), "SELECT ...", fetchsize=2)
        result = [row async for row in source]
        self.assertEqual(
            result,
            [
                {"id": 1, "value": "a"},
                {"id": 2, "value": "b"},
                {"id": 3, "value": "c"},
                {"id": 4, "value": "d"},
                {"id": 5, "value": "e"},
            ],
        )
        self.assertEqual(cursor.fetch_calls, 4)  # Three batches + EOF.
        self.assertTrue(cursor.closed)

    async def test_fetches_ahead_while_consumer_is_paused(self):
        fetched_second_batch = asyncio.Event()

        class ObservedCursor(FakeCursor):
            async def fetchmany(self, size):
                result = await super().fetchmany(size)
                if self.fetch_calls == 2:
                    fetched_second_batch.set()
                return result

        cursor = ObservedCursor([(1, "a"), (2, "b"), (3, "c")])
        source = AsyncSQLSource(FakeConnection(cursor), "SELECT ...", fetchsize=1)
        iterator = aiter(source)

        try:
            first_row = await anext(iterator)
            self.assertEqual(first_row, {"id": 1, "value": "a"})

            # The consumer is not requesting another row, but the producer
            # must still fetch the next batch in the background.
            await asyncio.wait_for(fetched_second_batch.wait(), timeout=2)
        finally:
            await iterator.aclose()

        self.assertTrue(cursor.closed)

    async def test_fetchall_when_fetchsize_is_nonpositive(self):
        for size in (0, -1):
            with self.subTest(fetchsize=size):
                cursor = FakeCursor([(1, "a"), (2, "b")])
                source = AsyncSQLSource(
                    FakeConnection(cursor), "SELECT ...", fetchsize=size
                )
                self.assertEqual(len([r async for r in source]), 2)
                self.assertEqual(cursor.fetchall_calls, 1)
                self.assertEqual(cursor.fetch_calls, 0)
                self.assertTrue(cursor.closed)

    async def test_empty_result(self):
        cursor = FakeCursor([])
        result = [r async for r in AsyncSQLSource(FakeConnection(cursor), "SELECT ...")]
        self.assertEqual(result, [])
        self.assertTrue(cursor.closed)

    async def test_names_initialisation_and_parameters(self):
        cursor = FakeCursor([(10, "x")], columns=("db_id", "db_val"))
        conn = FakeConnection(cursor)
        source = AsyncSQLSource(
            conn,
            "SELECT ... WHERE id = %s",
            names=("key", "label"),
            initsql="SET search_path TO public",
            cursorarg="my_cursor",
            parameters=(10,),
            fetchsize=1,
        )
        self.assertEqual([r async for r in source], [{"key": 10, "label": "x"}])
        self.assertEqual(conn.cursor_arguments, [("my_cursor",)])
        self.assertEqual(
            cursor.executions,
            [
                ("SET search_path TO public", None),
                ("SELECT ... WHERE id = %s", (10,)),
            ],
        )

    async def test_wrong_column_count_raises(self):
        cursor = FakeCursor([(1, "a")])
        source = AsyncSQLSource(
            FakeConnection(cursor), "SELECT ...", names=("only_one",)
        )
        with self.assertRaisesRegex(ValueError, "Incorrect number of names"):
            [r async for r in source]
        self.assertTrue(cursor.closed)

    async def test_producer_error_propagates(self):
        cursor = FakeCursor([(1, "a"), (2, "b")], error_at_fetch=2)
        source = AsyncSQLSource(FakeConnection(cursor), "SELECT ...", fetchsize=1)
        with self.assertRaisesRegex(RuntimeError, "simulated fetch failure"):
            [r async for r in source]
        self.assertTrue(cursor.closed)

    async def test_early_close_releases_cursor(self):
        cursor = FakeCursor([(i, str(i)) for i in range(20)])
        stream = aiter(
            AsyncSQLSource(FakeConnection(cursor), "SELECT ...", fetchsize=2)
        )
        self.assertEqual((await anext(stream))["id"], 0)
        await asyncio.wait_for(stream.aclose(), timeout=2)
        self.assertTrue(cursor.closed)

    def test_prefetch_must_be_positive_integer(self):
        for value in (0, -1, 2.5, "2", None, True):
            with self.subTest(prefetch=value), self.assertRaises(ValueError):
                AsyncSQLSource(
                    FakeConnection(FakeCursor([])),
                    "SELECT ...",
                    prefetch=value,
                )


class AsyncMergeJoiningSourceTest(unittest.IsolatedAsyncioTestCase):
    async def join(self, left, right):
        source = AsyncMergeJoiningSource(
            rows_as_source(left), "left_id", rows_as_source(right), "right_id"
        )
        return [row async for row in source]

    async def test_one_to_one(self):
        actual = await self.join(
            [{"left_id": 1, "left": "A"}, {"left_id": 2, "left": "B"}],
            [{"right_id": 1, "right": "X"}, {"right_id": 2, "right": "Y"}],
        )
        self.assertEqual(
            [(r["left"], r["right"]) for r in actual], [("A", "X"), ("B", "Y")]
        )

    async def test_many_to_many_duplicate_keys(self):
        actual = await self.join(
            [{"left_id": 1, "left": "A"}, {"left_id": 1, "left": "B"}],
            [{"right_id": 1, "right": "X"}, {"right_id": 1, "right": "Y"}],
        )
        self.assertEqual(
            [(r["left"], r["right"]) for r in actual],
            [("A", "X"), ("A", "Y"), ("B", "X"), ("B", "Y")],
        )

    async def test_unmatched_keys_are_skipped(self):
        actual = await self.join(
            [
                {"left_id": 1, "left": "A"},
                {"left_id": 3, "left": "C"},
                {"left_id": 5, "left": "E"},
            ],
            [
                {"right_id": 2, "right": "B"},
                {"right_id": 3, "right": "D"},
                {"right_id": 4, "right": "F"},
            ],
        )
        self.assertEqual([(r["left"], r["right"]) for r in actual], [("C", "D")])

    async def test_either_source_empty(self):
        self.assertEqual(await self.join([], [{"right_id": 1}]), [])
        self.assertEqual(await self.join([{"left_id": 1}], []), [])

    async def test_join_early_close_closes_inputs(self):
        closed = []
        source = AsyncMergeJoiningSource(
            rows_as_source([{"left_id": 1}], closed),
            "left_id",
            rows_as_source([{"right_id": 1}], closed),
            "right_id",
        )
        iterator = aiter(source)
        await anext(iterator)
        await iterator.aclose()
        self.assertEqual(len(closed), 2)


if __name__ == "__main__":
    unittest.main()
