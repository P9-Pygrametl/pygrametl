"""This module holds classes that can be used as data soures. Note that it is
easy to create other data sources: A data source must be iterable and
provide dicts that map from attribute names to attribute values.
"""

# Copyright (c) 2009-2023, Aalborg University (pygrametl@cs.aau.dk)
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# - Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.

# - Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from csv import DictReader
import os
from pathlib import Path
import sys

import pygrametl
from pygrametl import tables
from pygrametl import ConnectionWrapper


if sys.platform.startswith("java"):
    # Jython specific code
    from pygrametl.jythonmultiprocessing import Queue, Process as Worker
else:
    from freethreading import Queue, Worker
    import sqlite3  # Only used by SQLTransformingSource

try:
    from Queue import Empty  # Python 2
except ImportError:
    from queue import Empty  # Python 3


__all__ = [
    "CSVSource",
    "TypedCSVSource",
    "SQLSource",
    "PandasSource",
    "JoiningSource",
    "HashJoiningSource",
    "MergeJoiningSource",
    "BackgroundSource",
    "ProcessSource",
    "MappingSource",
    "TransformingSource",
    "SQLTransformingSource",
    "UnionSource",
    "CrossTabbingSource",
    "FilteringSource",
    "DynamicForEachSource",
    "RoundRobinSource",
    "UnpivotingSource",
]


class CSVSource(object):
    """A class for iterating a CSV file as dictionaries with pickling support."""

    def __init__(
        self,
        f,
        fieldnames=None,
        restkey=None,
        restval=None,
        dialect="excel",
        *args,
        **kwds
    ):
        """Arguments:

- f: An iterable object, file handle, or file path string/Path.
- fieldnames: Passed on to csv.DictReader
- restkey: Passed on to csv.DictReader
- restval: Passed on to csv.DictReader
- dialect: Passed on to csv.DictReader
- *args: Passed on to csv.DictReader
- **kwds: Passed on to csv.DictReader
        """
        self.f_arg = f
        self.fieldnames_arg = fieldnames
        self.restkey = restkey
        self.restval = restval
        self.dialect = dialect
        self.args = args
        self.kwds = kwds
        self._init_reader()

    def _init_reader(self, file_obj=None, fieldnames=None, offset=None):
        if file_obj is not None:
            self.file_obj = file_obj
            self.should_close = False
        elif isinstance(self.f_arg, (str, Path)):
            self.file_obj = open(self.f_arg, "r")
            self.should_close = True
        else:
            self.file_obj = self.f_arg
            self.should_close = False

        if offset is not None and hasattr(self.file_obj, "seek"):
            try:
                self.file_obj.seek(offset)
            except (OSError, ValueError):
                pass

        use_fieldnames = fieldnames if fieldnames is not None else self.fieldnames_arg

        self.reader = DictReader(
            self.file_obj,
            fieldnames=use_fieldnames,
            restkey=self.restkey,
            restval=self.restval,
            dialect=self.dialect,
            *self.args,
            **self.kwds
        )

    @property
    def fieldnames(self):
        return self.reader.fieldnames

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.reader)

    def next(self):  # For Python 2 / Jython compatibility
        return self.__next__()

    def __getstate__(self):
        filename = getattr(self.file_obj, "name", None)
        mode = getattr(self.file_obj, "mode", "r")
        encoding = getattr(self.file_obj, "encoding", None)
        offset = None

        if hasattr(self.file_obj, "tell"):
            try:
                offset = self.file_obj.tell()
            except (OSError, ValueError):
                offset = None

        string_content = None
        if not (filename and isinstance(filename, (str, Path)) and os.path.exists(filename)):
            if hasattr(self.file_obj, "getvalue"):
                try:
                    string_content = self.file_obj.getvalue()
                except Exception:
                    pass

        parsed_fieldnames = getattr(self.reader, "_fieldnames", None)
        if parsed_fieldnames is None:
            fieldnames_to_save = self.fieldnames_arg
        else:
            fieldnames_to_save = parsed_fieldnames

        return {
            "f_arg": self.f_arg if isinstance(self.f_arg, (str, Path)) else None,
            "filename": str(filename) if filename else None,
            "mode": mode,
            "encoding": encoding,
            "offset": offset,
            "string_content": string_content,
            "fieldnames_arg": self.fieldnames_arg,
            "fieldnames_to_save": fieldnames_to_save,
            "restkey": self.restkey,
            "restval": self.restval,
            "dialect": self.dialect,
            "args": self.args,
            "kwds": self.kwds,
        }

    def __setstate__(self, state):
        self.f_arg = state["f_arg"]
        self.fieldnames_arg = state["fieldnames_arg"]
        self.restkey = state["restkey"]
        self.restval = state["restval"]
        self.dialect = state["dialect"]
        self.args = state.get("args", ())
        self.kwds = state.get("kwds", {})

        filename = state.get("filename")
        mode = state.get("mode", "r")
        encoding = state.get("encoding")
        offset = state.get("offset")
        string_content = state.get("string_content")
        fieldnames_to_save = state.get("fieldnames_to_save")

        file_obj = None
        if filename and os.path.exists(filename):
            open_kwargs = {}
            if encoding:
                open_kwargs["encoding"] = encoding
            clean_mode = mode.replace("b", "") if isinstance(mode, str) else "r"
            file_obj = open(filename, clean_mode, **open_kwargs)
        elif string_content is not None:
            import io
            file_obj = io.StringIO(string_content)
        elif self.f_arg and os.path.exists(self.f_arg):
            file_obj = open(self.f_arg, "r")
        else:
            raise ValueError(
                f"Cannot unpickle CSVSource: file source {filename or self.f_arg!r} is not accessible."
            )

        self._init_reader(
            file_obj=file_obj,
            fieldnames=fieldnames_to_save,
            offset=offset,
        )


class TypedCSVSource(CSVSource):
    """A class for iterating a CSV file and type cast the values."""

    def __init__(
        self,
        f,
        casts,
        fieldnames=None,
        restkey=None,
        restval=None,
        dialect="excel",
        *args,
        **kwds
    ):
        if type(casts) is not dict:
            raise TypeError("The casts argument must be a dict")
        for v in casts.values():
            if not callable(v):
                raise TypeError("The values in casts must be callable")
        self._casts = casts
        CSVSource.__init__(
            self,
            f,
            fieldnames=fieldnames,
            restkey=restkey,
            restval=restval,
            dialect=dialect,
            *args,
            **kwds
        )

    def __next__(self):
        row = CSVSource.__next__(self)
        for att, func in self._casts.items():
            row[att] = func(row[att])
        return row

    def next(self):
        return self.__next__()

    def __getstate__(self):
        state = CSVSource.__getstate__(self)
        state["_casts"] = self._casts
        return state

    def __setstate__(self, state):
        self._casts = state["_casts"]
        CSVSource.__setstate__(self, state)


class SQLSource(object):
    """A class for iterating the result set of a single SQL query."""

    def __init__(
        self,
        connection,
        query,
        names=(),
        initsql=None,
        cursorarg=None,
        parameters=None,
        fetchsize=500,
    ):
        self.connection = connection
        self.query = query
        self.names = names
        self.initsql = initsql
        self.cursorarg = cursorarg
        self.parameters = parameters
        self.fetchsize = fetchsize
        self.executed = False
        self.cursor = None
        self._ensure_cursor()

    def _ensure_cursor(self):
        if self.cursor is None:
            if self.cursorarg is not None:
                self.cursor = self.connection.cursor(self.cursorarg)
            else:
                self.cursor = self.connection.cursor()
            if self.initsql and not self.executed:
                self.cursor.execute(self.initsql)

    def __getstate__(self):
        return {
            "connection": self.connection,
            "query": self.query,
            "names": self.names,
            "initsql": self.initsql,
            "cursorarg": self.cursorarg,
            "parameters": self.parameters,
            "fetchsize": self.fetchsize,
            "executed": self.executed,
        }

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.cursor = None
        self._ensure_cursor()

    def __iter__(self):
        try:
            if not self.executed:
                if self.parameters:
                    self.cursor.execute(self.query, self.parameters)
                else:
                    self.cursor.execute(self.query)
                names = None
                if self.names or self.cursor.description:
                    names = self.names or [t[0] for t in self.cursor.description]
            while True:
                if self.fetchsize <= 0:
                    data = self.cursor.fetchall()
                else:
                    data = self.cursor.fetchmany(self.fetchsize)

                if not data:
                    break
                if not names:
                    names = [t[0] for t in self.cursor.description]
                if len(names) != len(data[0]):
                    raise ValueError(
                        "Incorrect number of names provided. "
                        + "%d given, %d needed." % (len(names), len(data[0]))
                    )
                for row in data:
                    yield dict(zip(names, row))

                if self.fetchsize <= 0:
                    break
        finally:
            try:
                self.cursor.close()
            except Exception:
                pass


class PandasSource(object):
    """A source for iterating a Pandas DataFrame and cast each row to a dict."""

    def __init__(self, dataFrame):
        """Arguments:

- dataFrame: A Pandas DataFrame
        """
        self._dataFrame = dataFrame

    def __iter__(self):
        for _, series in self._dataFrame.iterrows():
            row = series.to_dict()
            yield row


class ProcessSource(object):
    """A class for iterating another source in a separate process"""

    def __init__(self, source, batchsize=500, queuesize=20):
        if not isinstance(batchsize, int) or batchsize < 1:
            raise ValueError("batchsize must be a positive integer")
        self.__source = source
        self.__batchsize = batchsize
        self.__queuesize = queuesize
        self.__queue = Queue(queuesize)
        p = Worker(target=self.__worker)
        p.name = "Process for ProcessSource"
        p.start()

    def __getstate__(self):
        return {
            "_source": self.__source,
            "_batchsize": self.__batchsize,
            "_queuesize": self.__queuesize,
            "_queue": self.__queue,
        }

    def __setstate__(self, state):
        self.__source = state["_source"]
        self.__batchsize = state["_batchsize"]
        self.__queuesize = state["_queuesize"]
        self.__queue = state["_queue"]

    def __worker(self):
        batch = []
        try:
            for row in self.__source:
                batch.append(row)
                if len(batch) == self.__batchsize:
                    self.__queue.put(batch)
                    batch = []
            if batch:
                self.__queue.put(batch)
            self.__queue.put("STOP")
        except Exception:
            e = sys.exc_info()[1]
            if batch:
                self.__queue.put(batch)
            self.__queue.put("EXCEPTION")
            self.__queue.put(e)

    def __iter__(self):
        while True:
            data = self.__queue.get()
            if data == "STOP":
                break
            elif data == "EXCEPTION":
                exc = self.__queue.get()
                raise exc
            for row in data:
                yield row

BackgroundSource = ProcessSource  # for compatability


class HashJoiningSource(object):
    """A class for equi-joining two data sources."""

    def __init__(self, src1, key1, src2, key2):
        """Arguments:

- src1: the first source. This source is iterated row by row.
- key1: the attribute of the first source to use in the join
- src2: the second source. The rows of this source are all loaded
          into memory.
- key2: the attriubte of the second source to use in the join.
        """
        self.__hash = {}
        self.__src1 = src1
        self.__key1 = key1
        self.__src2 = src2
        self.__key2 = key2

    def __buildhash(self):
        for row in self.__src2:
            keyval = row[self.__key2]
            rows = self.__hash.get(keyval, [])
            rows.append(row)
            self.__hash[keyval] = rows
        self.__ready = True

    def __iter__(self):
        self.__buildhash()
        for row in self.__src1:
            matches = self.__hash.get(row[self.__key1], [])
            for match in matches:
                newrow = row.copy()
                newrow.update(match)
                yield newrow


JoiningSource = HashJoiningSource  # for compatability


class MergeJoiningSource(object):
    """A class for merge-joining two sorted data sources"""

    def __init__(self, src1, key1, src2, key2):
        self.__src1 = src1
        self.__key1 = key1
        self.__src2 = src2
        self.__key2 = key2
        self.__next = None

    def __getstate__(self):
        return {
            "_src1": self.__src1,
            "_key1": self.__key1,
            "_src2": self.__src2,
            "_key2": self.__key2,
            "_next": self.__next,
        }

    def __setstate__(self, state):
        self.__src1 = state["_src1"]
        self.__key1 = state["_key1"]
        self.__src2 = state["_src2"]
        self.__key2 = state["_key2"]
        self.__next = state["_next"]

    def __iter__(self):
        iter1 = self.__src1.__iter__()
        iter2 = self.__src2.__iter__()

        row1 = next(iter1)
        keyval1 = row1[self.__key1]
        rows2 = self.__getnextrows(iter2)
        keyval2 = rows2[0][self.__key2]

        try:
            while True:
                if keyval1 == keyval2:
                    for part in rows2:
                        resrow = row1.copy()
                        resrow.update(part)
                        yield resrow
                    row1 = next(iter1)
                    keyval1 = row1[self.__key1]
                elif keyval1 < keyval2:
                    row1 = next(iter1)
                    keyval1 = row1[self.__key1]
                else:
                    rows2 = self.__getnextrows(iter2)
                    keyval2 = rows2[0][self.__key2]
        except StopIteration:
            return

    def __getnextrows(self, iterval):
        res = []
        keyval = None
        if self.__next is not None:
            res.append(self.__next)
            keyval = self.__next[self.__key2]
            self.__next = None
        while True:
            try:
                row = next(iterval)
            except StopIteration:
                if res:
                    return res
                else:
                    raise
            if keyval is None:
                keyval = row[self.__key2]
            if row[self.__key2] == keyval:
                res.append(row)
            else:
                self.__next = row
                return res


class MappingSource(object):
    """A class for iterating a source and applying a function to each column."""

    def __init__(self, source, callables):
        """Arguments:

- source: A data source
- callables: A dict mapping from attribute names to functions to
          apply to these names, e.g. type casting {'id':int, 'salary':float}
        """
        if type(callables) is not dict:
            raise TypeError("The callables argument must be a dict")
        for v in callables.values():
            if not callable(v):
                raise TypeError("The values in callables must be callable")

        self._source = source
        self._callables = callables

    def __iter__(self):
        for row in self._source:
            for att, func in self._callables.items():
                row[att] = func(row[att])
            yield row


class TransformingSource(object):
    """A source that applies functions to the rows from another source"""

    def __init__(self, source, *transformations):
        self.__source = source
        self.__transformations = transformations

    def __getstate__(self):
        return {
            "_source": self.__source,
            "_transformations": self.__transformations,
        }

    def __setstate__(self, state):
        self.__source = state["_source"]
        self.__transformations = state["_transformations"]

    def __iter__(self):
        for row in self.__source:
            for func in self.__transformations:
                func(row)
            yield row


class SQLTransformingSource(object):
    """A source that transforms rows from another source by loading them into a
    temporary table in an RDBMS and then retrieving them using an SQL query.

    .. Warning::
       Creates, empties, and drops the temporary table.
    """

    def __init__(
        self,
        source,
        temptablename,
        query,
        additionalcasts=None,
        batchsize=10000,
        perbatch=False,
        columnnames=None,
        usetruncate=True,
        targetconnection=None,
    ):
        self.__source = source
        self.__query = query
        self.__batchsize = batchsize
        self.__perbatch = perbatch
        self.__columnnames = columnnames

        self.__casts = {
            int: "INTEGER",
            float: "DOUBLE PRECISION",
            str: "VARCHAR(4000)",
        }

        if additionalcasts:
            self.__casts |= additionalcasts

        if targetconnection is None:
            self.__close = True
            targetconnection = sqlite3.connect(":memory:")
            usetruncate = False
        else:
            self.__close = False

        if "ConnectionWrapper" in type(targetconnection).__name__:
            self.__targetconnection = targetconnection
        else:
            self.__targetconnection = ConnectionWrapper(targetconnection)

            if self.__targetconnection == pygrametl.getdefaulttargetconnection():
                pygrametl._defaulttargetconnection = None

        self.__batch = []
        row = next(source)
        self.__batch.append(row)

        createsql = "CREATE TABLE {}({})".format(
            temptablename,
            ", ".join(
                [name + " " + self.__casts[type(value)] for name, value in row.items()]
            ),
        )
        self.__targetconnection.execute(createsql)
        self.__targetconnection.commit()

        self.__insertsql = (
            "INSERT INTO "
            + temptablename
            + "("
            + ", ".join([tables._quote(key) for key in row.keys()])
            + ") VALUES ("
            + ", ".join(["%%(%s)s" % (att,) for att in row.keys()])
            + ")"
        )

        self.__dropsql = "DROP TABLE " + temptablename
        if usetruncate:
            self.__deletefrom = "TRUNCATE TABLE " + temptablename
        else:
            self.__deletefrom = "DELETE FROM " + temptablename

    def __iter__(self):
        for row in self.__source:
            if len(self.__batch) == self.__batchsize:
                self.__insertnow()
                if self.__perbatch:
                    for transformed_row in self.__transform():
                        yield transformed_row
                    self.__targetconnection.execute(self.__deletefrom)

            self.__batch.append(row)

        self.__insertnow()
        for transformed_row in self.__transform():
            yield transformed_row

        self.__targetconnection.execute(self.__dropsql)
        self.__targetconnection.commit()
        if self.__close:
            self.__targetconnection.close()

    def __insertnow(self):
        if self.__batch:
            self.__targetconnection.executemany(self.__insertsql, self.__batch)
            self.__targetconnection.commit()
            self.__batch.clear()

    def __transform(self):
        self.__targetconnection.execute(self.__query)

        if self.__columnnames:
            return self.__targetconnection.rowfactory(self.__columnnames)
        else:
            return self.__targetconnection.rowfactory()


class CrossTabbingSource(object):
    """A source that produces a crosstab from another source"""

    def __init__(
        self,
        source,
        rowvaluesatt,
        colvaluesatt,
        values,
        aggregator=None,
        nonevalue=0,
        sortrows=False,
    ):
        self.__source = source
        self.__rowvaluesatt = rowvaluesatt
        self.__colvaluesatt = colvaluesatt
        self.__values = values
        if aggregator is None:
            from pygrametl.aggregators import Sum

            self.__aggregator = Sum()
        else:
            self.__aggregator = aggregator
        self.__nonevalue = nonevalue
        self.__sortrows = sortrows
        self.__allcolumns = set()
        self.__allrows = set()

    def __iter__(self):
        for data in self.__source:
            row = data[self.__rowvaluesatt]
            col = data[self.__colvaluesatt]
            self.__allrows.add(row)
            self.__allcolumns.add(col)
            self.__aggregator.process((row, col), data[self.__values])

        for row in self.__sortrows and sorted(self.__allrows) or self.__allrows:
            res = {self.__rowvaluesatt: row}
            for col in self.__allcolumns:
                res[col] = self.__aggregator.finish((row, col), self.__nonevalue)
            yield res


class FilteringSource(object):
    """A source that applies a filter to another source"""

    def __init__(self, source, filter=bool):
        self.__source = source
        self.__filter = filter

    def __iter__(self):
        for row in self.__source:
            if self.__filter(row):
                yield row


class UnionSource(object):
    """A source to union other sources (possibly with different types of rows)."""

    def __init__(self, *sources):
        self.__sources = sources

    def __iter__(self):
        for src in self.__sources:
            for row in src:
                yield row


class RoundRobinSource(object):
    """A source that reads sets of rows from sources in round robin-fashion"""

    def __init__(self, sources, batchsize=500):
        self.__sources = [iter(src) for src in sources]
        self.__sources.reverse()
        if not batchsize > 0:
            raise ValueError("batchsize must be positive")
        self.__batchsize = batchsize

    def __iter__(self):
        while self.__sources:
            for i in range(len(self.__sources) - 1, -1, -1):
                cursrc = self.__sources[i]
                try:
                    for _ in range(self.__batchsize):
                        yield next(cursrc)
                except StopIteration:
                    del self.__sources[i]
        return


class DynamicForEachSource(object):
    """A source that for each given argument creates a new source that will be iterated."""

    def __init__(self, seq, callee):
        self.__queue = Queue()
        if not callable(callee):
            raise TypeError("callee must be callable")
        self.__callee = callee
        for e in seq:
            self.__queue.put(e)

    def __iter__(self):
        while True:
            try:
                arg = self.__queue.get(False)
                src = self.__callee(arg)
                for row in src:
                    yield row
            except Empty:
                return


class UnpivotingSource(object):
    """A source that transforms rows from wide to long format."""

    def __init__(
        self,
        source,
        keyatts=(),
        unpivotatts=None,
        nameatt="name",
        valueatt="value",
        ignorenone=False,
    ):
        self.__source = source

        if type(keyatts) in pygrametl._stringtypes:
            keyatts = (keyatts,)
        self.__keyatts = tuple(keyatts)

        if unpivotatts is not None and type(unpivotatts) in pygrametl._stringtypes:
            unpivotatts = (unpivotatts,)
        self.__unpivotatts = tuple(unpivotatts) if unpivotatts is not None else None

        self.__nameatt = nameatt
        self.__valueatt = valueatt
        self.__ignorenone = ignorenone

        if nameatt == valueatt:
            raise ValueError("nameatt and valueatt must be different")
        if nameatt in self.__keyatts or valueatt in self.__keyatts:
            raise ValueError("nameatt and valueatt must not be among keyatts")
        if self.__unpivotatts is not None:
            for att in self.__unpivotatts:
                if att in self.__keyatts:
                    raise ValueError(
                        "unpivotatts must not contain attributes also in keyatts"
                    )

    def __iter__(self):
        for row in self.__source:
            resbase = {}
            for att in self.__keyatts:
                resbase[att] = row[att]

            if self.__unpivotatts is None:
                atts = [att for att in row if att not in self.__keyatts]
            else:
                atts = self.__unpivotatts

            for att in atts:
                value = row[att]
                if self.__ignorenone and value is None:
                    continue
                res = resbase.copy()
                res[self.__nameatt] = att
                res[self.__valueatt] = value
                yield res