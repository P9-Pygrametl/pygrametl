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
from csv import DictReader

import pygrametl
from pygrametl import ConnectionWrapper, tables
from pygrametl.nogil import Pickle

if sys.platform.startswith("java"):
    # Jython specific code
    from pygrametl.jythonmultiprocessing import Process as Worker, Queue
else:
    from freethreading import Queue, Worker
    import sqlite3  # Only used by SQLTransformingSource

try:
    from Queue import Empty  # Python 2
except ImportError:
    from queue import Empty  # Python 3


__all__ = [
    "BackgroundSource",
    "CSVSource",
    "CrossTabbingSource",
    "DynamicForEachSource",
    "FilteringSource",
    "HashJoiningSource",
    "JoiningSource",
    "MappingSource",
    "MergeJoiningSource",
    "PandasSource",
    "ProcessSource",
    "RoundRobinSource",
    "SQLSource",
    "SQLTransformingSource",
    "TransformingSource",
    "TypedCSVSource",
    "UnionSource",
    "UnpivotingSource",
]


def _reconstruct_csv_source(cls, state):
    """Reconstruct CSVSource or TypedCSVSource from pickled state."""
    filename = state.pop("filename")
    encoding = state.pop("encoding")
    offset = state.pop("offset")
    fieldnames = state.pop("saved_fieldnames")

    if not (filename and os.path.exists(filename)):
        raise ValueError(
            "Cannot unpickle CSVSource: file %r is not accessible"
            % filename
        )

    # Instantiate empty class instance without calling __init__
    obj = cls.__new__(cls)
    obj.__dict__.update(state)

    file_obj = open(filename, "r", encoding=encoding)
    obj._init_reader(file_obj, fieldnames, offset)
    return obj


class CSVSource(object):
    """A class for iterating a CSV file as dictionaries with pickling
    support."""

    def __init__(self, f, fieldnames=None, restkey=None, restval=None,
                 dialect="excel", *args, **kwds):
        """Arguments:

- f: An iterable object, file handle, or file path string/Path.
- fieldnames, restkey, restval, dialect, *args, **kwds:
          Passed on to csv.DictReader
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
        if file_obj is None and isinstance(self.f_arg, (str, Path)):
            file_obj = open(self.f_arg, "r")
        self.file_obj = file_obj if file_obj is not None else self.f_arg

        if offset is not None and hasattr(self.file_obj, "seek"):
            try:
                self.file_obj.seek(offset)
            except (OSError, ValueError):
                pass

        self.reader = DictReader(
            self.file_obj,
            fieldnames=fieldnames or self.fieldnames_arg,
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

    next = __next__  # Python 2 / Jython compatibility

    def __reduce__(self):
        state = self.__dict__.copy()
        del state["file_obj"], state["reader"]

        # f_arg may be an unpicklable file handle
        if not isinstance(state["f_arg"], (str, Path)):
            state["f_arg"] = None

        f = self.file_obj
        state["filename"] = getattr(f, "name", None) or (
            str(self.f_arg) if isinstance(self.f_arg, (str, Path)) else None
        )
        state["encoding"] = getattr(f, "encoding", None)
        state["offset"] = f.tell() if hasattr(f, "tell") else None
        state["content"] = f.getvalue() if hasattr(f, "getvalue") else None
        state["saved_fieldnames"] = (
            getattr(self.reader, "_fieldnames", None) or self.fieldnames_arg
        )
        return (_reconstruct_csv_source, (self.__class__, state))


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

    def __reduce__(self):
        # Obtain reduction tuple from base class which constructs _reconstruct_csv_source
        reconstruct_func, (cls, state) = CSVSource.__reduce__(self)
        state["_casts"] = self._casts
        return (reconstruct_func, (self.__class__, state))


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
        """Arguments:

        - connection: the PEP 249 connection to use. NOT a
          ConnectionWrapper!
        - query: the query that generates the result
        - names: names of attributes in the result. If not set,
          the names from the database are used. Default: ()
        - initsql: SQL that is executed before the query. The result of this
          initsql is not returned. Default: None.
        - cursorarg: if not None, this argument is used as an argument when
          the connection's cursor method is called. Default: None.
        - parameters: if not None, this sequence or mapping of parameters
          will be sent when the query is executed.
        - fetchsize: The amount of rows to fetch into memory for each round trip to the source.
          All rows will be fetched at once if fetchsize is set to 0 or less.
        """
        self.connection = connection
        if cursorarg is not None:
            self.cursor = connection.cursor(cursorarg)
        else:
            self.cursor = connection.cursor()
        if initsql:
            self.cursor.execute(initsql)
        self.query = query
        self.names = names
        self.executed = False
        self.parameters = parameters
        self.fetchsize = fetchsize

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
                    # We do this to support cursor objects that only have
                    # a meaningful .description after data has been fetched.
                    # This is, for example, the case when using a named
                    # psycopg2 cursor.
                    names = [t[0] for t in self.cursor.description]
                if len(names) != len(data[0]):
                    raise ValueError(
                        "Incorrect number of names provided. "
                        + "%d given, %d needed." % (len(names), len(data[0]))
                    )
                for row in data:
                    yield dict(zip(names, row))

                # It is not well defined in PEP 249 what fetchall will do if called twice
                # Therefore it is safest to break the loop if fetchall is used
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
        # Using the single-underscore method name prevents serialization / mangling failures
        p = Worker(target=self._worker)
        p.name = "Process for ProcessSource"
        p.start()

    def __reduce__(self):
        state = {
            "_source": self.__source,
            "_batchsize": self.__batchsize,
            "_queuesize": self.__queuesize,
            "_queue": self.__queue,
        }
        return (Pickle.reconstruct, (self.__class__, state))

    def _worker(self):
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
        """Arguments:

        - src1: a data source
        - key1: the attribute to use from src1
        - src2: a data source
        - key2: the attribute to use from src2
        """
        self.__src1 = src1
        self.__key1 = key1
        self.__src2 = src2
        self.__key2 = key2
        self.__next = None

    def __iter__(self):
        iter1 = self.__src1.__iter__()
        iter2 = self.__src2.__iter__()

        row1 = next(iter1)
        keyval1 = row1[self.__key1]
        rows2 = self.__getnextrows(iter2)
        keyval2 = rows2[0][self.__key2]

        try:
            while True:  # At one point there will be a StopIteration
                if keyval1 == keyval2:
                    # Output rows
                    for part in rows2:
                        resrow = row1.copy()
                        resrow.update(part)
                        yield resrow
                    row1 = next(iter1)
                    keyval1 = row1[self.__key1]
                elif keyval1 < keyval2:
                    row1 = next(iter1)
                    keyval1 = row1[self.__key1]
                else:  # k1 > k2
                    rows2 = self.__getnextrows(iter2)
                    keyval2 = rows2[0][self.__key2]
        except StopIteration:
            return  # Needed in Python 3.7+ due to PEP 479

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
                keyval = row[self.__key2]  # for the first row in this round
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
        """Arguments:

        - source: a data source
        - *transformations: the transformations to apply. Must be callables
          of the form func(row) where row is a dict. Will be applied in the
          given order.
        """
        self.__source = source
        self.__transformations = transformations

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
            + ", ".join([tables._quote(key) for key in row])
            + ") VALUES ("
            + ", ".join(["%%(%s)s" % (att,) for att in row])
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