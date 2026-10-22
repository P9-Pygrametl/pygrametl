"""Async/await version of pygrametl.tables.

Same classes and method names as pygrametl.tables, but every method that
touches the database is a coroutine and must be awaited.
"""

import asyncio
import inspect
import pygrametl
import locale
import os
import tempfile

from pygrametl.FIFODict import FIFODict
from operator import ge, gt, le, lt

__all__ = [
    "definequote",
    "setdefaulttargetconnection",
    "getdefaulttargetconnection",
    "endload_all",
    "Dimension",
    "CachedDimension",
    "FactTable",
    "BatchFactTable",
    "TypeOneSlowlyChangingDimension",
    "SlowlyChangingDimension",
    "SCDimension",
    "BulkFactTable",
    "_BaseBulkloadable"
]

_alltables = []
_defaulttargetconnection = None


def setdefaulttargetconnection(conn):
    global _defaulttargetconnection
    _defaulttargetconnection = conn


def getdefaulttargetconnection():
    return _defaulttargetconnection


async def endload_all(connection=None):
    """Async replacement for the endload loop in pygrametl.commit().

    If connection is given, only the tables that use that connection are
    finalized. Tables are finalized in the order they were created.
    """
    for table in _alltables:
        if connection is None or table.targetconnection is connection:
            await table.endload()

def discard_all(connection=None):
    """Throw away buffered state in the tables, for use after a rollback.

    If connection is given, only the tables that use that connection are
    affected. Not async, because nothing here touches the database.
    """
    for table in _alltables:
        if connection is None or table.targetconnection is connection:
            table.discard()

async def _maybe_await(x):
    """Let user callbacks and hooks (idfinder, rowexpander, _before_*,
    _after_*) be either sync or async."""
    return await x if inspect.isawaitable(x) else x


def _quote(x):
    return x


def definequote(quotechar):
    global _quote
    if quotechar is None:

        def _quote(x):
            return x
    elif isinstance(quotechar, str):

        def _quote(x):
            return quotechar + x + quotechar
    elif isinstance(quotechar, tuple) and len(quotechar) == 2:

        def _quote(x):
            return quotechar[0] + x + quotechar[1]
    else:
        raise AttributeError("Expected either a string or a tuple of two strings")


class Dimension(object):
    """A class for accessing a dimension. Does no caching.

    Hooks (the _before_* and _after_* methods) may be overridden in
    subclasses with either def or async def.

    Not safe for concurrent use by multiple tasks on one Dimension object:
    ensure() does a lookup followed by an insert, so two tasks ensuring the
    same new row can both insert it. Use one task per table object.
    """

    def __init__(
        self,
        name,
        key,
        attributes,
        lookupatts=(),
        idfinder=None,
        defaultidvalue=None,
        rowexpander=None,
        targetconnection=None,
    ):
        """Arguments:

        - name: the name of the dimension table in the DW.
        - key: the name of the primary key in the DW.
        - attributes: a sequence of the attribute names in the dimension
          table. Should not include the name of the primary key which is
          given in the key argument.
        - lookupatts: A subset of the attributes that uniquely identify
          a dimension member. These attributes are thus used for looking
          up members. If not given, it is assumed that
          lookupatts = attributes.
        - idfinder: A function(row, namemapping) -> key value that assigns
          a value to the primary key attribute based on the content of the
          row and namemapping. If not given, it is assumed that the primary
          key is an integer, and the assigned key value is then the current
          maximum plus one.
        - defaultidvalue: An optional value to return when a lookup fails.
          This should thus be the ID for a preloaded "Unknown" member.
        - rowexpander: A function(row, namemapping) -> row. This function
          is called by ensure before insertion if a lookup of the row fails.
          This is practical if expensive calculations only have to be done
          for rows that are not already present. For example, for a date
          dimension where the full date is used for looking up rows, a
          rowexpander can be set such that week day, week number, season,
          year, etc. are only calculated for dates that are not already
          represented. If not given, no automatic expansion of rows is
          done.
        - targetconnection: The ConnectionWrapper to use. If not given,
          the default target connection is used.
        """

        if type(key) not in pygrametl._stringtypes:
            raise ValueError("Key argument must be a string")
        if not len(attributes):
            raise ValueError("No attributes given")

        if targetconnection is None:
            targetconnection = getdefaulttargetconnection()
            if targetconnection is None:
                raise ValueError("No target connection available")
        self.targetconnection = targetconnection
        self.name = name
        self.attributes = attributes
        self.key = key
        self.all = [key]
        self.all.extend(attributes)
        if lookupatts == ():
            lookupatts = attributes
        elif not len(lookupatts):
            raise ValueError("Lookupatts contain no attributes")
        elif not set(lookupatts) <= set(self.all):
            raise ValueError("Lookupatts is not a subset of attributes")

        self.lookupatts = lookupatts
        self.defaultidvalue = defaultidvalue
        self.rowexpander = rowexpander
        self.quote = _quote
        self.quotelist = lambda x: [self.quote(xn) for xn in x]
        _alltables.append(self)

        self.keylookupsql = (
            "SELECT "
            + self.quote(key)
            + " FROM "
            + name
            + " WHERE "
            + " AND ".join(["%s = %%(%s)s" % (self.quote(lv), lv) for lv in lookupatts])
        )

        self.rowlookupsql = (
            "SELECT "
            + ", ".join(self.quotelist(self.all))
            + " FROM %s WHERE %s = %%(%s)s" % (name, self.quote(key), key)
        )

        self.insertsql = (
            "INSERT INTO "
            + name
            + "(%s" % (self.quote(key),)
            + (attributes and ", " or "")
            + ", ".join(self.quotelist(attributes))
            + ") VALUES ("
            + ", ".join(["%%(%s)s" % (att,) for att in self.all])
            + ")"
        )

        self.__maxid = None  # looked up lazily on the first insert
        self.__maxid_lock = asyncio.Lock()
        if idfinder is not None:
            self.idfinder = idfinder
        else:
            self.idfinder = self._getnextid

    # Methods
    async def lookup(self, row, namemapping={}):
        key = await _maybe_await(self._before_lookup(row, namemapping))
        if key is not None:
            return key

        keyvalue = (
            await self.targetconnection.fetchonetuple(
                self.keylookupsql, row, namemapping
            )
        )[0]
        if keyvalue is None:
            keyvalue = self.defaultidvalue

        await _maybe_await(self._after_lookup(row, namemapping, keyvalue))
        return keyvalue

    async def getbykey(self, keyvalue):
        if isinstance(keyvalue, dict):
            keyvalue = keyvalue[self.key]

        row = await _maybe_await(self._before_getbykey(keyvalue))
        if row is not None:
            return row
        row = await self.targetconnection.fetchone(
            self.rowlookupsql, {self.key: keyvalue}, names=self.all
        )
        await _maybe_await(self._after_getbykey(keyvalue, row))
        return row

    async def getbyvals(self, values, namemapping={}):
        res = await _maybe_await(self._before_getbyvals(values, namemapping))
        if res is not None:
            return res

        attstouse = [a for a in self.attributes if a in values or a in namemapping]
        sql = (
            "SELECT "
            + ", ".join(self.quotelist(self.all))
            + " FROM "
            + self.name
            + " WHERE "
            + " AND ".join(
                ["%s = %%(%s)s" % (self.quote(att), att) for att in attstouse]
            )
        )

        res = await self.targetconnection.rowfactory(
            sql, values, namemapping, self.all
        )
        await _maybe_await(self._after_getbyvals(values, namemapping, res))
        return res

    async def update(self, row, namemapping={}):
        res = await _maybe_await(self._before_update(row, namemapping))
        if res:
            return

        if self.key not in row:
            raise KeyError("The key value (%s) is missing in the row" % (self.key,))

        attstouse = [a for a in self.attributes if a in row or a in namemapping]
        if not attstouse:
            return

        sql = (
            "UPDATE "
            + self.name
            + " SET "
            + ", ".join(["%s = %%(%s)s" % (self.quote(att), att) for att in attstouse])
            + " WHERE %s = %%(%s)s" % (self.quote(self.key), self.key)
        )
        await self.targetconnection.execute(sql, row, namemapping)
        await _maybe_await(self._after_update(row, namemapping))

    async def ensure(self, row, namemapping={}):
        res = await self.lookup(row, namemapping)
        if res is not None and res != self.defaultidvalue:
            return res
        if self.rowexpander:
            row = await _maybe_await(self.rowexpander(row, namemapping))
            if not isinstance(row, dict):
                raise TypeError(
                    "The rowexpander %s did not return a row"
                    % self.rowexpander.__qualname__
                )
        return await self.insert(row, namemapping)

    async def insert(self, row, namemapping={}):
        res = await _maybe_await(self._before_insert(row, namemapping))
        if res is not None:
            return res

        key = namemapping.get(self.key) or self.key
        if row.get(key) is None:
            keyval = await _maybe_await(self.idfinder(row, namemapping))
            row = dict(row)  # Make a copy to change
            row[key] = keyval
        else:
            keyval = row[key]

        await self.targetconnection.execute(self.insertsql, row, namemapping)
        await _maybe_await(self._after_insert(row, namemapping, keyval))
        return keyval

    async def lookuprow(self, row, namemapping={}):
        res = await _maybe_await(self._before_lookuprow(row, namemapping))
        if res is not None:
            return res
        res = await self.getbykey(await self.lookup(row, namemapping))
        await _maybe_await(self._after_lookuprow(row, namemapping, res))
        return res

    async def _getnextid(self, ignoredrow, ignoredmapping):
        if self.__maxid is None:
            async with self.__maxid_lock:
                if self.__maxid is None:  # re-check: another task may have set it
                    row = await self.targetconnection.fetchonetuple(
                        "SELECT MAX(%s) FROM %s" % (self.quote(self.key), self.name)
                    )
                    self.__maxid = row[0] or 0
        self.__maxid += 1  # no await here, so no other task can interleave
        return self.__maxid

    async def endload(self):
        """Finalize the load."""
        pass

    def discard(self):
        """Forget state that may no longer match the database after a
        rollback. The next generated key re-reads MAX(key) from the DB."""
        self.__maxid = None

    def _before_lookup(self, row, namemapping):
        return None

    def _after_lookup(self, row, namemapping, resultkeyvalue):
        pass

    def _before_getbykey(self, keyvalue):
        return None

    def _after_getbykey(self, keyvalue, resultrow):
        pass

    def _after_getbyvals(self, values, namemapping, resultrows):
        pass

    def _before_insert(self, row, namemapping):
        return None

    def _after_insert(self, row, namemapping, newkeyvalue):
        pass

    def _before_lookuprow(self, row, namemapping):
        return None

    def _after_lookuprow(self, row, namemapping, resultrow):
        pass

    async def _before_getbyvals(self, values, namemapping):
        return None

    async def _before_update(self, row, namemapping):
        return None

    async def _after_update(self, row, namemapping):
        pass



class CachedDimension(Dimension):
    """A class for accessing a dimension. Does caching.
    
    We assume that the DB doesn't change or add any attribute
    values that are cached.
    For example, a DEFAULT value in the DB or automatic type coercion can
    break this assumption.
    """

    def __init__(
        self,
        name,
        key,
        attributes,
        lookupatts=(),
        idfinder=None,
        defaultidvalue=None,
        rowexpander=None,
        size=10000,
        prefill=False,
        cachefullrows=False,
        cacheoninsert=True,
        usefetchfirst=False,
        targetconnection=None,
    ):
        Dimension.__init__(
            self,
            name=name,
            key=key,
            attributes=attributes,
            lookupatts=lookupatts,
            idfinder=idfinder,
            defaultidvalue=defaultidvalue,
            rowexpander=rowexpander,
            targetconnection=targetconnection,
        )

        self.cacheoninsert = cacheoninsert
        self.__prefill = prefill
        self.__size = size
        self.cachefullrows = cachefullrows
        self.__newcaches()

        # The prefill query is only built here; it runs in prefillcache()
        self.__prefilled = not prefill
        self.__prefill_lock = asyncio.Lock()
        if prefill:
            if cachefullrows:
                self.__prefillpositions = tuple(
                    [self.all.index(att) for att in self.lookupatts]
                )
                sql = "SELECT %s FROM %s" % (
                    ", ".join(self.quotelist(self.all)),
                    name,
                )
            else:
                sql = "SELECT %s FROM %s" % (
                    ", ".join(self.quotelist([key] + list(self.lookupatts))),
                    name,
                )
                self.__prefillpositions = range(1, len(self.lookupatts) + 1)
            if size > 0 and usefetchfirst:
                sql += " FETCH FIRST %d ROWS ONLY" % size
            self.__prefillsql = sql

    def __newcaches(self):
        if self.__size > 0:
            if self.cachefullrows:
                self.__key2row = FIFODict(self.__size)
            self.__vals2key = FIFODict(self.__size)
        else:
            # Use dictionaries as unlimited caches
            if self.cachefullrows:
                self.__key2row = {}
            self.__vals2key = {}

    def discard(self):
        """Drop all cached rows after a rollback. If the dimension was
        prefilled, it is prefilled again on next use."""
        Dimension.discard(self)
        self.__newcaches()
        self.__prefilled = not self.__prefill

    async def prefillcache(self):
        """Fill the caches from the database. Runs at most once; called
        automatically by the first method that needs the caches."""
        if self.__prefilled:
            return
        async with self.__prefill_lock:
            if self.__prefilled:  # another task finished while we waited
                return
            if self.__size <= 0:
                data = await self.targetconnection.fetchalltuples(self.__prefillsql)
            else:
                data = await self.targetconnection.fetchmanytuples(
                    self.__prefillsql, self.__size
                )
            for rawrow in data:
                if self.cachefullrows:
                    self.__key2row[rawrow[0]] = rawrow
                t = tuple([rawrow[i] for i in self.__prefillpositions])
                self.__vals2key[t] = rawrow[0]
            self.__prefilled = True


    async def lookup(self, row, namemapping={}):
        if not self.__prefilled:
            await self.prefillcache()
        if (
            self.__prefill
            and self.cacheoninsert
            and (self.__size <= 0 or len(self.__vals2key) < self.__size)
        ):
            # Everything is cached. We don't have to look in the DB
            res = await _maybe_await(self._before_lookup(row, namemapping))
            if res is not None:
                return res
            else:
                return self.defaultidvalue
        else:
            # Something is not cached so we have to use the classical lookup.
            return await Dimension.lookup(self, row, namemapping)

    async def getbykey(self, keyvalue):
        if not self.__prefilled:
            await self.prefillcache()
        return await Dimension.getbykey(self, keyvalue)

    def _before_lookup(self, row, namemapping):
        namesinrow = [(namemapping.get(a) or a) for a in self.lookupatts]
        searchtuple = tuple([row[n] for n in namesinrow])
        return self.__vals2key.get(searchtuple, None)

    def _after_lookup(self, row, namemapping, resultkey):
        if resultkey is not None and resultkey != self.defaultidvalue:
            namesinrow = [(namemapping.get(a) or a) for a in self.lookupatts]
            searchtuple = tuple([row[n] for n in namesinrow])
            self.__vals2key[searchtuple] = resultkey

    def _before_getbykey(self, keyvalue):
        if self.cachefullrows:
            res = self.__key2row.get(keyvalue)
            if res is not None:
                return dict(zip(self.all, res))
        return None

    def _after_getbykey(self, keyvalue, resultrow):
        if self.cachefullrows and resultrow[self.key] is not None:
            # if resultrow[self.key] is None, no result was found in the db
            self.__key2row[keyvalue] = tuple([resultrow[a] for a in self.all])

    async def _before_update(self, row, namemapping):
        if not self.__prefilled:
            await self.prefillcache()
        # We have to remove old values from the caches.
        key = namemapping.get(self.key) or self.key
        for att in self.lookupatts:
            if (att in namemapping and namemapping[att] in row) or att in row:
                # A lookup attribute is about to be changed. Find the old
                # lookup values by means of the key.
                oldrow = await self.getbykey(row[key])
                searchtuple = tuple([oldrow[n] for n in self.lookupatts])
                if searchtuple in self.__vals2key:
                    del self.__vals2key[searchtuple]
                break

        if self.cachefullrows:
            if row[key] in self.__key2row:
                # The cached row is now incorrect.
                del self.__key2row[row[key]]

        return None

    async def _after_update(self, row, namemapping):
        if (
            self.__prefill
            and self.cacheoninsert
            and (self.__size <= 0 or len(self.__vals2key) < self.__size)
        ):
            # Everything is cached and we sometimes avoid looking in the DB.
            # In _before_update we deleted the cached data, so refill it.
            keyval = row[(namemapping.get(self.key) or self.key)]
            newrow = await self.getbykey(keyval)  # This also updates __key2row
            await _maybe_await(self._after_lookup(newrow, {}, keyval))  # Updates __vals2key

    async def _after_insert(self, row, namemapping, newkeyvalue):
        # After the insert, we can look the row up. Pretend that we
        # did that. Then we get the new data cached.
        if self.cacheoninsert:
            await _maybe_await(self._after_lookup(row, namemapping, newkeyvalue))
            if self.cachefullrows:
                tmp = pygrametl.project(self.all, row, namemapping)
                tmp[self.key] = newkeyvalue
                await _maybe_await(self._after_getbykey(newkeyvalue, tmp))



class TypeOneSlowlyChangingDimension(CachedDimension):
    """A class for accessing a slowly changing dimension of "type 1".

    Caching is used. We assume that the DB doesn't change or add any
    attribute values that are cached.
    For example, a DEFAULT value in the DB or automatic type coercion can
    break this assumption.

    Hooks (the _before_* and _after_* methods) may be overridden in
    subclasses with either def or async def.

    Not safe for concurrent use by multiple tasks on one object. Use one
    task per table object.
    """

    def __init__(
        self,
        name,
        key,
        attributes,
        lookupatts,
        type1atts=(),
        cachesize=10000,
        prefill=False,
        idfinder=None,
        usefetchfirst=False,
        cachefullrows=False,
        targetconnection=None,
    ):
        """Arguments:

        - name: the name of the dimension table in the DW.
        - key: the name of the primary key in the DW.
        - attributes: a sequence of the attribute names in the dimension
          table. Should not include the name of the primary key which is
          given in the key argument.
        - lookupatts: A subset of the attributes that uniquely identify
          a dimension member. These attributes are thus used for looking
          up members.
        - type1atts: A sequence of attributes that should have type1 updates
          applied, it cannot intersect with lookupatts. If not given, it is
          assumed that type1atts = attributes - lookupatts
        - cachesize: the maximum number of rows to cache. If less than or
          equal to 0, unlimited caching is used. Default: 10000
        - prefill: a flag deciding if the cache should be filled. Since
          __init__ cannot await, the cache is filled lazily by the first
          method that needs it (or by calling prefillcache()).
          Default: False
        - idfinder: A function(row, namemapping) -> key value that assigns
          a value to the primary key attribute based on the content of the
          row and namemapping. If not given, it is assumed that the primary
          key is an integer, and the assigned key value is then the current
          maximum plus one. May be a normal function or a coroutine function.
        - usefetchfirst: a flag deciding if the SQL:2008 FETCH FIRST
          clause is used when prefil is True. Depending on the used DBMS
          and DB driver, this can give significant savings wrt. to time and
          memory. Not all DBMSs support this clause yet. Default: False
        - cachefullrows: a flag deciding if full rows should be cached. If
          not, the cache only holds a mapping from lookupattributes to key
          values, and from key to the type 1 slowly changing attributes.
          Default: False
        - targetconnection: The ConnectionWrapper to use. If not given, the
          default target connection is used.
        """
        CachedDimension.__init__(
            self,
            name=name,
            key=key,
            attributes=attributes,
            lookupatts=lookupatts,
            idfinder=idfinder,
            defaultidvalue=None,
            rowexpander=None,
            size=cachesize,
            prefill=prefill,
            cachefullrows=cachefullrows,
            cacheoninsert=True,
            usefetchfirst=usefetchfirst,
            targetconnection=targetconnection,
        )

        if type1atts == ():
            type1atts = list(set(attributes) - set(lookupatts))
        elif not set(type1atts) < set(attributes):
            raise ValueError("Type1atts is not a subset of attributes")
        elif set(lookupatts) & set(type1atts):
            raise ValueError("Intersection between lookupatts and type1atts")

        # Ensures "lookupatts != attributes" as it prevents type 1 updates
        if not len(type1atts):
            raise ValueError("Type1atts contain no attributes")
        self.type1atts = type1atts

        self.__cachesize = cachesize
        # If entire rows are cached then we do not need a cache for type1atts
        self.__newsca()
        self.__scaprefill = bool(prefill) and not cachefullrows
        self.__scaprefilled = not self.__scaprefill
        self.__scaprefill_lock = asyncio.Lock()
        if self.__scaprefill:
            sql = "SELECT %s FROM %s" % (
                ", ".join(self.quotelist([key] + list(self.type1atts))),
                name,
            )
            if cachesize > 0 and usefetchfirst:
                sql += " FETCH FIRST %d ROWS ONLY" % cachesize
            self.__scaprefillsql = sql

    def __newsca(self):
        if self.cachefullrows:
            return
        if self.__cachesize > 0:
            self.__key2sca = FIFODict(self.__cachesize)
        else:
            self.__key2sca = {}

    async def prefillcache(self):
        """Fill the caches from the database. Each part runs at most once;
        called automatically by the first method that needs the caches."""
        await CachedDimension.prefillcache(self)
        await self.__prefillsca()

    async def __prefillsca(self):
        if self.__scaprefilled:
            return
        async with self.__scaprefill_lock:
            if self.__scaprefilled:  # another task finished while we waited
                return
            if self.__cachesize <= 0:
                data = await self.targetconnection.fetchalltuples(self.__scaprefillsql)
            else:
                data = await self.targetconnection.fetchmanytuples(
                    self.__scaprefillsql, self.__cachesize
                )
            for row in data:
                self.__key2sca[row[0]] = row[1:]
            self.__scaprefilled = True

    def discard(self):
        """Drop all cached rows after a rollback. If the dimension was
        prefilled, it is prefilled again on next use."""
        CachedDimension.discard(self)
        self.__newsca()
        self.__scaprefilled = not self.__scaprefill

    async def scdensure(self, row, namemapping={}):
   
        await self.prefillcache()

        keyval = await self.lookup(row, namemapping)
        key = namemapping.get(self.key) or self.key
        if keyval is None:
            # The first version of the row is inserted
            keyval = await self.insert(row, namemapping)
            row[key] = keyval
        else:
            # The row did exist so we update the type1atts provided
            row[key] = keyval

            # Checks if the new row contains any type1atts
            type1atts = set()
            for att in self.type1atts:
                if (namemapping.get(att) or att) in row:
                    type1atts.add(att)
            if not type1atts:
                return row[key]

            # Checks if any of the type1atts in the row are different
            if not self.cachefullrows and keyval in self.__key2sca:
                oldrow = dict(zip(self.type1atts, self.__key2sca[keyval]))
            else:
                oldrow = await self.getbykey(keyval)

            tmptype1atts = set()
            for att in type1atts:
                rowatt = row[(namemapping.get(att) or att)]
                if oldrow[att] != rowatt:
                    tmptype1atts.add(att)
                    oldrow[att] = rowatt  # Saved for updating the cache
            type1atts = tmptype1atts
            if not type1atts:
                return row[key]

            # Updates only the type1atts that were changed in the row, the
            # update method is not used as lookupatts can never change
            updatesql = (
                "UPDATE "
                + self.name
                + " SET "
                + ", ".join(
                    ["%s = %%(%s)s" % (self.quote(att), att) for att in type1atts]
                )
                + " WHERE %s = %%(%s)s" % (self.quote(key), key)
            )

            await self.targetconnection.execute(updatesql, row, namemapping)

            # Updates the caches that could be invalidated by scdensure
            if self.cachefullrows:
                await _maybe_await(self._after_getbykey(keyval, oldrow))
            else:
                self.__key2sca[keyval] = tuple(
                    [oldrow.get(att) for att in self.type1atts]
                )
        return row[key]

    def _after_getbykey(self, keyvalue, resultrow):
        if self.cachefullrows:
            CachedDimension._after_getbykey(self, keyvalue, resultrow)
        elif resultrow[self.key] is not None:
            self.__key2sca[keyvalue] = tuple([resultrow[a] for a in self.type1atts])

    async def _after_update(self, row, namemapping):
        await CachedDimension._after_update(self, row, namemapping)

        # Checks if changes were performed to the locally cached type1atts
        keyvalue = row[(namemapping.get(self.key) or self.key)]
        if self.cachefullrows or keyvalue not in self.__key2sca:
            return

        # A cached value could have been changed so the local cache is updated,
        # as the value is changed in place no rows are moved in the cache.
        oldtype1vals = dict(zip(self.type1atts, self.__key2sca[keyvalue]))
        newvals = []
        for a in self.type1atts:
            mapped = namemapping.get(a) or a
            # "in row" and not "or", so a new value like 0 or "" is not
            # replaced by the old cached value
            newvals.append(row[mapped] if mapped in row else oldtype1vals[a])
        self.__key2sca[keyvalue] = tuple(newvals)

    async def _after_insert(self, row, namemapping, newkeyvalue):
        await CachedDimension._after_insert(self, row, namemapping, newkeyvalue)
        # NB: Here we assume that the DB doesn't change or add anything. For
        # example, a DEFAULT value in the DB or automatic type coercion can
        # break this assumption.
        if not self.cachefullrows:
            tmp = pygrametl.project(self.type1atts, row, namemapping)
            self.__key2sca[newkeyvalue] = tuple([tmp[a] for a in self.type1atts])


# Marks "end was not given" in closecurrent, so None can still be passed
_NOTGIVEN = object()


class SlowlyChangingDimension(Dimension):
    """A class for accessing a slowly changing dimension of "type 2".

    "Type 1" updates can also be applied for a subset of the attributes.

    Caching is used. We assume that the DB doesn't change or add any
    attribute values that are cached.
    For example, a DEFAULT value in the DB or automatic type coercion can
    break this assumption.

    Hooks (the _before_* and _after_* methods), idfinder, fromfinder and
    tofinder may be normal functions or coroutine functions.

    Not safe for concurrent use by multiple tasks on one object. Use one
    task per table object.
    """

    def __init__(
        self,
        name,
        key,
        attributes,
        lookupatts,
        orderingatt=None,
        versionatt=None,
        fromatt=None,
        fromfinder=None,
        toatt=None,
        tofinder=None,
        minfrom=None,
        maxto=None,
        srcdateatt=None,
        srcdateparser=pygrametl.ymdparser,
        type1atts=(),
        cachesize=10000,
        prefill=False,
        idfinder=None,
        usefetchfirst=False,
        useorderby=True,
        targetconnection=None,
    ):
        """Arguments:

        - name: the name of the dimension table in the DW.
        - key: the name of the primary key in the DW.
        - attributes: a sequence of the attribute names in the dimension
          table. Should not include the name of the primary key which is
          given in the key argument, but should include versionatt,
          fromatt, and toatt.
        - lookupatts: a sequence with a subset of the attributes that
          uniquely identify a dimension member. These attributes are thus
          used for looking up members.
        - orderingatt: the name of the attribute used to identify the
          newest version. The version holding the greatest value is
          considered to be the newest. If orderingatt is None, versionatt
          is used. If versionatt is also None, toatt is used and NULL is
          considered as the greatest value. If toatt is also None, fromatt
          is used and NULL is considered as the smallest values. If
          orderingatt, versionatt, toatt, and fromatt are all None, an
          error is raised.
        - versionatt: the name of the attribute holding the version number
        - fromatt: the name of the attribute telling from when the version
          becomes valid. Not used if None. Default: None
        - fromfinder: a function(targetconnection, row, namemapping)
          returning a value for the fromatt for a new version (the function
          is first used when it is determined that a new version must be
          added; it is not applied to determine this). May be a normal
          function or a coroutine function.
          If fromfinder is None and srcdateatt is also None,
          pygrametl.today is used as fromfinder. If fromfinder is None
          and srcdateatt is not None,
          pygrametl.datereader(srcdateatt, srcdateparser) is used.
          In other words, if no date attribute and no special
          date function are given, new versions get the date of the current
          day. If a date attribute is given (but no date function), the
          date attribute's value is converted (by means of srcdateparser)
          and a new version gets the result of this as the date it is valid
          from. Default: None
        - toatt: the name of the attribute telling until when the version
          is valid. Not used if None. Default: None
        - tofinder: a function(targetconnection, row, namemapping)
          returning a value for the toatt. If not set, fromfinder is used
          (note that if fromfinder is None, it is set to a default
          function -- see the comments about fromfinder. The possibly
          modified value is used here.) May be a normal function or a
          coroutine function. Default: None
        - minfrom: the value to use for fromatt for the 1st version of a
          member if fromatt is not already set. If None, the value is
          found in the same way as for other new versions, i.e., as
          described for fromfinder. If fromatt should take the value
          NULL for the 1st version, set minfrom to a tuple holding a single
          element which is None: (None,). Note that minto affects the 1st
          version, not any following versions. Note also that if the member
          to insert already contains a value for fromatt, minfrom is
          ignored.
          Default: None
        - maxto: the value to use for toatt for new members. Default: None
        - srcdateatt: the name of the attribute in the source data that
          holds a date showing when a version is valid from. The data is
          converted to a datetime by applying srcdateparser on it.
          If not None, the date attribute is also used when comparing
          a potential new version to the newest version in the DB.
          If None, the date fields are not compared. Default: None
        - srcdateparser: a function that takes one argument (a date in the
          format scrdateatt has) and returns a datetime.datetime.
          If srcdateatt is None, srcdateparser is not used.
          Default: pygrametl.ymdparser (i.e., the default value is a
          function that parses a string of the form 'yyyy-MM-dd')
        - type1atts: a sequence of attributes that should have type1 updates
          applied. Each attribute can be its name as a string meaning all
          versions of it will be updated or a tuple with the its name as a
          string and a boolean flag specifying if all version of it should be
          updated (True) or only the latest one (False). Default: ()
        - cachesize: the maximum size of the cache. 0 disables caching
          and values smaller than 0 allows unlimited caching.
        - prefill: decides if the cache should be filled with the newest
          versions. Since __init__ cannot await, the cache is filled lazily
          by the first method that needs it (or by calling prefillcache()).
          Default: False
        - idfinder: a function(row, namemapping) -> key value that assigns
          a value to the primary key attribute based on the content of the
          row and namemapping. If not given, it is assumed that the primary
          key is an integer, and the assigned key value is then the current
          maximum plus one. May be a normal function or a coroutine function.
        - usefetchfirst: a flag deciding if the SQL:2008 FETCH FIRST
          clause is used when prefil is True. Depending on the used DBMS
          and DB driver, this can give significant savings wrt. to time and
          memory. Not all DBMSs support this clause yet. Default: False
        - targetconnection: The ConnectionWrapper to use. If not given,
          the default target connection is used.
        - useorderby: a flag deciding if ORDER BY is used in the SQL to
          select the newest version. If True, the DBMS thus does the
          sorting. If False, all versions are fetched and the highest
          version is found in Python. For some systems, this can lead to
          significant performance improvements. Default: True

        """
        # TODO: Should scdensure just override ensure instead of being a new
        #       method?

        Dimension.__init__(
            self,
            name=name,
            key=key,
            attributes=attributes,
            lookupatts=lookupatts,
            idfinder=idfinder,
            defaultidvalue=None,
            rowexpander=None,
            targetconnection=targetconnection,
        )

        if orderingatt is not None:
            self.orderingatt = orderingatt
        elif versionatt is not None:
            self.orderingatt = versionatt
        elif toatt is not None:
            self.orderingatt = toatt
        elif fromatt is not None:
            self.orderingatt = fromatt
        else:
            raise ValueError(
                "If orderingatt is None, either versionatt, "
                + "toatt, or fromatt must not be None"
            )

        self.versionatt = versionatt
        self.fromatt = fromatt
        if fromfinder is not None:
            self.fromfinder = fromfinder
        elif srcdateatt is not None:  # and fromfinder is None
            self.fromfinder = pygrametl.datereader(srcdateatt, srcdateparser)
        else:  # fromfinder is None and srcdateatt is None
            self.fromfinder = pygrametl.today
        self.toatt = toatt
        if tofinder is None:
            tofinder = self.fromfinder
        self.tofinder = tofinder
        self.minfrom = minfrom
        self.maxto = maxto
        self.srcdateatt = srcdateatt
        self.srcdateparser = srcdateparser
        self.type1atts = [att if type(att) is str else att[0] for att in type1atts]
        type1lookupatts = set(self.type1atts) & set(self.lookupatts)
        if type1lookupatts:
            raise ValueError(
                ", ".join(type1lookupatts)
                + " in both type1atts and lookupatts argument"
            )
        self.type1attsupdateall = dict(
            [(att, True) if type(att) is str else att for att in type1atts]
        )
        self.useorderby = useorderby
        self.__cachesize = cachesize
        self.__newcaches()
        self.__prefill = cachesize and prefill  # no prefilling if no caching
        self.__prefilled = not self.__prefill
        self.__prefill_lock = asyncio.Lock()

        # Check that versionatt, fromatt and toatt are also declared as
        # attributes
        for var in [orderingatt, versionatt, fromatt, toatt] + self.type1atts:
            if var and var not in attributes:
                raise ValueError("%s not present in attributes argument" % (var,))

        # Now extend the SQL from Dimension such that we use the versioning
        self.keylookupsql += " ORDER BY %s DESC" % (self.quote(self.orderingatt),)
        # There could be NULLs in toatt and fromatt
        if self.orderingatt == self.toatt:
            self.keylookupsql += " NULLS FIRST"
        elif self.orderingatt == self.fromatt:
            self.keylookupsql += " NULLS LAST"

        # Now create SQL for looking up the key with a local sort
        # This gives "SELECT key, version FROM name WHERE
        # lookupval1 = %(lookupval1)s AND lookupval2 = %(lookupval2)s AND ..."
        self.keyversionlookupsql = (
            "SELECT "
            + self.quote(key)
            + ", "
            + self.quote(self.orderingatt)
            + " FROM "
            + name
            + " WHERE "
            + " AND ".join(["%s = %%(%s)s" % (self.quote(lv), lv) for lv in lookupatts])
        )

        if toatt:
            self.updatetodatesql = "UPDATE %s SET %s = %%(%s)s WHERE %s = %%(%s)s" % (
                name,
                self.quote(toatt),
                toatt,
                self.quote(key),
                key,
            )

        if self.toatt or self.fromatt:
            # Set up SQL and functions for lookupasof
            keyvalidityatts = [self.key]
            if self.toatt:
                keyvalidityatts.append(self.toatt)
            if self.fromatt:
                keyvalidityatts.append(self.fromatt)
            # This gives "SELECT key, {fromatt and/or toatt} FROM name
            #             WHERE lookupval1 = %(lookupval1) AND
            #             lookupval2 = %(lookupval2)s AND ..."
            #             ORDER BY orderingatt ASC NULLS {LAST or FIRST}"
            self.keyvaliditylookupsql = (
                "SELECT "
                + ", ".join(self.quotelist(keyvalidityatts))
                + " FROM "
                + name
                + " WHERE "
                + " AND ".join(
                    ["%s = %%(%s)s" % (self.quote(lv), lv) for lv in lookupatts]
                )
                + " ORDER BY %s ASC" % (self.quote(self.orderingatt),)
            )
            # There could be NULLs in toatt and fromatt. See the explanation for
            # the orderingatt argument above
            if self.orderingatt == self.toatt:
                self.keyvaliditylookupsql += " NULLS LAST"
            elif self.orderingatt == self.fromatt:
                self.keyvaliditylookupsql += " NULLS FIRST"

        if self.__prefill:
            self.__buildprefillsql(usefetchfirst)

    def __newcaches(self):
        if self.__cachesize > 0:
            self.rowcache = FIFODict(self.__cachesize)
            self.keycache = FIFODict(self.__cachesize)
        elif self.__cachesize < 0:
            self.rowcache = {}
            self.keycache = {}
        # else cachesize == 0 and we do not create any caches

    def discard(self):
        """Drop all cached rows after a rollback. If the dimension was
        prefilled, it is prefilled again on next use."""
        Dimension.discard(self)
        self.__newcaches()
        self.__prefilled = not self.__prefill

    def __buildprefillsql(self, usefetchfirst):
        # Only builds the query; it runs in prefillcache()
        args = None
        if self.toatt:
            # We can use the toatt to see if rows are still current.
            # Select all attributes from the rows where maxto is set to the
            # default value (which may be NULL)
            sql = "SELECT %s FROM %s WHERE %s %s" % (
                ", ".join(self.quotelist(self.all)),
                self.name,
                self.quote(self.toatt),
                "IS NULL" if self.maxto is None else "= %(maxto)s",
            )
            if self.maxto is not None:
                args = {"maxto": self.maxto}
        else:
            # We have to find max(versionatt) for each group of lookupatts and
            # do a join to get the right rows.
            lookupattlist = ", ".join(self.quotelist(self.lookupatts))
            newestversions = "SELECT %s, MAX(%s) AS %s FROM %s GROUP BY %s" % (
                lookupattlist,
                self.quote(self.orderingatt),
                self.quote(self.orderingatt),
                self.name,
                lookupattlist,
            )
            joincond = " AND ".join(
                [
                    "A.%s = B.%s" % (self.quote(att), att)
                    for att in list(self.lookupatts) + [self.orderingatt]
                ]
            )
            sql = "SELECT %s FROM (%s) AS A, %s AS B WHERE %s" % (
                ", ".join(["B.%s AS %s" % (self.quote(att), att) for att in self.all]),
                newestversions,
                self.name,
                joincond,
            )

        # sql is a statement that fetches the newest versions from the database
        # in order to fill the caches, the FETCH FIRST clause is for a finite
        # cache, if the user set the flag that it is supported by the database.
        if self.__cachesize > 0 and usefetchfirst:
            sql += " FETCH FIRST %d ROWS ONLY" % self.__cachesize
        self.__prefillsql = sql
        self.__prefillargs = args
        self.__prefillpositions = [self.all.index(att) for att in self.lookupatts]

    async def prefillcache(self):
        """Fill the caches from the database. Runs at most once; called
        automatically by the first method that needs the caches."""
        if self.__prefilled:
            return
        async with self.__prefill_lock:
            if self.__prefilled:  # another task finished while we waited
                return
            if self.__cachesize < 0:
                allrawrows = await self.targetconnection.fetchalltuples(
                    self.__prefillsql, self.__prefillargs
                )
            else:
                allrawrows = await self.targetconnection.fetchmanytuples(
                    self.__prefillsql, self.__cachesize, self.__prefillargs
                )
            for rawrow in allrawrows:
                self.rowcache[rawrow[0]] = rawrow
                t = tuple([rawrow[i] for i in self.__prefillpositions])
                self.keycache[t] = rawrow[0]
            self.__prefilled = True

    async def lookup(self, row, namemapping={}):
        """Find the key for the newest version with the given values.

        Arguments:

        - row: a dict which must contain at least the lookup attributes.
        - namemapping: an optional namemapping (see module's documentation).
        """
        await self.prefillcache()
        if self.__prefill and (
            self.__cachesize < 0 or len(self.keycache) < self.__cachesize
        ):
            # Everything is cached. We don't have to look in the DB
            return await _maybe_await(self._before_lookup(row, namemapping))
        else:
            # Something is not cached so we have to use the classical lookup.
            # Note that __init__ updated self.keylookupsql to use ORDER BY ...
            if self.useorderby:
                return await Dimension.lookup(self, row, namemapping)
            else:
                return await self.__lookupnewestlocally(row, namemapping)

    async def getbykey(self, keyvalue):
        await self.prefillcache()
        return await Dimension.getbykey(self, keyvalue)

    async def __lookupnewestlocally(self, row, namemapping):
        """Find the key for the newest version of the row with the given values.

        Arguments:

        - row: a dict which must contain at least the lookup attributes.
        - namemapping: an optional namemapping (see module's documentation).
        """
        # Based on Dimension.lookup, but uses keyversionlookupsql and
        # finds the newest version locally (no sorting on the DBMS)
        key = await _maybe_await(self._before_lookup(row, namemapping))
        if key is not None:
            return key

        versions = await self.targetconnection.fetchalltuples(
            self.keyversionlookupsql, row, namemapping
        )
        if not versions:
            # There is no existing version
            keyvalue = None
        else:
            # Look in all (key, version) pairs and find the key for the newest
            # version
            keyvalue, versionvalue = versions[-1]
            for kv in versions:
                if kv[1] > versionvalue:
                    keyvalue, versionvalue = kv

        await _maybe_await(self._after_lookup(row, namemapping, keyvalue))
        return keyvalue

    async def scdensure(self, row, namemapping={}):
        """Lookup or insert a version of a slowly changing dimension member.

        .. Note:: Has side-effects on the given row.

        Arguments:

        - row: a dict containing the attributes for the member.
          key, versionatt, fromatt, and toatt are not required to be
          present but will be added (if defined).
        - namemapping: an optional namemapping (see module's documentation).
        """
        key = namemapping.get(self.key) or self.key
        if self.versionatt:
            versionatt = namemapping.get(self.versionatt) or self.versionatt
        else:
            versionatt = None
        if self.fromatt:  # this protects us against None in namemapping.
            fromatt = namemapping.get(self.fromatt) or self.fromatt
        else:
            fromatt = None
        if self.toatt:
            toatt = namemapping.get(self.toatt) or self.toatt
        else:
            toatt = None
        if self.srcdateatt:
            srcdateatt = namemapping.get(self.srcdateatt) or self.srcdateatt
        else:
            srcdateatt = None

        # Get the newest version and compare to that
        keyval = await self.lookup(row, namemapping)
        if keyval is None:
            # It is a new member. We add the first version.
            if versionatt:
                row[versionatt] = 1

            if fromatt and fromatt not in row:
                if self.minfrom is not None:
                    # We need the following hack to distinguish between
                    # 'not set' and 'use the value None'...
                    if self.minfrom == (None,):
                        row[fromatt] = None
                    else:
                        row[fromatt] = self.minfrom
                else:
                    row[fromatt] = await _maybe_await(
                        self.fromfinder(self.targetconnection, row, namemapping)
                    )
            if toatt and toatt not in row:
                row[toatt] = self.maxto
            row[key] = await self.insert(row, namemapping)
            return row[key]
        else:
            # There is an existing version. Check if the attributes are
            # identical
            type1updates = {}  # for type 1
            addnewversion = False  # for type 2
            other = await self.getbykey(keyval)  # the full existing version
            for att in self.all:
                # Special (non-)handling of versioning and key attributes:
                if att in (self.key, self.orderingatt, self.versionatt):
                    # Don't compare these - we don't expect them to have
                    # meaningful values in row
                    continue
                # We may have to compare the "from" and "to" dates
                elif att == self.toatt:
                    if other[self.toatt] != self.maxto:
                        # That version was closed manually or by closecurrent
                        # and we now have to add a new version
                        addnewversion = True
                elif att == self.fromatt:
                    if self.srcdateatt is None:  # We don't compare dates then
                        continue
                    else:
                        # We have to compare the dates in row[..] and other[..]
                        # We have to make sure that the dates are of comparable
                        # types.
                        rdt = self.srcdateparser(row[srcdateatt])
                        if rdt == other[self.fromatt]:
                            continue  # no change in the "from attribute"
                        elif isinstance(rdt, type(other[self.fromatt])):
                            # they are not equal but are of the same type, so
                            # we are dealing with a new date
                            addnewversion = True
                        else:
                            # They have different types (and are thus not
                            # equal). Try to convert to strings and see if they
                            # are equal.
                            modref = self.targetconnection.getunderlyingmodule()
                            rowdate = modref.Date(rdt.year, rdt.month, rdt.day)
                            if str(rowdate).strip("'\"") != str(
                                other[self.fromatt]
                            ).strip("'\""):
                                addnewversion = True
                # Handling of "normal" attributes:
                else:
                    mapped = namemapping.get(att) or att
                    if row[mapped] != other[att]:
                        if att in self.type1atts:
                            type1updates[att] = row[mapped]
                        else:
                            addnewversion = True
                if addnewversion and not self.type1atts:
                    # We don't have to look for possible type 1 updates
                    # and we already know that a type 2 update is needed.
                    break
                # else: continue

            if len(type1updates) > 0:
                # Some type 1 updates were found
                await self.__preparetype1updates(type1updates, other, addnewversion)

            if addnewversion:  # type 2
                # Make a new row version and insert it
                row.pop(key, None)
                if versionatt:
                    row[versionatt] = other[self.versionatt] + 1

                if fromatt:
                    row[fromatt] = await _maybe_await(
                        self.fromfinder(self.targetconnection, row, namemapping)
                    )
                if toatt:
                    row[toatt] = self.maxto
                row[key] = await self.insert(row, namemapping)
                # Update the todate attribute in the old row version in the DB
                # if it has not been set manually or by closecurrent
                if toatt and other[self.toatt] == self.maxto:
                    toattval = await _maybe_await(
                        self.tofinder(self.targetconnection, row, namemapping)
                    )
                    await self.targetconnection.execute(
                        self.updatetodatesql,
                        {self.key: keyval, self.toatt: toattval},
                    )
                # Only cache the newest version
                if self.__cachesize and keyval in self.rowcache:
                    del self.rowcache[keyval]
            else:
                # Update the row dict by giving version and dates and the key
                row[key] = keyval
                if self.versionatt:
                    row[versionatt] = other[self.versionatt]
                if self.fromatt:
                    row[fromatt] = other[self.fromatt]
                if self.toatt:
                    row[toatt] = other[self.toatt]

            return row[key]

    def _before_lookup(self, row, namemapping):
        if self.__cachesize:
            namesinrow = [(namemapping.get(a) or a) for a in self.lookupatts]
            searchtuple = tuple([row[n] for n in namesinrow])
            return self.keycache.get(searchtuple, None)
        return None

    def _after_lookup(self, row, namemapping, resultkey):
        if self.__cachesize and resultkey is not None:
            namesinrow = [(namemapping.get(a) or a) for a in self.lookupatts]
            searchtuple = tuple([row[n] for n in namesinrow])
            self.keycache[searchtuple] = resultkey

    def _before_getbykey(self, keyvalue):
        if self.__cachesize:
            res = self.rowcache.get(keyvalue)
            if res is not None:
                return dict(zip(self.all, res))
        return None

    def _after_getbykey(self, keyvalue, resultrow):
        if self.__cachesize and resultrow[self.key] is not None:
            # if resultrow[self.key] is None, no result was found in the db
            self.rowcache[keyvalue] = tuple([resultrow[a] for a in self.all])

    async def _before_update(self, row, namemapping):
        """ """
        # We have to remove old values from the caches if they exist.
        if self.__cachesize == 0:
            return None

        key = namemapping.get(self.key) or self.key
        for att in self.lookupatts:
            if (att in namemapping and namemapping[att] in row) or att in row:
                # A lookup attribute is about to be changed and we should make
                # sure that the cache does not map from the old value.  Here,
                # we can only see the new value, but we can get the old lookup
                # values by means of the key:
                oldrow = await self.getbykey(row[key])
                searchtuple = tuple([oldrow[n] for n in self.lookupatts])
                if searchtuple in self.keycache:
                    del self.keycache[searchtuple]
                break

        if row[key] in self.rowcache:
            # The cached row is now incorrect. We must make sure it is
            # not in the cache.
            del self.rowcache[row[key]]

        return None

    async def _after_insert(self, row, namemapping, newkeyvalue):
        """ """
        # After the insert, we can look it up. Pretend that we
        # did that. Then we get the new data cached.
        # NB: Here we assume that the DB doesn't change or add anything.
        # For example, a DEFAULT value in the DB or automatic type coercion can
        # break this assumption.
        # Note that we always cache inserted members (in CachedDimension
        # this is an option).
        if self.__cachesize:
            await _maybe_await(self._after_lookup(row, namemapping, newkeyvalue))
            tmp = pygrametl.project(self.all[1:], row, namemapping)
            tmp[self.key] = newkeyvalue
            await _maybe_await(self._after_getbykey(newkeyvalue, tmp))

    async def __preparetype1updates(self, updates, lookupvalues, type2changes):
        """ """
        # Perform type 1 updates for the latest version unless type2changes is
        # True as the latest version then is a new version about to be inserted
        updateslatest = {
            att: value
            for (att, value) in updates.items()
            if not self.type1attsupdateall[att]
        }
        if updateslatest and not type2changes:
            await self.__performtype1updates([lookupvalues[self.key]], updateslatest)

        # Perform type 1 updates for all version
        updatesall = {
            att: value
            for (att, value) in updates.items()
            if self.type1attsupdateall[att]
        }
        if updatesall:
            rows = await self.targetconnection.fetchalltuples(
                self.keylookupsql, lookupvalues
            )
            updatekeys = [e[0] for e in rows]
            updatekeys.reverse()
            await self.__performtype1updates(updatekeys, updatesall)

    async def __performtype1updates(self, updatekeys, updates):
        """ """
        # Generate SQL for the update
        valparts = ", ".join(["%s = %%(%s)s" % (self.quote(k), k) for k in updates])
        keyparts = ", ".join([str(k) for k in updatekeys])
        sql = "UPDATE %s SET %s WHERE %s IN (%s)" % (
            self.name,
            valparts,
            self.quote(self.key),
            keyparts,
        )

        # Execute SQL to perform the update
        await self.targetconnection.execute(sql, updates)

        # Remove from our own cache
        if self.__cachesize:
            for key in updatekeys:
                if key in self.rowcache:
                    del self.rowcache[key]

    async def closecurrent(self, row, namemapping={}, end=_NOTGIVEN):
        """Close the current version by setting its toatt if it is maxto.

        The newest version will have its toatt set to the given end
        argument (default pygrametl.today(), i.e., the current date) only
        if the value for the newest row's toatt currently is maxto.
        Otherwise no update will be done.
        If toatt is not defined an exception is raised.

        Arguments:

        - row: a dict which must contain at least the lookup attributes.
        - namemapping: an optional namemapping (see module's documentation)
        - end: the value to set for the newest version. Default: The current
          date as given by pygrametl.today(), found when the method is called
        """
        if self.toatt is None:
            raise RuntimeError("A toatt must be defined")
        if end is _NOTGIVEN:
            end = pygrametl.today()
        keyval = await self.lookup(row, namemapping)
        if keyval is None:
            raise RuntimeError("Member does not exist and cannot be closed")
        existing = await self.getbykey(keyval)
        if existing[self.toatt] == self.maxto:
            await self.update({self.key: keyval, self.toatt: end})

    async def lookuprowasof(self, row, when, inclusive, namemapping={}):
        """Find the entire row version that was valid at a given time.

        If both fromatt and toatt have been set, the method returns the row
        version where the given time is between them. If only toatt
        has been defined, the method returns the first row version
        where toatt is after the given time. If only fromatt is defined,
        the method returns the most recent row version where fromatt
        is before the given time. See also the description of the argument
        inclusive. For this to be possible, fromatt and/or toatt must have
        been set. If this is not the case, a RuntimeError is raised. If no
        valid version is found for the given time, a row where each
        attribute is None is returned

        Note that this function cannot exploit the cache and always results
        in the execution of a SQL query.

        Arguments:

        - row: a dict which must contain at least the lookup attributes.
        - when: the time when the version should be valid. This argument
          must be of a type that can be compared (using <, <=, ==, =>, >)
          to values in fromatt and/or toatt.
        - inclusive: decides if the values of fromatt and/or toatt are
          allowed to be equal to the value of when in the version to find.
          If only one of fromatt and toatt has been set, the argument
          should be a single Boolean. If both fromatt and toatt have been
          set, the argument should be a tuple of two Booleans where the
          first element decides if the fromatt value can be equal to the
          the value of when and the second element decides the same for
          toatt. This tuple must not be (False, False).
        - namemapping: an optional namemapping (see module's documentation).

        """
        key = await self.lookupasof(row, when, inclusive, namemapping)
        if key is None:
            # No need to try to find this row in the DB
            return {a: None for a in self.all}
        else:
            return await self.getbykey(key)

    async def lookupasof(self, row, when, inclusive, namemapping={}):
        """Find the key of the version that was valid at a given time.

        If both fromatt and toatt have been set, the method returns the key
        of the version where the given time is between them. If only toatt
        has been defined, the method returns the key of the first version
        where toatt is after the given time. If only fromatt is defined,
        the method returns the key of the most recent version where fromatt
        is before the given time. See also the description of the argument
        inclusive. For this to be possible, fromatt and/or toatt must have
        been set. If this is not the case, a RuntimeError is raised. If no
        valid version is found for the given time, None is returned.

        Note that this function cannot exploit the cache and always results
        in the execution of a SQL query.

        Arguments:

        - row: a dict which must contain at least the lookup attributes.
        - when: the time when the version should be valid. This argument
          must be of a type that can be compared (using <, <=, ==, =>, >)
          to values in fromatt and/or toatt.
        - inclusive: decides if the values of fromatt and/or toatt are
          allowed to be equal to the value of when in the version to find.
          If only one of fromatt and toatt has been set, the argument
          should be a single Boolean. If both fromatt and toatt have been
          set, the argument should be a tuple of two Booleans where the
          first element decides if the fromatt value can be equal to the
          the value of when and the second element decides the same for
          toatt. This tuple must not be (False, False).
        - namemapping: an optional namemapping (see module's documentation).

        """
        if self.fromatt and self.toatt:
            return await self._lookupasofusingfromattandtoatt(
                row, when, inclusive, namemapping
            )
        elif self.fromatt:
            return await self._lookupasofusingfromatt(row, when, inclusive, namemapping)
        elif self.toatt:
            return await self._lookupasofusingtoatt(row, when, inclusive, namemapping)
        else:
            raise RuntimeError(
                "fromatt and/or toatt must be set if lookupasof should be used"
            )

    async def _getversions(self, row, namemapping):
        """Return an ordered list of all versions of a given member"""
        # The constructed SQL depends on what arguments the user
        # passed to __init__.
        return await self.targetconnection.rowfactory(
            self.keyvaliditylookupsql, row, namemapping
        )

    async def _lookupasofusingtoatt(self, row, when, inclusive, namemapping):
        """Helper function for lookupasof"""
        # _getversions gives back all versions [1st, 2nd, ...] sorted on
        # fromatt and with NULLS LAST in this case (see how
        # self.keyvaliditylookupsql is made in __init__).  We iterate over the
        # list and when we find a version where toatt > when (or >= if
        # inclusive is True), it is the version that was valid at "when",
        # i.e., the one we are looking for. If toatt is None, we're looking at
        # the last version and, since it doesn't have an explicit timestamp,
        # we must assume that it was/will be valid at time "when".
        op = ge if inclusive else gt
        versions = await self._getversions(row, namemapping)
        for ver in versions:
            toattval = ver[self.toatt]
            if toattval is None or op(toattval, when):
                return ver[self.key]
        return None

    async def _lookupasofusingfromatt(self, row, when, inclusive, namemapping):
        """Helper function for lookupasof"""
        # _getversions gives back all versions [1st, 2nd, ...] sorted on
        # fromatt and with NULLS FIRST in this case (see how
        # self.keyvaliditylookupsql is made in __init__).  We revert the list
        # and iterate over it. When we find a version where fromatt < when (or
        # <= if inclusive is True), it is the most recent version that became
        # valid before "when", i.e., the one we are looking for. If fromatt is
        # None, we're looking at the 1st version and, since it doesn't have an
        # explicit timestamp, we must assume that it was valid at time "when".
        op = le if inclusive else lt
        versions = await self._getversions(row, namemapping)
        versions.reverse()
        for ver in versions:
            fromattval = ver[self.fromatt]
            if fromattval is None or op(fromattval, when):
                return ver[self.key]
        return None

    async def _lookupasofusingfromattandtoatt(self, row, when, inclusive, namemapping):
        """Helper function for lookupasof"""
        # At least one of the timestamps must be included as there otherwise
        # would be gaps between versions. In other words, [from, to), (from,
        # to], and [from, to] are possible, but (from, to) is not
        if not any(inclusive):
            raise ValueError(
                "At least one of inclusive[0] and inclusive[1] must be True"
            )
        # We do as in _lookupasofusingtoatt, but also have to check that the
        # version had become valid (i.e., fromatt < when (or <=))
        fromop = le if inclusive[0] else lt
        toop = ge if inclusive[1] else gt
        versions = await self._getversions(row, namemapping)

        for ver in versions:
            toattval = ver[self.toatt]
            if toattval is None or toop(toattval, when):
                fromattval = ver[self.fromatt]
                if fromattval is None or fromop(fromattval, when):
                    return ver[self.key]
                else:
                    # Different versions don't overlap in the dimension, so no
                    # need to look further
                    break
        return None


SCDimension = SlowlyChangingDimension




class FactTable(object):
    """A class for accessing a fact table in the DW.

    Hooks (the _before_* and _after_* methods) may be overridden in
    subclasses with either def or async def.

    Not safe for concurrent use by multiple tasks on one FactTable object:
    ensure() does a lookup followed by an insert, so two tasks ensuring the
    same new fact can both insert it. Use one task per table object.
    """

    def __init__(self, name, keyrefs, measures=(), targetconnection=None):
        if targetconnection is None:
            targetconnection = getdefaulttargetconnection()
        self.targetconnection = targetconnection
        self.name = name
        self.keyrefs = keyrefs
        self.measures = measures
        self.all = [k for k in keyrefs] + [m for m in measures]
        _alltables.append(self)

        self.quote = _quote
        self.quotelist = lambda x: [self.quote(xn) for xn in x]

        self.insertsql = (
            "INSERT INTO "
            + name
            + "("
            + ", ".join(self.quotelist(self.all))
            + ") VALUES ("
            + ", ".join(["%%(%s)s" % (att,) for att in self.all])
            + ")"
        )

        self.lookupsql = (
            "SELECT "
            + ",".join(self.quotelist(self.all))
            + " FROM "
            + name
            + " WHERE "
            + " AND ".join(["%s = %%(%s)s" % (self.quote(k), k) for k in self.keyrefs])
        )

    async def insert(self, row, namemapping={}):
        tmp = await _maybe_await(self._before_insert(row, namemapping))
        if tmp:
            return
        await self.targetconnection.execute(self.insertsql, row, namemapping)
        await _maybe_await(self._after_insert(row, namemapping))

    def _emptyfacttonone(self, argdict):
        """Return None if the given argument only contains None values,
        otherwise return the given argument
        """
        for k in self.keyrefs:
            if argdict[k] is not None:
                return argdict
        return None

    async def lookup(self, keyvalues, namemapping={}):
        res = await _maybe_await(self._before_lookup(keyvalues, namemapping))
        if res:
            return self._emptyfacttonone(res)
        res = await self.targetconnection.fetchone(
            self.lookupsql, keyvalues, namemapping, names=self.all
        )
        await _maybe_await(self._after_lookup(keyvalues, namemapping, res))
        return self._emptyfacttonone(res)

    async def ensure(self, row, compare=False, namemapping={}):
        res = await self.lookup(row, namemapping)
        if not res:
            await self.insert(row, namemapping)
            return False
        elif compare:
            for m in self.measures:
                mappedm = namemapping.get(m, m)
                if mappedm in row and row[mappedm] != res.get(m):
                    raise ValueError(
                        "The existing fact has a different measure value"
                        + " for '"
                        + m
                        + "': "
                        + str(res.get(m))
                        + " instead of "
                        + str(row.get(mappedm))
                        + " as in the given row"
                    )
        return True

    async def endload(self):
        """Finalize the load."""
        pass

    def discard(self):
        """Forget buffered state after a rollback. Nothing to do here."""
        pass

    async def _before_insert(self, row, namemapping):
        return None

    def _after_insert(self, row, namemapping):
        pass

    async def _before_lookup(self, keyvalues, namemapping):
        return None

    def _after_lookup(self, keyvalues, namemapping, resultrow):
        pass


class BatchFactTable(FactTable):
    """A class for accessing a fact table in the DW. This class performs
    insertions in batches.
    """

    def __init__(
        self,
        name,
        keyrefs,
        measures=(),
        batchsize=10000,
        usemultirow=False,
        targetconnection=None,
    ):
        FactTable.__init__(
            self,
            name=name,
            keyrefs=keyrefs,
            measures=measures,
            targetconnection=targetconnection,
        )

        self.__batchsize = batchsize
        self.__batch = []
        if usemultirow:
            self.__insertnow = self.__insertmultirow
            self.__basesql = self.insertsql[: self.insertsql.find(" (") + 1]
            self.__rowtovalue = lambda row: (
                "("
                + ",".join(map(lambda c: pygrametl.getsqlfriendlystr(row[c]), self.all))
                + ")"
            )
        else:
            self.__insertnow = self.__insertexecutemany

    async def _before_insert(self, row, namemapping):
        self.__batch.append(pygrametl.project(self.all, row, namemapping))
        if len(self.__batch) >= self.__batchsize:
            await self.__insertnow()
        return True  # signal that we did something

    async def _before_lookup(self, keyvalues, namemapping):
        await self.__insertnow()

    def discard(self):
        """Drop the rows still waiting to be inserted."""
        self.__batch = []

    async def endload(self):
        """Finalize the load."""
        await self.__insertnow()

    async def __insertmultirow(self):
        if self.__batch:
            batch, self.__batch = self.__batch, []
            try:
                values = map(self.__rowtovalue, batch)
                insertsql = self.__basesql + ",".join(values)
                await self.targetconnection.execute(insertsql)
            except BaseException:  # includes task cancellation
                self.__batch = batch + self.__batch  # put the rows back, in order
                raise

    async def __insertexecutemany(self):
        if self.__batch:
            batch, self.__batch = self.__batch, []
            try:
                await self.targetconnection.executemany(self.insertsql, batch)
            except BaseException:
                self.__batch = batch + self.__batch
                raise

    @property
    def awaitingrows(self):
        """Return the amount of rows awaiting to be loaded into the table"""
        return len(self.__batch)


class _BaseBulkloadable(object):
    """Async port of pygrametl.tables._BaseBulkloadable. insert, endload and
    the bulk load itself are coroutines. The bulkloader may be a normal
    function or a coroutine function. Arguments are as in pygrametl.tables,
    plus targetconnection."""

    def __init__(
        self,
        name,
        atts,
        bulkloader,
        fieldsep="\t",
        rowsep="\n",
        nullsubst=None,
        tempdest=None,
        bulksize=500000,
        usefilename=False,
        strconverter=pygrametl.getdbfriendlystr,
        encoding=None,
        dependson=(),
        targetconnection=None,
    ):
        # endload_all(connection) selects tables by targetconnection, so a
        # table without one would silently never be flushed. Fail early.
        if targetconnection is None:
            targetconnection = getdefaulttargetconnection()
            if targetconnection is None:
                raise ValueError("No target connection available")
        self.targetconnection = targetconnection

        self.name = name
        self.atts = atts
        self.__close = False
        if tempdest is None:
            self.__close = True
            self.__namedtempfile = tempfile.NamedTemporaryFile()
            tempdest = self.__namedtempfile.file
            self.__filename = self.__namedtempfile.name
        else:
            if usefilename and not os.path.exists(tempdest.name):
                raise ValueError(
                    "Usefilename cannot be used with invalid "
                    "tempdest path '%s'" % tempdest.name
                )
            self.__filename = tempdest.name
        self.fieldsep = fieldsep
        self.rowsep = rowsep
        self.nullsubst = nullsubst
        self.bulkloader = bulkloader
        self.tempdest = tempdest

        self.bulksize = bulksize
        self.usefilename = usefilename
        self.strconverter = strconverter
        if encoding is not None:
            self.encoding = encoding
        else:
            self.encoding = locale.getpreferredencoding()

        # Ensure objects that are not tables are not silently removed
        if not (set(dependson) <= set(_alltables)):
            raise ValueError("Dependson must contain tables only")
        self.dependson = [d for d in dependson if hasattr(d, "_bulkloadnow")]

        self.__count = 0
        self.__ready = True

    def __preparetempfile(self):
        self.__namedtempfile = tempfile.NamedTemporaryFile()
        self.tempdest = self.__namedtempfile.file
        self.__filename = self.__namedtempfile.name
        self.__ready = True

    @property
    def awaitingrows(self):
        """Return the amount of rows awaiting to be loaded into the table"""
        return self.__count

    async def insert(self, row, namemapping={}):
        """Insert (eventually) a row into the table."""
        if not self.__ready:
            self.__preparetempfile()
        rawdata = [row[namemapping.get(att) or att] for att in self.atts]
        data = [self.strconverter(val, self.nullsubst) for val in rawdata]
        try:
            line = self.fieldsep.join(data)
        except TypeError as e:
            expl = "Could not join values into a single string"
            if self.nullsubst is None and None in rawdata:
                expl += ". A nullsubst must be defined."
            raise TypeError(expl, e)
        self.tempdest.write(bytes("%s%s" % (line, self.rowsep), self.encoding))
        self.__count += 1
        if self.__count >= self.bulksize:
            await self._bulkloadnow()

    async def _bulkloadnow(self):
        if self.__count == 0:
            return

        for b in self.dependson:
            await b._bulkloadnow()

        self.tempdest.flush()
        self.tempdest.seek(0)
        try:
            await _maybe_await(
                self.bulkloader(
                    self.name,
                    self.atts,
                    self.fieldsep,
                    self.rowsep,
                    self.nullsubst,
                    self.usefilename and self.__filename or self.tempdest,
                )
            )
        except BaseException:  # includes task cancellation
            # Keep the rows and move back to the end of the file, so later
            # inserts append instead of overwriting, and discard() still works
            self.tempdest.seek(0, os.SEEK_END)
            raise
        self.tempdest.seek(0)
        self.tempdest.truncate(0)
        self.__count = 0

    async def endload(self):
        """Finalize the load."""
        await self._bulkloadnow()
        if self.__close:
            try:
                self.__namedtempfile.close()
            except OSError:
                pass
            self.__ready = False

    def discard(self):
        """Drop the rows still waiting to be loaded (use after a rollback)."""
        if self.__ready:
            self.tempdest.seek(0)
            self.tempdest.truncate(0)
        self.__count = 0


class BulkFactTable(_BaseBulkloadable):
    """Async port of pygrametl.tables.BulkFactTable. Adds facts only, reads
    are not supported. The bulkloader
    m(name, attributes, fieldsep, rowsep, nullsubst, tempdest) may be a
    normal function or a coroutine function."""

    def __init__(
        self,
        name,
        keyrefs,
        measures,
        bulkloader,
        fieldsep="\t",
        rowsep="\n",
        nullsubst=None,
        tempdest=None,
        bulksize=500000,
        usefilename=False,
        strconverter=pygrametl.getdbfriendlystr,
        encoding=None,
        dependson=(),
        targetconnection=None,
    ):
        self.keyrefs = keyrefs
        self.measures = measures
        self.all = [k for k in keyrefs] + [m for m in measures]

        _BaseBulkloadable.__init__(
            self,
            name=name,
            atts=[k for k in keyrefs] + [m for m in measures],
            bulkloader=bulkloader,
            fieldsep=fieldsep,
            rowsep=rowsep,
            nullsubst=nullsubst,
            tempdest=tempdest,
            bulksize=bulksize,
            usefilename=usefilename,
            strconverter=strconverter,
            encoding=encoding,
            dependson=dependson,
            targetconnection=targetconnection,
        )

        _alltables.append(self)