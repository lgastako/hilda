from functools import wraps
from collections import defaultdict


def memoize(f):
    @wraps(f)
    def wrapped(self, *args, **kwargs):
        if not hasattr(self, "_memo_cache"):
            self._memo_cache = defaultdict(dict)
        memo_cache = self._memo_cache
        method_cache = self._memo_cache[f.__name__]
        key = (args, repr(kwargs))
        if key not in method_cache:
            method_cache[key] = apply(f, [self] + list(args), kwargs)
        return method_cache[key]
    return wrapped


class Column(object):
    def __init__(self, name):
        self.name = name


class Table(object):
    def __init__(self, driver, name):
        self.driver = driver
        self.name = name

    def columns(self):
        cursor = self.driver.cursor()
        rows = cursor.execute("PRAGMA table_info(%s)" % self.name).fetchall()
        return [Column(row[1]) for row in rows]


class Database(object):
    def __init__(self, driver):
        self.driver = driver

    @memoize
    def tables(self):
        cursor = self.driver.cursor()
        rows = cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table'
            ORDER BY name;
        """).fetchall()
        return [Table(self.driver, row[0]) for row in rows]

    def _get_table_map(self):
        # TODO: memoize maybe, etc. once this is finalized.  Also need
        # to handle schema.name etc
        return dict((table.name, table) for table in self.tables())

    def get_table(self, name):
        # Do we want to let this raise a KeyError if you specify a
        # table that doesn't exist or convert it to a custom
        # exception?
        return self._get_table_map()[name]

    def forget(self):
        if hasattr(self, "_memo_cache"):
            del self.__dict__["_memo_cache"]
