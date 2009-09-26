from functools import wraps
from collections import defaultdict
from collections import namedtuple


# TODO: manage lifecycle of cursor correctly.  Probably shouldn't be
# creating new ones all over the place?


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


def colonize(column):
    return ":" + column


class Column(object):
    def __init__(self, name, table):
        self.name = name
        self.table = table

    def __eq__(self, other):
        return Selection(self, "=", other)

    def __ne__(self, other):
        return Selection(self, "!=", other)


class Table(object):
    def __init__(self, driver, name):
        self.driver = driver
        self.name = name

    @memoize
    def columns(self):
        cursor = self.driver.cursor()
        rows = cursor.execute("PRAGMA table_info(%s)" % self.name).fetchall()
        return [Column(row[1], self) for row in rows]

    def _make_column_property(self):
        columns = self.columns()
        column_tuple_type = namedtuple("%sColumns" % self.name.title(),
                                       [c.name for c in columns])
        column_dict = dict((str(c.name), c) for c in columns)
        return column_tuple_type(**column_dict)

    c = property(_make_column_property)

    def insert(self, *args, **kwargs):
        cursor = self.driver.cursor()
        columns = kwargs.keys()
        column_specification = ", ".join(columns)
        value_template = ", ".join(map(colonize, columns))
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (self.name,
                                                   column_specification,
                                                   value_template)
        return cursor.execute(sql, kwargs)

    @memoize
    def _make_record(self):
        return namedtuple("%sRecord" % self.name.title(),
                          [c.name for c in self.columns()])

    record = property(_make_record)

    def select(self, where=None, limit=None):
        cursor = self.driver.cursor()
        sql = "SELECT * FROM %s" % self.name
        if where:
            sql += " WHERE " + where
        if limit:
            sql += " LIMIT %d" + limit
        return map(self.record._make, cursor.execute(sql).fetchall())

    def select_where(self, limit=None, **kwargs):
        cursor = self.driver.cursor()

        sql = "SELECT * FROM %s" % self.name
        if len(kwargs):
            columns = kwargs.keys()
            column_param_pairs = [(column, colonize(column)) for column in columns]
            clauses = ["%s = %s" % pair for pair in column_param_pairs]
            where = " AND ".join(clauses)
            sql += " WHERE " + where
        if limit is not None:
            sql += " LIMIT %d" % limit
        return map(self.record._make, cursor.execute(sql, kwargs).fetchall())

    def select_one_where(self, **kwargs):
        results = self.select_where(limit=2, **kwargs)
        if len(results) <= 0:
            raise NoResultFound
        if len(results) > 1:
            raise TooManyResultsFound
        return results[0]


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

class Selection(object):
    def __init__(self, column1, comparison_type, argument):
        self.__dict__.update(locals())
        del self.self

    def _render_argument(self):
        arg = self.argument
        if isinstance(arg, Column):
            return "%s.%s" % (arg.table.name, arg.name)
        return str(arg)

    def to_sql_fragment(self):
        return "%s.%s %s %s" % (self.column1.table.name,
                                self.column1.name,
                                self.comparison_type,
                                self._render_argument())
