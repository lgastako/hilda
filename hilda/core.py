import operator
import copy

from collections import namedtuple

from hilda.memoizer import memoize
from hilda.memoizer import unmemoize_instance

from hilda.exceptions import NoResultFound
from hilda.exceptions import TooManyResultsFound


def identity(x):
    return x


def colonize(column):
    return ":" + column


def sql_group(s):
    return "(%s)" % s


def _bind_selection(op):

    def _bound_selection(self, other):
        return Selection(self, op, other)

    return _bound_selection


class Column(object):

    def __init__(self, name, table, alias=None):
        self.name = name
        self.table = table
        self.alias = alias

    __eq__ = _bind_selection("=")
    __ne__ = _bind_selection("<>")
    __lt__ = _bind_selection("<")
    __gt__ = _bind_selection(">")
    __le__ = _bind_selection("<=")
    __ge__ = _bind_selection(">=")

    def __call__(self, alias):
        aliased_column = copy.copy(self)
        aliased_column.alias = alias
        return aliased_column

    @property
    def aliased_name(self):
        return self.alias or self.name


class SelectMixin(object):

    def get_cursor(self):
        raise NotImplementedError("Subclasses must implement.")

    def _tables_clause(self):
        raise NotImplementedError("Subclasses must implement.")

    _base_where = NotImplemented

    def select(self, where=None, limit=None):
        cursor = self.get_cursor()
        sql = "SELECT * FROM %s" % self._tables_clause()
        base_where = self._base_where
        if base_where or where:
            sql += " WHERE "
        if base_where:
            sql += base_where
        if where:
            if base_where:
                sql += " AND "
            sql += where
        if limit:
            sql += " LIMIT %d" + limit
        return map(self.record._make, cursor.execute(sql).fetchall())

    def select_where(self, limit=None, **kwargs):
        cursor = self.get_cursor()

        sql = "SELECT * FROM %s" % self._tables_clause()
        if len(kwargs):
            columns = kwargs.keys()
            column_param_pairs = [(column, colonize(column))
                                  for column in columns]
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


class Table(SelectMixin):

    def __init__(self, driver, name):
        super(Table, self).__init__()
        self.driver = driver
        self.name = name
        self._base_where = None

    def get_cursor(self):
        return self.driver.cursor()

    def _tables_clause(self):
        return self.name

    @memoize
    def columns(self):
        cursor = self.get_cursor()
        rows = cursor.execute("PRAGMA table_info(%s)" % self.name).fetchall()
        return [Column(row[1], self) for row in rows]

    def _make_column_property(self):
        columns = self.columns()
        column_tuple_type = namedtuple("%sColumns" % self.name.title(),
                                       [c.name for c in columns])
        column_dict = dict((str(c.name), c) for c in columns)
        return column_tuple_type(**column_dict)

    c = property(_make_column_property)

    def insert(self, **kwargs):
        cursor = self.get_cursor()
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
        unmemoize_instance(self)

    def create_join(self, *args, **kwargs):
        assert len(kwargs) == 0 or (len(kwargs) == 1 and "aliases" in kwargs)
        return Join(args, aliases=kwargs.get("aliases"))


class Alias(object):

    def __init__(self, entity, alias):
        self.entity = entity
        self.alias = alias


class Selection(object):

    def __init__(self, column1, operator, argument):
        self.column1 = column1
        self.operator = operator
        self.argument = argument

    def _render_argument(self):
        arg = self.argument
        if isinstance(arg, Column):
            return "%s.%s" % (arg.table.name, arg.name)
        return str(arg)

    def tables(self):
        result = set([self.column1.table])
        if isinstance(self.argument, Column):
            result.add(self.argument.table)
        return result

    def to_sql_fragment(self):
        return "%s.%s %s %s" % (self.column1.table.name,
                                self.column1.name,
                                self.operator,
                                self._render_argument())


class Join(SelectMixin):

    def __init__(self, selections, aliases=None):
        super(Join, self).__init__()
        self.selections = selections
        self.aliases = aliases

    def get_cursor(self):
        return self.selections[0].column1.table.get_cursor()

    def tables(self):
        return reduce(set.union, [s.tables() for s in self.selections])

    def _table_names(self):
        return tuple(sorted([t.name for t in self.tables()]))

    def _tables_clause(self):
        return ", ".join(self._table_names())

    def _namedtuple_name(self):
        return "".join([n.title() for n in self._table_names()])

    def _columns(self):
        # We probably can't do this this way for real, beacuse we'll
        # need to differentiate between columns with the same name in
        # different tables, but for now...
        return tuple(sorted(reduce(operator.add,
                                   [t.columns() for t in self.tables()])))

    def _aliased_columns(self):
        if self.aliases:

            def swap_in_alias(column):
                # TODO: efficiency++
                for aliased_column in self.aliases:
                    if aliased_column.table == column.table and \
                            aliased_column.name == column.name:
                        return aliased_column
                return column

        else:
            swap_in_alias = identity
        return map(swap_in_alias, self._columns())

    @memoize
    def _make_record(self):
        fields = [c.aliased_name for c in self._aliased_columns()]
        return namedtuple("%sRecord" % self._namedtuple_name(), fields)

    record = property(_make_record)

    @property
    def _base_where(self):
        return sql_group(" AND ".join([s.to_sql_fragment()
                                       for s in self.selections]))
