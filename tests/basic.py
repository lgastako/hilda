#!/usr/bin/env python
import unittest
import sqlite3

from hilda import Database

# TODO: Dialects
# TODO: Handle the notion of caching the meta data so it doesn't have to
#       reinterrogate the database for every instance of Database.


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.db1 = sqlite3.connect(":memory:")
        cursor = self.db1.cursor()
        cursor.execute("""CREATE TABLE foo (
                              id INTEGER PRIMARY KEY,
                              value VARCHAR(255)
                          );""")

        cursor.execute("""CREATE TABLE bar (
                              id INTEGER PRIMARY KEY,
                              value VARCHAR(255) NOT NULL
                          );""")

        self.db2 = sqlite3.connect(":memory:")
        cursor = self.db2.cursor()
        cursor.execute("CREATE TABLE baz (bif INTEGER PRIMARY KEY)")

    def test_can_list_tables(self):
        database = Database(self.db1)
        tables = database.tables()
        self.assertEqual(2, len(tables))
        expected_table_names = ("foo" ,"bar")
        for table in tables:
            self.assert_(bool(table))
            self.assert_(table.name in expected_table_names)

    def test_listed_tables_are_cached(self):
        database = Database(self.db2)
        table1 = database.tables()[0]
        table2 = database.tables()[0]
        self.assert_(table1 == table2)

    def test_listed_tables_are_not_cached_after_forget(self):
        database = Database(self.db2)
        table1 = database.tables()[0]
        database.forget()
        table2 = database.tables()[0]
        self.assert_(table1 != table2)

    def test_can_get_columns_from_table(self):
        database = Database(self.db2)
        table = database.tables()[0]
        columns = table.columns()
        self.assertEqual(1, len(columns)) # TODO: Should be 2 to incl "id"
        expected_column_names = ("bif")   # TODO: How do we get "id" ?
        for column in columns:
            self.assert_(bool(column))
            self.assert_(column.name in expected_column_names)

    def test_can_get_a_table_by_name(self):
        database = Database(self.db1)
        foo = database.get_table("foo")
        bar = database.get_table("bar")
        self.assertEqual("foo", foo.name)
        self.assertEqual("bar", bar.name)

    def test_can_insert_a_single_row_into_a_table(self):
        database = Database(self.db1)
        foo = database.get_table("foo")
        foo.insert(value="Kate Austin")

        cursor = self.db1.cursor()
        rows = cursor.execute("SELECT * FROM foo").fetchall()
        self.assertEqual(1, len(rows))
        self.assertEqual((1, "Kate Austin"), rows[0])

    def test_can_select_a_single_row_from_a_table_by_text_where_clause(self):
        database = Database(self.db1)
        foo = database.get_table("foo")
        foo.insert(value="Kate Austin")
        foo.insert(value="Juliet Burke")

        # We pull them out in reverse order just to avoid a simple
        # iteration satisfying the test by conincidence
        juliets = foo.select(where="id = 2")
        kates   = foo.select(where="id = 1")
        self.assertEqual(1, len(juliets))
        self.assertEqual(1, len(kates))

        juliet = juliets[0]
        kate   = kates[0]
        self.assertEqual("Juliet Burke", juliet.value)
        self.assertEqual("Kate Austin", kate.value)

    def test_can_select_all_rows_from_a_table_with_no_where_clause(self):
        database = Database(self.db1)
        foo = database.get_table("foo")
        foo.insert(value="Kate Austin")
        foo.insert(value="Juliet Burke")

        ladies = foo.select()
        self.assertEqual(2, len(ladies))

        self.assertEqual(["Kate Austin", "Juliet Burke"],
                         [lady.value for lady in ladies])

    def tearDown(self):
        self.db1.close()
        self.db2.close()


if __name__ == "__main__":
    unittest.main()
