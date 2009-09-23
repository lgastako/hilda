#!/usr/bin/env python
import unittest
import sqlite3

from hilda import Database

# TODO: Dialects


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

    def tearDown(self):
        self.db1.close()
        self.db2.close()


if __name__ == "__main__":
    unittest.main()
