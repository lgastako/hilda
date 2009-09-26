#!/usr/bin/env python
import unittest
import sqlite3

from hilda import Database

# TODO: Dialects
# TODO: Handle the notion of caching the meta data so it doesn't have to
#       reinterrogate the database for every instance of Database.


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.tv_movie_db = sqlite3.connect(":memory:")
        cursor = self.tv_movie_db.cursor()
        cursor.execute("""CREATE TABLE productions (
                              id INTEGER PRIMARY KEY,
                              type INTEGER NOT NULL,
                              name VARCHAR(255) NOT NULL
                          );""")
        cursor.execute("""CREATE TABLE episodes (
                              id INTEGER PRIMARY KEY,
                              production_id INTEGER NOT NULL,
                              season_number INTEGER,
                              episode_number INTEGER NOT NULL,
                              episode_name VARCHAR(255)
                          );""")
        cursor.execute("""CREATE TABLE episodes_productions (
                              episode_id INTEGER NOT NULL,
                              production_id INTEGER NOT NULL
                          );""")
        cursor.execute("""CREATE TABLE actors (
                              id INTEGER PRIMARY KEY,
                              first_name VARCHAR(255),
                              last_name VARCHAR(255),
                              headshot_url VARCHAR(4096)
                          );""")
        cursor.execute("""CREATE TABLE characters (
                              id INTEGER PRIMARY KEY,
                              name VARCHAR(255) NOT NULL
                          );""")
        cursor.execute("""CREATE TABLE roles(
                              id INTEGER PRIMARY KEY,
                              episode_id INTEGER NOT NULL,
                              actor_id INTEGER NOT NULL,
                              character_id INTEGER NOT NULL
                          );""")
        self.database = Database(self.tv_movie_db)

    def test_can_list_tables(self):
        tables = self.database.tables()
        self.assertEqual(6, len(tables))
        expected_table_names = ("productions",
                                "episodes",
                                "episodes_productions",
                                "actors",
                                "characters",
                                "roles")
        for table in tables:
            self.assert_(bool(table))
            self.assert_(table.name in expected_table_names)

    def test_listed_tables_are_cached(self):
        table1 = self.database.tables()[0]
        table2 = self.database.tables()[0]
        self.assert_(table1 == table2)

    def test_listed_tables_are_not_cached_after_forget(self):
        table1 = self.database.tables()[0]
        self.database.forget()
        table2 = self.database.tables()[0]
        self.assert_(table1 != table2)

    def test_can_get_columns_from_table(self):
        productions = self.database.get_table("productions")
        columns = productions.columns()
        self.assertEqual(3, len(columns))
        expected_column_names = ("id", "type", "name")
        for column in columns:
            self.assert_(bool(column))
            self.assert_(column.name in expected_column_names)

    def test_can_get_a_table_by_name(self):
        productions = self.database.get_table("productions")
        episodes = self.database.get_table("episodes")
        self.assertEqual("productions", productions.name)
        self.assertEqual("episodes", episodes.name)

    def test_can_insert_a_single_row_into_a_table(self):
        characters = self.database.get_table("characters")
        characters.insert(name="Kate Austin")

        cursor = self.tv_movie_db.cursor()
        rows = cursor.execute("SELECT * FROM characters").fetchall()
        self.assertEqual(1, len(rows))
        self.assertEqual((1, "Kate Austin"), rows[0])

    def test_can_select_a_single_row_from_a_table_by_text_where_clause(self):
        characters = self.database.get_table("characters")
        characters.insert(name="Kate Austin")
        characters.insert(name="Juliet Burke")

        # We pull them out in reverse order just to avoid a simple
        # iteration satisfying the test by conincidence
        juliets = characters.select(where="id = 2")
        kates   = characters.select(where="id = 1")
        self.assertEqual(1, len(juliets))
        self.assertEqual(1, len(kates))

        juliet = juliets[0]
        kate   = kates[0]
        self.assertEqual("Juliet Burke", juliet.name)
        self.assertEqual("Kate Austin", kate.name)

    def test_can_select_all_rows_from_a_table_with_no_where_clause(self):
        characters = self.database.get_table("characters")
        characters.insert(name="Kate Austin")
        characters.insert(name="Juliet Burke")

        ladies = characters.select()
        self.assertEqual(2, len(ladies))

        self.assertEqual(["Kate Austin", "Juliet Burke"],
                         [lady.name for lady in ladies])

    def test_can_do_simplified_select_where(self):
        characters = self.database.get_table("characters")
        characters.insert(name="Kate Austin")
        characters.insert(name="Juliet Burke")

        juliets = characters.select_where(name="Juliet Burke")
        self.assertEqual(1, len(juliets))
        self.assertEqual((2, "Juliet Burke"), juliets[0])

    def test_can_do_select_with_limit(self):
        characters = self.database.get_table("characters")
        characters.insert(name="Kate Austin")
        characters.insert(name="Juliet Burke")

        all = characters.select()
        some = characters.select_where(limit=1)
        self.assertEqual(2, len(all))
        self.assertEqual(1, len(some))

    def test_can_do_simplified_select_one_where(self):
        characters = self.database.get_table("characters")
        characters.insert(name="Kate Austin")
        characters.insert(name="Juliet Burke")

        juliet = characters.select_one_where(name="Juliet Burke")
        self.assertEqual((2, "Juliet Burke"), juliet)

    def test_can_access_table_from_column(self):
        characters = self.database.get_table("characters")
        name = characters.c.name
        self.assertEqual(characters, name.table)
    def tearDown(self):
        self.tv_movie_db.close()


if __name__ == "__main__":
    unittest.main()
