import os
import types
import unittest
import sqlite3
import psycopg2
from arbalest.sql import Database
from test import CONNECTION


class DatabaseShould(unittest.TestCase):
    def test_query_table_with_sqlite3_connection(self):
        database_file = 'sometable.db'
        db = Database(sqlite3.connect(database_file))
        db.open()
        db.execute('CREATE TABLE IF NOT EXISTS SomeTable (id VARCHAR(255))')
        db.execute('INSERT INTO SomeTable VALUES (1)')
        db.execute('INSERT INTO SomeTable VALUES (?)', (2,))
        db.execute('SELECT * FROM SomeTable')

        rows = db.fetchall()
        self.assertEqual(True, isinstance(rows, types.GeneratorType))
        self.assertEqual(['1', '2'], [row[0] for row in rows])
        db.rollback()
        db.close()
        os.remove(database_file)

    def test_query_table_with_psycopg2_connection(self):
        db = Database(psycopg2.connect(CONNECTION))
        db.open()
        db.execute('CREATE TABLE SomeTable (id VARCHAR(255))')
        db.execute('INSERT INTO SomeTable VALUES (1)')
        db.execute('INSERT INTO SomeTable VALUES (2)')
        db.execute('SELECT * FROM SomeTable')

        rows = db.fetchall()
        self.assertEqual(True, isinstance(rows, types.GeneratorType))
        self.assertEqual([('1',), ('2',)], list(rows))
        db.rollback()
