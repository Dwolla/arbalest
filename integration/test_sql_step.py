import unittest
from arbalest.redshift import TargetTable
from psycopg2.extensions import AsIs
from arbalest.redshift.schema import JsonObject, Property
from arbalest.redshift.step import SqlStep
from arbalest.sql import Database
import psycopg2
from test import CONNECTION, TABLE_NAME


class SqlStepShould(unittest.TestCase):
    def setUp(self):
        self.schema = JsonObject(TABLE_NAME, Property('id', 'VARCHAR(36)'))
        self.database = Database(psycopg2.connect(CONNECTION))
        self.database.open()
        table = TargetTable(self.schema, self.database)
        table.create()
        self.database.commit()

    def tearDown(self):
        self.database.open()
        self.database.execute('DROP TABLE IF EXISTS %s', (AsIs(TABLE_NAME),))
        self.database.commit()

    def test_run(self):
        SqlStep(self.database, ("INSERT INTO %s VALUES('%s')", TABLE_NAME, 1),
                ("INSERT INTO %s VALUES('%s')", TABLE_NAME, 2),
                ("INSERT INTO {0} VALUES ('3')".format(TABLE_NAME))).run()

        self.database.open()
        self.database.execute(
            "SELECT COUNT(*) FROM %s",
            (AsIs(TABLE_NAME),))
        count = int(self.database.cursor.fetchone()[0])

        self.assertEqual(3, count)

        self.database.execute(
            "SELECT * FROM %s",
            (AsIs(TABLE_NAME),))
        values = list(self.database.cursor.fetchall())

        self.assertEqual([('1',), ('2',), ('3',)], values)

    def test_execute(self):
        self.database.open()
        sql = SqlStep(self.database)
        sql.execute(("INSERT INTO %s VALUES('%s')", TABLE_NAME, 1))
        sql.execute(("INSERT INTO %s VALUES('%s')", TABLE_NAME, 2))
        sql.execute(("INSERT INTO {0} VALUES ('3')".format(TABLE_NAME)))
        self.database.commit()

        self.database.open()
        self.database.execute(
            "SELECT COUNT(*) FROM %s",
            (AsIs(TABLE_NAME),))
        count = int(self.database.cursor.fetchone()[0])

        self.assertEqual(3, count)

        self.database.execute(
            "SELECT * FROM %s",
            (AsIs(TABLE_NAME),))
        values = list(self.database.cursor.fetchall())

        self.assertEqual([('1',), ('2',), ('3',)], values)
