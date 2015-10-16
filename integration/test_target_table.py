import unittest

import psycopg2
from psycopg2.extensions import AsIs
from arbalest.redshift import TargetTable
from arbalest.sql import Database
from test import TABLE_NAME, CONNECTION
from arbalest.redshift.schema import JsonObject, Property


class TargetTableShould(unittest.TestCase):
    def setUp(self):
        self.schema = JsonObject(TABLE_NAME, Property('id', 'VARCHAR(36)'))
        self.database = Database(psycopg2.connect(CONNECTION))
        self.database.open()

    def tearDown(self):
        self.database.rollback()

    def assertColumns(self, table_name, schema):
        self.database.execute(
            "SELECT column_name, udt_name FROM information_schema.columns "
            "WHERE table_name = '%s'",
            (AsIs(table_name),))
        columns = self.database.cursor.fetchall()

        self.assertEqual(len(schema.schema), len(columns))

        for i, column in enumerate(columns):
            self.assertEqual(schema.schema[i].column_name.lower(), column[0])
            self.assertEqual(True, column[1] in schema.schema[i].type.lower())

    def test_not_exist(self):
        table = TargetTable(self.schema, self.database)

        self.assertEqual(False, table.exists())

    def test_exist(self):
        table = TargetTable(self.schema, self.database)
        table.create()

        self.assertEqual(True, table.exists())

    def test_create_when_column_name_not_defined(self):
        schema = JsonObject(TABLE_NAME,
                            Property('property1', 'VARCHAR(10)'),
                            Property('property2', 'TIMESTAMP'))
        table = TargetTable(schema, self.database)
        table.create()

        self.assertColumns(schema.table, schema)

    def test_create_when_column_name_defined(self):
        schema = JsonObject(TABLE_NAME,
                            Property('property1', 'VARCHAR(10)', 'someColumn'),
                            Property('property2', 'TIMESTAMP', 'anotherColumn'))
        table = TargetTable(schema, self.database)
        table.create()

        self.assertColumns(schema.table, schema)

    def test_stage_update_when_column_name_not_defined(self):
        schema = JsonObject(TABLE_NAME,
                            Property('property1', 'VARCHAR(10)'),
                            Property('property2', 'TIMESTAMP'))
        table = TargetTable(schema, self.database)
        table.stage_update()

        self.assertColumns(schema.update_table, schema)

    def test_stage_update_when_column_name_defined(self):
        schema = JsonObject(TABLE_NAME,
                            Property('property1', 'VARCHAR(10)', 'someColumn'),
                            Property('property2', 'TIMESTAMP', 'anotherColumn'))
        table = TargetTable(schema, self.database)
        table.stage_update()

        self.assertColumns(schema.update_table, schema)

    def test_promote_update(self):
        table = TargetTable(self.schema, self.database)
        table.stage_update()
        table.promote_update()

        self.assertColumns(self.schema.table, self.schema)

    def test_insert_update(self):
        table = TargetTable(self.schema, self.database)
        table.create()
        table.stage_update()
        self.database.execute("INSERT INTO %s VALUES('%s')",
                              (AsIs(TABLE_NAME), AsIs(1),))
        self.database.execute("INSERT INTO %s VALUES('%s')",
                              (AsIs(self.schema.update_table), AsIs(2),))
        self.database.execute("INSERT INTO %s VALUES('%s')",
                              (AsIs(self.schema.update_table), AsIs(3),))

        table.insert_update()

        self.database.execute(
            "SELECT * FROM %s",
            (AsIs(TABLE_NAME),))
        values = list(self.database.cursor.fetchall())

        self.assertEqual([('1',), ('2',), ('3',)], values)

    def test_drop(self):
        table = TargetTable(self.schema, self.database)
        table.create()
        table.drop()

        self.assertEqual(False, table.exists())
