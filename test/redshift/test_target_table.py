import unittest
from mock import patch, Mock
from arbalest.redshift import TargetTable
from arbalest.redshift.schema import JsonObject, Property
from arbalest.sql import Database
from test import TABLE_NAME


class TargetTableShould(unittest.TestCase):
    def setUp(self):
        self.schema = JsonObject(TABLE_NAME, Property('id', 'VARCHAR(36)'))

    def test_create_when_column_name_not_defined(self):
        with patch.object(Database, 'execute') as execute:
            schema = JsonObject(TABLE_NAME,
                                Property('property1', 'VARCHAR(10)'),
                                Property('property2', 'TIMESTAMP'))
            table = TargetTable(schema, Database(Mock()))

            table.create()

            expected_sql = 'CREATE TABLE {0} (property1 VARCHAR(10), ' \
                           'property2 TIMESTAMP)'.format(TABLE_NAME)

            execute.assert_called_once_with(expected_sql)

    def test_create_when_column_name_defined(self):
        with patch.object(Database, 'execute') as execute:
            schema = JsonObject(TABLE_NAME,
                                Property('property1', 'VARCHAR(10)',
                                         'someColumn'),
                                Property('property2', 'TIMESTAMP',
                                         'anotherColumn'))
            table = TargetTable(schema, Database(Mock()))

            table.create()

            expected_sql = 'CREATE TABLE {0} (someColumn VARCHAR(10), ' \
                           'anotherColumn TIMESTAMP)'.format(TABLE_NAME)

            execute.assert_called_once_with(expected_sql)

    def test_create_when_column_name_not_defined_for_nested_property(
            self):
        with patch.object(Database, 'execute') as execute:
            schema = JsonObject(TABLE_NAME,
                                Property('property1', 'VARCHAR(10)'),
                                Property('property2',
                                         Property('timestamp', 'TIMESTAMP')))
            table = TargetTable(schema, Database(Mock()))

            table.create()

            expected_sql = 'CREATE TABLE {0} (property1 VARCHAR(10), ' \
                           'property2_timestamp TIMESTAMP)'.format(TABLE_NAME)

            execute.assert_called_once_with(
                expected_sql)

    def test_create_when_column_name_defined_for_nested_property(self):
        with patch.object(Database, 'execute') as execute:
            schema = JsonObject(TABLE_NAME,
                                Property('property1', 'VARCHAR(10)'),
                                Property('property2',
                                         Property('timestamp', 'TIMESTAMP',
                                                  'anotherColumn')))
            table = TargetTable(schema, Database(Mock()))

            table.create()

            expected_sql = 'CREATE TABLE {0} (property1 VARCHAR(10), ' \
                           'anotherColumn TIMESTAMP)'.format(TABLE_NAME)

            execute.assert_called_once_with(
                expected_sql)

    def test_stage_update_when_column_name_not_defined(self):
        with patch.object(Database, 'execute') as execute:
            schema = JsonObject(TABLE_NAME,
                                Property('property1', 'VARCHAR(10)'),
                                Property('property2', 'TIMESTAMP'))
            table = TargetTable(schema, Database(Mock()))

            table.stage_update()

            expected_sql = 'CREATE TABLE {0}_update (property1 VARCHAR(10), ' \
                           'property2 TIMESTAMP)'.format(TABLE_NAME)

            execute.assert_called_once_with(
                expected_sql)

    def test_stage_update_when_column_name_defined(self):
        with patch.object(Database, 'execute') as execute:
            schema = JsonObject(TABLE_NAME,
                                Property('property1', 'VARCHAR(10)',
                                         'someColumn'),
                                Property('property2', 'TIMESTAMP',
                                         'anotherColumn'))
            table = TargetTable(schema, Database(Mock()))

            table.stage_update()

            expected_sql = 'CREATE TABLE {0}_update (someColumn VARCHAR(10), ' \
                           'anotherColumn TIMESTAMP)'.format(TABLE_NAME)

            execute.assert_called_once_with(expected_sql)

    def test_stage_update_when_column_name_not_defined_for_nested_property(
            self):
        with patch.object(Database, 'execute') as execute:
            schema = JsonObject(TABLE_NAME,
                                Property('property1', 'VARCHAR(10)'),
                                Property('property2',
                                         Property('timestamp', 'TIMESTAMP')))
            table = TargetTable(schema, Database(Mock()))

            table.stage_update()

            expected_sql = 'CREATE TABLE {0}_update (property1 VARCHAR(10), ' \
                           'property2_timestamp TIMESTAMP)'.format(TABLE_NAME)

            execute.assert_called_once_with(
                expected_sql)

    def test_stage_update_when_column_name_defined_for_nested_property(self):
        with patch.object(Database, 'execute') as execute:
            schema = JsonObject(TABLE_NAME,
                                Property('property1', 'VARCHAR(10)'),
                                Property('property2',
                                         Property('timestamp', 'TIMESTAMP',
                                                  'anotherColumn')))
            table = TargetTable(schema, Database(Mock()))

            table.stage_update()

            expected_sql = 'CREATE TABLE {0}_update (property1 VARCHAR(10), ' \
                           'anotherColumn TIMESTAMP)'.format(TABLE_NAME)

            execute.assert_called_once_with(
                expected_sql)
