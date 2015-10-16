import unittest
from mock import Mock
from arbalest.pipeline import SqlTimeSeriesImport
from arbalest.redshift.schema import Property, JsonObject


def assert_schema_equal(test_case, expected, actual):
    test_case.assertEqual(expected.table, actual.table)
    test_case.assertEqual(expected.update_table, actual.update_table)
    test_case.assertEqual(expected.paths(), actual.paths())
    for i, _ in enumerate(expected.schema):
        expected_property = expected.schema[i]
        actual_property = actual.schema[i]
        test_case.assertEqual(expected_property.name, actual_property.name)
        test_case.assertEqual(expected_property.type, actual_property.type)
        test_case.assertEqual(expected_property.column_name,
                              actual_property.column_name)


class SqlTimeSeriesUpdateShould(unittest.TestCase):
    def test_bulk_copy(self):
        paths = ['test.table/2014-11-10', 'test.table/2014-11-11']
        properties = Property('userid', 'VARCHAR(36)'), Property('timestamp',
                                                                 'TIMESTAMP')
        series_column = 'test'
        max_error = 20

        import_pipeline = Mock()
        sources = Mock()
        sources.get = Mock(return_value=paths)

        time_series_import = SqlTimeSeriesImport('test_table', '2014-11-10',
                                                 sources,
                                                 *properties)
        time_series_import.bulk_copy(import_pipeline, '', max_error,
                                     series_column)

        first_bulk_copy, second_bulk_copy = \
            [c[1] for c in import_pipeline.bulk_copy.call_args_list]

        self.__assert_bulk_copy(first_bulk_copy, '', paths[0],
                                self.__expected_schema('test_table_2014_11_10'),
                                max_error)
        self.__assert_bulk_copy(second_bulk_copy, '', paths[1],
                                self.__expected_schema('test_table_2014_11_11'),
                                max_error)
        self.assertEqual(import_pipeline.bulk_copy.call_count, 2)
        import_pipeline.sql.assert_called_once_with(
            *self.expected_update_sql(series_column))

    def test_bulk_copy_for_hourly_time_series(self):
        paths = ['test.table/2014-11-10/00', 'test.table/2014-11-11/01']
        properties = Property('userid', 'VARCHAR(36)'), Property('timestamp',
                                                                 'TIMESTAMP')
        series_column = 'test'
        max_error = 20

        import_pipeline = Mock()
        sources = Mock()
        sources.get = Mock(return_value=paths)

        time_series_import = SqlTimeSeriesImport('test_table',
                                                 '2014-11-10 00:00:00', sources,
                                                 *properties)
        time_series_import.bulk_copy(import_pipeline, '', max_error,
                                     series_column)

        first_bulk_copy, second_bulk_copy = \
            [c[1] for c in import_pipeline.bulk_copy.call_args_list]

        self.__assert_bulk_copy(first_bulk_copy, '', paths[0],
                                self.__expected_schema(
                                    'test_table_2014_11_10_00'),
                                max_error)
        self.__assert_bulk_copy(second_bulk_copy, '', paths[1],
                                self.__expected_schema(
                                    'test_table_2014_11_11_01'),
                                max_error)
        self.assertEqual(import_pipeline.bulk_copy.call_count, 2)
        import_pipeline.sql.assert_called_once_with(
            *self.expected_hourly_update_sql(series_column))

    def __assert_bulk_copy(self, bulk_copy_argument_group,
                           metadata, source, schema,
                           max_error_count):
        self.assertEqual(bulk_copy_argument_group['metadata'], metadata)
        self.assertEqual(bulk_copy_argument_group['source'], source)
        assert_schema_equal(self, schema, bulk_copy_argument_group['schema'])
        self.assertEqual(bulk_copy_argument_group['max_error_count'],
                         max_error_count)

    @staticmethod
    def __expected_schema(table):
        return JsonObject(table,
                          Property('userid', 'VARCHAR(36)'),
                          Property('timestamp', 'TIMESTAMP'))

    @staticmethod
    def expected_update_sql(series_column):
        expected_staging_sql = "create or replace view " \
                               "test_table_update as " \
                               "select * from test_table_2014_11_10 " \
                               "union " \
                               "select * from test_table_2014_11_11"
        expected_delete_sql = ("delete from test_table "
                               "where {0} >= '%s'".format(series_column),
                               '2014-11-10')
        expected_insert_into_sql = 'insert into test_table ' \
                                   'select * from test_table_update'
        expected_delete_staging_sql = 'drop view test_table_update'
        expected_drop_first_time_series_sql = 'drop table ' \
                                              'test_table_2014_11_10'
        expected_drop_second_time_series_sql = 'drop table ' \
                                               'test_table_2014_11_11'

        return [expected_staging_sql, expected_delete_sql,
                expected_insert_into_sql, expected_delete_staging_sql,
                expected_drop_first_time_series_sql,
                expected_drop_second_time_series_sql]

    @staticmethod
    def expected_hourly_update_sql(series_column):
        expected_staging_sql = "create or replace view " \
                               "test_table_update as " \
                               "select * from test_table_2014_11_10_00 " \
                               "union " \
                               "select * from test_table_2014_11_11_01"
        expected_delete_sql = ("delete from test_table "
                               "where {0} >= '%s'".format(series_column),
                               '2014-11-10 00:00:00')
        expected_insert_into_sql = 'insert into test_table ' \
                                   'select * from test_table_update'
        expected_delete_staging_sql = 'drop view test_table_update'
        expected_drop_first_time_series_sql = 'drop table ' \
                                              'test_table_2014_11_10_00'
        expected_drop_second_time_series_sql = 'drop table ' \
                                               'test_table_2014_11_11_01'

        return [expected_staging_sql, expected_delete_sql,
                expected_insert_into_sql, expected_delete_staging_sql,
                expected_drop_first_time_series_sql,
                expected_drop_second_time_series_sql]
