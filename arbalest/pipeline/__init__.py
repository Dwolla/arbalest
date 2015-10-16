import json
import datetime

from arbalest.s3 import normalize_path
from arbalest.redshift.schema import JsonObject


def table_name(table, suffix=None):
    table_suffix = '' if suffix is None else '_' + suffix
    return table + table_suffix


def is_day(date):
    try:
        date_format = '%Y-%m-%d'
        is_valid = datetime.datetime.strptime(date, date_format).strftime(
            date_format) == date
    except TypeError:
        is_valid = False
    except ValueError:
        is_valid = False

    return is_valid


def is_day_hour(date):
    try:
        date_format = '%Y-%m-%d/%H'
        is_valid = datetime.datetime.strptime(date, date_format).strftime(
            date_format) == date
    except TypeError:
        is_valid = False
    except ValueError:
        is_valid = False

    return is_valid


class S3SortedDataSources(object):
    def __init__(self, metadata, source, bucket, start=None,
                 end=None):
        self.metadata = metadata
        self.source = source
        self.bucket = bucket
        self.start = start
        self.end = end
        self.file_name = '{0}_source_journal.json'.format(source)

    @property
    def source_journal_key(self):
        return normalize_path('{0}/{1}'.format(self.metadata, self.file_name))

    def get(self):
        paths = self.__get_paths()

        start_key = normalize_path(
            '{0}/{1}'.format(self.source, self.start)) + '/'
        start = None if not self.start else self.__get_first_key_index(
            start_key, paths)

        end_key = normalize_path('{0}/{1}'.format(self.source, self.end)) + '/'
        end = None if not self.end else self.__get_first_key_index(end_key,
                                                                   paths) + 1

        return (normalize_path(path) for path in paths[start:end])

    def committed(self):
        return self.bucket.get(self.source_journal_key)

    def commit(self, point):
        self.bucket.save(self.source_journal_key,
                         json.dumps({'committed': point}))

    def rollback(self):
        self.bucket.delete(self.source_journal_key)

    @staticmethod
    def _get_date_from(cursor):
        date = None
        if is_day(cursor):
            date = cursor
        elif is_day_hour(cursor):
            date = cursor.split('/')[0]
        return date

    @staticmethod
    def __get_first_key_index(path, paths):
        indices = [i for i, s in enumerate(paths) if path in s]
        return indices[0]

    def __get_directory_keys(self, path):
        try:
            return [k.name for k in self.bucket.list(path, '/') if
                    k.name.endswith('/')]
        except StopIteration:
            return []

    def __get_paths(self):
        paths = []
        branches = self.__get_directory_keys(normalize_path(self.source) + '/')

        start_date = self._get_date_from(self.start)
        start = None if not start_date else branches.index(
            normalize_path('{0}/{1}'.format(self.source, start_date)) + '/')

        end_date = self._get_date_from(self.end)
        end = None if not end_date else branches.index(
            normalize_path('{0}/{1}'.format(self.source, end_date)) + '/') + 1

        if is_day_hour(self.start) or is_day_hour(self.end):
            for branch in branches[start:end]:
                children = self.__get_directory_keys(
                    normalize_path(branch) + '/')
                if children:
                    paths = paths + children
                else:
                    paths.append(branch)
        else:
            paths = branches

        return paths


class _SqlSeriesDataUpdate(object):
    def __init__(self, target_table, series_column, start,
                 source_tables):
        self.target_table = target_table
        self.series_column = series_column
        self.start = start
        self.source_tables = source_tables

    def statements(self):
        union_target_tables_sql = ' union '.join(
            ['select * from {0}'.format(table) for table in self.source_tables])
        create_or_replace_update_sql = 'create or replace view {0}_update as ' \
                                       '{1}'.format(self.target_table,
                                                    union_target_tables_sql)
        delete_sql = "delete from {0} where {1} >= '%s'".format(
            self.target_table, self.series_column)
        insert_into_sql = 'insert into {0} select * from {0}_update'.format(
            self.target_table)
        delete_staging_sql = 'drop view {0}_update'.format(self.target_table)

        drop_time_series_statements = ['drop table {0}'.format(table) for
                                       table in
                                       self.source_tables]
        return [create_or_replace_update_sql,
                (delete_sql, self.start), insert_into_sql,
                delete_staging_sql] + drop_time_series_statements


class SqlTimeSeriesImport(object):
    def __init__(self, destination_table, update_date, sources, *args):
        self.destination_table = destination_table
        self.update_date = update_date
        self.sources = sources
        self.properties = args

    def bulk_copy(self, pipeline, metadata, max_error, order_by_column):
        dates = []
        source_tables = []
        for source in self.sources.get():
            date = self.__get_date_from_path(source)
            target_table = '{0}'.format(
                source.replace('-', '_').replace('.', '_').replace('/', '_'))

            pipeline.bulk_copy(metadata=metadata,
                               source=source,
                               schema=JsonObject(target_table,
                                                 *self.properties),
                               max_error_count=max_error)

            self.sources.commit(self.update_date or date)
            dates.append(date)
            source_tables.append(target_table)

        update_statements = _SqlSeriesDataUpdate(
            target_table=self.destination_table,
            series_column=order_by_column,
            start=self.update_date or dates[0],
            source_tables=source_tables).statements()
        pipeline.sql(*update_statements)

    @staticmethod
    def __get_date_from_path(source):
        try:
            return datetime.datetime.strptime(source.split('/')[-1],
                                              '%Y-%d-%m').strftime('%Y-%d-%m')
        except ValueError:
            return None
