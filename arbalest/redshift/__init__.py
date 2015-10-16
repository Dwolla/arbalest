import sqlite3
from arbalest.redshift.manifest import SqlManifest
from arbalest.redshift.step import BulkCopyFromS3JsonStep, SqlStep, \
    ManifestCopyFromS3JsonStep
from psycopg2.extensions import AsIs
from arbalest.core import Pipeline
from arbalest.s3 import Bucket
from arbalest.sql import Database


class TargetTable(object):
    def __init__(self, schema, database):
        self.schema = schema
        self.database = database

    def exists(self):
        self.database.execute(
            "SELECT 1 FROM pg_tables WHERE tablename = '%s'",
            (AsIs(self.schema.table),))
        return self.database.cursor.fetchone() is not None

    def create(self):
        columns = [
            '{0} {1}'.format(schema_property.column_name, schema_property.type)
            for schema_property in self.schema.schema]
        sql = 'CREATE TABLE {0} ({1})'.format(self.schema.table,
                                              ', '.join(columns))
        return self.database.execute(sql)

    def stage_update(self):
        columns = [
            '{0} {1}'.format(schema_property.column_name, schema_property.type)
            for schema_property in self.schema.schema]
        sql = 'CREATE TABLE {0} ({1})'.format(self.schema.update_table,
                                              ', '.join(columns))
        return self.database.execute(sql)

    def promote_update(self):
        return self.database.execute('ALTER TABLE %s RENAME TO %s',
                                     (AsIs(self.schema.update_table),
                                      AsIs(self.schema.table),))

    def insert_update(self):
        return self.database.execute('INSERT INTO %s SELECT * FROM %s',
                                     (AsIs(self.schema.table),
                                      AsIs(self.schema.update_table),))

    def drop(self):
        return self.database.execute('DROP TABLE %s',
                                     (AsIs(self.schema.table),))


class S3BulkCopyPipeline(Pipeline):
    def __init__(self, aws_access_key_id, aws_secret_access_key, bucket,
                 db_connection):
        super(S3BulkCopyPipeline, self).__init__()

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        if isinstance(bucket, basestring):
            self.bucket = Bucket(aws_access_key_id, aws_secret_access_key,
                                 bucket)
        else:
            self.bucket = bucket

        if isinstance(db_connection, Database):
            self.database = db_connection
        else:
            self.database = Database(db_connection)

    def step(self, metadata, source, schema, max_error_count=1):
        bulk_copy_step = BulkCopyFromS3JsonStep(metadata=metadata,
                                                source=source,
                                                schema=schema,
                                                aws_access_key_id=self.aws_access_key_id,
                                                aws_secret_access_key=self.aws_secret_access_key,
                                                bucket=self.bucket,
                                                table=TargetTable(schema,
                                                                  self.database))
        bulk_copy_step.max_error_count = max_error_count
        self.steps().append(bulk_copy_step)
        return self

    def sql(self, *args):
        self.steps().append(SqlStep(self.database, *args))
        return self


class S3CopyPipeline(Pipeline):
    def __init__(self, aws_access_key_id, aws_secret_access_key, bucket,
                 db_connection):
        super(S3CopyPipeline, self).__init__()

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        if isinstance(bucket, basestring):
            self.bucket = Bucket(aws_access_key_id, aws_secret_access_key,
                                 bucket)
        else:
            self.bucket = bucket

        if isinstance(db_connection, Database):
            self.database = db_connection
        else:
            self.database = Database(db_connection)

    def bulk_copy(self, metadata, source, schema, max_error_count=1):
        bulk_copy_step = BulkCopyFromS3JsonStep(metadata=metadata,
                                                source=source,
                                                schema=schema,
                                                aws_access_key_id=self.aws_access_key_id,
                                                aws_secret_access_key=self.aws_secret_access_key,
                                                bucket=self.bucket,
                                                table=TargetTable(schema,
                                                                  self.database))
        self.__add_copy_step(bulk_copy_step, max_error_count)
        return self

    def manifest_copy(self, metadata, source, schema, max_error_count=1):
        manifest_copy_step = ManifestCopyFromS3JsonStep(metadata=metadata,
                                                        source=source,
                                                        schema=schema,
                                                        aws_access_key_id=self.aws_access_key_id,
                                                        aws_secret_access_key=self.aws_secret_access_key,
                                                        bucket=self.bucket,
                                                        table=TargetTable(
                                                            schema,
                                                            self.database))
        self.__add_copy_step(manifest_copy_step, max_error_count)
        return self

    def sql_manifest_copy(self, metadata, source, schema, max_error_count=1):
        sql_manifest_copy_step = ManifestCopyFromS3JsonStep(metadata=metadata,
                                                            source=source,
                                                            schema=schema,
                                                            aws_access_key_id=self.aws_access_key_id,
                                                            aws_secret_access_key=self.aws_secret_access_key,
                                                            bucket=self.bucket,
                                                            table=TargetTable(
                                                                schema,
                                                                self.database))

        sql_manifest = SqlManifest(metadata, source, schema, self.bucket,
                                   self.database)
        sql_manifest.database = Database(
            sqlite3.connect(sql_manifest.journal_file_name))
        sql_manifest_copy_step.manifest = sql_manifest
        self.__add_copy_step(sql_manifest_copy_step, max_error_count)
        return self

    def sql(self, *args):
        self.steps().append(SqlStep(self.database, *args))
        return self

    def __add_copy_step(self, manifest_copy_step, max_error_count):
        manifest_copy_step.max_error_count = max_error_count
        self.steps().append(manifest_copy_step)
