from psycopg2.extensions import AsIs
from arbalest.core import PipelineStep
from arbalest.redshift.manifest import Manifest
from arbalest.redshift.runner import S3JsonStepRunner
from arbalest.s3 import normalize_path


class BulkCopyFromS3JsonStep(PipelineStep):
    def __init__(self, metadata, source, schema, aws_access_key_id,
                 aws_secret_access_key, bucket, table):
        self.metadata = metadata
        self.source = source
        self.schema = schema
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket = bucket
        self.table = table
        self.runner = S3JsonStepRunner(metadata, schema, bucket, table)
        self.sql = SqlStep(table.database)
        self.max_error_count = 1

    @property
    def source_key(self):
        return normalize_path(self.source)

    @property
    def schema_key(self):
        return self.runner.schema_key

    @property
    def source_url(self):
        return 's3://{0}/{1}'.format(self.bucket.name, self.source_key)

    @property
    def schema_url(self):
        return self.runner.schema_url

    @property
    def copy_sql(self):
        return "COPY %s FROM '%s' " \
               "CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' " \
               "JSON '%s' " \
               "TIMEFORMAT 'auto' " \
               "MAXERROR %s"

    @property
    def validate_sql(self):
        return self.copy_sql + " NOLOAD"

    def run(self):
        self.runner.stage()
        self.__execute(self.copy_sql)
        self.__promote()
        self.runner.commit()

    def validate(self):
        self.runner.stage()
        self.__execute(self.validate_sql)
        self.__promote()
        self.runner.rollback()

    def __execute(self, sql):
        self.table.stage_update()
        self.sql.execute((
            sql, self.schema.update_table, self.source_url,
            self.aws_access_key_id,
            self.aws_secret_access_key, self.schema_url, self.max_error_count))

    def __promote(self):
        if self.table.exists():
            self.table.drop()
        self.table.promote_update()


class ManifestCopyFromS3JsonStep(PipelineStep):
    def __init__(self, metadata, source, schema, aws_access_key_id,
                 aws_secret_access_key, bucket, table):
        self.metadata = metadata
        self.source = source
        self.schema = schema
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.bucket = bucket
        self.table = table
        self.runner = S3JsonStepRunner(metadata, schema, bucket, table)
        self.manifest = Manifest(metadata, source, schema, bucket)
        self.sql = SqlStep(table.database)
        self.max_error_count = 1

    @property
    def schema_key(self):
        return self.runner.schema_key

    @property
    def schema_url(self):
        return self.runner.schema_url

    @property
    def copy_sql(self):
        return "COPY %s FROM '%s' " \
               "CREDENTIALS 'aws_access_key_id=%s;" \
               "aws_secret_access_key=%s' " \
               "JSON '%s' " \
               "TIMEFORMAT 'auto' " \
               "MANIFEST " \
               "MAXERROR %s"

    @property
    def validate_sql(self):
        return self.copy_sql + " NOLOAD"

    def run(self):
        self.runner.stage()
        updated_journal = self.manifest.save()
        self.__execute(self.copy_sql)
        self.manifest.commit(updated_journal)
        self.runner.commit()

    def validate(self):
        self.runner.stage()
        self.manifest.save()
        self.__execute(self.validate_sql)
        self.runner.rollback()

    def __execute(self, sql):
        if self.table.exists() and not self.manifest.journal_exists():
            self.table.drop()
            self.table.create()
        elif not self.table.exists():
            self.table.create()
        self.sql.execute((sql, self.schema.table, self.manifest.manifest_url,
                          self.aws_access_key_id, self.aws_secret_access_key,
                          self.schema_url,
                          self.max_error_count))


class SqlStep(PipelineStep):
    def __init__(self, database, *args):
        self.database = database
        self.statements = list(args)

    def run(self):
        self.database.open()
        self.execute(self.statements)
        self.database.commit()

    def validate(self):
        self.database.open()
        self.execute(self.statements)
        self.database.rollback()

    def execute(self, statements):
        if not isinstance(statements, list):
            self.__execute(statements)
        else:
            for statement in statements:
                self.__execute(statement)

    def __execute(self, statement):
        if isinstance(statement, basestring):
            self.database.execute(statement)
        else:
            query = statement[0]
            params = statement[1:]
            self.database.execute(query, tuple([AsIs(x) for x in params]))
