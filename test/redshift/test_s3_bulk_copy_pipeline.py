import unittest

from mock import Mock, create_autospec
from arbalest.core import PipelineException
from arbalest.redshift import S3BulkCopyPipeline, TargetTable
from arbalest.redshift.schema import JsonObject, Property
from arbalest.redshift.step import BulkCopyFromS3JsonStep, SqlStep
from arbalest.sql import Database
from test import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, TABLE_NAME


class S3BulkCopyPipelineShould(unittest.TestCase):
    def test_throw_pipeline_exception_when_no_steps_on_run(self):
        self.assertRaises(PipelineException,
                          S3BulkCopyPipeline(AWS_ACCESS_KEY_ID,
                                             AWS_SECRET_ACCESS_KEY, Mock(),
                                             Mock()).run)

    def test_throw_pipeline_exception_when_no_steps_on_validate(self):
        self.assertRaises(PipelineException,
                          S3BulkCopyPipeline(AWS_ACCESS_KEY_ID,
                                             AWS_SECRET_ACCESS_KEY, Mock(),
                                             Mock()).validate)

    def test_add_step(self):
        schema = JsonObject(TABLE_NAME, Property('id', 'VARCHAR(36)'))
        bucket = Mock()
        database = create_autospec(Database)
        expected = BulkCopyFromS3JsonStep(metadata='',
                                          source='',
                                          schema=schema,
                                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                          bucket=bucket,
                                          table=TargetTable(schema,
                                                            database))

        pipeline = S3BulkCopyPipeline(AWS_ACCESS_KEY_ID,
                                      AWS_SECRET_ACCESS_KEY, bucket,
                                      database)
        pipeline.step(metadata='',
                      source='',
                      schema=schema)

        step = pipeline.steps()[0]

        self.assertEqual(expected.metadata, step.metadata)
        self.assertEqual(expected.source, step.source)
        self.assertEqual(expected.schema, step.schema)
        self.assertEqual(expected.aws_access_key_id, step.aws_access_key_id)
        self.assertEqual(expected.aws_secret_access_key,
                         step.aws_secret_access_key)
        self.assertEqual(expected.bucket, step.bucket)
        self.assertEqual(expected.table.schema, step.table.schema)
        self.assertEqual(expected.table.database, step.table.database)

    def test_add_sql(self):
        bucket = Mock()
        database = create_autospec(Database)
        expected = SqlStep(database,
                           ("INSERT INTO %s VALUES('%s')", TABLE_NAME, 1),
                           ("INSERT INTO %s VALUES('%s')", TABLE_NAME, 2),
                           ("INSERT INTO {0} VALUES ('3')".format(
                               TABLE_NAME)))

        pipeline = S3BulkCopyPipeline(AWS_ACCESS_KEY_ID,
                                      AWS_SECRET_ACCESS_KEY, bucket,
                                      database)

        pipeline.sql(("INSERT INTO %s VALUES('%s')", TABLE_NAME, 1),
                     ("INSERT INTO %s VALUES('%s')", TABLE_NAME, 2),
                     ("INSERT INTO {0} VALUES ('3')".format(
                         TABLE_NAME)))

        step = pipeline.steps()[0]

        self.assertEqual(expected.statements, step.statements)
