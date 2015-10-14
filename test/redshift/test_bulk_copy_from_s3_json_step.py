import json
import unittest
from mock import Mock, patch
from arbalest.redshift import TargetTable
from arbalest.redshift.schema import JsonObject
from arbalest.redshift.step import BulkCopyFromS3JsonStep
from arbalest.s3 import Bucket
from test import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME, \
    TABLE_NAME

SOURCE = 'arbalest_test.event.created'


class BulkCopyFromS3JsonStepShould(unittest.TestCase):
    def setUp(self):
        self.bucket = Bucket(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                             BUCKET_NAME, Mock())
        self.bucket.save = Mock()
        self.bucket.delete = Mock()
        self.schema = JsonObject(TABLE_NAME).property('eventId', 'VARCHAR(36)')
        self.table = TargetTable(self.schema, Mock())
        self.step = BulkCopyFromS3JsonStep(metadata='', source=SOURCE,
                                  schema=self.schema,
                                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                  bucket=self.bucket, table=self.table)
        self.step.sql = Mock()

    def assert_migration_with_drop(self, target_table):
        target_table.database.open.assert_called_once_with()
        target_table.stage_update.assert_called_once_with()
        target_table.drop.assert_called_once_with()
        target_table.promote_update.assert_called_once_with()

    def assert_migration_without_drop(self, target_table):
        target_table.database.open.assert_called_once_with()
        target_table.stage_update.assert_called_once_with()
        self.assertEqual(False, target_table.drop.called)
        target_table.promote_update.assert_called_once_with()

    def test_save_schema_to_s3_bucket_on_run(self):
        self.step.run()
        self.bucket.save.assert_called_once_with(self.step.schema_key,
                                                 json.dumps(
                                                     self.schema.paths()))

    def test_copy_schema_to_redshift_with_drop_on_run(self):
        with patch.object(TargetTable, 'exists') as exists:
            exists.return_value = True
            target_table = TargetTable(self.schema, Mock())
            target_table.stage_update = Mock()
            target_table.drop = Mock()
            target_table.promote_update = Mock()
            step = BulkCopyFromS3JsonStep(metadata='',
                                      source=SOURCE,
                                      schema=self.schema,
                                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                      bucket=self.bucket,
                                      table=target_table)
            step.sql = Mock()
            step.run()

            self.assert_migration_with_drop(target_table)
            target_table.database.commit.assert_called_once_with()

    def test_copy_schema_to_redshift_without_drop_on_run(self):
        with patch.object(TargetTable, 'exists') as exists:
            exists.return_value = False
            target_table = TargetTable(self.schema, Mock())
            target_table.stage_update = Mock()
            target_table.drop = Mock()
            target_table.promote_update = Mock()
            step = BulkCopyFromS3JsonStep(metadata='',
                                      source=SOURCE,
                                      schema=self.schema,
                                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                      bucket=self.bucket,
                                      table=target_table)
            step.sql = Mock()
            step.run()

            self.assert_migration_without_drop(target_table)
            target_table.database.commit.assert_called_once_with()

    def test_validate_with_drop_on_run(self):
        with patch.object(TargetTable, 'exists') as exists:
            exists.return_value = True
            target_table = TargetTable(self.schema, Mock())
            target_table.stage_update = Mock()
            target_table.drop = Mock()
            target_table.promote_update = Mock()
            step = BulkCopyFromS3JsonStep(metadata='',
                                      source=SOURCE,
                                      schema=self.schema,
                                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                      bucket=self.bucket,
                                      table=target_table)
            step.sql = Mock()
            step.validate()

            self.assert_migration_with_drop(target_table)
            target_table.database.rollback.assert_called_once_with()

    def test_validate_without_drop_on_run(self):
        with patch.object(TargetTable, 'exists') as exists:
            exists.return_value = False
            target_table = TargetTable(self.schema, Mock())
            target_table.stage_update = Mock()
            target_table.drop = Mock()
            target_table.promote_update = Mock()
            step = BulkCopyFromS3JsonStep(metadata='',
                                      source=SOURCE,
                                      schema=self.schema,
                                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                      bucket=self.bucket,
                                      table=target_table)
            step.sql = Mock()
            step.validate()

            self.assert_migration_without_drop(target_table)
            target_table.database.rollback.assert_called_once_with()

    def test_source_url(self):
        self.assertEqual('s3://bucket/arbalest_test.event.created',
                         self.step.source_url)

    def test_schema_url(self):
        self.assertEqual('s3://bucket/event_created_jsonpath.json',
                         self.step.schema_url)

    def test_copy_sql(self):
        expected_copy_table_sql = "COPY %s FROM '%s' CREDENTIALS " \
                                  "'aws_access_key_id=%s;" \
                                  "aws_secret_access_key=%s' " \
                                  "JSON '%s' TIMEFORMAT 'auto' " \
                                  "MAXERROR %s"
        self.assertEqual(expected_copy_table_sql, self.step.copy_sql)

    def test_validate_sql(self):
        expected_validate_sql = "COPY %s FROM '%s' CREDENTIALS " \
                                "'aws_access_key_id=%s;" \
                                "aws_secret_access_key=%s' " \
                                "JSON '%s' TIMEFORMAT 'auto' " \
                                "MAXERROR %s " \
                                "NOLOAD"
        self.assertEqual(expected_validate_sql, self.step.validate_sql)

    def test_delete_schema_from_s3_bucket_on_run(self):
        self.step.run()
        self.bucket.delete.assert_called_once_with(self.step.schema_key)
