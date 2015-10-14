import json
import unittest
from arbalest.redshift import TargetTable
from mock import Mock, patch
from arbalest.redshift.schema import JsonObject
from arbalest.redshift.step import ManifestCopyFromS3JsonStep
from arbalest.s3 import Bucket
from test import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME, \
    TABLE_NAME

SOURCE = 'arbalest_test.event.created'
EXPECTED_COPY_SQL = "COPY %s FROM '%s' " \
                    "CREDENTIALS 'aws_access_key_id=%s;" \
                    "aws_secret_access_key=%s' " \
                    "JSON '%s' " \
                    "TIMEFORMAT 'auto' " \
                    "MANIFEST " \
                    "MAXERROR %s"
EXPECTED_VALIDATE_SQL = "COPY %s FROM '%s' " \
                    "CREDENTIALS 'aws_access_key_id=%s;" \
                    "aws_secret_access_key=%s' " \
                    "JSON '%s' " \
                    "TIMEFORMAT 'auto' " \
                    "MANIFEST " \
                    "MAXERROR %s " \
                    "NOLOAD"


class ManifestCopyFromS3JsonStepShould(unittest.TestCase):
    def setUp(self):
        self.bucket = Bucket(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                             BUCKET_NAME, Mock())
        self.bucket.save = Mock()
        self.bucket.delete = Mock()
        self.schema = JsonObject(TABLE_NAME).property('eventId', 'VARCHAR(36)')
        self.table = TargetTable(self.schema, Mock())
        self.step = ManifestCopyFromS3JsonStep(metadata='', source=SOURCE,
                                               schema=self.schema,
                                               aws_access_key_id=AWS_ACCESS_KEY_ID,
                                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                               bucket=self.bucket,
                                               table=self.table)
        self.step.manifest = Mock()
        self.updated_journal = [
            'object_path/00c68a1e-85f2-49e5-9d07-6922046dbc5a',
            'object_path/19440481-7766-4061-bd42-4a54fa0aac7c',
            'object_path/2014-09-02/19440481-7766-4061-bd42-4a54fa0aac7c']
        self.step.manifest.save = Mock(return_value=self.updated_journal)
        self.step.sql = Mock()

    def assert_migration_with_drop(self, step):
        step.table.database.open.assert_called_once_with()
        step.manifest.save.assert_called_once_with()
        step.table.drop.assert_called_once_with()
        step.table.create.assert_called_once_with()

    def assert_migration_without_drop(self, step):
        step.table.database.open.assert_called_once_with()
        step.manifest.save.assert_called_once_with()
        self.assertEqual(False, step.table.drop.called)
        step.table.create.assert_called_once_with()

    def assert_migration_without_drop_and_create(self, step):
        step.table.database.open.assert_called_once_with()
        step.manifest.save.assert_called_once_with()
        self.assertEqual(False, step.table.drop.called)
        self.assertEqual(False, step.table.create.called)

    def assert_copy_schema_to_redshift_with_drop(self, sql, execute):
        with patch.object(TargetTable, 'exists') as exists:
            exists.return_value = True
            target_table = TargetTable(self.schema, Mock())
            target_table.stage_update = Mock()
            target_table.drop = Mock()
            target_table.create = Mock()
            target_table.insert_update = Mock()
            step = ManifestCopyFromS3JsonStep(metadata='',
                                              source=SOURCE,
                                              schema=self.schema,
                                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                              bucket=self.bucket,
                                              table=target_table)
            step.manifest.save = Mock(return_value=self.updated_journal)
            step.manifest.journal_exists = Mock(return_value=False)
            step.sql = Mock()
            execute(step)

            self.assert_migration_with_drop(step)

            step.sql.execute.assert_called_once_with((sql,
                                                      self.schema.table,
                                                      step.manifest.manifest_url,
                                                      AWS_ACCESS_KEY_ID,
                                                      AWS_SECRET_ACCESS_KEY,
                                                      step.schema_url,
                                                      step.max_error_count))

            return target_table.database

    def assert_copy_schema_to_redshift_without_drop(self, sql, execute):
        with patch.object(TargetTable, 'exists') as exists:
            exists.return_value = False
            target_table = TargetTable(self.schema, Mock())
            target_table.stage_update = Mock()
            target_table.drop = Mock()
            target_table.create = Mock()
            target_table.insert_update = Mock()
            step = ManifestCopyFromS3JsonStep(metadata='',
                                              source=SOURCE,
                                              schema=self.schema,
                                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                              bucket=self.bucket,
                                              table=target_table)
            step.manifest.save = Mock(return_value=self.updated_journal)
            step.manifest.journal_exists = Mock(return_value=False)
            step.sql = Mock()
            execute(step)

            self.assert_migration_without_drop(step)

            step.sql.execute.assert_called_once_with((sql,
                                                      self.schema.table,
                                                      step.manifest.manifest_url,
                                                      AWS_ACCESS_KEY_ID,
                                                      AWS_SECRET_ACCESS_KEY,
                                                      step.schema_url,
                                                      step.max_error_count))

            return target_table.database

    def assert_copy_schema_to_redshift_without_drop_and_create(self, sql,
                                                               execute):
        with patch.object(TargetTable, 'exists') as exists:
            exists.return_value = True
            target_table = TargetTable(self.schema, Mock())
            target_table.stage_update = Mock()
            target_table.drop = Mock()
            target_table.create = Mock()
            target_table.insert_update = Mock()
            step = ManifestCopyFromS3JsonStep(metadata='',
                                              source=SOURCE,
                                              schema=self.schema,
                                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                              bucket=self.bucket,
                                              table=target_table)
            step.manifest.save = Mock(return_value=self.updated_journal)
            step.manifest.journal_exists = Mock(return_value=True)
            step.sql = Mock()
            execute(step)

            self.assert_migration_without_drop_and_create(step)

            step.sql.execute.assert_called_once_with((sql,
                                                      self.schema.table,
                                                      step.manifest.manifest_url,
                                                      AWS_ACCESS_KEY_ID,
                                                      AWS_SECRET_ACCESS_KEY,
                                                      step.schema_url,
                                                      step.max_error_count))

            return target_table.database

    def test_save_schema_to_s3_bucket_on_run(self):
        self.step.run()
        self.bucket.save.assert_called_once_with(self.step.schema_key,
                                                 json.dumps(
                                                     self.schema.paths()))

    def test_copy_schema_to_redshift_with_drop_when_no_journal_on_run(self):
        database = self.assert_copy_schema_to_redshift_with_drop(
            EXPECTED_COPY_SQL,
            lambda step: step.run())
        database.commit.assert_called_once_with()

    def test_copy_schema_to_redshift_without_drop_when_no_journal_on_run(self):
        database = self.assert_copy_schema_to_redshift_without_drop(
            EXPECTED_COPY_SQL,
            lambda step: step.run())
        database.commit.assert_called_once_with()

    def test_copy_schema_to_redshift_without_drop_and_create_when_journal_on_run(
            self):
        database = self.assert_copy_schema_to_redshift_without_drop_and_create(
            EXPECTED_COPY_SQL,
            lambda step: step.run())
        database.commit.assert_called_once_with()

    def test_copy_schema_to_redshift_with_drop_when_no_journal_on_validate(
            self):
        database = self.assert_copy_schema_to_redshift_with_drop(
            EXPECTED_VALIDATE_SQL,
            lambda step: step.validate())

        self.assertEqual(False, database.commit.called)
        database.rollback.assert_called_once_with()

    def test_copy_schema_to_redshift_without_drop_when_no_journal_on_validate(
            self):
        database = self.assert_copy_schema_to_redshift_without_drop(
            EXPECTED_VALIDATE_SQL,
            lambda step: step.validate())

        self.assertEqual(False, database.commit.called)
        database.rollback.assert_called_once_with()

    def test_copy_schema_to_redshift_without_drop_and_create_when_journal_on_validate(
            self):
        database = self.assert_copy_schema_to_redshift_without_drop_and_create(
            EXPECTED_VALIDATE_SQL,
            lambda step: step.validate())

        self.assertEqual(False, database.commit.called)
        database.rollback.assert_called_once_with()

    def test_schema_url(self):
        self.assertEqual('s3://bucket/event_created_jsonpath.json',
                         self.step.schema_url)

    def test_commit_manifest_on_run(self):
        self.step.run()
        self.step.manifest.commit.assert_called_once_with(
            self.step.manifest.save())

    def test_not_commit_manifest_on_validate(self):
        self.step.validate()
        self.assertEqual(False, self.step.manifest.commit.called)

    def test_delete_schema_from_s3_bucket_on_run(self):
        self.step.run()
        self.bucket.delete.assert_called_once_with(self.step.schema_key)
