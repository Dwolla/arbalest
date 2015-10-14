import unittest

from boto.s3.key import Key
from mock import Mock, patch, mock_open, create_autospec, call
from arbalest.redshift.manifest import SqlManifest
from arbalest.redshift.schema import Property, JsonObject
from arbalest.s3 import Bucket
from arbalest.sql import Database
from test import BUCKET_NAME, TABLE_NAME, AWS_ACCESS_KEY_ID, \
    AWS_SECRET_ACCESS_KEY


class SqlManifestShould(unittest.TestCase):
    def setUp(self):
        self.schema = JsonObject(TABLE_NAME, Property('id', 'VARCHAR(36)'))
        self.bucket = Bucket(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                             BUCKET_NAME, Mock())
        self.bucket.save = Mock()
        self.database = create_autospec(Database)
        self.key_names = [
            'object_path/00c68a1e-85f2-49e5-9d07-6922046dbc5a',
            'object_path/19440481-7766-4061-bd42-4a54fa0aac7c',
            'object_path/2014-09-02/19440481-7766-4061-bd42-4a54fa0aac7c',
            'object_path/282e6063-ecef-4e45-bdfb-9fdfb39840cd',
            'object_path/35cbf09a-b2dc-43f2-96f6-7d7573906268',
            'object_path/80536e83-6bbe-4a42-ade1-533d99321a6c',
            'object_path/cf00b394-3ff3-4418-b244-2ccf104fcc40',
            'object_path/e822e2ae-61f5-4be0-aacd-ca6de70faad1']
        self.bucket.list = Mock(
            return_value=[self.mock_key(key) for key in self.key_names])
        self.manifest = SqlManifest(metadata='',
                                    source='',
                                    schema=self.schema,
                                    bucket=self.bucket,
                                    db_connection=self.database)
        self.expected_manifest = {'entries': [
            {
                'url': 's3://{0}/object_path/00c68a1e-85f2-49e5-9d07-6922046dbc5a'.format(
                    BUCKET_NAME), 'mandatory': True},
            {
                'url': 's3://{0}/object_path/19440481-7766-4061-bd42-4a54fa0aac7c'.format(
                    BUCKET_NAME), 'mandatory': True},
            {
                'url': 's3://{0}/object_path/2014-09-02/19440481-7766-4061-bd42-4a54fa0aac7c'.format(
                    BUCKET_NAME), 'mandatory': True},
            {
                'url': 's3://{0}/object_path/282e6063-ecef-4e45-bdfb-9fdfb39840cd'.format(
                    BUCKET_NAME), 'mandatory': True},
            {
                'url': 's3://{0}/object_path/35cbf09a-b2dc-43f2-96f6-7d7573906268'.format(
                    BUCKET_NAME), 'mandatory': True},
            {
                'url': 's3://{0}/object_path/80536e83-6bbe-4a42-ade1-533d99321a6c'.format(
                    BUCKET_NAME), 'mandatory': True},
            {
                'url': 's3://{0}/object_path/cf00b394-3ff3-4418-b244-2ccf104fcc40'.format(
                    BUCKET_NAME), 'mandatory': True},
            {
                'url': 's3://{0}/object_path/e822e2ae-61f5-4be0-aacd-ca6de70faad1'.format(
                    BUCKET_NAME), 'mandatory': True}
        ]}

    def mock_key(self, name):
        return Key(Mock(), name)

    def mock_key_exists(self, key_name, exists):
        key = self.mock_key(key_name)
        key.exists = Mock(return_value=exists)
        key.get_contents_to_filename = Mock()
        self.bucket.get = Mock(return_value=key)
        return key

    def mock_journal(self, exists, key_names=None):
        self.mock_key_exists(self.manifest.journal_key, exists)
        if exists:
            self.database.fetchall = Mock(
                return_value=((key,) for key in key_names))

    def test_have_all_keys(self):
        self.assertEqual(self.key_names, list(self.manifest.all_keys))

    def test_have_manifest_key(self):
        self.assertEqual('/event_created_manifest.json',
                         self.manifest.manifest_key)

    def test_have_journal_key(self):
        self.assertEqual('/event_created_journal.db',
                         self.manifest.journal_key)

    def test_have_manifest_url(self):
        self.assertEqual(
            's3://{0}/event_created_manifest.json'.format(BUCKET_NAME),
            self.manifest.manifest_url)

    def test_have_empty_journal(self):
        self.mock_journal(False)

        self.assertEqual([], list(self.manifest.journal()))
        self.database.open.assert_called_once_with()
        sql = 'CREATE TABLE IF NOT EXISTS journal (key TEXT)'
        self.database.execute.assert_called_once_with(sql)
        self.database.commit.assert_called_once_with()
        self.database.close.assert_called_once_with()

    def test_have_journal(self):
        self.mock_journal(True, self.key_names)

        self.assertEqual(self.key_names, list(self.manifest.journal()))
        self.database.open.assert_called_once_with()
        sql = 'SELECT key FROM journal'
        self.database.execute.assert_called_once_with(sql)

    def test_have_manifest_when_journal_is_empty(self):
        self.mock_journal(False)

        expected_entries = set(
            [entry['url'] for entry in
             self.expected_manifest['entries']])
        actual_entries = set(
            [entry['url'] for entry in
             self.manifest.get()['manifest']['entries']])

        self.assertEqual(expected_entries, actual_entries)

    def test_update_existing_manifest_when_key_not_in_journal(self):
        self.mock_journal(True, list(self.key_names))
        self.key_names.append(
            'object_path/5acd5fb0-be96-451a-be32-b65c4461b3f4')
        self.bucket.list = Mock(
            return_value=[self.mock_key(key) for key in self.key_names])

        self.assertEqual([{
            'url': 's3://{0}/object_path/5acd5fb0-be96-451a-be32-b65c4461b3f4'.format(
                BUCKET_NAME),
            'mandatory': True}],
            list(self.manifest.get()['manifest']['entries']))

    def test_save(self):
        f = mock_open()
        self.mock_journal(False)
        key = self.mock_key(self.manifest.manifest_key)
        key.set_contents_from_filename = Mock()
        key.get_contents_to_filename = Mock()
        self.bucket.get = Mock(return_value=key)

        with patch('__builtin__.open', f, create=True):
            self.manifest.save()

            f.assert_called_once_with(self.manifest.file_name, 'wb')

            handle = f()
            self.assertEqual(call(0), handle.seek.call_args_list[0])
            self.assertEqual(2, handle.truncate.call_count)
            self.assertEqual([call('{\n'),
                              call('"entries": [\n'),
                              call(
                                  '{"url": "s3://bucket/object_path/35cbf09a-b2dc-43f2-96f6-7d7573906268", "mandatory": true},\n'),
                              call(
                                  '{"url": "s3://bucket/object_path/80536e83-6bbe-4a42-ade1-533d99321a6c", "mandatory": true},\n'),
                              call(
                                  '{"url": "s3://bucket/object_path/e822e2ae-61f5-4be0-aacd-ca6de70faad1", "mandatory": true},\n'),
                              call(
                                  '{"url": "s3://bucket/object_path/cf00b394-3ff3-4418-b244-2ccf104fcc40", "mandatory": true},\n'),
                              call(
                                  '{"url": "s3://bucket/object_path/2014-09-02/19440481-7766-4061-bd42-4a54fa0aac7c", "mandatory": true},\n'),
                              call(
                                  '{"url": "s3://bucket/object_path/282e6063-ecef-4e45-bdfb-9fdfb39840cd", "mandatory": true},\n'),
                              call(
                                  '{"url": "s3://bucket/object_path/19440481-7766-4061-bd42-4a54fa0aac7c", "mandatory": true},\n'),
                              call(
                                  '{"url": "s3://bucket/object_path/00c68a1e-85f2-49e5-9d07-6922046dbc5a", "mandatory": true},\n'),
                              call(
                                  '{"url": "s3://bucket/object_path/00c68a1e-85f2-49e5-9d07-6922046dbc5a", "mandatory": true}\n'),
                              call(']}')], handle.write.call_args_list)

            key.set_contents_from_filename.assert_called_once_with(
                self.manifest.file_name)

    def test_commit(self):
        key = self.mock_key(self.manifest.journal_key)
        key.set_contents_from_filename = Mock()
        self.bucket.get = Mock(return_value=key)

        self.manifest.commit(self.key_names)

        self.database.open.assert_called_once_with()
        inserts = [call('DELETE FROM journal'),
                   call('INSERT INTO journal VALUES (?)',
                        (
                            'object_path/00c68a1e-85f2-49e5-9d07-6922046dbc5a',)),
                   call('INSERT INTO journal VALUES (?)',
                        (
                            'object_path/19440481-7766-4061-bd42-4a54fa0aac7c',)),
                   call('INSERT INTO journal VALUES (?)',
                        (
                            'object_path/2014-09-02/19440481-7766-4061-bd42-4a54fa0aac7c',)),
                   call('INSERT INTO journal VALUES (?)',
                        (
                            'object_path/282e6063-ecef-4e45-bdfb-9fdfb39840cd',)),
                   call('INSERT INTO journal VALUES (?)',
                        (
                            'object_path/35cbf09a-b2dc-43f2-96f6-7d7573906268',)),
                   call('INSERT INTO journal VALUES (?)',
                        (
                            'object_path/80536e83-6bbe-4a42-ade1-533d99321a6c',)),
                   call('INSERT INTO journal VALUES (?)',
                        (
                            'object_path/cf00b394-3ff3-4418-b244-2ccf104fcc40',)),
                   call('INSERT INTO journal VALUES (?)',
                        (
                            'object_path/e822e2ae-61f5-4be0-aacd-ca6de70faad1',))]
        self.database.execute.assert_has_calls(inserts)
        self.database.commit.assert_called_once_with()
        self.database.close.assert_called_once_with()
        key.set_contents_from_filename.assert_called_once_with(
            self.manifest.journal_file_name)

    def test_exist(self):
        exists = True
        self.mock_key_exists(self.manifest.manifest_key, exists)

        self.assertEqual(exists, self.manifest.exists())

    def test_not_exist(self):
        exists = False
        self.mock_key_exists(self.manifest.manifest_key, exists)

        self.assertEqual(exists, self.manifest.exists())

    def test_have_journal_existence(self):
        exists = True
        self.mock_key_exists(self.manifest.journal_key, exists)

        self.assertEqual(exists, self.manifest.journal_exists())

    def test_not_have_journal_existence(self):
        exists = False
        self.mock_key_exists(self.manifest.journal_key, exists)

        self.assertEqual(exists, self.manifest.journal_exists())
