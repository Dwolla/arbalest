import json
import unittest
from boto.s3.key import Key
from mock import Mock
from arbalest.redshift.manifest import Manifest
from arbalest.redshift.schema import JsonObject, Property
from arbalest.s3 import Bucket
from test import BUCKET_NAME, TABLE_NAME, AWS_ACCESS_KEY_ID, \
    AWS_SECRET_ACCESS_KEY


class ManifestShould(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.schema = JsonObject(TABLE_NAME, Property('id', 'VARCHAR(36)'))
        self.bucket = Bucket(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                             BUCKET_NAME, Mock())
        self.bucket.save = Mock()
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
        self.manifest = Manifest(metadata='',
                                 source='',
                                 schema=self.schema,
                                 bucket=self.bucket)
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
        self.bucket.get = Mock(return_value=key)
        return key

    def mock_journal(self, exists, key_names=None):
        key = self.mock_key_exists(self.manifest.journal_key, exists)
        if exists:
            key.get_contents_as_string = Mock(
                return_value=json.dumps(key_names))

    def test_have_all_keys(self):
        self.assertEqual(self.key_names, self.manifest.all_keys)

    def test_have_manifest_key(self):
        self.assertEqual('/event_created_manifest.json',
                         self.manifest.manifest_key)

    def test_have_journal_key(self):
        self.assertEqual('/event_created_journal.json',
                         self.manifest.journal_key)

    def test_have_manifest_url(self):
        self.assertEqual(
            's3://{0}/event_created_manifest.json'.format(BUCKET_NAME),
            self.manifest.manifest_url)

    def test_have_empty_journal(self):
        self.mock_journal(False)

        self.assertEqual([], self.manifest.journal())

    def test_have_journal(self):
        self.mock_journal(True, self.key_names)

        self.assertEqual(self.key_names, self.manifest.journal())

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

        self.assertEqual({'entries': [{
                                          'url': 's3://{0}/object_path/5acd5fb0-be96-451a-be32-b65c4461b3f4'.format(
                                              BUCKET_NAME),
                                          'mandatory': True}]},
                         self.manifest.get()['manifest'])

    def test_save_and_have_updated_journal(self):
        self.mock_journal(False)
        updated_journal = self.manifest.save()

        self.assertEqual(set(self.key_names), set(updated_journal))

        save = self.bucket.save.call_args_list[0][0]

        self.assertEqual(self.manifest.manifest_key, save[0])

        expected_entries = set(
            [entry['url'] for entry in self.expected_manifest['entries']])
        actual_entries = set(
            [entry['url'] for entry in json.loads(save[1])['entries']])

        self.assertEqual(expected_entries, actual_entries)

    def test_commit_and_save_journal(self):
        self.manifest.commit(self.key_names)

        self.bucket.save.assert_called_once_with(self.manifest.journal_key,
                                                 json.dumps(self.key_names))

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
