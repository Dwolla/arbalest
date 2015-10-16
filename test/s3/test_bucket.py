import unittest
from boto.s3.key import Key
from mock import Mock, patch
from arbalest.s3 import Bucket
from test import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME


class BucketShould(unittest.TestCase):
    def test_save_key_as_string(self):
        with patch.object(Key,
                          'set_contents_from_string') as \
                set_contents_from_string:
            connection = Mock()
            key = Key()
            contents = 'contents'
            Bucket(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME,
                   connection).save(key, contents)

            set_contents_from_string.assert_called_once_with(contents)

    def test_delete_key(self):
        with patch.object(Key, 'delete') as delete:
            connection = Mock()
            key = Key()
            Bucket(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME,
                   connection).delete(key)

            delete.assert_called_once()

    def test_get_key(self):
        self.assertNotEqual(None,
                            Bucket(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                                   BUCKET_NAME,
                                   Mock()).get('key'))

    def test_list(self):
        connection = Mock()
        bucket = Mock()
        connection.get_bucket.return_value = bucket
        Bucket(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME,
                   connection).list('/')
        bucket.list.assert_called_once_with('/', '', '', None, None)
