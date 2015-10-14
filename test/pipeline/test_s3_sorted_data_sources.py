import json
import unittest

from boto.s3.key import Key
from mock import create_autospec, Mock, call
from arbalest.s3 import Bucket

from arbalest.pipeline import S3SortedDataSources


def mock_key(name):
    return Key(Mock(), name)


class S3SortedDataSourcesShould(unittest.TestCase):
    def setUp(self):
        parents = ['event.entity.created/2014-11-03/',
                   'event.entity.created/2014-11-04/',
                   'event.entity.created/2014-11-05/',
                   'event.entity.created/2014-11-06/',
                   'event.entity.created/2014-11-07/']
        first_children = ['event.entity.created/2014-11-04/00/',
                          'event.entity.created/2014-11-04/01/']
        second_children = ['event.entity.created/2014-11-05/00/']
        self.bucket = create_autospec(Bucket)
        self.bucket.list = Mock(
            side_effect=[[mock_key(key) for key in parents],
                         [mock_key(key) for key in first_children],
                         [mock_key(key) for key in second_children]])

    def test_have_source_journal_key(self):
        source = S3SortedDataSources('', 'event.entity.created', self.bucket)
        self.assertEqual('/event.entity.created_source_journal.json',
                         source.source_journal_key)

    def test_get_all_dates_as_sources(self):
        source = S3SortedDataSources('', 'event.entity.created', self.bucket)

        self.assertEqual(['event.entity.created/2014-11-03',
                          'event.entity.created/2014-11-04',
                          'event.entity.created/2014-11-05',
                          'event.entity.created/2014-11-06',
                          'event.entity.created/2014-11-07'],
                         list(source.get()))

        self.bucket.list.assert_called_once_with(source.source + '/', '/')

    def test_get_all_dates_as_sources_with_empty_dates(self):
        source = S3SortedDataSources('', 'event.entity.created', self.bucket,
                                     '', '')

        self.assertEqual(['event.entity.created/2014-11-03',
                          'event.entity.created/2014-11-04',
                          'event.entity.created/2014-11-05',
                          'event.entity.created/2014-11-06',
                          'event.entity.created/2014-11-07'],
                         list(source.get()))

        self.bucket.list.assert_called_once_with(source.source + '/', '/')

    def test_get_all_dates_including_and_after_start_date_as_sources(self):
        source = S3SortedDataSources('', 'event.entity.created', self.bucket,
                                     '2014-11-04')
        self.assertEqual(['event.entity.created/2014-11-04',
                          'event.entity.created/2014-11-05',
                          'event.entity.created/2014-11-06',
                          'event.entity.created/2014-11-07'],
                         list(source.get()))

        self.bucket.list.assert_called_once_with(source.source + '/', '/')

    def test_get_all_dates_including_and_before_end_date_as_sources(self):
        source = S3SortedDataSources('', 'event.entity.created', self.bucket,
                                     end='2014-11-05')
        self.assertEqual(['event.entity.created/2014-11-03',
                          'event.entity.created/2014-11-04',
                          'event.entity.created/2014-11-05'],
                         list(source.get()))

        self.bucket.list.assert_called_once_with(source.source + '/', '/')

    def test_get_all_dates_including_and_between_start_and_end_date_as_sources(
            self):
        source = S3SortedDataSources('', 'event.entity.created', self.bucket,
                                     start='2014-11-04/01',
                                     end='2014-11-06')

        self.assertEqual(['event.entity.created/2014-11-04/01',
                          'event.entity.created/2014-11-05/00',
                          'event.entity.created/2014-11-06'],
                         list(source.get()))

        self.bucket.list.assert_has_calls(
            [call(source.source + '/', '/'),
             call('event.entity.created/2014-11-04/', '/'),
             call('event.entity.created/2014-11-05/', '/'),
             call('event.entity.created/2014-11-06/', '/')])

    def test_committed(self):
        source = S3SortedDataSources('', 'event.entity.created', self.bucket)
        key = mock_key(source.source_journal_key)
        key.exists = Mock(return_value=True)
        source.bucket.get = Mock(return_value=key)

        self.assertEqual(True, source.committed().exists())

    def test_commit(self):
        source = S3SortedDataSources('', 'event.entity.created', self.bucket)
        committed_point = '2014-11-04'
        source.commit(committed_point)

        self.bucket.save.assert_called_once_with(source.source_journal_key,
                                                 json.dumps({
                                                     'committed': committed_point}))

    def test_rollback(self):
        source = S3SortedDataSources('', 'event.entity.created', self.bucket)
        source.rollback()

        self.bucket.delete.assert_called_once_with(source.source_journal_key)
