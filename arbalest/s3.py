import posixpath
from boto.s3.connection import S3Connection
from boto.s3.key import Key


def normalize_path(path):
    return posixpath.normpath(path)


class Bucket(object):
    def __init__(self, aws_access_key_id, aws_secret_access_key, name,
                 s3_connection=None):
        self.name = name
        if s3_connection is None:
            self.connection = S3Connection(aws_access_key_id,
                                           aws_secret_access_key)
        else:
            self.connection = s3_connection

        self.bucket = self.connection.get_bucket(name)

    def save(self, key, contents):
        self.get(key).set_contents_from_string(contents)

    def delete(self, key):
        self.get(key).delete()

    def get(self, key):
        if isinstance(key, Key):
            return key
        else:
            return Key(self.bucket, key)

    def list(self, prefix='', delimiter='', marker='', headers=None,
             encoding_type=None):
        return self.bucket.list(prefix, delimiter, marker, headers,
                                encoding_type)
