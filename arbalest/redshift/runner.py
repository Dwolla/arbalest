import json
from arbalest.s3 import normalize_path


class S3JsonStepRunner(object):
    def __init__(self, metadata, schema, bucket, table):
        self.metadata = metadata
        self.bucket = bucket
        self.table = table
        self.schema = schema

    @property
    def schema_key(self):
        return normalize_path(
            '{0}/{1}'.format(self.metadata, self.schema.file_name))

    @property
    def schema_url(self):
        return 's3://{0}{1}'.format(self.bucket.name, self.schema_key)

    def stage(self):
        self.bucket.save(self.schema_key, json.dumps(self.schema.paths()))
        self.table.database.open()

    def commit(self):
        self.table.database.commit()
        self.bucket.delete(self.schema_key)

    def rollback(self):
        self.table.database.rollback()
        self.bucket.delete(self.schema_key)
