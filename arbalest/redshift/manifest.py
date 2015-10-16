import json
from arbalest.s3 import normalize_path
from arbalest.sql import Database


class Manifest(object):
    def __init__(self, metadata, source, schema, bucket):
        self.metadata = metadata
        self.source = source
        self.schema = schema
        self.bucket = bucket
        self.file_name = '{0}_manifest.json'.format(schema.table)
        self.journal_file_name = '{0}_journal.json'.format(schema.table)

    @property
    def all_keys(self):
        return [k.name for k in self.bucket.list(self.source) if
                not k.name.endswith('/')]

    @property
    def manifest_key(self):
        return normalize_path('{0}/{1}'.format(self.metadata, self.file_name))

    @property
    def journal_key(self):
        return normalize_path(
            '{0}/{1}'.format(self.metadata, self.journal_file_name))

    @property
    def manifest_url(self):
        return 's3://{0}{1}'.format(self.bucket.name, self.manifest_key)

    def journal(self):
        journal = self.bucket.get(self.journal_key)
        if journal.exists():
            return json.loads(journal.get_contents_as_string())
        else:
            return []

    def get(self):
        updated_journal = self.all_keys
        journal = self.journal()
        keys = list(set(updated_journal) - set(journal))

        return {
            'manifest': {
                'entries': [
                    {'url': 's3://{0}/{1}'.format(self.bucket.name, key),
                     'mandatory': True} for key in keys
                ]
            },
            'updated_journal': updated_journal
        }

    def save(self):
        manifest = self.get()
        self.bucket.save(self.manifest_key, json.dumps(manifest['manifest']))
        return manifest['updated_journal']

    def commit(self, saved_keys):
        self.bucket.save(self.journal_key, json.dumps(saved_keys))

    def exists(self):
        return self.bucket.get(self.manifest_key).exists()

    def journal_exists(self):
        return self.bucket.get(self.journal_key).exists()


class SqlManifest(object):
    def __init__(self, metadata, source, schema, bucket, db_connection):
        self.metadata = metadata
        self.source = source
        self.schema = schema
        self.bucket = bucket

        if isinstance(db_connection, Database):
            self.database = db_connection
        else:
            self.database = Database(db_connection)

        self.file_name = '{0}_manifest.json'.format(schema.table)
        self.journal_file_name = '{0}_journal.db'.format(schema.table)

    @property
    def all_keys(self):
        return (k.name for k in self.bucket.list(self.source) if
                not k.name.endswith('/'))

    @property
    def manifest_key(self):
        return normalize_path('{0}/{1}'.format(self.metadata, self.file_name))

    @property
    def journal_key(self):
        return normalize_path(
            '{0}/{1}'.format(self.metadata, self.journal_file_name))

    @property
    def manifest_url(self):
        return 's3://{0}{1}'.format(self.bucket.name, self.manifest_key)

    def journal(self):
        journal = self.bucket.get(self.journal_key)
        if journal.exists():
            journal.get_contents_to_filename(self.journal_file_name)
            self.database.open()
            self.database.execute('SELECT key FROM journal')
            return (row[0] for row in self.database.fetchall())
        else:
            self.database.open()
            self.database.execute(
                'CREATE TABLE IF NOT EXISTS journal (key TEXT)')
            self.database.commit()
            self.database.close()
            return []

    def get(self):
        updated_journal = self.all_keys
        journal = self.journal()
        keys = list(set(updated_journal) - set(journal))

        return {
            'manifest': {
                'entries': (
                    {'url': 's3://{0}/{1}'.format(self.bucket.name, key),
                     'mandatory': True} for key in keys
                )
            },
            'updated_journal': updated_journal
        }

    def save(self):
        entries = self.get()['manifest']['entries']
        offset = 0
        offsets = []
        last_entry = None

        with open(self.file_name, 'wb') as f:
            f.seek(0)
            f.truncate()
            offset += self.__write(f, '{\n')
            offsets.append(offset)
            offset += self.__write(f, '"entries": [\n')
            offsets.append(offset)

            for entry in entries:
                line = json.dumps(entry) + ',\n'
                offset += self.__write(f, line)
                offsets.append(offset)
                last_entry = line
            f.seek(offsets[-2])
            f.truncate()
            if last_entry is not None:
                line = last_entry[0:-2] + '\n'
                f.write(line)
            f.write(']}')

        self.bucket.get(self.manifest_key).set_contents_from_filename(
            self.file_name)

    def commit(self, saved_keys):
        self.database.open()
        self.database.execute('DELETE FROM journal')
        for key in saved_keys:
            self.database.execute('INSERT INTO journal VALUES (?)', (key,))
        self.database.commit()
        self.database.close()
        self.bucket.get(self.journal_key).set_contents_from_filename(
            self.journal_file_name)

    def exists(self):
        return self.bucket.get(self.manifest_key).exists()

    def journal_exists(self):
        return self.bucket.get(self.journal_key).exists()

    @staticmethod
    def __write(fd, line):
        fd.write(line)
        return len(line)
