class Database(object):
    cursor = None

    def __init__(self, connection):
        self.connection = connection

    def open(self):
        if self.cursor is None:
            self.cursor = self.connection.cursor()

    def execute(self, sql, params=None):
        try:
            return self.cursor.execute(sql, params)
        except ValueError, e:
            if params is None:
                return self.cursor.execute(sql, '')
            else:
                raise ValueError(e)

    def fetchall(self):
        for row in self.cursor:
            yield row

    def commit(self):
        return self.connection.commit()

    def rollback(self):
        return self.connection.rollback()

    def close(self):
        return self.connection.close()
