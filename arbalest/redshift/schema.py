from arbalest.core import SchemaException


class Property(object):
    def __init__(self, name, column_type_or_child_property, column_name=None):
        self.name = name
        self.keys = [name]
        self.__column_name = column_name
        self.__column_type_or_child_property = column_type_or_child_property

        if isinstance(column_type_or_child_property, basestring):
            self.__validate_supported_type(column_type_or_child_property)
            self.type = column_type_or_child_property
            self.column_name = self.__name(column_name, name)
        elif isinstance(column_type_or_child_property, Property):
            child_column_name = column_name
            child_property = column_type_or_child_property

            while isinstance(child_property, Property):
                self.keys.append(child_property.name)
                child_column_name = child_property.__column_name
                child_property = child_property.__column_type_or_child_property

            self.__validate_supported_type(child_property)
            self.type = child_property
            self.column_name = self.__name(child_column_name,
                                           '_'.join(self.keys))

    @staticmethod
    def __name(column_name, name):
        return column_name if column_name is not None else name

    @staticmethod
    def __validate_supported_type(column_type):
        types = ['SMALLINT',
                 'INT2',
                 'INTEGER',
                 'INT',
                 'INT4',
                 'BIGINT',
                 'INT8',
                 'DECIMAL',
                 'NUMERIC',
                 'REAL',
                 'FLOAT4',
                 'DOUBLE PRECISION',
                 'FLOAT8',
                 'FLOAT',
                 'BOOLEAN',
                 'BOOL',
                 'CHAR',
                 'CHARACTER',
                 'NCHAR',
                 'BPCHAR',
                 'VARCHAR',
                 'CHARACTER VARYING',
                 'NVARCHAR',
                 'TEXT',
                 'DATE',
                 'TIMESTAMP']
        if True not in [type_name in str(column_type).upper() for type_name in
                        types]:
            raise SchemaException(
                'Invalid column type: {0}'.format(column_type))


class JsonObject(object):
    def __init__(self, table, *args):
        self.table = table
        self.update_table = table + '_update'
        self.properties = []
        self.schema = []
        for json_property in args:
            self.__append_property(json_property)
        self.file_name = '{0}_jsonpath.json'.format(table)

    def property(self, name, column_type, column_name=None):
        self.__append_property(Property(name, column_type, column_name))
        return self

    def paths(self):
        return {
            'jsonpaths': [self.__path(schema_property) for schema_property
                          in self.schema]
        }

    def __append_property(self, json_object_property):
        name = json_object_property.column_name
        self.__validate_unique_property_name(name)
        self.properties.append(name)
        self.schema.append(json_object_property)

    def __path(self, schema_property):
        return '$' + ''.join(
            ["['{0}']".format(key) for key in schema_property.keys])

    def __validate_unique_property_name(self, property_name):
        if property_name in self.properties:
            raise SchemaException(
                'Cannot define duplicate column of name: {0}'.format(
                    property_name))
