import unittest
from arbalest.core import SchemaException
from arbalest.redshift.schema import Property


class PropertyShould(unittest.TestCase):
    def test_throw_schema_exception_when_adding_invalid_type(self):
        self.assertRaises(SchemaException, Property, 'property1', 'BLOB')

    def test_preserve_periods_for_default_column_name(self):
        self.assertEqual('some.column.name',
                         Property('some.column.name',
                                  'VARCHAR(MAX)').column_name)

    def test_have_column_name(self):
        self.assertEqual('my_column_name',
                         Property('some.column.name', 'VARCHAR(MAX)',
                                  'my_column_name').column_name)

    def test_replace_nested_properties_with_underscores_for_default_column_name(
            self):
        self.assertEqual('parent_child_grandchild',
                         Property('parent', Property('child',
                                                     Property('grandchild',
                                                              'VARCHAR(MAX)'))).column_name)

    def test_have_column_name_for_nested_properties(self):
        self.assertEqual('my_column_name',
                         Property('parent', Property('child',
                                                     Property('grandchild',
                                                              'VARCHAR(MAX)',
                                                              'my_column_name'))).column_name)
