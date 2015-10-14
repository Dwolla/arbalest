import unittest
from arbalest.core import SchemaException
from arbalest.redshift.schema import JsonObject, Property
from test import TABLE_NAME


class JsonObjectSchemaShould(unittest.TestCase):
    def test_have_jsonpath_file_name(self):
        self.assertEqual('{0}_jsonpath.json'.format(TABLE_NAME),
                         JsonObject(TABLE_NAME).file_name)

    def test_have_empty_paths(self):
        self.assertEqual({'jsonpaths': []}, JsonObject(TABLE_NAME).paths())

    def test_throw_schema_exception_when_adding_duplicate_property(self):
        self.assertRaises(SchemaException,
                          JsonObject(TABLE_NAME)
                          .property('property1', 'VARCHAR(10)')
                          .property, 'property1', 'VARCHAR(10)')

    def test_add_property_and_update_paths(self):
        self.assertEqual({'jsonpaths': ["$['property1']"]},
                         JsonObject(TABLE_NAME)
                         .property('property1', 'VARCHAR(10)')
                         .paths())
        self.assertEqual({'jsonpaths': ["$['property1']",
                                        "$['property2']"]},
                         JsonObject(TABLE_NAME)
                         .property('property1', 'VARCHAR(10)')
                         .property('property2', 'TIMESTAMP')
                         .paths())

    def test_have_paths_for_nested_objects(self):
        schema = JsonObject(TABLE_NAME,
                            Property('property1', 'VARCHAR(10)'),
                            Property('property2', 'TIMESTAMP'),
                            Property('property3.dottedName',
                                     'DOUBLE PRECISION'),
                            Property('property4', Property('child',
                                                           Property(
                                                               'subchild',
                                                               'BOOLEAN'))))

        self.assertEqual({'jsonpaths': ["$['property1']",
                                        "$['property2']",
                                        "$['property3.dottedName']",
                                        "$['property4']['child']['subchild']"]},
                         schema.paths())
