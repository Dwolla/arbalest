import unittest
from mock import Mock
from arbalest.redshift.step import SqlStep


class SqlStepShould(unittest.TestCase):
    def assert_sql(self, database, select, select_with_where):
        actual_select_with_where = database.execute.call_args_list[0][0]
        actual_select = database.execute.call_args_list[1][0]
        self.assertEqual(2, len(actual_select_with_where))
        self.assertEqual(1, len(actual_select))
        self.assertEqual(select_with_where[0], actual_select_with_where[0])
        self.assertEqual(select, actual_select[0])

    def test_execute_statements_and_commit_on_run(self):
        database = Mock()
        select_with_where = (
            'SELECT * FROM some_table WHERE some_column1 = %s', "some_value")
        select = ('SELECT * FROM some_table')
        step = SqlStep(database, select_with_where, select)

        step.run()

        database.open.assert_called_once_with()
        self.assert_sql(database, select, select_with_where)
        database.commit.assert_called_once_with()

    def test_execute_statements_and_rollback_on_validate(self):
        database = Mock()
        select_with_where = (
            'SELECT * FROM some_table WHERE some_column1 = %s', "some_value")
        select = ('SELECT * FROM some_table')
        step = SqlStep(database, select_with_where, select)

        step.validate()

        database.open.assert_called_once_with()
        self.assert_sql(database, select, select_with_where)
        database.rollback.assert_called_once_with()

    def test_execute(self):
        database = Mock()
        select_with_where = (
            'SELECT * FROM some_table WHERE some_column1 = %s', "some_value")
        select = ('SELECT * FROM some_table')
        step = SqlStep(database)

        step.execute((select_with_where))
        step.execute(select)

        self.assert_sql(database, select, select_with_where)
