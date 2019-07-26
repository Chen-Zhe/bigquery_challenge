import unittest

from query.sql.tables.collection import SqlTableCollection, DataTable
from http_json.errors import ServerException
from datetime import date


class DataTableTests(unittest.TestCase):
    def test_valid_date_range(self):
        a = DataTable("test", "2019-01-01", "2019-01-01")
        self.assertIsNotNone(a)

    def test_invalid_date_range(self):
        with self.assertRaises(ServerException):
            a = DataTable("test", "2019-01-01", "2018-12-31")


class CollectionTests(unittest.TestCase):
    group_name = "test"

    c = SqlTableCollection([group_name])
    tables = [DataTable("test-1", "2016-01-01", "2016-12-31"),
              DataTable("test-2", "2017-01-01", "2017-12-31"),
              DataTable("test-3", "2018-01-01", "2018-12-31")]
    for t in tables:
        c.register_table(group_name, t)

    def test_get_tables_valid_date(self):
        actual = self.c.get_tables(self.group_name, date(2016, 12, 31), date(2017, 1, 2))
        expected = self.tables[0:2]

        self.assertEqual(expected, actual)

    def test_get_tables_invalid_date(self):
        actual = self.c.get_tables(self.group_name, date(2018, 12, 31), date(2017, 1, 2))

        self.assertTrue(len(actual) == 0)


if __name__ == '__main__':
    unittest.main()
