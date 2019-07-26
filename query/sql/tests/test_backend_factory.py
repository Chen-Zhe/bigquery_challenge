import unittest

from http_json.errors import ServerException
from query.sql.backend_factory import get_sql_backend, SqlBackend
from query.sql.google_bigquery import BigQueryBackend
from query.sql.sqlite import Sqlite3Backend


class TestBackendFactory(unittest.TestCase):

    @unittest.skip("Relative path is not working for test cases")
    def test_valid_name(self):
        self.assertIsInstance(get_sql_backend(SqlBackend.SQLITE), Sqlite3Backend)
        self.assertIsInstance(get_sql_backend(SqlBackend.BIG_QUERY), BigQueryBackend)

    def test_invalid_name(self):
        with self.assertRaises(ServerException):
            get_sql_backend(None)


if __name__ == '__main__':
    unittest.main()
