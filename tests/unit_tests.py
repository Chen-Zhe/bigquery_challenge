import unittest
import json

import pandas as pd

from query.query_commons import df2json_list
from errors import handle_exceptions


data = [[1, "2", 3], [1, "2", 5]]
columns = ["c1", "c2", "c3"]
test_df = pd.DataFrame(data=data, columns=columns)


class QueryCommonsTest(unittest.TestCase):
    def test_df2json_list(self):
        expected = [{k:v for k, v in zip(columns, row)} for row in data]
        self.assertEqual(expected, df2json_list(test_df))


class ErrorHandlingTest(unittest.TestCase):
    def test_error_handle_decorator(self):
        """Error handler should not fail under any exceptions"""

        @handle_exceptions
        def test_exception():
            raise Exception("test")

        expected = json.dumps({"status": "Error - Server", "data": list()})
        self.assertEqual(expected, test_exception().content)





class TableCollectionTest(unittest.TestCase):
    def test_


if __name__ == '__main__':
    unittest.main()
