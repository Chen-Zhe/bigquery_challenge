import unittest

import pandas as pd

from cache.backend.serialize import DataFrameSerializer

data = [[1, "2", 3], [1, "2", 5]]
columns = ["c1", "c2", "c3"]
test_df = pd.DataFrame(data=data, columns=columns)


class SerializeTest(unittest.TestCase):
    def test_dataframe_serialization(self):
        actual = DataFrameSerializer.deserialize(DataFrameSerializer.serialize(test_df))
        self.assertTrue(test_df.equals(actual))

    def test_none_case(self):
        actual = DataFrameSerializer.deserialize(DataFrameSerializer.serialize(None))
        self.assertTrue(actual is None)


if __name__ == '__main__':
    unittest.main()
