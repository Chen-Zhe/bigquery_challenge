import unittest

from query.types.date.date_filter import SqlDateFilter
from datetime import date


class DateFilterTests(unittest.TestCase):
    def test_sql_date_condition_generation(self):
        actual = SqlDateFilter.gen_sql_date_range_condition("pickup_datetime",
                                                   [(date(2018, 1, 1), date(2018, 3, 5)), (date(2018, 6, 5), date(2018, 6, 5))])

        expected = "((date(pickup_datetime)>='2018-01-01' AND date(pickup_datetime)<='2018-03-05') OR (date(pickup_datetime)='2018-06-05'))"

        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
