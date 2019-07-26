import unittest

from query.query_functions import *
from conf import SqliteConfig

backend = SqlBackend.SQLITE
SqliteConfig.database_file = "../../sample_data/nyc_taxi_sample_data.db"


class IntegrationTests(unittest.TestCase):
    def test_total_trips(self):
        response = total_trips_over_date_range("2016-01-01", "2016-01-10")
        expected = {"data": [{"date": "2016-01-01", "total_trips": 2.0},
                             {"date": "2016-01-05", "total_trips": 1.0},
                             {"date": "2016-01-06", "total_trips": 3.0},
                             {"date": "2016-01-07", "total_trips": 4.0},
                             {"date": "2016-01-08", "total_trips": 3.0},
                             {"date": "2016-01-10", "total_trips": 2.0}],
                    "status": "OK"}
        self.assertEqual(json.loads(response.content), expected)

    def test_average_fare_heatmap(self):
        response = average_fare_heatmap_of_date("2016-01-06")
        expected = {"data": [{"s2id": "84e2b3b73", "fare_amount": 31.0},
                             {"s2id": "851e4e971", "fare_amount": 22.5}],
                    "status": "OK"}
        self.assertEqual(json.loads(response.content), expected)

    def test_average_speed_of_date(self):
        response = average_speed_of_date("2016-01-07")
        expected_avg_speed = 20.90659

        actual = json.loads(response.content)["data"][0]["average_speed"]

        self.assertAlmostEqual(expected_avg_speed, actual, delta=0.0001)


if __name__ == '__main__':
    unittest.main()
