from query.sql.tables.collection import DataTable, SqlTableCollection

tables = SqlTableCollection(["tlc_green_trips", "tlc_yellow_trips"])

tables.register_table("tlc_green_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2014",
                                                   "2014-01-01", "2014-12-31"))
tables.register_table("tlc_green_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2015",
                                                   "2015-01-01", "2015-12-31"))
tables.register_table("tlc_green_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2016",
                                                   "2016-01-01", "2016-12-31"))
tables.register_table("tlc_green_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2017",
                                                   "2017-01-01", "2017-12-31"))
tables.register_table("tlc_yellow_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2015",
                                                    "2015-01-01", "2015-12-31"))
tables.register_table("tlc_yellow_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2016",
                                                    "2016-01-01", "2016-12-31"))
tables.register_table("tlc_yellow_trips", DataTable("bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2017",
                                                    "2017-01-01", "2017-12-31"))
