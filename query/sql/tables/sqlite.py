from query.sql.tables.collection import DataTable, SqlTableCollection


tables = SqlTableCollection(["tlc_green_trips", "tlc_yellow_trips"])

tables.register_table("tlc_green_trips", DataTable("tlc_green_trips_2016", "2016-01-01", "2016-12-31"))

tables.register_table("tlc_yellow_trips", DataTable("tlc_yellow_trips_2016", "2016-01-01", "2016-12-31"))
