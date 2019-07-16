from query.backend_factory import get_sql_backend
from errors import *
from datetime import datetime


class DateFilterFrontend:

    supported_date_format = "%Y-%m-%d"
    condition_placeholder = "$DateCondition"

    def __init__(self, backend_name):
        self.backend = get_sql_backend(backend_name)

    def date_range_query(self, query, date_col, start_date, end_date):
        self.validate_date_string(start_date)
        self.validate_date_string(end_date)
        if query.find(self.supported_date_format) != -1:
            query = query.replace(self.condition_placeholder, f"date({date_col}) >= {start_date} AND date({date_col}) <= {end_date}")
            return self.backend.query(query)
        else:
            raise ServerException("Incorrect query configuration")

    @staticmethod
    def validate_date_string(date_string):
        """
        Check if the given date string conforms to the required ISO format (YYYY-MM-DD). Raise exception if it doesn't
        :param date_string: string representing a date
        :return: Nothing
        """

        try:
            datetime.strptime(date_string, format)
        except ValueError as e:
            raise RequestException(f"Incorrect date format resulting in {str(e)}")
