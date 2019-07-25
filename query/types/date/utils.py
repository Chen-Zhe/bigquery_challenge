from datetime import datetime
import logging

from conf import DataFormatConfig
from http_json.errors import RequestException

logger = logging.getLogger(__name__)


class DateFormat:
    supported_date_format = DataFormatConfig.supported_date_format

    @staticmethod
    def validate_date_string(date_string):
        """
        Check if the given date string conforms to the supported date format. Raise exception if it doesn't
        :param date_string: string representing a date
        :return: Date object
        """

        try:
            return datetime.strptime(date_string, DateFormat.supported_date_format).date()
        except ValueError as e:
            raise RequestException(f"Incorrect date format in {date_string} resulting in {str(e)}")
        except Exception as e:
            logger.exception(f"Error occurred when parsing date string: {str(e)}")
            raise RequestException("Invalid date string")

    @staticmethod
    def date_to_string(date):
        """
        convert date/datetime to supported date format string
        :param date: date/datetime object
        :return: string
        """
        return date.strftime(DateFormat.supported_date_format)
