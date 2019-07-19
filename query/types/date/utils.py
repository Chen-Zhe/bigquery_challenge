from datetime import datetime
from errors import RequestException

supported_date_format = "%Y-%m-%d"


def validate_date_string(date_string):
    """
    Check if the given date string conforms to the required ISO format (YYYY-MM-DD). Raise exception if it doesn't
    :param date_string: string representing a date
    :return: Date object
    """

    try:
        return datetime.strptime(date_string, supported_date_format).date()
    except ValueError as e:
        raise RequestException(f"Incorrect date format in {date_string} resulting in {str(e)}")
    except Exception:
        raise RequestException("Invalid date string")
