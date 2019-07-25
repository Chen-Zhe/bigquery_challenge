import json
from enum import Enum


class QueryResponse:
    """
    Data object to store SQL query response
    """
    def __init__(self, response, exceed_limit, is_empty):
        self.response = response
        self.exceed_limit = exceed_limit
        self.is_empty = is_empty


class HttpResponse:
    """
    Data object to store HTTP response
    """
    class ContentType(Enum):
        JSON = "application/json"
        TEXT = "text/plain"
        HTML = "text/html"

    class Status(Enum):
        OK = 200
        ACCEPTED = 202
        NO_CONTENT = 204
        BAD_REQUEST = 400
        NOT_FOUND = 404
        INTERNAL_SERVER_ERROR = 500
        UNKNOWN_ERROR = 520

    def __init__(self, content, content_type, status):
        self.content = content
        self.content_type = content_type.value
        self.status = status.value


def df2json_list(df):
    """Pandas dataframe to row-wise list of dict"""
    return df.to_dict(orient="records")


def json2http(json_list, partial=False):
    """
    wrap JSON into HttpResponse
    :param json_list: list of dict
    :param partial: whether the query reached pre-defined limit
    :return: HttpResponse
    """
    if partial:
        status = "Partial Result"
    else:
        status = "OK"
    json_dict = {"data": json_list, "status": status}
    return HttpResponse(json.dumps(json_dict), HttpResponse.ContentType.JSON, HttpResponse.Status.OK)
