from enum import Enum
import json


class QueryResponse:
    def __init__(self, response, exceed_limit, empty_response):
        self.response = response
        self.exceed_limit = exceed_limit
        self.empty_response = empty_response


class HttpResponse:
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


def df2json_dict(df):
    return {"data": [{key: value for (key, value) in zip(df.columns, row)} for row in df.values], "status": "OK"}


def json2http(json_dict):
    return HttpResponse(json.dumps(json_dict), HttpResponse.ContentType.JSON, HttpResponse.Status.OK)