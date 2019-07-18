from functools import wraps
import json
from query.query_commons import HttpResponse
import logging


logger = logging.getLogger(__name__)


class RequestException(Exception):
    pass


class ServerException(Exception):
    pass


class QueryGenerationException(ServerException):
    pass


def handle_exceptions(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        error_msg = {"status": "", "data": list()}

        try:
            return f(*args, **kwargs)
        
        except RequestException as e:
            error_msg["status"] = f"Error - Request: {str(e)}"
            return HttpResponse(json.dumps(error_msg), HttpResponse.ContentType.JSON, HttpResponse.Status.BAD_REQUEST)
        
        except ServerException:
            logger.exception("Server Exception Occurred")
            error_msg["status"] = "Error - Server"
            return HttpResponse(json.dumps(error_msg), HttpResponse.ContentType.JSON, HttpResponse.Status.INTERNAL_SERVER_ERROR)

        except Exception:
            logger.exception("Internal Server Error Occurred")
            error_msg["status"] = "Error - Server"
            return HttpResponse(json.dumps(error_msg), HttpResponse.ContentType.JSON, HttpResponse.Status.INTERNAL_SERVER_ERROR)

    return decorated
