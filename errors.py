from functools import wraps
import json
from query.query_commons import HttpResponse
import logging


logger = logging.getLogger(__name__)


class RequestException(Exception):
	pass

class ServerException(Exception):
	pass

def handle_exceptions(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        
        except RequestException as e:
            error = {"status": "RequestError", "reason": str(e)}
            return HttpResponse(json.dumps(error), HttpResponse.ContentType.JSON, HttpResponse.Status.BAD_REQUEST)
        
        except ServerException as e:
            logger.exception("Server Exception Occurred")
            error = {"status": "ServerError", "reason": ""}
            return HttpResponse(json.dumps(error), HttpResponse.ContentType.JSON, HttpResponse.Status.INTERNAL_SERVER_ERROR)

        except Exception as e:
            logger.exception("Internal Server Error Occurred")
            error = {"status": "ServerError", "reason": ""}
            return HttpResponse(json.dumps(error), HttpResponse.ContentType.JSON, HttpResponse.Status.INTERNAL_SERVER_ERROR)

    return decorated
