from functools import wraps
import json
from query.query_commons import HttpResponse


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
            error = {"status": "ServerError", "reason": str(e)}
            return HttpResponse(json.dumps(error), HttpResponse.ContentType.JSON, HttpResponse.Status.INTERNAL_SERVER_ERROR)

        except Exception as e:
            error = {"status": "UnknownError", "reason": str(e)}
            return HttpResponse(json.dumps(error), HttpResponse.ContentType.JSON, HttpResponse.Status.UNKNOWN_ERROR)

    return decorated