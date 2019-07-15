from flask import Flask, escape, request, Response
from query_backend.query_functions import *
import json
from functools import wraps
from errors import *

app = Flask(__name__)

def handle_exceptions(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        
        except RequestException as e:
            error = {"status": "RequestError", "reason": str(e)}
            response = Response(json.dumps(error))
            response.content_type = "application/json"
            response.status_code = 400
            return response
        
        except ServerException as e:
            error = {"status": "ServerError", "reason": str(e)}
            response = Response(json.dumps(error))
            response.content_type = "application/json"
            response.status_code = 500
            return response

        except Exception as e:
            error = {"status": "UnknownError", "reason": str(e)}
            response = Response(json.dumps(error))
            response.content_type = "application/json"
            response.status_code = 520
            return response

    return decorated

@app.route('/')
def hello():
    return 'Hello, World!'


@app.route('/total_trips')
@handle_exceptions
def total_trips_query():
    start_date = request.args.get("start", None)
    end_date = request.args.get("end", None)

    query_result = total_trips_over_date_range(start_date, end_date)
    response = Response(json.dumps(query_result))
    response.content_type = "application/json"
    response.status_code = 200

    return response


if __name__ == '__main__':
    app.debug = True
    app.run()
