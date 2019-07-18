from flask import Flask, request, Response
from query.query_functions import *

app = Flask(__name__)


@app.route('/')
def hello():
    return 'Hello, World!'


@app.route('/total_trips')
def total_trips_query():
    start_date = request.args.get("start", None)
    end_date = request.args.get("end", None)
    r = total_trips_over_date_range(start_date, end_date)
    return Response(response=r.content, content_type=r.content_type, status=r.status)


if __name__ == '__main__':
    app.debug = True
    app.run()
