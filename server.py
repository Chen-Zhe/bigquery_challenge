from flask import Flask, escape, request, Response
from query_backend.query_functions import *
import json

app = Flask(__name__)

@app.route('/')
def hello():
    name = request.args.get("name", "World")
    return f'Hello, {escape(name)}!'


@app.route('/q')
def query():
    query_result = test()
    response = Response(json.dumps(query_result))
    response.content_type = "application/json"

    response.status_code = 200
    return response


if __name__ == '__main__':
    app.debug = True
    app.run()