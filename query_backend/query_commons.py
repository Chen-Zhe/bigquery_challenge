class QueryResponse:
    def __init__(self, response, exceed_limit, empty_response):
        self.response = response
        self.exceed_limit = exceed_limit
        self.empty_response = empty_response
