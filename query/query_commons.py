class QueryResponse:
    """
    Data object to store SQL query response
    """
    def __init__(self, response, exceed_limit, is_empty):
        self.response = response
        self.exceed_limit = exceed_limit
        self.is_empty = is_empty
