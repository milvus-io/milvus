import json
from decorest import GET, POST, DELETE
from decorest import HttpStatus, RestClient
from decorest import accept, body, content, endpoint, form
from decorest import header, multipart, on, query, stream, timeout


class Entity(RestClient):

    @POST("distance")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def calc_distance(self, payload):
        """ Calculate distance between two points """

    @DELETE("entities")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def delete(self, payload):
        """delete entities"""

    @POST("entities")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def insert(self, payload):
        """insert entities"""

    @POST("persist")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def flush(self, payload):
        """flush entities"""

    @POST("persist/segment-info")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def get_persistent_segment_info(self, payload):
        """get persistent segment info"""

    @POST("persist/state")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def get_flush_state(self, payload):
        """get flush state"""

    @POST("query")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def query(self, payload):
        """query entities"""

    @POST("query-segment-info")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def get_query_segment_info(self, payload):
        """get query segment info"""

    @POST("search")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def search(self, payload):
        """search entities"""
    