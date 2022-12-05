import json
from decorest import GET, POST, DELETE
from decorest import HttpStatus, RestClient
from decorest import accept, body, content, endpoint, form
from decorest import header, multipart, on, query, stream, timeout


class Index(RestClient):

    @DELETE("/index")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def drop_index(self, payload):
        """Drop an index"""

    @GET("/index")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def describe_index(self, payload):
        """Describe an index"""

    @POST("index")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def create_index(self, payload):
        """create index"""

    @GET("index/progress")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def get_index_build_progress(self, payload):
        """get index build progress"""

    @GET("index/state")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def get_index_state(self, payload):
        """get index state"""
