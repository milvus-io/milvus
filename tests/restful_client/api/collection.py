import json

from decorest import GET, POST, DELETE
from decorest import HttpStatus, RestClient
from decorest import accept, body, content, endpoint, form
from decorest import header, multipart, on, query, stream, timeout


class Collection(RestClient):
    
    @DELETE("collection")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def drop_collection(self, payload):
        """Drop a collection"""

    @GET("collection")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def describe_collection(self, payload):
        """Describe a collection"""

    @POST("collection")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def create_collection(self, payload):
        """Create a collection"""

    @GET("collection/existence")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())
    def has_collection(self, payload):
        """Check if a collection exists"""

    @DELETE("collection/load")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())    
    def release_collection(self, payload):
        """Release a collection"""
    
    @POST("collection/load")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())    
    def load_collection(self, payload):
        """Load a collection"""

    @GET("collection/statistics")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())    
    def get_collection_statistics(self, payload):
        """Get collection statistics"""

    @GET("collections")
    @body("payload", lambda p: json.dumps(p))
    @on(200, lambda r: r.json())     
    def show_collections(self, payload):
        """Show collections"""


if __name__ == '__main__':
    client = Collection("http://localhost:19121/api/v1")
    print(client)
