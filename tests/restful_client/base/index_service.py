from api.index import Index
from models import common, schema, milvus, server

TIMEOUT = 30


class IndexService:

    def __init__(self, endpoint=None, timeout=None):
        if timeout is None:
            timeout = TIMEOUT
        if endpoint is None:
            endpoint = "http://localhost:9091/api/v1"
        self._index = Index(endpoint=endpoint)

    def drop_index(self, base, collection_name, db_name, field_name, index_name):
        payload = server.DropIndexRequest(base=base, collection_name=collection_name,
                                          db_name=db_name, field_name=field_name, index_name=index_name)
        payload = payload.dict()
        return self._index.drop_index(payload)

    def describe_index(self, base, collection_name, db_name, field_name, index_name):
        payload = server.DescribeIndexRequest(base=base, collection_name=collection_name,
                                              db_name=db_name, field_name=field_name, index_name=index_name)
        payload = payload.dict()
        return self._index.describe_index(payload)

    def create_index(self, base=None, collection_name=None, db_name=None, extra_params=None,
                     field_name=None, index_name=None):
        payload = {
            "base": base,
            "collection_name": collection_name,
            "db_name": db_name,
            "extra_params": extra_params,
            "field_name": field_name,
            "index_name": index_name
        }
        # payload = server.CreateIndexRequest(base=base, collection_name=collection_name, db_name=db_name,
        #                                     extra_params=extra_params, field_name=field_name, index_name=index_name)
        # payload = payload.dict()
        return self._index.create_index(payload)

    def get_index_build_progress(self, base, collection_name, db_name, field_name, index_name):
        payload = server.GetIndexBuildProgressRequest(base=base, collection_name=collection_name,
                                                      db_name=db_name, field_name=field_name, index_name=index_name)
        payload = payload.dict()
        return self._index.get_index_build_progress(payload)

    def get_index_state(self, base, collection_name, db_name, field_name, index_name):
        payload = server.GetIndexStateRequest(base=base, collection_name=collection_name,
                                              db_name=db_name, field_name=field_name, index_name=index_name)
        payload = payload.dict()
        return self._index.get_index_state(payload)




