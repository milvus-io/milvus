from time import sleep

from decorest import HttpStatus, RestClient
from models.schema import CollectionSchema
from base.collection_service import CollectionService
from base.index_service import IndexService
from base.entity_service import EntityService

from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct

class Base:
    """init base class"""

    endpoint = None
    collection_service = None
    index_service = None
    entity_service = None
    collection_name = None
    collection_object_list = []

    def setup_class(self):
        log.info("setup class")

    def teardown_class(self):
        log.info("teardown class")

    def setup_method(self, method):
        log.info(("*" * 35) + " setup " + ("*" * 35))
        log.info("[setup_method] Start setup test case %s." % method.__name__)
        host = cf.param_info.param_host
        port = cf.param_info.param_port
        self.endpoint = "http://" + host + ":" + str(port) + "/api/v1"
        self.collection_service = CollectionService(self.endpoint)
        self.index_service = IndexService(self.endpoint)
        self.entity_service = EntityService(self.endpoint)

    def teardown_method(self, method):
        res = self.collection_service.has_collection(collection_name=self.collection_name)
        log.info(f"collection {self.collection_name} exists: {res}")
        if res["value"] is True:
            res = self.collection_service.drop_collection(self.collection_name)
            log.info(f"drop collection {self.collection_name} res: {res}")
        res = self.collection_service.show_collections()
        all_collections = res["collection_names"]
        union_collections = set(all_collections) & set(self.collection_object_list)
        for collection in union_collections:
            res = self.collection_service.drop_collection(collection)
            log.info(f"drop collection {collection} res: {res}")
        log.info("[teardown_method] Start teardown test case %s." % method.__name__)
        log.info(("*" * 35) + " teardown " + ("*" * 35))


class TestBase(Base):
    """init test base class"""

    def init_collection(self, name=None, schema=None):
        collection_name = cf.gen_unique_str("test") if name is None else name
        self.collection_name = collection_name
        self.collection_object_list.append(collection_name)
        if schema is None:
            schema = cf.gen_default_schema(collection_name=collection_name)
        # create collection
        res = self.collection_service.create_collection(collection_name=collection_name, schema=schema)

        log.info(f"create collection name: {collection_name} with schema: {schema}")
        return collection_name, schema




