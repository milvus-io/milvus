from time import sleep
from common import common_type as ct
from common import common_func as cf
from base.client_base import TestBase
from utils.util_log import test_log as log


class TestDefault(TestBase):

    def test_e2e(self):
        collection_name, schema = self.init_collection()
        nb = ct.default_nb
        # insert
        res = self.entity_service.insert(collection_name=collection_name, fields_data=cf.gen_fields_data(schema, nb=nb),
                                         num_rows=nb)
        log.info(f"insert {nb} rows into collection {collection_name}, response: {res}")
        # flush
        res = self.entity_service.flush(collection_names=[collection_name])
        log.info(f"flush collection {collection_name}, response: {res}")
        # create index for vector field
        vector_field_name = cf.get_vector_field(schema)
        vector_index_params = cf.gen_index_params(index_type="HNSW")
        res = self.index_service.create_index(collection_name=collection_name, field_name=vector_field_name,
                                              extra_params=vector_index_params)
        log.info(f"create index for vector field {vector_field_name}, response: {res}")
        # load
        res = self.collection_service.load_collection(collection_name=collection_name)
        log.info(f"load collection {collection_name}, response: {res}")

        sleep(5)
        # search
        vectors = cf.gen_vectors(nq=ct.default_nq, schema=schema)
        res = self.entity_service.search(collection_name=collection_name, vectors=vectors,
                                         output_fields=[ct.default_int64_field_name],
                                         search_params=cf.gen_search_params())
        log.info(f"search collection {collection_name}, response: {res}")

        # hybrid search
        res = self.entity_service.search(collection_name=collection_name, vectors=vectors,
                                         output_fields=[ct.default_int64_field_name],
                                         search_params=cf.gen_search_params(),
                                         dsl=ct.default_dsl)
        log.info(f"hybrid search collection {collection_name}, response: {res}")
        # query
        res = self.entity_service.query(collection_name=collection_name, expr=ct.default_expr)

        log.info(f"query collection {collection_name}, response: {res}")


