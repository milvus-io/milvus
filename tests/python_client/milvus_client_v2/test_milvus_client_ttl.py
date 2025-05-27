import pytest
import numpy as np
import time
from common.common_type import CaseLabel, CheckTasks
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log
from utils.util_pymilvus import *
from base.client_v2_base import TestMilvusClientV2Base
from pymilvus import DataType


class TestMilvusClientTTL(TestMilvusClientV2Base):
    """ Test case of Time To Live """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("flush_enable", [True, False])
    def test_milvus_client_ttl_default(self, flush_enable):
        """
        target: verify that data is invisible after ttl
        method: create collection with ttl, insert data, wait for ttl, search data
        expected: data is invisible
        """
        client = self._client()
        ttl = 3
        nb=1000
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("embeddings", DataType.FLOAT_VECTOR, dim=ct.default_dim)
        schema.add_field("visible", DataType.BOOL, nullable=True)
        self.create_collection(client, collection_name, schema=schema, properties={"collection.ttl.seconds": ttl})

        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="embeddings", index_type="IVF_FLAT", metric_type="COSINE", nlist=128)
        self.create_index(client, collection_name, index_params=index_params)

        # load collection
        self.load_collection(client, collection_name)

        # insert data
        insert_times = 3
        for i in range(insert_times):
            vectors = cf.gen_vectors(nb, ct.default_dim)
            rows = []
            start_id = i * nb
            for j in range(nb):
                row = {
                    "id": start_id + j,
                    "embeddings": list(vectors[j]),
                    "visible": False
                }
                rows.append(row)
            self.insert(client, collection_name, rows)
        
        # flush collection if flush_enable is True
        if flush_enable:
            self.flush(client, collection_name)

        # wait for ttl
        time.sleep(ttl+1)

        # search data
        nq = 1
        search_vectors = cf.gen_vectors(nq, ct.default_dim)
        res = self.search(client, collection_name, search_vectors, search_params={}, limit=10)[0]
        assert len(res[0]) == 0
        # query count(*)
        res = self.query(client, collection_name, filter='', output_fields=["count(*)"])[0]
        assert res["count(*)"] == 0

        # insert more data
        for i in range(insert_times):
            vectors = cf.gen_vectors(nb, ct.default_dim)
            rows = []
            start_id = i * nb
            for j in range(nb):
                row = {
                    "id": start_id + j,
                    "embeddings": list(vectors[j]),
                    "visible": True
                }
                rows.append(row)
            self.insert(client, collection_name, rows)

        # flush collection if flush_enable is True
        if flush_enable:
            self.flush(client, collection_name)

        # search data again after insert more data
        res = self.search(client, collection_name, search_vectors, search_params={}, limit=10)[0]
        assert len(res[0]) > 0
        # query count(*)
        res = self.query(client, collection_name, filter='visible==False', output_fields=["count(*)"])[0]
        assert res["count(*)"] == 0

        # query count(visible)
        res = self.query(client, collection_name, filter='visible==True', output_fields=["count(*)"])[0]
        assert res["count(*)"] > 0
        
        # alter ttl to 1000s
        self.alter_collection(client, collection_name, properties={"collection.ttl.seconds": 1000})
        res = self.query(client, collection_name, filter='visible==False', output_fields=["count(*)"])[0]
        assert res["count(*)"] == insert_times * nb

        res = self.query(client, collection_name, filter='', output_fields=["count(*)"])[0]
        assert res["count(*)"] == insert_times * nb * 2
        