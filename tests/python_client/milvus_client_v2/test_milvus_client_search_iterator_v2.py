import random
import pytest
from pymilvus import DataType
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base

prefix = "search_collection"
default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params

vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
field_name = ct.default_float_vec_field_name
binary_field_name = ct.default_binary_vec_field_name


@pytest.mark.xdist_group("TestSearchIteratorShared")
@pytest.mark.tags(CaseLabel.GPU)
class TestSearchIteratorShared(TestMilvusClientV2Base):
    """Shared collection for search iterator tests.
    Schema: int64(PK), float, varchar(65535), json, float_vector(128), dynamic=False
    Data: 3000 rows
    Index: COSINE on float_vector
    """
    shared_alias = "TestSearchIteratorShared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchIteratorShared" + cf.gen_unique_str("_")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=default_dim)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        data = cf.gen_row_data_by_schema(nb=3000, schema=schema)
        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type="COSINE")
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("batch_size", [10, 100, 777, 1000])
    def test_search_iterator_with_different_limit(self, batch_size):
        """
        target: test search iterator normal
        method: 1. search iterator
                2. check the result, expect pk not repeat and meet the expr requirements
        expected: search successfully
        """
        client = self._client(alias=self.shared_alias)
        # 2. search iterator
        search_params = {"metric_type": "COSINE"}
        self.search_iterator(client, self.collection_name, data=vectors[:1],
                             batch_size=batch_size,
                             search_params=search_params,
                             anns_field=ct.default_float_vec_field_name,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={"batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_iterator_invalid_nq(self):
        """
        target: test search iterator normal
        method: 1. search iterator
                2. check the result, expect pk
        expected: search successfully
        """
        client = self._client(alias=self.shared_alias)
        batch_size = 100
        # 2. search iterator
        search_params = {"metric_type": "COSINE"}
        self.search_iterator(client, self.collection_name, data=vectors[:2],
                             batch_size=batch_size,
                             search_params=search_params,
                             anns_field=ct.default_float_vec_field_name,
                             check_task=CheckTasks.err_res,
                             check_items={"err_code": 1,
                                          "err_msg": "does not support processing multiple vectors"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_iterator_not_support_search_by_pk(self):
        """
        target: test search iterator does not support search by pk
        method: 1. search iterator by pk
        expected: search failed with error
        """
        client = self._client(alias=self.shared_alias)
        batch_size = 100
        # 2. search iterator by pk (no data, only ids)
        search_params = {"metric_type": "COSINE"}
        ids_to_search = [1]
        self.search_iterator(client, self.collection_name,
                             data=None,
                             batch_size=batch_size,
                             search_params=search_params,
                             anns_field=ct.default_float_vec_field_name,
                             ids=ids_to_search,
                             check_task=CheckTasks.err_res,
                             check_items={"err_code": 999,
                                          "err_msg": "object of type 'NoneType' has no len()"})

        self.search_iterator(client, self.collection_name,
                             data=vectors[:1],
                             batch_size=batch_size,
                             search_params=search_params,
                             anns_field=ct.default_float_vec_field_name,
                             ids=ids_to_search,
                             check_task=CheckTasks.err_res,
                             check_items={"err_code": 999,
                                          "err_msg": "Either ids or data must be provided, not both"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_iterator_with_expression(self):
        """
        target: test search iterator normal (COSINE metric)
        method: 1. search iterator
                2. check the result, expect pk not repeat and meet the expr requirements
        expected: search successfully
        """
        client = self._client(alias=self.shared_alias)
        batch_size = 100
        # 2. search iterator
        search_params = {"metric_type": "COSINE"}
        expression = "1000 <= int64 < 2000"
        self.search_iterator(client, self.collection_name, data=vectors[:1],
                             batch_size=batch_size,
                             search_params=search_params,
                             anns_field=ct.default_float_vec_field_name,
                             filter=expression,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={})


class TestSearchIteratorIndependent(TestMilvusClientV2Base):
    """ Test case of search iterator """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("metric_type", ct.dense_metrics)
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    def test_range_search_iterator_default(self, metric_type, vector_data_type):
        """
        target: test iterator range search
        method: 1. search iterator
                2. check the result, expect pk not repeat and meet the range requirements
        expected: search successfully
        """
        # 1. initialize with data
        batch_size = 100
        dim = default_dim
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, vector_data_type, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        # create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type=metric_type)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        search_vector = cf.gen_vectors(1, dim, vector_data_type)
        search_params = {"metric_type": metric_type}
        self.search_iterator(client, collection_name, data=search_vector,
                             batch_size=batch_size,
                             search_params=search_params,
                             anns_field=ct.default_float_vec_field_name,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={"metric_type": metric_type,
                                          "batch_size": batch_size})

        limit = 200
        res = self.search(client, collection_name,
                          data=search_vector,
                          anns_field=ct.default_float_vec_field_name,
                          search_params=search_params,
                          limit=200,
                          check_task=CheckTasks.check_search_results,
                          check_items={"nq": 1, "limit": limit,
                                       "enable_milvus_client_api": True,
                                       "pk_name": ct.default_int64_field_name})[0]
        # 2. search iterator with range
        if metric_type != "L2":
            radius = res[0][limit // 2]["distance"] - 0.1  # pick a radius to make sure there exists results
            range_filter = res[0][0]["distance"] + 0.1
            search_params = {"metric_type": metric_type,
                             "params": {"radius": radius, "range_filter": range_filter}}
            self.search_iterator(client, collection_name, data=search_vector,
                                 batch_size=batch_size,
                                 search_params=search_params,
                                 anns_field=ct.default_float_vec_field_name,
                                 check_task=CheckTasks.check_search_iterator,
                                 check_items={"metric_type": metric_type, "batch_size": batch_size,
                                              "radius": radius,
                                              "range_filter": range_filter})
        else:
            radius = res[0][limit // 2]["distance"] + 0.1
            range_filter = res[0][0]["distance"] - 0.1
            search_params = {"metric_type": metric_type,
                             "params": {"radius": radius, "range_filter": range_filter}}
            self.search_iterator(client, collection_name, data=search_vector,
                                 batch_size=batch_size,
                                 search_params=search_params,
                                 anns_field=ct.default_float_vec_field_name,
                                 check_task=CheckTasks.check_search_iterator,
                                 check_items={"metric_type": metric_type, "batch_size": batch_size,
                                              "radius": radius,
                                              "range_filter": range_filter})

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_iterator_binary(self):
        """
        target: test search iterator binary
        method: 1. search iterator
                2. check the result, expect pk
        expected: search successfully
        """
        # 1. initialize with data
        batch_size = 200
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_binary_vec_field_name, DataType.BINARY_VECTOR, dim=ct.default_dim)
        self.create_collection(client, collection_name, schema=schema)
        # Insert binary data
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        # Create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name, index_type="BIN_FLAT",
                      metric_type="JACCARD")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # 2. search iterator
        _, binary_search_vectors = cf.gen_binary_vectors(2, ct.default_dim)
        self.search_iterator(client, collection_name, data=binary_search_vectors[:1],
                             batch_size=batch_size,
                             search_params=ct.default_search_binary_params,
                             anns_field=ct.default_binary_vec_field_name,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={"batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metrics", ["L2", "IP"])
    def test_search_iterator_with_expression(self, metrics):
        """
        target: test search iterator normal (non-COSINE metrics)
        method: 1. search iterator
                2. check the result, expect pk not repeat and meet the expr requirements
        expected: search successfully
        """
        # 1. initialize with data
        batch_size = 100
        dim = 128
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(ct.default_int64_field_name, DataType.INT64, is_primary=True)
        schema.add_field(ct.default_float_field_name, DataType.FLOAT)
        schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(ct.default_json_field_name, DataType.JSON)
        schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        # create index and load
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type=metrics)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        # 2. search iterator
        search_params = {"metric_type": metrics}
        expression = "1000 <= int64 < 2000"
        self.search_iterator(client, collection_name, data=vectors[:1],
                             batch_size=batch_size,
                             search_params=search_params,
                             anns_field=ct.default_float_vec_field_name,
                             filter=expression,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={})
