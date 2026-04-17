import pytest
from pymilvus import DataType

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks

default_nb = ct.default_nb
default_dim = ct.default_dim


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
        self.collection_name = "TestSearchIteratorShared" + cf.gen_unique_str("search_iterator")

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
    def test_search_iterator_with_different_batch_size(self, batch_size):
        """
        target: verify search iterator returns correct batch sizes with unique PKs
        method: 1. run search iterator with various batch_size values on shared COSINE collection
                2. check batch_size constraint via check_search_iterator
        expected: each batch ≤ batch_size, all PKs unique across batches
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_vectors(1, default_dim)
        search_params = {"metric_type": "COSINE"}
        self.search_iterator(
            client,
            self.collection_name,
            data=search_vectors,
            batch_size=batch_size,
            search_params=search_params,
            anns_field=ct.default_float_vec_field_name,
            check_task=CheckTasks.check_search_iterator,
            check_items={"batch_size": batch_size},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_iterator_invalid_nq(self):
        """
        target: verify search iterator rejects nq > 1 (multiple vectors)
        method: 1. run search iterator with 2 vectors on shared collection
        expected: error indicating multiple vectors not supported
        """
        client = self._client(alias=self.shared_alias)
        batch_size = 100
        search_vectors = cf.gen_vectors(2, default_dim)
        search_params = {"metric_type": "COSINE"}
        self.search_iterator(
            client,
            self.collection_name,
            data=search_vectors,
            batch_size=batch_size,
            search_params=search_params,
            anns_field=ct.default_float_vec_field_name,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1, "err_msg": "does not support processing multiple vectors"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_iterator_not_support_search_by_pk(self):
        """
        target: verify search iterator does not support search-by-pk
        method: 1. search iterator with data=None + ids → error (NoneType)
                2. search iterator with data + ids → error (both provided)
        expected: both cases return error
        """
        client = self._client(alias=self.shared_alias)
        batch_size = 100
        search_vectors = cf.gen_vectors(1, default_dim)
        search_params = {"metric_type": "COSINE"}
        ids_to_search = [1]
        self.search_iterator(
            client,
            self.collection_name,
            data=None,
            batch_size=batch_size,
            search_params=search_params,
            anns_field=ct.default_float_vec_field_name,
            ids=ids_to_search,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 999, "err_msg": "object of type 'NoneType' has no len()"},
        )

        self.search_iterator(
            client,
            self.collection_name,
            data=search_vectors,
            batch_size=batch_size,
            search_params=search_params,
            anns_field=ct.default_float_vec_field_name,
            ids=ids_to_search,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 999, "err_msg": "Either ids or data must be provided, not both"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_iterator_with_expression(self):
        """
        target: verify search iterator with expression filter returns correct batches (COSINE)
        method: 1. run search iterator with filter "1000 <= int64 < 2000" on shared collection
                2. check batch_size via check_search_iterator
        expected: iterator returns batches of correct size with unique PKs
        """
        client = self._client(alias=self.shared_alias)
        batch_size = 100
        search_vectors = cf.gen_vectors(1, default_dim)
        search_params = {"metric_type": "COSINE"}
        expression = f"1000 <= {ct.default_int64_field_name} < 2000"
        self.search_iterator(
            client,
            self.collection_name,
            data=search_vectors,
            batch_size=batch_size,
            search_params=search_params,
            anns_field=ct.default_float_vec_field_name,
            filter=expression,
            check_task=CheckTasks.check_search_iterator,
            check_items={"batch_size": batch_size, "pk_range": (1000, 2000)},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_range_search_iterator_cosine(self):
        """
        target: verify range search iterator works with COSINE metric on shared collection
        method: 1. regular search to get distance reference points
                2. run range search iterator with radius/range_filter derived from step 1
                3. check range constraints in iterator results
        expected: range iterator results within [radius, range_filter], batches correct size
        """
        client = self._client(alias=self.shared_alias)
        batch_size = 100
        limit = 200
        search_vector = cf.gen_vectors(1, default_dim)
        search_params = {"metric_type": "COSINE"}

        # get distance reference from regular search
        res = self.search(
            client,
            self.collection_name,
            data=search_vector,
            anns_field=ct.default_float_vec_field_name,
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": 1,
                "limit": limit,
                "metric": "COSINE",
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )[0]

        # COSINE: higher distance = more similar, so radius < range_filter
        radius = res[0][limit // 2]["distance"] - 0.1
        range_filter = res[0][0]["distance"] + 0.1
        range_search_params = {"metric_type": "COSINE", "params": {"radius": radius, "range_filter": range_filter}}
        self.search_iterator(
            client,
            self.collection_name,
            data=search_vector,
            batch_size=batch_size,
            search_params=range_search_params,
            anns_field=ct.default_float_vec_field_name,
            check_task=CheckTasks.check_search_iterator,
            check_items={
                "metric_type": "COSINE",
                "batch_size": batch_size,
                "radius": radius,
                "range_filter": range_filter,
            },
        )


class TestSearchIteratorIndependent(TestMilvusClientV2Base):
    """Independent tests for search iterator scenarios requiring unique schemas
    (different metrics, vector types, range search, binary vectors)
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("metric_type", ["L2", "IP"])
    @pytest.mark.parametrize("vector_data_type", ct.all_dense_vector_types)
    def test_range_search_iterator_default(self, metric_type, vector_data_type):
        """
        target: verify iterator and range search iterator work across all dense metrics and vector types
        method: 1. create collection with given vector_data_type, build index with metric_type
                2. run basic search iterator, check batch_size and metric ordering
                3. run regular search to get distance reference points
                4. run range search iterator with radius/range_filter derived from step 3
                5. check range constraints in iterator results
        expected: iterator respects batch_size; range iterator results within [radius, range_filter]
        """
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
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type=metric_type)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        search_vector = cf.gen_vectors(1, dim, vector_data_type)
        search_params = {"metric_type": metric_type}
        self.search_iterator(
            client,
            collection_name,
            data=search_vector,
            batch_size=batch_size,
            search_params=search_params,
            anns_field=ct.default_float_vec_field_name,
            check_task=CheckTasks.check_search_iterator,
            check_items={"metric_type": metric_type, "batch_size": batch_size},
        )

        limit = 200
        res = self.search(
            client,
            collection_name,
            data=search_vector,
            anns_field=ct.default_float_vec_field_name,
            search_params=search_params,
            limit=limit,
            check_task=CheckTasks.check_search_results,
            check_items={
                "nq": 1,
                "limit": limit,
                "metric": metric_type,
                "enable_milvus_client_api": True,
                "pk_name": ct.default_int64_field_name,
            },
        )[0]
        # range search iterator with radius/range_filter derived from regular search distances
        if metric_type != "L2":
            radius = res[0][limit // 2]["distance"] - 0.1
            range_filter = res[0][0]["distance"] + 0.1
        else:
            radius = res[0][limit // 2]["distance"] + 0.1
            range_filter = res[0][0]["distance"] - 0.1
        range_search_params = {"metric_type": metric_type, "params": {"radius": radius, "range_filter": range_filter}}
        self.search_iterator(
            client,
            collection_name,
            data=search_vector,
            batch_size=batch_size,
            search_params=range_search_params,
            anns_field=ct.default_float_vec_field_name,
            check_task=CheckTasks.check_search_iterator,
            check_items={
                "metric_type": metric_type,
                "batch_size": batch_size,
                "radius": radius,
                "range_filter": range_filter,
            },
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_iterator_binary(self):
        """
        target: verify search iterator works with binary vectors (BIN_FLAT/JACCARD)
        method: 1. create collection with binary vector, insert data
                2. run search iterator with JACCARD metric
                3. check batch_size via check_search_iterator
        expected: iterator returns batches of correct size with unique PKs
        """
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
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_binary_vec_field_name, index_type="BIN_FLAT", metric_type="JACCARD")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        _, binary_search_vectors = cf.gen_binary_vectors(1, ct.default_dim)
        self.search_iterator(
            client,
            collection_name,
            data=binary_search_vectors,
            batch_size=batch_size,
            search_params=ct.default_search_binary_params,
            anns_field=ct.default_binary_vec_field_name,
            check_task=CheckTasks.check_search_iterator,
            check_items={"batch_size": batch_size},
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric_type", ["L2", "IP"])
    def test_search_iterator_with_expression(self, metric_type):
        """
        target: verify search iterator with expression filter works with L2/IP metrics
        method: 1. create collection with given metric, insert data
                2. run search iterator with filter "1000 <= int64 < 2000"
                3. check batch_size via check_search_iterator
        expected: iterator returns batches of correct size with unique PKs
        """
        batch_size = 100
        dim = ct.default_dim
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
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_float_vec_field_name, metric_type=metric_type)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
        search_vectors = cf.gen_vectors(1, dim)
        search_params = {"metric_type": metric_type}
        expression = f"1000 <= {ct.default_int64_field_name} < 2000"
        self.search_iterator(
            client,
            collection_name,
            data=search_vectors,
            batch_size=batch_size,
            search_params=search_params,
            anns_field=ct.default_float_vec_field_name,
            filter=expression,
            check_task=CheckTasks.check_search_iterator,
            check_items={"batch_size": batch_size, "pk_range": (1000, 2000)},
        )
