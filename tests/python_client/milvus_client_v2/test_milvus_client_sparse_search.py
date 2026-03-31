import pytest
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base

default_nb = ct.default_nb
default_nq = ct.default_nq
default_limit = ct.default_limit


@pytest.mark.xdist_group("TestSparseSearchShared")
@pytest.mark.tags(CaseLabel.L1)
class TestSparseSearchShared(TestMilvusClientV2Base):
    """Shared collection for sparse vector search read-only tests.
    Schema: int64(PK), float, varchar(65535), sparse_vector
    Data: 4000 rows
    Index: SPARSE_INVERTED_INDEX / IP
    """
    shared_alias = "TestSparseSearchShared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSparseSearchShared" + cf.gen_unique_str("sparse_search")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = cf.gen_default_sparse_schema(auto_id=False)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        data = cf.gen_row_data_by_schema(nb=4000, schema=schema)
        self.insert(client, self.collection_name, data=data)
        self.flush(client, self.collection_name)

        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_sparse_vec_field_name,
                      index_type="SPARSE_INVERTED_INDEX", metric_type="IP", params={})
        self.create_index(client, self.collection_name, index_params=idx)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    def test_sparse_search_default(self):
        """
        target: verify basic sparse vector search returns correct results with IP distance ordering
        method: 1. search on shared sparse collection with default search params
                2. check nq, limit, output_fields, and IP distance descending order via check_task
        expected: search returns nq groups, each with limit results, distances sorted descending (IP)
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_sparse_vectors(default_nq)
        self.search(client, self.collection_name,
                    data=search_vectors,
                    anns_field=ct.default_sparse_vec_field_name,
                    search_params=ct.default_sparse_search_params,
                    limit=default_limit,
                    output_fields=[ct.default_sparse_vec_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "metric": "IP",
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name,
                                 "output_fields": [ct.default_sparse_vec_field_name]})

    @pytest.mark.tags(CaseLabel.L1)
    def test_sparse_search_with_filter(self):
        """
        target: verify sparse search with scalar filter correctly filters results
        method: 1. search with filter "int64 < 100" on shared sparse collection
                2. check nq, limit, distance order via check_task
                3. manually assert every returned hit satisfies int64 < 100
        expected: all returned results have int64 < 100, no false positives from filter
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_sparse_vectors(default_nq)
        filter_limit = 100
        expr = f"{ct.default_int64_field_name} < {filter_limit}"
        search_res, _ = self.search(client, self.collection_name,
                                    data=search_vectors,
                                    anns_field=ct.default_sparse_vec_field_name,
                                    search_params=ct.default_sparse_search_params,
                                    limit=default_limit,
                                    filter=expr,
                                    output_fields=[ct.default_int64_field_name],
                                    check_task=CheckTasks.check_search_results,
                                    check_items={"nq": default_nq,
                                                 "limit": default_limit,
                                                 "metric": "IP",
                                                 "enable_milvus_client_api": True,
                                                 "pk_name": ct.default_int64_field_name,
                                                 "output_fields": [ct.default_int64_field_name]})
        for hits in search_res:
            for hit in hits:
                assert hit[ct.default_int64_field_name] < filter_limit, \
                    f"filter not effective: got {ct.default_int64_field_name}={hit[ct.default_int64_field_name]}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_sparse_search_output_field(self):
        """
        target: verify sparse search returns exactly the requested output fields
        method: 1. search with output_fields=[float, sparse_vector]
                2. check_task verifies returned field set matches requested fields exactly
        expected: each hit contains float and sparse_vector fields, no extra or missing fields
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_sparse_vectors(default_nq)
        self.search(client, self.collection_name,
                    data=search_vectors,
                    anns_field=ct.default_sparse_vec_field_name,
                    search_params=ct.default_sparse_search_params,
                    limit=default_limit,
                    output_fields=[ct.default_float_field_name, ct.default_sparse_vec_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "metric": "IP",
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name,
                                 "output_fields": [ct.default_float_field_name,
                                                   ct.default_sparse_vec_field_name]})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("batch_size", [10, 100, 500])
    def test_sparse_search_iterator(self, batch_size):
        """
        target: verify sparse search iterator works correctly with various batch sizes
        method: 1. create search iterator with batch_size={10,100,500} and limit=500
                2. check_search_iterator verifies: each batch <= batch_size, no duplicate PKs,
                   total results > 0
        expected: iterator exhausts all results, PKs are unique across all batches
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_sparse_vectors(1)
        self.search_iterator(client, self.collection_name,
                             data=search_vectors,
                             batch_size=batch_size,
                             limit=500,
                             anns_field=ct.default_sparse_vec_field_name,
                             search_params=ct.default_sparse_search_params,
                             check_task=CheckTasks.check_search_iterator,
                             check_items={"batch_size": batch_size})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric_type", ["L2", "COSINE"])
    def test_sparse_search_invalid_metric_type(self, metric_type):
        """
        target: verify sparse vector search rejects unsupported metric types
        method: 1. search with metric_type={L2,COSINE} on sparse vector field (only IP is valid)
                2. check_task verifies error response with code 1100
        expected: search fails with error message containing "only IP is supported"
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_sparse_vectors(1)
        search_params = {"metric_type": metric_type, "params": {}}
        self.search(client, self.collection_name,
                    data=search_vectors,
                    anns_field=ct.default_sparse_vec_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1100,
                                 ct.err_msg: f"metric type not match: invalid parameter"
                                             f"[expected=IP][actual={metric_type}]"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nq", [1, 100])
    def test_sparse_search_different_nq(self, nq):
        """
        target: verify sparse search handles different numbers of query vectors correctly
        method: 1. search with nq={1,100} sparse query vectors
                2. check_task verifies len(search_res) == nq and each query returns limit results
        expected: search returns exactly nq groups of results with correct limit and IP ordering
        """
        client = self._client(alias=self.shared_alias)
        search_vectors = cf.gen_sparse_vectors(nq)
        self.search(client, self.collection_name,
                    data=search_vectors,
                    anns_field=ct.default_sparse_vec_field_name,
                    search_params=ct.default_sparse_search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": nq,
                                 "limit": default_limit,
                                 "metric": "IP",
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})


class TestSparseSearchIndependent(TestMilvusClientV2Base):
    """Test cases that require independent collection setup (custom index/mmap/delete/dim)."""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index", ct.sparse_supported_index_types)
    @pytest.mark.parametrize("inverted_index_algo", ct.inverted_index_algo)
    def test_sparse_index_search(self, index, inverted_index_algo):
        """
        target: verify all sparse index types × inverted_index_algo combinations produce correct search results
        method: 1. create collection, insert 3000 rows, build index with parametrized type/algo
                2. search with dim_max_score_ratio=1.05 and output sparse_vector field
                3. check_task verifies nq, limit, output_fields, and IP distance descending order
        expected: search returns correct results for every index type and algo variant
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 3000

        # create collection with sparse schema
        schema = cf.gen_default_sparse_schema(auto_id=False)
        self.create_collection(client, collection_name, schema=schema)

        # insert data
        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # create sparse index
        params = cf.get_index_params_params(index)
        params.update({"inverted_index_algo": inverted_index_algo})
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_sparse_vec_field_name,
                      index_type=index, metric_type="IP", params=params)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # search
        _params = cf.get_search_params_params(index)
        _params.update({"dim_max_score_ratio": 1.05})
        search_params = {"metric_type": "IP", "params": _params}
        search_vectors = cf.gen_sparse_vectors(default_nq)
        self.search(client, collection_name,
                    data=search_vectors,
                    anns_field=ct.default_sparse_vec_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    output_fields=[ct.default_sparse_vec_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "metric": "IP",
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name,
                                 "output_fields": [ct.default_sparse_vec_field_name]})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.sparse_supported_index_types)
    @pytest.mark.parametrize("dim", [32768, ct.max_sparse_vector_dim])
    def test_sparse_index_dim(self, index, dim):
        """
        target: verify sparse index and search work correctly with high-dimensional sparse vectors
        method: 1. create collection, insert sparse vectors with dim={32768, max_sparse_vector_dim}
                   (nb reduced to 100 for max_dim to avoid OOM)
                2. build index and search with default_limit
                3. check_task verifies nq, limit, and IP distance ordering
        expected: search returns correct results even at extreme sparse dimensions
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        # reduce nb for extremely high dims to avoid OOM
        nb = 100 if dim == ct.max_sparse_vector_dim else default_nb

        # create collection with sparse schema
        schema = cf.gen_default_sparse_schema(auto_id=False)
        self.create_collection(client, collection_name, schema=schema)

        # insert data — override sparse vectors with custom dim
        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        sparse_vectors = cf.gen_sparse_vectors(nb, dim=dim)
        for i in range(nb):
            data[i][ct.default_sparse_vec_field_name] = sparse_vectors[i]
        self.insert(client, collection_name, data=data)

        # create sparse index
        params = cf.get_index_params_params(index)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_sparse_vec_field_name,
                      index_type=index, metric_type="IP", params=params)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # search
        search_vectors = cf.gen_sparse_vectors(default_nq)
        self.search(client, collection_name,
                    data=search_vectors,
                    anns_field=ct.default_sparse_vec_field_name,
                    search_params=ct.default_sparse_search_params,
                    limit=default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "metric": "IP",
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.sparse_supported_index_types)
    @pytest.mark.parametrize("inverted_index_algo", ct.inverted_index_algo)
    def test_sparse_index_enable_mmap_search(self, index, inverted_index_algo):
        """
        target: verify sparse search works correctly after enabling mmap on both collection and index
        method: 1. create collection, insert 3000 rows, build sparse index with parametrized type/algo
                2. enable mmap on collection and index, assert properties are set to 'True'
                3. insert 2000 more rows (start=3000), flush and load
                4. search and verify nq, limit, output_fields, IP distance order via check_task
                5. query specific PKs [0,1,10,100] and verify exact match on returned int64 values
        expected: mmap does not affect search correctness; data from both batches is queryable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        first_nb = 3000

        schema = cf.gen_default_sparse_schema(auto_id=False)
        self.create_collection(client, collection_name, schema=schema)

        # insert first batch
        data = cf.gen_row_data_by_schema(nb=first_nb, schema=schema, start=0)
        self.insert(client, collection_name, data=data)

        # create sparse index
        params = cf.get_index_params_params(index)
        params.update({"inverted_index_algo": inverted_index_algo})
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_sparse_vec_field_name,
                      index_type=index, metric_type="IP", params=params)
        self.create_index(client, collection_name, index_params=idx)

        # enable mmap on collection
        self.alter_collection_properties(client, collection_name, properties={'mmap.enabled': True})
        desc, _ = self.describe_collection(client, collection_name)
        assert desc.get("properties", {}).get("mmap.enabled") == 'True'

        # enable mmap on index
        self.alter_index_properties(client, collection_name,
                                    index_name=ct.default_sparse_vec_field_name,
                                    properties={'mmap.enabled': True})
        index_info, _ = self.describe_index(client, collection_name,
                                            index_name=ct.default_sparse_vec_field_name)
        assert index_info.get("mmap.enabled") == 'True'

        # insert second batch
        second_nb = 2000
        data2 = cf.gen_row_data_by_schema(nb=second_nb, schema=schema, start=first_nb)
        self.insert(client, collection_name, data=data2)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # search
        _search_params = cf.get_search_params_params(index)
        search_params = {"metric_type": "IP", "params": _search_params}
        search_vectors = cf.gen_sparse_vectors(default_nq)
        self.search(client, collection_name,
                    data=search_vectors,
                    anns_field=ct.default_sparse_vec_field_name,
                    search_params=search_params,
                    limit=default_limit,
                    output_fields=[ct.default_sparse_vec_field_name],
                    check_task=CheckTasks.check_search_results,
                    check_items={"nq": default_nq,
                                 "limit": default_limit,
                                 "metric": "IP",
                                 "enable_milvus_client_api": True,
                                 "pk_name": ct.default_int64_field_name,
                                 "output_fields": [ct.default_sparse_vec_field_name]})

        # query to verify data from both batches
        expr_id_list = [0, 1, 10, 100]
        term_expr = f'{ct.default_int64_field_name} in {expr_id_list}'
        res, _ = self.query(client, collection_name, filter=term_expr,
                            output_fields=[ct.default_int64_field_name])
        assert len(res) == len(expr_id_list)
        returned_ids = sorted([r[ct.default_int64_field_name] for r in res])
        assert returned_ids == sorted(expr_id_list)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index", ct.sparse_supported_index_types)
    def test_search_sparse_ratio(self, index):
        """
        target: verify sparse search behavior with valid and invalid dim_max_score_ratio values
        method: 1. create collection, insert 4000 rows, build index with drop_ratio_build=0.01
                2. verify index exists via list_indexes
                3. search with valid dim_max_score_ratio={0.5, 0.99, 1, 1.3}:
                   assert results non-empty and distances sorted descending (IP)
                4. search with invalid dim_max_score_ratio={0.49, 1.4}:
                   assert error code 999 with range validation message
        expected: valid ratios return correctly ordered results; out-of-range ratios are rejected
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 4000
        drop_ratio_build = 0.01

        schema = cf.gen_default_sparse_schema(auto_id=False)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # create sparse index with drop_ratio_build
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_sparse_vec_field_name,
                      index_type=index, metric_type="IP",
                      params={"drop_ratio_build": drop_ratio_build})
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # verify index exists
        indexes, _ = self.list_indexes(client, collection_name)
        assert ct.default_sparse_vec_field_name in indexes

        # search with valid dim_max_score_ratio values
        search_vectors = cf.gen_sparse_vectors(default_nq)
        _params = {"drop_ratio_search": 0.2}
        for dim_max_score_ratio in [0.5, 0.99, 1, 1.3]:
            _params.update({"dim_max_score_ratio": dim_max_score_ratio})
            search_params = {"metric_type": "IP", "params": _params}
            search_res, _ = self.search(client, collection_name,
                                        data=search_vectors,
                                        anns_field=ct.default_sparse_vec_field_name,
                                        search_params=search_params,
                                        limit=default_limit)
            assert len(search_res) == default_nq
            for hits in search_res:
                assert len(hits) > 0, f"no results for dim_max_score_ratio={dim_max_score_ratio}"
                distances = [hit['distance'] for hit in hits]
                assert distances == sorted(distances, reverse=True), \
                    f"distances not sorted descending for IP with ratio={dim_max_score_ratio}"

        # search with invalid dim_max_score_ratio values
        error = {ct.err_code: 999,
                 ct.err_msg: "should be in range [0.500000, 1.300000]"}
        for invalid_ratio in [0.49, 1.4]:
            _params.update({"dim_max_score_ratio": invalid_ratio})
            search_params = {"metric_type": "IP", "params": _params}
            self.search(client, collection_name,
                        data=search_vectors,
                        anns_field=ct.default_sparse_vec_field_name,
                        search_params=search_params,
                        limit=default_limit,
                        check_task=CheckTasks.err_res,
                        check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("index", ct.sparse_supported_index_types)
    def test_sparse_search_after_delete(self, index):
        """
        target: verify deleted entities are excluded from sparse search results
        method: 1. create collection, insert 2000 rows, build index and load
                2. delete first 1000 rows (int64 in [0..999])
                3. search and output int64 field
                4. manually assert every returned PK is NOT in the deleted set
        expected: no deleted PK appears in any search result across all nq queries
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 2000

        schema = cf.gen_default_sparse_schema(auto_id=False)
        self.create_collection(client, collection_name, schema=schema)

        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        params = cf.get_index_params_params(index)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(field_name=ct.default_sparse_vec_field_name,
                      index_type=index, metric_type="IP", params=params)
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # delete first half
        delete_ids = list(range(nb // 2))
        delete_expr = f"{ct.default_int64_field_name} in {delete_ids}"
        self.delete(client, collection_name, filter=delete_expr)

        # search and verify deleted PKs not in results
        search_vectors = cf.gen_sparse_vectors(default_nq)
        search_res, _ = self.search(client, collection_name,
                                    data=search_vectors,
                                    anns_field=ct.default_sparse_vec_field_name,
                                    search_params=ct.default_sparse_search_params,
                                    limit=default_limit,
                                    output_fields=[ct.default_int64_field_name])
        assert len(search_res) == default_nq
        deleted_set = set(delete_ids)
        for hits in search_res:
            assert len(hits) > 0
            for hit in hits:
                assert hit[ct.default_int64_field_name] not in deleted_set, \
                    f"deleted PK {hit[ct.default_int64_field_name]} found in search results"
