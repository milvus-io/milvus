import logging
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from base.client_v2_base import TestMilvusClientV2Base
import pytest
from idx_gpu_hnsw_sq import GPU_HNSW_SQ

index_type = "GPU_HNSW_SQ"
success = "success"
pk_field_name = 'id'
vector_field_name = 'vector'
dim = ct.default_dim
default_nb = ct.default_nb
default_build_params = {"M": 16, "efConstruction": 200, "sq_type": "SQ8"}
default_search_params = {"ef": 64, "refine_k": 1}


class TestGpuHnswSQBuildParams(TestMilvusClientV2Base):
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("params", GPU_HNSW_SQ.build_params)
    def test_gpu_hnsw_sq_build_params(self, params):
        """
        Test the build params of GPU_HNSW_SQ index
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        all_rows = cf.gen_row_data_by_schema(
            nb=default_nb,
            schema=schema,
            start=0,
            random_pk=False
        )

        self.insert(client, collection_name, all_rows)
        self.flush(client, collection_name)

        # create index
        build_params = params.get("params", None)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name,
                               metric_type=cf.get_default_metric_for_vector_type(vector_type=DataType.FLOAT_VECTOR),
                               index_type=index_type,
                               params=build_params)
        # build index
        if params.get("expected", None) != success:
            self.create_index(client, collection_name, index_params,
                              check_task=CheckTasks.err_res,
                              check_items=params.get("expected"))
        else:
            self.create_index(client, collection_name, index_params)
            self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)

            # load collection
            self.load_collection(client, collection_name)

            # search
            nq = 2
            search_vectors = cf.gen_vectors(nq, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
            if params.get("relaxed_limit"):
                results = client.search(collection_name, search_vectors,
                                        search_params=default_search_params,
                                        limit=ct.default_limit)
                for r in results:
                    assert len(r) > 0, f"expected > 0 results but got {len(r)}"
            else:
                self.search(client, collection_name, search_vectors,
                            search_params=default_search_params,
                            limit=ct.default_limit,
                            check_task=CheckTasks.check_search_results,
                            check_items={"enable_milvus_client_api": True,
                                         "nq": nq,
                                         "limit": ct.default_limit,
                                         "pk_name": pk_field_name})

            # verify the index params are persisted
            idx_info = client.describe_index(collection_name, vector_field_name)
            if build_params is not None:
                for key, value in build_params.items():
                    if value is not None:
                        assert key in idx_info.keys()
                        assert str(value) in idx_info.values()

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("vector_data_type", ct.all_vector_types)
    def test_gpu_hnsw_sq_on_all_vector_types(self, vector_data_type):
        """
        Test GPU_HNSW_SQ index on all the vector types and metrics
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        if vector_data_type == DataType.SPARSE_FLOAT_VECTOR:
            schema.add_field(vector_field_name, datatype=vector_data_type)
        else:
            schema.add_field(vector_field_name, datatype=vector_data_type, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        all_rows = cf.gen_row_data_by_schema(
            nb=default_nb,
            schema=schema,
            start=0,
            random_pk=False
        )

        self.insert(client, collection_name, all_rows)
        self.flush(client, collection_name)

        # create index
        index_params = self.prepare_index_params(client)[0]
        metric_type = cf.get_default_metric_for_vector_type(vector_data_type)
        index_params.add_index(field_name=vector_field_name,
                               metric_type=metric_type,
                               index_type=index_type,
                               params=default_build_params)
        if vector_data_type not in GPU_HNSW_SQ.supported_vector_types:
            self.create_index(client, collection_name, index_params,
                              check_task=CheckTasks.err_res,
                              check_items={"err_code": 999,
                                           "err_msg": f"can't build with this index GPU_HNSW_SQ: invalid parameter"})

        else:
            self.create_index(client, collection_name, index_params)
            self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
            # load collection
            self.load_collection(client, collection_name)
            # search
            nq = 2
            search_vectors = cf.gen_vectors(nq, dim=dim, vector_data_type=vector_data_type)
            self.search(client, collection_name, search_vectors,
                        search_params=default_search_params,
                        limit=ct.default_limit,
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": nq,
                                     "limit": ct.default_limit,
                                     "pk_name": pk_field_name})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric", GPU_HNSW_SQ.supported_metrics)
    def test_gpu_hnsw_sq_on_all_metrics(self, metric):
        """
        Test GPU_HNSW_SQ index on all supported metrics
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        self.create_collection(client, collection_name, schema=schema)

        all_rows = cf.gen_row_data_by_schema(
            nb=default_nb,
            schema=schema,
            start=0,
            random_pk=False
        )

        self.insert(client, collection_name, all_rows)
        self.flush(client, collection_name)

        # create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name,
                               metric_type=metric,
                               index_type=index_type,
                               params=default_build_params)
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        # load collection
        self.load_collection(client, collection_name)
        # search
        nq = 2
        search_vectors = cf.gen_vectors(nq, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        self.search(client, collection_name, search_vectors,
                    search_params=default_search_params,
                    limit=ct.default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": nq,
                                 "limit": ct.default_limit,
                                 "pk_name": pk_field_name})


class TestGpuHnswSQFilteredSearch(TestMilvusClientV2Base):
    """Delete-then-query / filtered-search parity for GPU_HNSW_SQ.

    GPU_HNSW_SQ is an alias of GPU_HNSW and shares its GPU search/filter path, so
    it must apply the delete/TTL/partition BitsetView too (design doc
    20260619-gpu-hnsw.md, "Filtered search"): a search after a delete excludes the
    deleted rows and keeps the live ones, with no invalid_args error.
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("metric", GPU_HNSW_SQ.supported_metrics)
    def test_gpu_hnsw_sq_delete_then_search(self, metric):
        """
        Delete a subset of rows, then search/query and verify the deleted rows
        are absent and the live rows are still returned (no invalid_args).
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema, _ = self.create_schema(client)
        schema.add_field(pk_field_name, datatype=DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(vector_field_name, datatype=DataType.FLOAT_VECTOR, dim=dim)
        # Strong consistency so the delete is visible to the immediately-following query
        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        # insert, then flush so the segment seals and gets a GPU_HNSW_SQ index
        nb = default_nb
        random_vectors = list(cf.gen_vectors(nb, dim, vector_data_type=DataType.FLOAT_VECTOR))
        rows = [{pk_field_name: i, vector_field_name: random_vectors[i]} for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=vector_field_name,
                               metric_type=metric,
                               index_type=index_type,
                               params=default_build_params)
        self.create_index(client, collection_name, index_params)
        self.wait_for_index_ready(client, collection_name, index_name=vector_field_name)
        self.load_collection(client, collection_name)

        # delete the first delete_num primary keys
        delete_num = 100
        deleted_pks = [i for i in range(delete_num)]
        self.delete(client, collection_name, ids=deleted_pks)

        # search over the whole collection: deleted ids must not appear, and the
        # call must succeed (no invalid_args from a non-empty delete bitset)
        surviving_ids = [i for i in range(delete_num, nb)]
        nq = 2
        search_vectors = cf.gen_vectors(nq, dim=dim, vector_data_type=DataType.FLOAT_VECTOR)
        self.search(client, collection_name, search_vectors,
                    search_params=default_search_params,
                    limit=ct.default_limit,
                    check_task=CheckTasks.check_search_results,
                    check_items={"enable_milvus_client_api": True,
                                 "nq": nq,
                                 "limit": ct.default_limit,
                                 "pk_name": pk_field_name})
        res, _ = self.search(client, collection_name, search_vectors,
                             search_params=default_search_params,
                             limit=ct.default_limit)
        for hits in res:
            for hit in hits:
                assert hit.get("id") not in deleted_pks, \
                    f"deleted id {hit.get('id')} returned by GPU_HNSW_SQ search after delete"

        # query the deleted range: must be empty; query the live range: must be present
        deleted_expr = f"{pk_field_name} < {delete_num}"
        self.query(client, collection_name, filter=deleted_expr,
                   check_task=CheckTasks.check_query_results,
                   check_items={"exp_res": [], "pk_name": pk_field_name})
        live_res, _ = self.query(client, collection_name,
                                 filter=f"{pk_field_name} >= {delete_num} and {pk_field_name} < {delete_num + 10}",
                                 output_fields=[pk_field_name])
        returned = sorted(r[pk_field_name] for r in live_res)
        assert returned == surviving_ids[:10], \
            f"live rows missing after delete: expected {surviving_ids[:10]}, got {returned}"


@pytest.mark.xdist_group("TestGpuHnswSQSearchParams")
class TestGpuHnswSQSearchParams(TestMilvusClientV2Base):
    """Test search with pagination functionality for GPU_HNSW_SQ index"""

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestGpuHnswSQSearchParams" + cf.gen_unique_str("_")
        self.float_vector_field_name = vector_field_name
        self.float_vector_dim = dim
        self.primary_keys = []
        self.enable_dynamic_field = False
        self.datas = []

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        """
        Initialize collection before test class runs
        """
        client = self._client()
        collection_schema = self.create_schema(client)[0]
        collection_schema.add_field(pk_field_name, DataType.INT64, is_primary=True, auto_id=False)
        collection_schema.add_field(self.float_vector_field_name, DataType.FLOAT_VECTOR, dim=128)
        self.create_collection(client, self.collection_name, schema=collection_schema,
                               enable_dynamic_field=self.enable_dynamic_field, force_teardown=False)
        all_data = cf.gen_row_data_by_schema(
            nb=default_nb,
            schema=collection_schema,
            start=0,
            random_pk=False
        )
        self.insert(client, self.collection_name, data=all_data)
        self.primary_keys.extend([i for i in range(default_nb)])

        self.flush(client, self.collection_name)
        # Create GPU_HNSW_SQ index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.float_vector_field_name,
                               metric_type="COSINE",
                               index_type=index_type,
                               params=default_build_params)
        self.create_index(client, self.collection_name, index_params=index_params)
        self.wait_for_index_ready(client, self.collection_name, index_name=self.float_vector_field_name)
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(), self.collection_name)
        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("params", GPU_HNSW_SQ.search_params)
    def test_gpu_hnsw_sq_search_params(self, params):
        """
        Test the search params of GPU_HNSW_SQ index
        """
        client = self._client()
        collection_name = self.collection_name
        nq = 2
        search_vectors = cf.gen_vectors(nq, dim=self.float_vector_dim, vector_data_type=DataType.FLOAT_VECTOR)
        search_params = params.get("params", None)
        if params.get("expected", None) != success:
            self.search(client, collection_name, search_vectors,
                        search_params=search_params,
                        limit=ct.default_limit,
                        check_task=CheckTasks.err_res,
                        check_items=params.get("expected"))
        else:
            self.search(client, collection_name, search_vectors,
                        search_params=search_params,
                        limit=ct.default_limit,
                        check_task=CheckTasks.check_search_results,
                        check_items={"enable_milvus_client_api": True,
                                     "nq": nq,
                                     "limit": ct.default_limit,
                                     "pk_name": pk_field_name})
