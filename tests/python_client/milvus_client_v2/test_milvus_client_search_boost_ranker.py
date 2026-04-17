import pytest
from pymilvus import DataType, Function, FunctionScore, FunctionType

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *

default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit


@pytest.mark.xdist_group("TestSearchBoostRanker")
class TestSearchBoostRanker(TestMilvusClientV2Base):
    """Test search with Boost Ranker (FunctionScore) functionality"""

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSearchBoostRanker" + cf.gen_unique_str("_")
        self.pk_field_name = "id"
        self.vector_field_name = "vector"
        self.int64_field_name = "int64_field"
        self.float_field_name = "float_field"
        self.varchar_field_name = "varchar_field"
        self.dim = 128
        self.primary_keys = []
        self.datas = []

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        """Initialize collection with schema and data before test class runs"""
        client = self._client()

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(self.pk_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(self.vector_field_name, DataType.FLOAT_VECTOR, dim=self.dim)
        schema.add_field(self.int64_field_name, DataType.INT64)
        schema.add_field(self.float_field_name, DataType.FLOAT)
        schema.add_field(self.varchar_field_name, DataType.VARCHAR, max_length=256)

        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        # Insert data
        data = cf.gen_row_data_by_schema(nb=default_nb, schema=schema)
        self.insert(client, self.collection_name, data=data)
        self.datas = data
        self.primary_keys = [row[self.pk_field_name] for row in data]

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.vector_field_name, metric_type="COSINE", index_type="AUTOINDEX")
        self.create_index(client, self.collection_name, index_params=index_params)

        # Load collection
        self.load_collection(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(), self.collection_name)

        request.addfinalizer(teardown)

    def _make_boost_function(self, name="boost1", weight="1.5", filter_expr=None):
        """Helper to create a single boost Function"""
        params = {"reranker": "boost", "weight": weight}
        if filter_expr is not None:
            params["filter"] = filter_expr
        return Function(
            name=name,
            function_type=FunctionType.RERANK,
            input_field_names=[],
            output_field_names=[],
            params=params,
        )

    def _make_function_score(self, functions, params=None):
        """Helper to create a FunctionScore"""
        if not isinstance(functions, list):
            functions = [functions]
        return FunctionScore(functions=functions, params=params)

    # ==================== Positive Tests ====================

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_boost_ranker_single(self):
        """
        target: search with a single boost ranker with filter and weight
        method: create FunctionScore with one boost function, pass as ranker to search
        expected: search returns results successfully
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn = self._make_boost_function(weight="2.0", filter_expr=f"{self.int64_field_name} > 0")
        fs = self._make_function_score(boost_fn)

        res, _ = self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            ranker=fs,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "limit": default_limit,
                "pk_name": self.pk_field_name,
            },
        )
        assert len(res) == default_nq

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_no_filter(self):
        """
        target: search with boost ranker without filter (weight applies to all results)
        method: create boost function without filter param
        expected: search returns results successfully
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn = self._make_boost_function(weight="1.5")
        fs = self._make_function_score(boost_fn)

        res, _ = self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            ranker=fs,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "limit": default_limit,
                "pk_name": self.pk_field_name,
            },
        )
        assert len(res) == default_nq

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_multiple_functions(self):
        """
        target: search with multiple boost functions in one FunctionScore
        method: create FunctionScore with two boost functions with different filters
        expected: search returns results successfully
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn1 = self._make_boost_function(name="boost1", weight="2.0", filter_expr=f"{self.int64_field_name} > 0")
        boost_fn2 = self._make_boost_function(name="boost2", weight="0.5", filter_expr=f"{self.int64_field_name} <= 0")
        fs = self._make_function_score([boost_fn1, boost_fn2])

        res, _ = self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            ranker=fs,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "limit": default_limit,
                "pk_name": self.pk_field_name,
            },
        )
        assert len(res) == default_nq

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_boost_mode_sum(self):
        """
        target: search with boost_mode="sum" in FunctionScore params
        method: create FunctionScore with boost_mode="sum"
        expected: search returns results with sum-based scoring
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn = self._make_boost_function(weight="2.0", filter_expr=f"{self.int64_field_name} > 0")
        fs = self._make_function_score(boost_fn, params={"boost_mode": "sum"})

        res, _ = self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            ranker=fs,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "limit": default_limit,
                "pk_name": self.pk_field_name,
            },
        )
        assert len(res) == default_nq

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_boost_mode_multiply(self):
        """
        target: search with boost_mode="multiply" (default) in FunctionScore params
        method: create FunctionScore with explicit boost_mode="multiply"
        expected: search returns results successfully
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn = self._make_boost_function(weight="1.5", filter_expr=f"{self.int64_field_name} > 0")
        fs = self._make_function_score(boost_fn, params={"boost_mode": "multiply"})

        res, _ = self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            ranker=fs,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "limit": default_limit,
                "pk_name": self.pk_field_name,
            },
        )
        assert len(res) == default_nq

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_function_mode_sum(self):
        """
        target: search with multiple functions and function_mode="sum"
        method: create FunctionScore with two boost functions and function_mode="sum"
        expected: search returns results successfully
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn1 = self._make_boost_function(name="boost1", weight="1.5", filter_expr=f"{self.int64_field_name} > 0")
        boost_fn2 = self._make_boost_function(name="boost2", weight="0.8", filter_expr=f"{self.float_field_name} > 0")
        fs = self._make_function_score([boost_fn1, boost_fn2], params={"function_mode": "sum"})

        res, _ = self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            ranker=fs,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "limit": default_limit,
                "pk_name": self.pk_field_name,
            },
        )
        assert len(res) == default_nq

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_with_filter_expr(self):
        """
        target: combine boost ranker with a search-level filter expression
        method: pass both ranker=FunctionScore and filter=expr to search
        expected: search returns filtered results with boost scoring
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn = self._make_boost_function(weight="2.0", filter_expr=f"{self.int64_field_name} > 500")
        fs = self._make_function_score(boost_fn)

        search_filter = f"{self.int64_field_name} >= 0"
        res, _ = self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            filter=search_filter,
            ranker=fs,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "limit": default_limit,
                "pk_name": self.pk_field_name,
            },
        )
        assert len(res) == default_nq

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_with_output_fields(self):
        """
        target: search with boost ranker and output_fields
        method: pass output_fields along with ranker
        expected: search returns results with the requested output fields
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn = self._make_boost_function(weight="1.5")
        fs = self._make_function_score(boost_fn)

        output_fields = [self.int64_field_name, self.float_field_name, self.varchar_field_name]
        res, _ = self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            output_fields=output_fields,
            ranker=fs,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "limit": default_limit,
                "pk_name": self.pk_field_name,
            },
        )
        assert len(res) == default_nq
        # Verify output fields are present in the entity sub-dict
        for hits in res:
            for hit in hits:
                entity = hit.get("entity", hit)
                for field in output_fields:
                    assert field in entity, f"Expected field '{field}' in result entity"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("limit", [1, 10, 100])
    def test_search_boost_ranker_with_limit(self, limit):
        """
        target: search with boost ranker and various limit values
        method: pass different limit values with boost ranker
        expected: search returns correct number of results
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn = self._make_boost_function(weight="1.5")
        fs = self._make_function_score(boost_fn)

        res, _ = self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=limit,
            ranker=fs,
            check_task=CheckTasks.check_search_results,
            check_items={
                "enable_milvus_client_api": True,
                "nq": default_nq,
                "limit": limit,
                "pk_name": self.pk_field_name,
            },
        )
        assert len(res) == default_nq

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_score_affected(self):
        """
        target: verify boost ranker actually changes scores compared to search without ranker
        method: search with and without boost ranker, compare scores
        expected: scores should differ when boost is applied
        """
        client = self._client()
        vectors = cf.gen_vectors(1, self.dim)

        # Search without ranker
        res_no_ranker, _ = self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
        )

        # Search with a large boost weight
        boost_fn = self._make_boost_function(weight="10.0")
        fs = self._make_function_score(boost_fn, params={"boost_mode": "multiply"})
        res_with_ranker, _ = self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            ranker=fs,
        )

        # Verify scores are different (boost should affect scores)
        scores_no_ranker = [hit["distance"] for hit in res_no_ranker[0]]
        scores_with_ranker = [hit["distance"] for hit in res_with_ranker[0]]
        assert scores_no_ranker != scores_with_ranker, (
            "Boost ranker should change scores compared to search without ranker"
        )

    # ==================== Negative Tests ====================

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_boost_ranker_invalid_weight(self):
        """
        target: search with invalid weight value in boost function
        method: set weight="invalid_float"
        expected: search raises an error
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn = self._make_boost_function(weight="invalid_float")
        fs = self._make_function_score(boost_fn)

        self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            ranker=fs,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "invalid parameter"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_boost_ranker_missing_weight(self):
        """
        target: search with boost function missing weight param
        method: create boost function without weight in params
        expected: search raises an error
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn = Function(
            name="boost_no_weight",
            function_type=FunctionType.RERANK,
            input_field_names=[],
            output_field_names=[],
            params={"reranker": "boost"},
        )
        fs = self._make_function_score(boost_fn)

        self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            ranker=fs,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "must set weight params"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_boost_ranker_with_group_by(self):
        """
        target: search with boost ranker combined with group_by
        method: pass both ranker and group_by_field to search
        expected: search raises an error (incompatible)
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn = self._make_boost_function(weight="1.5")
        fs = self._make_function_score(boost_fn)

        self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            ranker=fs,
            group_by_field=self.int64_field_name,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "segment scorer with group_by"},
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_boost_ranker_invalid_filter(self):
        """
        target: search with invalid filter expression in boost function
        method: set filter to an invalid expression
        expected: search raises an error
        """
        client = self._client()
        vectors = cf.gen_vectors(default_nq, self.dim)
        boost_fn = self._make_boost_function(weight="1.5", filter_expr="invalid_field @@@ 123")
        fs = self._make_function_score(boost_fn)

        self.search(
            client,
            self.collection_name,
            vectors,
            anns_field=self.vector_field_name,
            limit=default_limit,
            ranker=fs,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 1100, "err_msg": "parse expr failed"},
        )
