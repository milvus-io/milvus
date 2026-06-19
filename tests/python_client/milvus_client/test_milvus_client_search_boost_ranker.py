import math

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType, Function, FunctionScore, FunctionType

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

    def _prepare_deterministic_collection(self, client, metric_type="COSINE"):
        collection_name = "TestSearchBoostRankerCorrectness" + cf.gen_unique_str("_")
        dim = 2
        rows = [
            {
                self.pk_field_name: 1,
                self.vector_field_name: [1.0, 0.0],
                self.int64_field_name: 1,
                self.float_field_name: 0.1,
                self.varchar_field_name: "odd",
            },
            {
                self.pk_field_name: 2,
                self.vector_field_name: [0.8, 0.6],
                self.int64_field_name: 2,
                self.float_field_name: 0.2,
                self.varchar_field_name: "even",
            },
            {
                self.pk_field_name: 3,
                self.vector_field_name: [0.6, 0.8],
                self.int64_field_name: 3,
                self.float_field_name: 0.3,
                self.varchar_field_name: "odd",
            },
            {
                self.pk_field_name: 4,
                self.vector_field_name: [0.0, 1.0],
                self.int64_field_name: 4,
                self.float_field_name: 0.4,
                self.varchar_field_name: "even",
            },
        ]

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(self.pk_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(self.vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(self.int64_field_name, DataType.INT64)
        schema.add_field(self.float_field_name, DataType.FLOAT)
        schema.add_field(self.varchar_field_name, DataType.VARCHAR, max_length=256)

        self.create_collection(client, collection_name, schema=schema)
        self.insert(client, collection_name, data=rows)
        self.flush(client, collection_name)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name=self.vector_field_name, metric_type=metric_type, index_type="FLAT", params={})
        self.create_index(client, collection_name, index_params=index_params)
        self.load_collection(client, collection_name)
        return collection_name, dim

    def _search_boost_correctness(self, client, collection_name, ranker):
        res, _ = self.search(
            client,
            collection_name,
            [[1.0, 0.0]],
            anns_field=self.vector_field_name,
            search_params={},
            limit=4,
            ranker=ranker,
            output_fields=[self.pk_field_name],
        )
        hits = res[0]
        return [hit[self.pk_field_name] for hit in hits], [hit["distance"] for hit in hits]

    def _assert_boost_scores(self, actual_ids, actual_scores, expected_scores):
        expected_ids = [pk for pk, _ in sorted(expected_scores.items(), key=lambda item: (-item[1], item[0]))]
        assert actual_ids == expected_ids
        for pk, score in zip(actual_ids, actual_scores):
            assert math.isclose(score, expected_scores[pk], rel_tol=1e-5, abs_tol=1e-5), (
                f"expected score for pk {pk} to be {expected_scores[pk]}, got {score}"
            )

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

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_multiply_correctness(self):
        """
        target: verify boost_mode=multiply applies only matching filter rows and sorts by boosted score
        method: use deterministic vectors and scalar filter with known COSINE base scores
        expected: returned IDs and distances match base_score * boost_weight for matched rows
        """
        client = self._client()
        collection_name, _ = self._prepare_deterministic_collection(client)
        try:
            boost_fn = self._make_boost_function(weight="2.0", filter_expr=f"{self.int64_field_name} in [2, 4]")
            fs = self._make_function_score(boost_fn, params={"boost_mode": "multiply"})

            actual_ids, actual_scores = self._search_boost_correctness(client, collection_name, fs)
            self._assert_boost_scores(
                actual_ids,
                actual_scores,
                {
                    1: 1.0,
                    2: 1.6,
                    3: 0.6,
                    4: 0.0,
                },
            )
        finally:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_sum_correctness(self):
        """
        target: verify boost_mode=sum adds boost scores only to rows matching the scorer filter
        method: use deterministic vectors and scalar filter with known COSINE base scores
        expected: returned IDs and distances match base_score + boost_weight for matched rows
        """
        client = self._client()
        collection_name, _ = self._prepare_deterministic_collection(client)
        try:
            boost_fn = self._make_boost_function(weight="0.5", filter_expr=f"{self.int64_field_name} in [2, 4]")
            fs = self._make_function_score(boost_fn, params={"boost_mode": "sum"})

            actual_ids, actual_scores = self._search_boost_correctness(client, collection_name, fs)
            self._assert_boost_scores(
                actual_ids,
                actual_scores,
                {
                    1: 1.0,
                    2: 1.3,
                    3: 0.6,
                    4: 0.5,
                },
            )
        finally:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_all_boost_functions_miss_keep_base_scores(self):
        """
        target: verify null boost scores are skipped when all boost function filters miss
        method: use two boost functions whose filters match no row in a deterministic collection
        expected: every row keeps its original base score and ordering
        """
        client = self._client()
        collection_name, _ = self._prepare_deterministic_collection(client)
        try:
            boost_fn1 = self._make_boost_function(
                name="boost_none_int", weight="2.0", filter_expr=f"{self.int64_field_name} > 100"
            )
            boost_fn2 = self._make_boost_function(
                name="boost_none_string", weight="3.0", filter_expr=f"{self.varchar_field_name} == 'missing'"
            )
            fs = self._make_function_score([boost_fn1, boost_fn2], params={"function_mode": "sum", "boost_mode": "sum"})

            actual_ids, actual_scores = self._search_boost_correctness(client, collection_name, fs)
            self._assert_boost_scores(
                actual_ids,
                actual_scores,
                {
                    1: 1.0,
                    2: 0.8,
                    3: 0.6,
                    4: 0.0,
                },
            )
        finally:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_function_mode_sum_correctness(self):
        """
        target: verify multiple boost functions combine with function_mode=sum before final boost
        method: use overlapping deterministic filters and boost_mode=sum
        expected: rows receive the sum of matching function scores and are sorted by final score
        """
        client = self._client()
        collection_name, _ = self._prepare_deterministic_collection(client)
        try:
            boost_fn1 = self._make_boost_function(
                name="boost_even", weight="0.5", filter_expr=f"{self.int64_field_name} in [2, 4]"
            )
            boost_fn2 = self._make_boost_function(
                name="boost_high", weight="0.25", filter_expr=f"{self.int64_field_name} >= 3"
            )
            fs = self._make_function_score([boost_fn1, boost_fn2], params={"function_mode": "sum", "boost_mode": "sum"})

            actual_ids, actual_scores = self._search_boost_correctness(client, collection_name, fs)
            self._assert_boost_scores(
                actual_ids,
                actual_scores,
                {
                    1: 1.0,
                    2: 1.3,
                    3: 0.85,
                    4: 0.75,
                },
            )
        finally:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_function_mode_multiply_correctness(self):
        """
        target: verify multiple boost functions combine with function_mode=multiply before final boost
        method: use overlapping deterministic filters and boost_mode=multiply
        expected: rows matching one or more functions use the multiplied function score; unmatched rows keep base score
        """
        client = self._client()
        collection_name, _ = self._prepare_deterministic_collection(client)
        try:
            boost_fn1 = self._make_boost_function(
                name="boost_even", weight="2.0", filter_expr=f"{self.int64_field_name} in [2, 4]"
            )
            boost_fn2 = self._make_boost_function(
                name="boost_high", weight="3.0", filter_expr=f"{self.int64_field_name} >= 3"
            )
            fs = self._make_function_score(
                [boost_fn1, boost_fn2], params={"function_mode": "multiply", "boost_mode": "multiply"}
            )

            actual_ids, actual_scores = self._search_boost_correctness(client, collection_name, fs)
            self._assert_boost_scores(
                actual_ids,
                actual_scores,
                {
                    1: 1.0,
                    2: 1.6,
                    3: 1.8,
                    4: 0.0,
                },
            )
        finally:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_function_multiply_boost_sum_correctness(self):
        """
        target: verify function_mode=multiply and boost_mode=sum produce exact boosted scores
        method: use overlapping deterministic filters so one row matches both functions and one row matches none
        expected: matching function scores are multiplied, then added to the base score; unmatched rows keep base score
        """
        client = self._client()
        collection_name, _ = self._prepare_deterministic_collection(client)
        try:
            boost_fn1 = self._make_boost_function(
                name="boost_even", weight="2.0", filter_expr=f"{self.int64_field_name} in [2, 4]"
            )
            boost_fn2 = self._make_boost_function(
                name="boost_high", weight="3.0", filter_expr=f"{self.int64_field_name} >= 3"
            )
            fs = self._make_function_score(
                [boost_fn1, boost_fn2], params={"function_mode": "multiply", "boost_mode": "sum"}
            )

            actual_ids, actual_scores = self._search_boost_correctness(client, collection_name, fs)
            self._assert_boost_scores(
                actual_ids,
                actual_scores,
                {
                    1: 1.0,
                    2: 2.8,
                    3: 3.6,
                    4: 6.0,
                },
            )
        finally:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_l2_multiply_correctness(self):
        """
        target: verify boost_mode=multiply can improve a deterministic L2 result
        method: multiply row 3's L2 distance by 0.25 to move it ahead of row 2
        expected: returned distances match boosted L2 distances and are sorted ascending
        """
        client = self._client()
        collection_name, _ = self._prepare_deterministic_collection(client, metric_type="L2")
        try:
            boost_fn = self._make_boost_function(weight="0.25", filter_expr=f"{self.int64_field_name} == 3")
            fs = self._make_function_score(boost_fn, params={"boost_mode": "multiply"})

            actual_ids, actual_scores = self._search_boost_correctness(client, collection_name, fs)
            assert actual_ids == [1, 3, 2, 4]
            expected_scores = {
                1: 0.0,
                2: 0.4,
                3: 0.2,
                4: 2.0,
            }
            for pk, score in zip(actual_ids, actual_scores):
                assert math.isclose(score, expected_scores[pk], rel_tol=1e-5, abs_tol=1e-5), (
                    f"expected score for pk {pk} to be {expected_scores[pk]}, got {score}"
                )
        finally:
            self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_boost_ranker_l2_sum_correctness(self):
        """
        target: verify boost_mode=sum applies to deterministic L2 internal scores
        method: add 0.7 to row 3's internal score to move it ahead of row 2
        expected: returned distances match proxy-restored scores and are sorted ascending
        """
        client = self._client()
        collection_name, _ = self._prepare_deterministic_collection(client, metric_type="L2")
        try:
            boost_fn = self._make_boost_function(weight="0.7", filter_expr=f"{self.int64_field_name} == 3")
            fs = self._make_function_score(boost_fn, params={"boost_mode": "sum"})

            actual_ids, actual_scores = self._search_boost_correctness(client, collection_name, fs)
            assert actual_ids == [1, 3, 2, 4]
            expected_scores = {
                1: 0.0,
                2: 0.4,
                3: 0.1,
                4: 2.0,
            }
            for pk, score in zip(actual_ids, actual_scores):
                assert math.isclose(score, expected_scores[pk], rel_tol=1e-5, abs_tol=1e-5), (
                    f"expected score for pk {pk} to be {expected_scores[pk]}, got {score}"
                )
        finally:
            self.drop_collection(client, collection_name)

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
