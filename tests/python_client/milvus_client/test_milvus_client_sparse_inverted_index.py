import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType, Function, FunctionType

ROW_COUNT = 3000
IP_SCORE_RATIO = 1.003
SPARSE_QUERY = {1: 1.0}

IP_INDEX_FIELDS = {
    "sparse_default": {},
    "sparse_sindi": {
        "inverted_index_algo": "SINDI",
        "sindi_window_size": 65535,
    },
    "sparse_block_maxscore": {
        "inverted_index_algo": "BLOCK_MAX_MAXSCORE",
        "block_max_block_size": 128,
    },
    "sparse_block_wand": {
        "inverted_index_algo": "BLOCK_MAX_WAND",
        "block_max_block_size": 128,
    },
    "sparse_stream": {
        "inverted_index_algo": "DAAT_MAXSCORE",
        "inverted_index_codec": "block_streamvbyte",
    },
    "sparse_masked": {
        "inverted_index_algo": "DAAT_MAXSCORE",
        "inverted_index_codec": "block_maskedvbyte",
    },
    "sparse_fp16": {
        "inverted_index_algo": "DAAT_MAXSCORE",
        "quant_type": "fp16",
    },
    "sparse_fp32": {
        "inverted_index_algo": "DAAT_MAXSCORE",
        "quant_type": "fp32",
    },
    "sparse_sindi_w1024": {
        "inverted_index_algo": "SINDI",
        "sindi_window_size": 1024,
    },
    "sparse_sindi_w4096": {
        "inverted_index_algo": "SINDI",
        "sindi_window_size": 4096,
    },
}

BM25_INDEX_FIELDS = {
    "bm25_sindi": {
        "inverted_index_algo": "SINDI",
        "sindi_window_size": 65535,
        "bm25_k1": 1.2,
        "bm25_b": 0.75,
    },
    "bm25_u16": {
        "inverted_index_algo": "DAAT_MAXSCORE",
        "quant_type": "u16",
        "bm25_k1": 1.2,
        "bm25_b": 0.75,
    },
    "bm25_u32": {
        "inverted_index_algo": "DAAT_MAXSCORE",
        "quant_type": "u32",
        "bm25_k1": 1.2,
        "bm25_b": 0.75,
    },
    "bm25_sindi_low": {
        "inverted_index_algo": "SINDI",
        "sindi_window_size": 65535,
        "bm25_k1": 0.8,
        "bm25_b": 0.25,
    },
    "bm25_sindi_high": {
        "inverted_index_algo": "SINDI",
        "sindi_window_size": 65535,
        "bm25_k1": 2.0,
        "bm25_b": 1.0,
    },
    "bm25_sindi_implicit": {
        "inverted_index_algo": "SINDI",
        "sindi_window_size": 65535,
    },
    "bm25_sindi_jieba": {
        "inverted_index_algo": "SINDI",
        "sindi_window_size": 65535,
        "bm25_k1": 1.2,
        "bm25_b": 0.75,
    },
}

BM25_ANALYZER_PARAMS = {
    "bm25_sindi_jieba": {"tokenizer": "jieba"},
}

ORDINARY_IP_INDEX_FIELDS = {
    "sparse_taat_naive": {"inverted_index_algo": "TAAT_NAIVE"},
    "sparse_daat_maxscore": {"inverted_index_algo": "DAAT_MAXSCORE"},
    "sparse_daat_wand": {"inverted_index_algo": "DAAT_WAND"},
    "sparse_sindi_no_drop_build": {"inverted_index_algo": "SINDI"},
    "sparse_sindi_drop_build": {
        "inverted_index_algo": "SINDI",
        "drop_ratio_build": 0.4,
    },
}

LIFECYCLE_ENCODING_INDEX_FIELDS = {
    "sparse_stream": {
        "inverted_index_algo": "DAAT_MAXSCORE",
        "inverted_index_codec": "block_streamvbyte",
    },
    "sparse_masked": {
        "inverted_index_algo": "DAAT_MAXSCORE",
        "inverted_index_codec": "block_maskedvbyte",
    },
    "sparse_sindi": {
        "inverted_index_algo": "SINDI",
        "sindi_window_size": 65535,
    },
}

DROP_RATIO_EFFECT_FIELD = "sparse_sindi_drop_ratio_effect"
DROP_RATIO_EFFECT_QUERY = {1: 1.0, 2: 0.39, 3: 0.38, 4: 0.37, 5: 0.36}

LIFECYCLE_IP_INDEX_FIELDS = {
    **LIFECYCLE_ENCODING_INDEX_FIELDS,
    DROP_RATIO_EFFECT_FIELD: {
        "inverted_index_algo": "SINDI",
        "sindi_window_size": 65535,
    },
}


def _ip_vector(row_id):
    return {
        1: IP_SCORE_RATIO ** (ROW_COUNT - row_id),
        2: 1.0,
        3: 0.5,
        4: 0.25,
    }


def _bm25_document(row_id):
    if row_id == 0:
        return "alpha alpha alpha beta"
    if row_id == 1:
        return "alpha beta"
    if row_id == 2:
        return "alpha"
    return "gamma delta"


def _bm25_jieba_document(row_id):
    if row_id == 0:
        return "稀疏 稀疏 稀疏 向量"
    if row_id == 1:
        return "稀疏 向量"
    if row_id == 2:
        return "稀疏"
    return "全文 检索"


def _drop_ratio_effect_vector(row_id):
    if row_id == 0:
        return {1: 1.0}
    if row_id == 1:
        return {4: 3.0, 5: 3.0}
    if row_id == 2:
        return {2: 1.0, 3: 1.0}
    return {10000 + row_id: 1.0}


class _SparseInvertedIndexV3Base(TestMilvusClientV2Base):
    def _search_hits(
        self, client, collection_name, field_name, data, metric_type="IP", limit=10, filter=None, params=None
    ):
        result, _ = self.search(
            client,
            collection_name,
            data=data,
            anns_field=field_name,
            search_params={"metric_type": metric_type, "params": params or {}},
            limit=limit,
            filter=filter,
            output_fields=["id"],
        )
        assert len(result) == 1
        return result[0]

    @staticmethod
    def _assert_scores_descending(hits):
        scores = [hit["distance"] for hit in hits]
        assert scores == sorted(scores, reverse=True)

    @staticmethod
    def _hit_ids(hits):
        return [hit["id"] for hit in hits]

    def _assert_exact_ip_topk(self, hits, limit=10):
        assert self._hit_ids(hits) == list(range(limit))
        self._assert_scores_descending(hits)

    def _assert_index_definition(self, client, collection_name, field_name, metric_type, expected_params):
        index_info, _ = self.describe_index(client, collection_name, index_name=field_name)
        assert index_info["index_type"] == "SPARSE_INVERTED_INDEX"
        assert index_info["metric_type"] == metric_type
        returned_params = index_info.get("index_param", index_info)
        actual_params = returned_params.get("params", returned_params)
        for name, expected_value in expected_params.items():
            assert str(actual_params.get(name)) == str(expected_value)

    def _assert_index_params(self, client, collection_name, field_name, expected_params):
        index_info, _ = self.describe_index(client, collection_name, index_name=field_name)
        returned_params = index_info.get("index_param", index_info)
        actual_params = returned_params.get("params", returned_params)
        for name, expected_value in expected_params.items():
            assert str(actual_params.get(name)) == str(expected_value)

    def _assert_collection_indexes(self, client, collection_name, metric_type, expected_fields):
        for field_name, expected_params in expected_fields.items():
            self._assert_index_definition(client, collection_name, field_name, metric_type, expected_params)

    def _iterator_hits(self, client, collection_name, field_name, search_params):
        iterator, _ = self.search_iterator(
            client,
            collection_name,
            data=[SPARSE_QUERY],
            batch_size=4,
            limit=10,
            anns_field=field_name,
            search_params=search_params,
            output_fields=["id"],
        )
        hits = []
        while True:
            batch = iterator.next()
            if not batch:
                iterator.close()
                break
            hits.extend(batch)
        return hits

    def _compact_release_reload(self, client, collection_name):
        compact_id = self.compact(client, collection_name)[0]
        assert self.wait_for_compaction_ready(client, compact_id, timeout=300)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)


@pytest.mark.xdist_group("TestSparseInvertedIndexV3IPShared")
@pytest.mark.tags(CaseLabel.L1)
class TestSparseInvertedIndexV3IPShared(_SparseInvertedIndexV3Base):
    """Shared IP collection with one indexed sparse field per build configuration."""

    shared_alias = "TestSparseInvertedIndexV3IPShared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSparseInvertedIndexV3IPShared" + cf.gen_unique_str("sparse_v3_ip")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        for field_name in IP_INDEX_FIELDS:
            schema.add_field(field_name, DataType.SPARSE_FLOAT_VECTOR)

        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)
        rows = []
        for row_id in range(ROW_COUNT):
            row = {"id": row_id}
            for field_name in IP_INDEX_FIELDS:
                row[field_name] = _ip_vector(row_id)
            rows.append(row)
        self.insert(client, self.collection_name, data=rows)
        self.flush(client, self.collection_name)

        index_params = self.prepare_index_params(client)[0]
        for field_name, params in IP_INDEX_FIELDS.items():
            index_params.add_index(
                field_name=field_name,
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="IP",
                params=params,
            )
        self.create_index(client, self.collection_name, index_params=index_params)
        self._assert_collection_indexes(client, self.collection_name, "IP", IP_INDEX_FIELDS)
        self.load_collection(client, self.collection_name)
        self._compact_release_reload(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("limit", [1, 10, 100])
    def test_ip_sindi_search_topk_and_score_order(self, limit):
        """
        target: verify SINDI IP search returns deterministic topK results
        method: search a fixed sparse dimension whose fp16-distinguishable values decrease by id
        expected: ids are returned in ascending id order with descending scores
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(client, self.collection_name, "sparse_sindi", [SPARSE_QUERY], limit=limit)
        self._assert_exact_ip_topk(hits, limit=limit)

    @pytest.mark.tags(CaseLabel.L1)
    def test_ip_sindi_search_empty_query(self):
        """
        target: verify an empty sparse query is accepted by SINDI
        method: search with an empty sparse vector
        expected: search succeeds and returns no correlated entities
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(client, self.collection_name, "sparse_sindi", [{}])
        assert hits == []

    @pytest.mark.tags(CaseLabel.L1)
    def test_ip_sindi_search_no_overlap_query(self):
        """
        target: verify SINDI returns no hit for a query with no indexed dimension overlap
        method: search using a dimension absent from all inserted vectors
        expected: result list is empty
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(client, self.collection_name, "sparse_sindi", [{9999: 1.0}])
        assert hits == []

    @pytest.mark.tags(CaseLabel.L1)
    def test_ip_default_index_params_search_success(self):
        """
        target: verify SPARSE_INVERTED_INDEX remains usable without inverted_index_algo
        method: build the shared default sparse field without algorithm parameters and search it
        expected: search returns correctly ordered IP results
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(client, self.collection_name, "sparse_default", [SPARSE_QUERY])
        self._assert_exact_ip_topk(hits)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "field_name",
        ["sparse_block_maxscore", "sparse_block_wand", "sparse_sindi"],
    )
    def test_new_inverted_index_algo_search(self, field_name):
        """
        target: verify new sparse inverted index algorithms support IP search
        method: search fields built with BLOCK_MAX_MAXSCORE, BLOCK_MAX_WAND, or SINDI
        expected: each field returns the deterministic topK results without search-side algorithm parameters
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(client, self.collection_name, field_name, [SPARSE_QUERY])
        self._assert_exact_ip_topk(hits)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field_name", ["sparse_stream", "sparse_masked"])
    def test_block_codec_search_after_fixture_reload(self, field_name):
        """
        target: verify compressed ordinary sparse indexes remain searchable after fixture reload
        method: search streamvbyte and maskedvbyte fields after compact/release/load in class setup
        expected: both encodings return deterministic ordered IP results
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(client, self.collection_name, field_name, [SPARSE_QUERY])
        self._assert_exact_ip_topk(hits)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field_name", ["sparse_fp16", "sparse_fp32"])
    def test_ip_quant_type_search(self, field_name):
        """
        target: verify valid IP quantization types support search
        method: search fields built with fp16 and fp32 posting values
        expected: each configuration returns ordered valid results
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(client, self.collection_name, field_name, [SPARSE_QUERY])
        self._assert_exact_ip_topk(hits)
        self._assert_index_params(client, self.collection_name, field_name, IP_INDEX_FIELDS[field_name])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "field_name",
        ["sparse_sindi_w1024", "sparse_sindi_w4096", "sparse_sindi"],
    )
    def test_sindi_window_size_search(self, field_name):
        """
        target: verify valid SINDI window sizes build and search
        method: search indexes configured with window sizes 1024, 4096, and 65535
        expected: all configurations return deterministic topK results
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(client, self.collection_name, field_name, [SPARSE_QUERY])
        self._assert_exact_ip_topk(hits)
        self._assert_index_params(client, self.collection_name, field_name, IP_INDEX_FIELDS[field_name])

    @pytest.mark.tags(CaseLabel.L1)
    def test_block_max_block_size_search(self):
        """
        target: verify BLOCK_MAX_WAND accepts block max metadata configuration
        method: search an index built with block_max_block_size=128
        expected: search succeeds with ordered results
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(client, self.collection_name, "sparse_block_wand", [SPARSE_QUERY])
        self._assert_exact_ip_topk(hits)
        self._assert_index_params(
            client,
            self.collection_name,
            "sparse_block_wand",
            {"inverted_index_algo": "BLOCK_MAX_WAND", "block_max_block_size": 128},
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim_max_score_ratio", [0.5, 1.3])
    def test_block_max_dim_max_score_ratio(self, dim_max_score_ratio):
        """
        target: verify pruning ratio boundary values for Block-Max search
        method: search BLOCK_MAX_WAND with lower and upper valid ratio boundaries
        expected: requests succeed and return score ordered results
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(
            client,
            self.collection_name,
            "sparse_block_wand",
            [SPARSE_QUERY],
            params={"dim_max_score_ratio": dim_max_score_ratio},
        )
        self._assert_exact_ip_topk(hits)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dim_max_score_ratio", [0.49, 1.4])
    def test_block_max_rejects_invalid_dim_max_score_ratio(self, dim_max_score_ratio):
        """
        target: verify Block-Max search rejects out-of-range pruning ratios
        method: search BLOCK_MAX_WAND with dim_max_score_ratio below and above the valid range
        expected: search fails with a range validation error
        """
        client = self._client(alias=self.shared_alias)
        self.search(
            client,
            self.collection_name,
            data=[SPARSE_QUERY],
            anns_field="sparse_block_wand",
            search_params={"metric_type": "IP", "params": {"dim_max_score_ratio": dim_max_score_ratio}},
            limit=10,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 999, ct.err_msg: "should be in range [0.500000, 1.300000]"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_ip_sindi_search_with_scalar_filter(self):
        """
        target: verify SINDI honors scalar filters
        method: search while retaining only ids greater than or equal to half the inserted rows
        expected: all returned ids satisfy the filter and preserve ranking
        """
        client = self._client(alias=self.shared_alias)
        minimum_id = ROW_COUNT // 2
        hits = self._search_hits(
            client,
            self.collection_name,
            "sparse_sindi",
            [SPARSE_QUERY],
            filter=f"id >= {minimum_id}",
        )
        assert self._hit_ids(hits) == list(range(minimum_id, minimum_id + 10))
        self._assert_scores_descending(hits)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("filtered_ratio", [0.4, 0.9])
    def test_ip_sindi_search_with_high_filter_ratio(self, filtered_ratio):
        """
        target: verify SINDI remains valid when scalar filtering removes many candidates
        method: filter away 40 percent and 90 percent of indexed rows
        expected: returned ids are legal and results remain ordered
        """
        client = self._client(alias=self.shared_alias)
        minimum_id = int(ROW_COUNT * filtered_ratio)
        hits = self._search_hits(
            client,
            self.collection_name,
            "sparse_sindi",
            [SPARSE_QUERY],
            filter=f"id >= {minimum_id}",
        )
        assert self._hit_ids(hits) == list(range(minimum_id, minimum_id + 10))
        self._assert_scores_descending(hits)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("filtered_ratio", [0.4, 0.9])
    def test_block_max_wand_search_with_high_filter_ratio(self, filtered_ratio):
        """
        target: verify BLOCK_MAX_WAND correctly handles aggressive scalar filtering
        method: filter away 40 percent and 90 percent of rows before searching
        expected: only the highest scoring eligible IDs are returned in order
        """
        client = self._client(alias=self.shared_alias)
        minimum_id = int(ROW_COUNT * filtered_ratio)
        hits = self._search_hits(
            client,
            self.collection_name,
            "sparse_block_wand",
            [SPARSE_QUERY],
            filter=f"id >= {minimum_id}",
        )
        assert self._hit_ids(hits) == list(range(minimum_id, minimum_id + 10))
        self._assert_scores_descending(hits)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("field_name", ["sparse_sindi", "sparse_block_wand"])
    def test_sparse_search_iterator_returns_valid_ordered_hits(self, field_name):
        """
        target: verify iterated sparse search works on SINDI and non-SINDI indexes
        method: compare ten iterator hits with regular topK search in iterator batches of four
        expected: iterator returns the same IDs and distances as regular search without duplicates
        """
        client = self._client(alias=self.shared_alias)
        hits = self._iterator_hits(
            client,
            self.collection_name,
            field_name,
            {"metric_type": "IP", "params": {}},
        )
        assert len(hits) == 10
        assert len(set(self._hit_ids(hits))) == len(hits)
        assert all(0 <= hit["id"] < ROW_COUNT for hit in hits)
        self._assert_scores_descending(hits)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("field_name", ["sparse_sindi", "sparse_block_wand"])
    def test_sparse_range_search_iterator_respects_ip_bounds(self, field_name):
        """
        target: verify range iterator bounds work on SINDI and non-SINDI sparse indexes
        method: derive IP range bounds from a regular topK search and iterate the same range
        expected: iterator results are unique, score ordered, and satisfy the range bounds
        """
        client = self._client(alias=self.shared_alias)
        baseline = self._search_hits(client, self.collection_name, field_name, [SPARSE_QUERY])
        radius = baseline[5]["distance"] - 0.1
        range_filter = baseline[0]["distance"] + 0.1
        hits = self._iterator_hits(
            client,
            self.collection_name,
            field_name,
            {"metric_type": "IP", "params": {"radius": radius, "range_filter": range_filter}},
        )
        assert hits
        assert len(set(self._hit_ids(hits))) == len(hits)
        assert all(radius < hit["distance"] <= range_filter for hit in hits)
        self._assert_scores_descending(hits)


@pytest.mark.xdist_group("TestSparseInvertedIndexV3OrdinaryIPShared")
@pytest.mark.tags(CaseLabel.L1)
class TestSparseInvertedIndexV3OrdinaryIPShared(_SparseInvertedIndexV3Base):
    """Shared IP collection for ordinary sparse algorithms and legacy build parameter compatibility."""

    shared_alias = "TestSparseInvertedIndexV3OrdinaryIPShared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSparseInvertedIndexV3OrdinaryIPShared" + cf.gen_unique_str("sparse_v3_ordinary")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        for field_name in ORDINARY_IP_INDEX_FIELDS:
            schema.add_field(field_name, DataType.SPARSE_FLOAT_VECTOR)

        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)
        rows = []
        for row_id in range(ROW_COUNT):
            row = {"id": row_id}
            for field_name in ORDINARY_IP_INDEX_FIELDS:
                row[field_name] = _ip_vector(row_id)
            rows.append(row)
        self.insert(client, self.collection_name, data=rows)
        self.flush(client, self.collection_name)

        index_params = self.prepare_index_params(client)[0]
        for field_name, params in ORDINARY_IP_INDEX_FIELDS.items():
            index_params.add_index(
                field_name=field_name,
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="IP",
                params=params,
            )
        self.create_index(client, self.collection_name, index_params=index_params)
        self._assert_collection_indexes(client, self.collection_name, "IP", ORDINARY_IP_INDEX_FIELDS)
        self.load_collection(client, self.collection_name)
        self._compact_release_reload(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)

        request.addfinalizer(teardown)

    @pytest.mark.parametrize(
        "field_name",
        ["sparse_taat_naive", "sparse_daat_maxscore", "sparse_daat_wand"],
    )
    def test_ordinary_sparse_algorithm_search_after_reload(self, field_name):
        """
        target: verify ordinary sparse algorithms build, reload, and execute IP search
        method: search fields configured with TAAT_NAIVE, DAAT_MAXSCORE, and DAAT_WAND
        expected: each algorithm returns deterministic ordered topK results
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(client, self.collection_name, field_name, [SPARSE_QUERY])
        self._assert_exact_ip_topk(hits)

    def test_drop_ratio_build_remains_accepted_without_changing_results(self):
        """
        target: verify the deprecated drop_ratio_build parameter remains build-compatible
        method: compare SINDI fields built with and without drop_ratio_build=0.4
        expected: both indexes reload and return identical deterministic topK results
        """
        client = self._client(alias=self.shared_alias)
        baseline = self._search_hits(client, self.collection_name, "sparse_sindi_no_drop_build", [SPARSE_QUERY])
        with_drop = self._search_hits(client, self.collection_name, "sparse_sindi_drop_build", [SPARSE_QUERY])
        self._assert_exact_ip_topk(baseline)
        assert self._hit_ids(with_drop) == self._hit_ids(baseline)
        assert [hit["distance"] for hit in with_drop] == pytest.approx([hit["distance"] for hit in baseline])


@pytest.mark.xdist_group("TestSparseInvertedIndexV3BM25Shared")
@pytest.mark.tags(CaseLabel.L1)
class TestSparseInvertedIndexV3BM25Shared(_SparseInvertedIndexV3Base):
    """Shared BM25 collection with SINDI and ordinary quantized inverted indexes."""

    shared_alias = "TestSparseInvertedIndexV3BM25Shared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSparseInvertedIndexV3BM25Shared" + cf.gen_unique_str("sparse_v3_bm25")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        for field_name in BM25_INDEX_FIELDS:
            text_field = f"text_{field_name}"
            schema.add_field(
                text_field,
                DataType.VARCHAR,
                max_length=256,
                enable_analyzer=True,
                analyzer_params=BM25_ANALYZER_PARAMS.get(field_name, {"tokenizer": "standard"}),
            )
            schema.add_field(field_name, DataType.SPARSE_FLOAT_VECTOR)
            schema.add_function(
                Function(
                    name=f"func_{field_name}",
                    function_type=FunctionType.BM25,
                    input_field_names=[text_field],
                    output_field_names=[field_name],
                    params={},
                )
            )

        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)
        rows = []
        for row_id in range(ROW_COUNT):
            row = {"id": row_id}
            for field_name in BM25_INDEX_FIELDS:
                document_factory = _bm25_jieba_document if field_name == "bm25_sindi_jieba" else _bm25_document
                row[f"text_{field_name}"] = document_factory(row_id)
            rows.append(row)
        self.insert(client, self.collection_name, data=rows)
        self.flush(client, self.collection_name)

        index_params = self.prepare_index_params(client)[0]
        for field_name, params in BM25_INDEX_FIELDS.items():
            index_params.add_index(
                field_name=field_name,
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="BM25",
                params=params,
            )
        self.create_index(client, self.collection_name, index_params=index_params)
        self._assert_collection_indexes(client, self.collection_name, "BM25", BM25_INDEX_FIELDS)
        self.load_collection(client, self.collection_name)
        self._compact_release_reload(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)

        request.addfinalizer(teardown)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("field_name", ["bm25_sindi", "bm25_sindi_low", "bm25_sindi_high"])
    def test_bm25_sindi_search_with_bm25_parameter_combinations(self, field_name):
        """
        target: verify full text search works on SINDI indexes with explicit BM25 parameter combinations
        method: search a fixed standard-analyzer token using three k1/b configurations
        expected: matching documents are returned and k1/b-sensitive rankings are preserved
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(
            client,
            self.collection_name,
            field_name,
            ["alpha"],
            metric_type="BM25",
            limit=10,
        )
        assert set(self._hit_ids(hits)) == {0, 1, 2}
        self._assert_scores_descending(hits)
        self._assert_index_params(client, self.collection_name, field_name, BM25_INDEX_FIELDS[field_name])
        if field_name == "bm25_sindi_high":
            assert self._hit_ids(hits)[:2] == [2, 0]
        else:
            assert self._hit_ids(hits)[:2] == [0, 2]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("field_name", ["bm25_u16", "bm25_u32"])
    def test_bm25_quant_type_search(self, field_name):
        """
        target: verify valid BM25 posting quantization types support full text search
        method: search BM25 indexes built with u16 and u32 values
        expected: matching documents are returned with descending scores
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(
            client,
            self.collection_name,
            field_name,
            ["alpha"],
            metric_type="BM25",
            limit=10,
        )
        assert set(self._hit_ids(hits)) == {0, 1, 2}
        self._assert_scores_descending(hits)
        self._assert_index_params(client, self.collection_name, field_name, BM25_INDEX_FIELDS[field_name])

    @pytest.mark.tags(CaseLabel.L1)
    def test_bm25_sindi_uses_default_bm25_params_when_omitted(self):
        """
        target: verify BM25 index parameters remain optional on the user path
        method: build SINDI without k1/b and search standard-analyzed text
        expected: omitted parameters return the same search results as explicitly configured defaults
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(
            client,
            self.collection_name,
            "bm25_sindi_implicit",
            ["alpha"],
            metric_type="BM25",
            limit=10,
        )
        assert set(self._hit_ids(hits)) == {0, 1, 2}
        self._assert_scores_descending(hits)
        explicit_default = self._search_hits(
            client,
            self.collection_name,
            "bm25_sindi",
            ["alpha"],
            metric_type="BM25",
            limit=10,
        )
        assert self._hit_ids(hits) == self._hit_ids(explicit_default)
        assert [hit["distance"] for hit in hits] == pytest.approx([hit["distance"] for hit in explicit_default])

    @pytest.mark.tags(CaseLabel.L1)
    def test_bm25_sindi_search_with_jieba_analyzer(self):
        """
        target: verify SINDI BM25 works with a non-standard analyzer
        method: search Chinese data analyzed with jieba using a matching query token
        expected: only documents containing the query token are returned in score order
        """
        client = self._client(alias=self.shared_alias)
        hits = self._search_hits(
            client,
            self.collection_name,
            "bm25_sindi_jieba",
            ["稀疏"],
            metric_type="BM25",
            limit=10,
        )
        assert set(self._hit_ids(hits)) == {0, 1, 2}
        self._assert_scores_descending(hits)


@pytest.mark.tags(CaseLabel.L1)
@pytest.mark.xdist_group("TestSparseInvertedIndexV3LifecycleShared")
class TestSparseInvertedIndexV3LifecycleShared(_SparseInvertedIndexV3Base):
    """Shared IP collection for state-changing reload, mmap, and search-parameter checks."""

    shared_alias = "TestSparseInvertedIndexV3LifecycleShared"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSparseInvertedIndexV3LifecycleShared" + cf.gen_unique_str("sparse_v3_lifecycle")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        for field_name in LIFECYCLE_IP_INDEX_FIELDS:
            schema.add_field(field_name, DataType.SPARSE_FLOAT_VECTOR)

        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)
        rows = []
        for row_id in range(ROW_COUNT):
            row = {"id": row_id}
            for field_name in LIFECYCLE_ENCODING_INDEX_FIELDS:
                row[field_name] = _ip_vector(row_id)
            row[DROP_RATIO_EFFECT_FIELD] = _drop_ratio_effect_vector(row_id)
            rows.append(row)
        self.insert(client, self.collection_name, data=rows)
        self.flush(client, self.collection_name)

        index_params = self.prepare_index_params(client)[0]
        for field_name, params in LIFECYCLE_IP_INDEX_FIELDS.items():
            index_params.add_index(
                field_name=field_name,
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="IP",
                params=params,
            )
        self.create_index(client, self.collection_name, index_params=index_params)
        self._assert_collection_indexes(client, self.collection_name, "IP", LIFECYCLE_IP_INDEX_FIELDS)
        self.load_collection(client, self.collection_name)
        self._compact_release_reload(client, self.collection_name)

        def teardown():
            self.drop_collection(self._client(alias=self.shared_alias), self.collection_name)

        request.addfinalizer(teardown)

    def _search_all_encodings(self, client, collection_name):
        return {
            field_name: self._search_hits(client, collection_name, field_name, [SPARSE_QUERY])
            for field_name in LIFECYCLE_ENCODING_INDEX_FIELDS
        }

    def _assert_encoding_hits(self, hits_by_field):
        for hits in hits_by_field.values():
            self._assert_exact_ip_topk(hits)

    @pytest.mark.tags(CaseLabel.L1)
    def test_encoding_search_stable_after_release_reload(self):
        """
        target: verify encoded sparse index data remains usable across a collection reload
        method: search streamvbyte, maskedvbyte, and SINDI fields before and after release/load
        expected: each encoding returns the same IDs and distances after reload
        """
        client = self._client(alias=self.shared_alias)
        collection_name = self.collection_name

        baseline = self._search_all_encodings(client, collection_name)
        self._assert_encoding_hits(baseline)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        reloaded = self._search_all_encodings(client, collection_name)

        for field_name in LIFECYCLE_ENCODING_INDEX_FIELDS:
            assert self._hit_ids(reloaded[field_name]) == self._hit_ids(baseline[field_name])
            assert [hit["distance"] for hit in reloaded[field_name]] == pytest.approx(
                [hit["distance"] for hit in baseline[field_name]]
            )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("enabled", [True, False])
    def test_encoding_search_with_mmap_setting(self, enabled):
        """
        target: verify sparse encoding variants load and search with mmap explicitly configured
        method: set mmap on the collection and each index before loading and searching
        expected: requested mmap value persists after release/load and all indexed fields remain searchable
        """
        client = self._client(alias=self.shared_alias)
        collection_name = self.collection_name
        self.release_collection(client, collection_name)
        try:
            self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": enabled})
            collection_info, _ = self.describe_collection(client, collection_name)
            assert collection_info.get("properties", {}).get("mmap.enabled") == str(enabled)
            for field_name in LIFECYCLE_IP_INDEX_FIELDS:
                self.alter_index_properties(
                    client,
                    collection_name,
                    index_name=field_name,
                    properties={"mmap.enabled": enabled},
                )
                index_info, _ = self.describe_index(client, collection_name, index_name=field_name)
                assert index_info.get("mmap.enabled") == str(enabled)
            self.load_collection(client, collection_name)
            self._assert_encoding_hits(self._search_all_encodings(client, collection_name))
            self.release_collection(client, collection_name)
            self.load_collection(client, collection_name)

            collection_info, _ = self.describe_collection(client, collection_name)
            assert collection_info.get("properties", {}).get("mmap.enabled") == str(enabled)
            for field_name in LIFECYCLE_IP_INDEX_FIELDS:
                index_info, _ = self.describe_index(client, collection_name, index_name=field_name)
                assert index_info.get("mmap.enabled") == str(enabled)
            self._assert_encoding_hits(self._search_all_encodings(client, collection_name))
        finally:
            self.release_collection(client, collection_name)
            self.drop_collection_properties(client, collection_name, property_keys=["mmap.enabled"])
            for field_name in LIFECYCLE_IP_INDEX_FIELDS:
                self.drop_index_properties(
                    client, collection_name, index_name=field_name, property_keys=["mmap.enabled"]
                )
            collection_info, _ = self.describe_collection(client, collection_name)
            assert "mmap.enabled" not in collection_info.get("properties", {})
            for field_name in LIFECYCLE_IP_INDEX_FIELDS:
                index_info, _ = self.describe_index(client, collection_name, index_name=field_name)
                assert index_info.get("mmap.enabled") is None
            self.load_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_sindi_drop_ratio_search_changes_effective_query(self):
        """
        target: verify SINDI applies drop_ratio_search instead of only accepting the parameter
        method: compare a query whose top hit changes when low-weight dimensions are dropped
        expected: drop_ratio_search=0.4 changes the ranking produced by drop_ratio_search=0.0
        """
        client = self._client(alias=self.shared_alias)
        no_drop = self._search_hits(
            client,
            self.collection_name,
            DROP_RATIO_EFFECT_FIELD,
            [DROP_RATIO_EFFECT_QUERY],
            limit=3,
            params={"drop_ratio_search": 0.0},
        )
        with_drop = self._search_hits(
            client,
            self.collection_name,
            DROP_RATIO_EFFECT_FIELD,
            [DROP_RATIO_EFFECT_QUERY],
            limit=3,
            params={"drop_ratio_search": 0.4},
        )

        assert no_drop[0]["id"] == 1
        assert with_drop[0]["id"] == 0
        assert self._hit_ids(no_drop) != self._hit_ids(with_drop)


@pytest.mark.xdist_group("TestSparseInvertedIndexV3Negative")
@pytest.mark.tags(CaseLabel.L2)
class TestSparseInvertedIndexV3Negative(_SparseInvertedIndexV3Base):
    """Shared schema for invalid and unvalidated index parameter checks."""

    shared_alias = "TestSparseInvertedIndexV3Negative"

    def setup_class(self):
        super().setup_class(self)
        self.collection_name = "TestSparseInvertedIndexV3Negative" + cf.gen_unique_str("sparse_v3_bad")
        self.bm25_collection_name = "TestSparseInvertedIndexV3Negative" + cf.gen_unique_str("sparse_v3_bad_bm25")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_collection(self, request):
        client = self._client(alias=self.shared_alias)
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        for field_name in [
            "bad_metric_l2",
            "bad_metric_cosine",
            "bad_ip_quant",
            "bad_ip_quant_literal",
            "bad_algo",
            "bad_window_low",
            "bad_window_high",
            "bad_codec",
            "bad_block_size_zero",
            "bad_block_size_negative",
        ]:
            schema.add_field(field_name, DataType.SPARSE_FLOAT_VECTOR)
        self.create_collection(client, self.collection_name, schema=schema, force_teardown=False)

        bm25_schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        bm25_schema.add_field("id", DataType.INT64, is_primary=True)
        bm25_schema.add_field("bad_text", DataType.VARCHAR, max_length=256, enable_analyzer=True)
        bm25_schema.add_field("bad_bm25_quant", DataType.SPARSE_FLOAT_VECTOR)
        bm25_schema.add_field("bad_bm25_quant_literal", DataType.SPARSE_FLOAT_VECTOR)
        bm25_schema.add_function(
            Function(
                name="bad_bm25_function",
                function_type=FunctionType.BM25,
                input_field_names=["bad_text"],
                output_field_names=["bad_bm25_quant"],
                params={},
            )
        )
        bm25_schema.add_function(
            Function(
                name="bad_bm25_literal_function",
                function_type=FunctionType.BM25,
                input_field_names=["bad_text"],
                output_field_names=["bad_bm25_quant_literal"],
                params={},
            )
        )
        self.create_collection(client, self.bm25_collection_name, schema=bm25_schema, force_teardown=False)

        def teardown():
            client = self._client(alias=self.shared_alias)
            self.drop_collection(client, self.collection_name)
            self.drop_collection(client, self.bm25_collection_name)

        request.addfinalizer(teardown)

    def _collection_for_field(self, field_name):
        if field_name.startswith("bad_bm25"):
            return self.bm25_collection_name
        return self.collection_name

    def _assert_create_index_fails(self, field_name, metric_type, params, error_message):
        client = self._client(alias=self.shared_alias)
        collection_name = self._collection_for_field(field_name)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=field_name,
            index_type="SPARSE_INVERTED_INDEX",
            metric_type=metric_type,
            params=params,
        )
        self.create_index(
            client,
            collection_name,
            index_params=index_params,
            check_task=CheckTasks.err_res,
            check_items={ct.err_code: 1100, ct.err_msg: error_message},
        )
        indexes, _ = self.list_indexes(client, collection_name, field_name=field_name)
        assert indexes == []

    @pytest.mark.parametrize(
        "field_name, metric_type",
        [("bad_metric_l2", "L2"), ("bad_metric_cosine", "COSINE")],
    )
    def test_sparse_index_rejects_unsupported_metric(self, field_name, metric_type):
        """
        target: verify sparse inverted indexes reject dense-vector metrics
        method: create an index with L2 or COSINE metric
        expected: index creation fails during parameter validation
        """
        self._assert_create_index_fails(
            field_name,
            metric_type,
            {},
            "only IP&BM25 is the supported metric type for sparse index: invalid parameter[expected=valid index params][actual=invalid index params]",
        )

    def test_ip_rejects_bm25_quant_type(self):
        """
        target: verify IP sparse indexing rejects BM25 quantization storage
        method: create an IP index with quant_type=u16
        expected: index creation fails and identifies the invalid quantization parameter
        """
        self._assert_create_index_fails("bad_ip_quant", "IP", {"quant_type": "u16"}, "quant_type")

    def test_bm25_rejects_ip_quant_type(self):
        """
        target: verify BM25 sparse indexing rejects IP quantization storage
        method: create a BM25 index with quant_type=fp16
        expected: index creation fails and identifies the invalid quantization parameter
        """
        self._assert_create_index_fails("bad_bm25_quant", "BM25", {"quant_type": "fp16"}, "quant_type")

    @pytest.mark.parametrize(
        "field_name, metric_type",
        [("bad_ip_quant_literal", "IP"), ("bad_bm25_quant_literal", "BM25")],
    )
    def test_sparse_index_rejects_unknown_quant_type(self, field_name, metric_type):
        """
        target: verify sparse indexing rejects unknown quantization storage values
        method: create IP and BM25 indexes with quant_type=invalid
        expected: index creation fails and identifies the invalid parameter
        """
        self._assert_create_index_fails(field_name, metric_type, {"quant_type": "invalid"}, "quant_type")

    def test_sparse_index_rejects_invalid_algo(self):
        """
        target: verify the Milvus index parameter layer rejects unsupported sparse algorithms
        method: create an index with an unknown inverted_index_algo
        expected: index creation fails with a supported-algorithm validation error
        """
        self._assert_create_index_fails(
            "bad_algo",
            "IP",
            {"inverted_index_algo": "INVALID_ALGO"},
            "sparse inverted index algo INVALID_ALGO not found or not supported",
        )

    @pytest.mark.parametrize(
        "field_name, window_size",
        [("bad_window_low", 512), ("bad_window_high", 65536)],
    )
    def test_sindi_rejects_invalid_window_size(self, field_name, window_size):
        """
        target: verify SINDI rejects out-of-range docid window sizes
        method: create SINDI indexes below and above the valid window range
        expected: index creation fails and reports the SINDI window parameter
        """
        self._assert_create_index_fails(
            field_name,
            "IP",
            {"inverted_index_algo": "SINDI", "sindi_window_size": window_size},
            "sindi_window_size",
        )

    @pytest.mark.xfail(reason="Known issue #50108: server accepts unsupported sparse inverted index codec values")
    def test_sparse_index_rejects_invalid_codec(self):
        """
        target: verify ordinary sparse indexes reject unsupported posting-list codecs
        method: create a DAAT_MAXSCORE index with an unknown inverted_index_codec
        expected: index creation fails and identifies the invalid codec parameter
        """
        self._assert_create_index_fails(
            "bad_codec",
            "IP",
            {"inverted_index_algo": "DAAT_MAXSCORE", "inverted_index_codec": "invalid_codec"},
            "inverted_index_codec",
        )

    @pytest.mark.xfail(reason="Known issue #50108: server accepts non-positive block_max_block_size values")
    @pytest.mark.parametrize(
        "field_name, block_size",
        [("bad_block_size_zero", 0), ("bad_block_size_negative", -1)],
    )
    def test_block_max_rejects_invalid_block_size(self, field_name, block_size):
        """
        target: verify Block-Max sparse indexes reject non-positive block size params
        method: create BLOCK_MAX_WAND indexes with zero and negative block_max_block_size
        expected: index creation fails and identifies the invalid block size parameter
        """
        self._assert_create_index_fails(
            field_name,
            "IP",
            {"inverted_index_algo": "BLOCK_MAX_WAND", "block_max_block_size": block_size},
            "block_max_block_size",
        )
