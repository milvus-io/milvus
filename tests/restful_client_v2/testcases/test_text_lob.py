import hashlib
import random
import time

import pytest
from api.milvus import IndexClient
from base.testbase import TestBase
from pymilvus import connections, utility
from pymilvus.grpc_gen import common_pb2
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

DIM = 16
ID_FIELD = "id"
VECTOR_FIELD = "vector"
CONTENT_FIELD = "content"
CONTENT_ZH_FIELD = "content_zh"
CONTENT_ALT_FIELD = "content_alt"
CONTENT_DEFAULT_FIELD = "content_default"
VARCHAR_TEXT_FIELD = "varchar_text"
CONTENT_SPARSE_FIELD = "content_sparse"
CONTENT_ZH_SPARSE_FIELD = "content_zh_sparse"
CONTENT_DEFAULT_SPARSE_FIELD = "content_default_sparse"

STANDARD_ANALYZER = {"tokenizer": "standard"}
JIEBA_ANALYZER = {
    "tokenizer": {
        "type": "jieba",
        "dict": ["向量数据库", "混合搜索", "稀疏向量"],
        "mode": "exact",
        "hmm": False,
    }
}
DEFAULT_TEXT = "milvus text lob sentinel vector database"
EXPLICIT_DEFAULT_TEXT = "explicit default text lob marker atlas"
TEXT_FIELDS = [CONTENT_FIELD, CONTENT_ZH_FIELD, CONTENT_ALT_FIELD, CONTENT_DEFAULT_FIELD]

INDEXED_SEALED_ROWS = 3000
UNINDEXED_SEALED_ROWS = 1000
GROWING_ROWS = 500
TOTAL_ROWS = INDEXED_SEALED_ROWS + UNINDEXED_SEALED_ROWS + GROWING_ROWS
TEXT_LOB_SETUP_TIMEOUT = 300
INDEXED_SEALED_RANGE = range(0, INDEXED_SEALED_ROWS)
UNINDEXED_SEALED_RANGE = range(INDEXED_SEALED_ROWS, INDEXED_SEALED_ROWS + UNINDEXED_SEALED_ROWS)
GROWING_RANGE = range(INDEXED_SEALED_ROWS + UNINDEXED_SEALED_ROWS, TOTAL_ROWS)
LOB_MARKER_IDS = {
    "sealed_indexed": 7,
    "sealed_unindexed": INDEXED_SEALED_ROWS + UNINDEXED_SEALED_ROWS // 2,
    "growing": INDEXED_SEALED_ROWS + UNINDEXED_SEALED_ROWS + GROWING_ROWS // 2,
}
LOB_SEARCH_VECTOR = [1.0] + [0.0] * (DIM - 1)


def make_text(size, seed):
    """Return deterministic ASCII text with exact byte length."""
    if size == 0:
        return ""
    base = f"seed {seed} vector database milvus text lob storage bm25 payload boundary checksum {seed} "
    return (base * ((size // len(base)) + 1))[:size]


def payload_meta(value):
    if value is None:
        return {"state": "null", "bytes": None, "chars": None, "prefix": None, "suffix": None, "sha256": None}
    encoded = value.encode("utf-8")
    return {
        "state": "empty" if value == "" else "value",
        "bytes": len(encoded),
        "chars": len(value),
        "prefix": value[:32],
        "suffix": value[-32:] if value else "",
        "sha256": hashlib.sha256(encoded).hexdigest(),
    }


def assert_text_payload(actual, expected):
    actual_meta = payload_meta(actual)
    for key in ["state", "bytes", "chars", "prefix", "suffix", "sha256"]:
        assert actual_meta[key] == expected[key], (
            f"text payload mismatch on {key}: actual={actual_meta[key]!r}, "
            f"expected={expected[key]!r}, actual_meta={actual_meta}, expected_meta={expected}"
        )


def vector_for_pk(pk):
    rng = random.Random(19530 + pk)
    return [rng.random() for _ in range(DIM)]


def build_row(
    pk,
    scenario,
    content,
    content_zh=None,
    content_alt=None,
    varchar_text=None,
    content_default=DEFAULT_TEXT,
    vector=None,
):
    if content_zh is None and content is not None and len(content) <= 4096:
        content_zh = content
    if content_alt is None and content is not None:
        content_alt = f"alternate text lob payload for {scenario}: {content[:128]}"
    if varchar_text is None:
        varchar_text = content if content is not None and len(content) <= 60000 else f"{scenario} vector database"

    row = {
        ID_FIELD: pk,
        VECTOR_FIELD: vector if vector is not None else vector_for_pk(pk),
        "category": scenario,
        "score": float(pk) / 10.0,
        "json_meta": {
            "scenario": scenario,
            "content_sha256": None if content is None else hashlib.sha256(content.encode("utf-8")).hexdigest(),
            "content_bytes": None if content is None else len(content.encode("utf-8")),
        },
        CONTENT_FIELD: content,
        CONTENT_ZH_FIELD: content_zh,
        CONTENT_ALT_FIELD: content_alt,
        VARCHAR_TEXT_FIELD: varchar_text,
        "dynamic_note": f"dynamic-{scenario}",
    }
    if content_default is not None:
        row[CONTENT_DEFAULT_FIELD] = content_default
    return row


def build_text_lob_rows():
    rows = [
        build_row(0, "small", "vector database milvus text lob smoke"),
        build_row(1, "empty", "", content_zh="", content_alt="", varchar_text=""),
        build_row(2, "null", None, content_zh=None, content_alt=None, varchar_text=None),
        build_row(
            3,
            "unicode",
            "Milvus stores multilingual text: English 中文 日本語 Русский العربية emoji 😀🚀 데이터베이스",
            content_zh="向量数据库 支持 中文检索 和 混合搜索",
            varchar_text="Milvus stores multilingual text vector database emoji",
        ),
        build_row(8, "bm25_low", "vector database"),
        build_row(9, "bm25_mid", "vector database " * 4 + "milvus retrieval"),
        build_row(10, "bm25_high", "vector database " * 12 + "milvus bm25 ranking ranking"),
        build_row(
            11,
            "chinese",
            "english sidecar text for chinese bm25",
            content_zh="向量数据库 支持 中文检索。Milvus 提供 混合搜索 和 稀疏向量 检索。",
            varchar_text="chinese vector database sidecar",
        ),
        build_row(12, "sentinel", "explicit row that verifies sentinel text output"),
        build_row(
            13,
            "default_explicit",
            "row that explicitly sets default text field",
            content_default=EXPLICIT_DEFAULT_TEXT,
        ),
        build_row(4, "below_64k", make_text(64 * 1024 - 17, "0-below")),
        build_row(5, "at_64k", make_text(64 * 1024, "0-at")),
        build_row(6, "above_64k", make_text(64 * 1024 + 4096, "0-above")),
        build_row(
            7,
            "sealed_indexed_lob",
            make_text(1024 * 1024, "0-one-mib"),
            content_zh=None,
            content_alt=make_text(128 * 1024, "0-alt-one-mib"),
            varchar_text="one mib vector database",
            vector=LOB_SEARCH_VECTOR,
        ),
    ]
    rows_by_id = {row[ID_FIELD]: row for row in rows}
    for pk in INDEXED_SEALED_RANGE:
        rows_by_id.setdefault(
            pk,
            build_row(pk, "sealed_indexed", f"indexed sealed text lob fixture row {pk} vector database"),
        )
    for pk in UNINDEXED_SEALED_RANGE:
        rows_by_id[pk] = build_row(
            pk,
            "sealed_unindexed_lob" if pk == LOB_MARKER_IDS["sealed_unindexed"] else "sealed_unindexed",
            make_text(256 * 1024, "1-sealed-unindexed")
            if pk == LOB_MARKER_IDS["sealed_unindexed"]
            else f"unindexed sealed text lob fixture row {pk} vector database",
            content_alt=make_text(128 * 1024, "1-alt-sealed-unindexed")
            if pk == LOB_MARKER_IDS["sealed_unindexed"]
            else None,
            vector=LOB_SEARCH_VECTOR if pk == LOB_MARKER_IDS["sealed_unindexed"] else None,
        )
    for pk in GROWING_RANGE:
        rows_by_id[pk] = build_row(
            pk,
            "growing_lob" if pk == LOB_MARKER_IDS["growing"] else "growing",
            make_text(256 * 1024, "2-growing")
            if pk == LOB_MARKER_IDS["growing"]
            else f"growing text lob fixture row {pk} vector database",
            content_alt=make_text(128 * 1024, "2-alt-growing") if pk == LOB_MARKER_IDS["growing"] else None,
            vector=LOB_SEARCH_VECTOR if pk == LOB_MARKER_IDS["growing"] else None,
        )
    return [rows_by_id[pk] for pk in range(TOTAL_ROWS)]


def expected_payloads(rows):
    return {
        row[ID_FIELD]: {field: payload_meta(row.get(field)) for field in TEXT_FIELDS + [VARCHAR_TEXT_FIELD]}
        for row in rows
    }


def text_field(field_name, analyzer_params, nullable=True):
    return {
        "fieldName": field_name,
        "dataType": "Text",
        "nullable": nullable,
        "elementTypeParams": {
            "enable_analyzer": True,
            "enable_match": True,
            "analyzer_params": analyzer_params,
        },
    }


def build_collection_payload(collection_name):
    return {
        "collectionName": collection_name,
        "schema": {
            "autoId": False,
            "enableDynamicField": True,
            "fields": [
                {"fieldName": ID_FIELD, "dataType": "Int64", "isPrimary": True},
                {"fieldName": VECTOR_FIELD, "dataType": "FloatVector", "elementTypeParams": {"dim": str(DIM)}},
                {"fieldName": "category", "dataType": "VarChar", "elementTypeParams": {"max_length": "64"}},
                {"fieldName": "score", "dataType": "Float"},
                {"fieldName": "json_meta", "dataType": "JSON"},
                text_field(CONTENT_FIELD, STANDARD_ANALYZER),
                text_field(CONTENT_ZH_FIELD, JIEBA_ANALYZER),
                text_field(CONTENT_ALT_FIELD, STANDARD_ANALYZER),
                text_field(CONTENT_DEFAULT_FIELD, STANDARD_ANALYZER),
                {
                    "fieldName": VARCHAR_TEXT_FIELD,
                    "dataType": "VarChar",
                    "nullable": True,
                    "elementTypeParams": {
                        "max_length": "65535",
                        "enable_analyzer": True,
                        "enable_match": True,
                        "analyzer_params": STANDARD_ANALYZER,
                    },
                },
                {"fieldName": CONTENT_SPARSE_FIELD, "dataType": "SparseFloatVector"},
                {"fieldName": CONTENT_ZH_SPARSE_FIELD, "dataType": "SparseFloatVector"},
                {"fieldName": CONTENT_DEFAULT_SPARSE_FIELD, "dataType": "SparseFloatVector"},
            ],
            "functions": [
                {
                    "name": "content_bm25",
                    "type": "BM25",
                    "inputFieldNames": [CONTENT_FIELD],
                    "outputFieldNames": [CONTENT_SPARSE_FIELD],
                    "params": {},
                },
                {
                    "name": "content_zh_bm25",
                    "type": "BM25",
                    "inputFieldNames": [CONTENT_ZH_FIELD],
                    "outputFieldNames": [CONTENT_ZH_SPARSE_FIELD],
                    "params": {},
                },
                {
                    "name": "content_default_bm25",
                    "type": "BM25",
                    "inputFieldNames": [CONTENT_DEFAULT_FIELD],
                    "outputFieldNames": [CONTENT_DEFAULT_SPARSE_FIELD],
                    "params": {},
                },
            ],
        },
        "params": {"consistencyLevel": "Strong"},
    }


def build_dense_index_payload(collection_name):
    return {
        "collectionName": collection_name,
        "indexParams": [
            {
                "fieldName": VECTOR_FIELD,
                "indexName": VECTOR_FIELD,
                "indexType": "HNSW",
                "metricType": "COSINE",
                "params": {"M": 16, "efConstruction": 200},
            },
        ],
    }


def build_text_index_payload(collection_name):
    return {
        "collectionName": collection_name,
        "indexParams": [
            {
                "fieldName": CONTENT_SPARSE_FIELD,
                "indexName": CONTENT_SPARSE_FIELD,
                "indexType": "SPARSE_INVERTED_INDEX",
                "metricType": "BM25",
            },
            {
                "fieldName": CONTENT_ZH_SPARSE_FIELD,
                "indexName": CONTENT_ZH_SPARSE_FIELD,
                "indexType": "SPARSE_INVERTED_INDEX",
                "metricType": "BM25",
            },
            {
                "fieldName": CONTENT_DEFAULT_SPARSE_FIELD,
                "indexName": CONTENT_DEFAULT_SPARSE_FIELD,
                "indexType": "SPARSE_INVERTED_INDEX",
                "metricType": "BM25",
            },
            {
                "fieldName": VARCHAR_TEXT_FIELD,
                "indexName": VARCHAR_TEXT_FIELD,
                "indexType": "AUTOINDEX",
            },
        ],
    }


def wait_for_sealed_layout(collection_name, connection_alias, timeout=120):
    """Wait until public segment metadata exposes the 3,000 + 1,000 sealed layout."""
    deadline = time.monotonic() + timeout
    last_infos = []
    while time.monotonic() < deadline:
        last_infos = utility.get_query_segment_info(collection_name, using=connection_alias)
        if len(last_infos) == 2 and sorted(info.num_rows for info in last_infos) == [
            UNINDEXED_SEALED_ROWS,
            INDEXED_SEALED_ROWS,
        ]:
            assert all(info.state == common_pb2.SegmentState.Sealed for info in last_infos)
            return last_infos
        time.sleep(1)
    raise AssertionError(f"expected 3,000 + 500 sealed rows, last segment infos: {last_infos}")


def flush_collection(collection_client, collection_name, timeout=30):
    """Retry only the expected flush rate-limit response within a bounded window."""
    deadline = time.monotonic() + timeout
    while True:
        rsp = collection_client.flush(collection_name)
        if rsp["code"] == 0:
            return
        is_rate_limited = rsp["code"] == 1807 and "rate limit exceeded" in rsp.get("message", "")
        if not is_rate_limited or time.monotonic() >= deadline:
            raise AssertionError(f"flush failed: {rsp}")
        time.sleep(1)


def remaining_setup_timeout(deadline, phase, cap):
    """Return a phase timeout bounded by the shared fixture setup deadline."""
    remaining = int(deadline - time.monotonic())
    assert remaining > 0, f"TEXT LOB fixture setup budget exhausted before {phase}"
    return min(cap, remaining)


def assert_rows_payload(rows_by_id, expected, fields):
    assert set(rows_by_id) == set(expected), f"row ids mismatch: actual={set(rows_by_id)}, expected={set(expected)}"
    for pk, row in rows_by_id.items():
        for field in fields:
            assert_text_payload(row.get(field), expected[pk][field])


@pytest.mark.xdist_group("TestTextLOB")
class TestTextLOB(TestBase):
    shared_rows = []
    shared_expected = {}
    shared_ids = []
    shared_sealed_segment_ids = []

    def setup_class(self):
        self.collection_name = gen_collection_name(prefix="rest_text_lob")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_text_lob_collection(self, request, init_class_config):
        setup_deadline = time.monotonic() + TEXT_LOB_SETUP_TIMEOUT
        collection_client, vector_client = self._class_scope_clients()
        index_client = IndexClient(self.endpoint, self.api_key)
        connection_alias = self.collection_name
        connections.connect(alias=connection_alias, uri=self.endpoint, token=self.api_key)

        def teardown():
            collection_client.collection_drop({"collectionName": self.collection_name})
            connections.disconnect(connection_alias)

        request.addfinalizer(teardown)
        self.__class__.shared_rows = build_text_lob_rows()
        self.__class__.shared_expected = expected_payloads(self.shared_rows)
        self.__class__.shared_ids = [row[ID_FIELD] for row in self.shared_rows]
        assert len(self.shared_rows) == TOTAL_ROWS

        rsp = collection_client.collection_create(build_collection_payload(self.collection_name))
        assert rsp["code"] == 0, rsp

        indexed_rows = self.shared_rows[:INDEXED_SEALED_ROWS]
        rsp = vector_client.vector_insert({"collectionName": self.collection_name, "data": indexed_rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == INDEXED_SEALED_ROWS
        flush_collection(
            collection_client,
            self.collection_name,
            timeout=remaining_setup_timeout(setup_deadline, "indexed sealed flush", 30),
        )

        unindexed_rows = self.shared_rows[INDEXED_SEALED_ROWS : INDEXED_SEALED_ROWS + UNINDEXED_SEALED_ROWS]
        rsp = vector_client.vector_insert({"collectionName": self.collection_name, "data": unindexed_rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == UNINDEXED_SEALED_ROWS
        flush_collection(
            collection_client,
            self.collection_name,
            timeout=remaining_setup_timeout(setup_deadline, "unindexed sealed flush", 30),
        )

        rsp = index_client.index_create(build_dense_index_payload(self.collection_name))
        assert rsp["code"] == 0, rsp
        assert utility.wait_for_index_building_complete(
            self.collection_name,
            index_name=VECTOR_FIELD,
            timeout=remaining_setup_timeout(setup_deadline, "dense index build", 300),
            using=connection_alias,
        ), "dense index did not complete within the shared fixture setup budget"

        rsp = index_client.index_create(build_text_index_payload(self.collection_name))
        assert rsp["code"] == 0, rsp
        for index_name in [
            CONTENT_SPARSE_FIELD,
            CONTENT_ZH_SPARSE_FIELD,
            CONTENT_DEFAULT_SPARSE_FIELD,
            VARCHAR_TEXT_FIELD,
        ]:
            assert utility.wait_for_index_building_complete(
                self.collection_name,
                index_name=index_name,
                timeout=remaining_setup_timeout(setup_deadline, f"{index_name} index build", 300),
                using=connection_alias,
            ), f"{index_name} index did not complete within the shared fixture setup budget"

        rsp = collection_client.collection_load(collection_name=self.collection_name)
        assert rsp["code"] == 0, rsp
        collection_client.wait_load_completed(
            self.collection_name,
            timeout=remaining_setup_timeout(setup_deadline, "collection load", 120),
        )
        sealed_infos = wait_for_sealed_layout(
            self.collection_name,
            connection_alias,
            timeout=remaining_setup_timeout(setup_deadline, "sealed segment layout", 120),
        )
        self.__class__.shared_sealed_segment_ids = [info.segmentID for info in sealed_infos]

        growing_rows = self.shared_rows[INDEXED_SEALED_ROWS + UNINDEXED_SEALED_ROWS :]
        rsp = vector_client.vector_insert({"collectionName": self.collection_name, "data": growing_rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == GROWING_ROWS

        rsp = vector_client.vector_query(
            {
                "collectionName": self.collection_name,
                "filter": f"{ID_FIELD} >= 0",
                "outputFields": ["count(*)"],
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["data"] == [{"count(*)": TOTAL_ROWS}], rsp

        after_growing_infos = utility.get_query_segment_info(self.collection_name, using=connection_alias)
        assert {info.segmentID for info in after_growing_infos} == set(self.shared_sealed_segment_ids)
        sealed_rows = sum(info.num_rows for info in after_growing_infos)
        assert sealed_rows == INDEXED_SEALED_ROWS + UNINDEXED_SEALED_ROWS
        assert TOTAL_ROWS - sealed_rows == GROWING_ROWS

    def query_page(self, output_fields, limit, offset=0):
        rsp = self.vector_client.vector_query(
            {
                "collectionName": self.collection_name,
                "filter": f"{ID_FIELD} >= 0",
                "outputFields": list(dict.fromkeys([ID_FIELD] + output_fields)),
                "limit": limit,
                "offset": offset,
            },
            timeout=30,
        )
        assert rsp["code"] == 0, rsp
        return rsp["data"]

    def query_rows(self, output_fields):
        rows = self.query_page(output_fields, len(self.shared_rows))
        return {int(row[ID_FIELD]): row for row in rows}

    @pytest.mark.tags(CaseLabel.L0)
    def test_text_lob_shared_schema_and_payloads(self):
        """
        target: verify TEXT LOB schema, BM25 indexes, and exact payload retrieval
        method: describe the shared collection and query 4,500 rows across indexed sealed,
            unindexed sealed, and growing paths
        expected: every path participates and every TEXT payload checksum matches inserted data
        """
        rsp = self.collection_client.collection_describe(self.collection_name)
        assert rsp["code"] == 0, rsp
        fields = {field["name"]: field for field in rsp["data"]["fields"]}
        for field_name in TEXT_FIELDS:
            assert fields[field_name]["type"].lower() == "text"

        functions = {function["name"]: function for function in rsp["data"]["functions"]}
        for function_name, input_field, output_field in [
            ("content_bm25", CONTENT_FIELD, CONTENT_SPARSE_FIELD),
            ("content_zh_bm25", CONTENT_ZH_FIELD, CONTENT_ZH_SPARSE_FIELD),
            ("content_default_bm25", CONTENT_DEFAULT_FIELD, CONTENT_DEFAULT_SPARSE_FIELD),
        ]:
            function = functions[function_name]
            assert function["type"] == 1
            assert function["inputFieldNames"] == [input_field]
            assert function["outputFieldNames"] == [output_field]

        rsp = self.index_client.index_describe(self.collection_name, VECTOR_FIELD)
        assert rsp["code"] == 0, rsp
        dense_index = next(item for item in rsp["data"] if item["indexName"] == VECTOR_FIELD)
        assert dense_index["indexType"] == "HNSW"
        assert dense_index["metricType"] == "COSINE"

        for index_name in [CONTENT_SPARSE_FIELD, CONTENT_ZH_SPARSE_FIELD, CONTENT_DEFAULT_SPARSE_FIELD]:
            rsp = self.index_client.index_describe(self.collection_name, index_name)
            assert rsp["code"] == 0, rsp
            index = next(item for item in rsp["data"] if item["indexName"] == index_name)
            assert index["indexType"] == "SPARSE_INVERTED_INDEX"
            assert index["metricType"] == "BM25"

        rows_by_id = self.query_rows(TEXT_FIELDS + [VARCHAR_TEXT_FIELD])
        assert_rows_payload(rows_by_id, self.shared_expected, TEXT_FIELDS + [VARCHAR_TEXT_FIELD])
        assert len(set(rows_by_id).intersection(INDEXED_SEALED_RANGE)) == INDEXED_SEALED_ROWS
        assert len(set(rows_by_id).intersection(UNINDEXED_SEALED_RANGE)) == UNINDEXED_SEALED_ROWS
        assert len(set(rows_by_id).intersection(GROWING_RANGE)) == GROWING_ROWS

        for marker_id in LOB_MARKER_IDS.values():
            assert self.shared_expected[marker_id][CONTENT_FIELD]["bytes"] > 64 * 1024
            assert self.shared_expected[marker_id][CONTENT_ALT_FIELD]["bytes"] > 64 * 1024

    @pytest.mark.tags(CaseLabel.L0)
    def test_text_lob_dense_search_output_fields(self):
        """
        target: verify dense vector search returns TEXT LOB output fields
        method: search the shared vector used only by one LOB row in each segment path
        expected: all three paths return intact content and content_alt payloads
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [LOB_SEARCH_VECTOR],
                "annsField": VECTOR_FIELD,
                "limit": 3,
                "searchParams": {"metricType": "COSINE", "params": {"ef": 64}},
                "outputFields": [ID_FIELD, CONTENT_FIELD, CONTENT_ALT_FIELD],
                "orderByFields": [f"{ID_FIELD}:asc"],
            },
            timeout=30,
        )
        assert rsp["code"] == 0, rsp
        assert len(rsp["data"]) == len(LOB_MARKER_IDS)
        ids = [int(hit[ID_FIELD]) for hit in rsp["data"]]
        assert ids == sorted(LOB_MARKER_IDS.values())
        for hit in rsp["data"]:
            pk = int(hit[ID_FIELD])
            assert_text_payload(hit.get(CONTENT_FIELD), self.shared_expected[pk][CONTENT_FIELD])
            assert_text_payload(hit.get(CONTENT_ALT_FIELD), self.shared_expected[pk][CONTENT_ALT_FIELD])


class TestTextMutation(TestBase):
    def create_collection(self, nullable):
        self.name = gen_collection_name(prefix="rest_text_mutation")
        rsp = self.collection_client.collection_create(
            {
                "collectionName": self.name,
                "schema": {
                    "autoId": False,
                    "enableDynamicField": False,
                    "fields": [
                        {"fieldName": ID_FIELD, "dataType": "Int64", "isPrimary": True},
                        {
                            "fieldName": VECTOR_FIELD,
                            "dataType": "FloatVector",
                            "elementTypeParams": {"dim": str(DIM)},
                        },
                        text_field(CONTENT_FIELD, STANDARD_ANALYZER, nullable=nullable),
                    ],
                },
                "indexParams": [
                    {
                        "fieldName": VECTOR_FIELD,
                        "indexName": VECTOR_FIELD,
                        "metricType": "COSINE",
                    }
                ],
            }
        )
        assert rsp["code"] == 0, rsp
        self.collection_client.wait_load_completed(self.name, timeout=120)

    @pytest.mark.tags(CaseLabel.L0)
    def test_text_partial_upsert_omitted_nullable_field(self):
        """
        target: verify partial upsert inserts a new row without a nullable TEXT value
        method: partially upsert an unknown primary key while omitting its nullable TEXT field
        expected: the row is inserted and querying the TEXT field returns null
        """
        self.create_collection(nullable=True)
        rsp = self.vector_client.vector_upsert(
            {
                "collectionName": self.name,
                "partialUpdate": True,
                "data": [{ID_FIELD: 100, VECTOR_FIELD: vector_for_pk(100)}],
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["upsertCount"] == 1

        rsp = self.vector_client.vector_query(
            {
                "collectionName": self.name,
                "filter": f"{ID_FIELD} == 100",
                "outputFields": [ID_FIELD, CONTENT_FIELD],
            },
            timeout=30,
        )
        assert rsp["code"] == 0, rsp
        assert len(rsp["data"]) == 1
        assert int(rsp["data"][0][ID_FIELD]) == 100
        assert rsp["data"][0].get(CONTENT_FIELD) is None

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize(
        ("row", "expected_message"),
        [
            ({ID_FIELD: 1, VECTOR_FIELD: vector_for_pk(1)}, f"field {CONTENT_FIELD} is required"),
            (
                {ID_FIELD: 2, VECTOR_FIELD: vector_for_pk(2), CONTENT_FIELD: None},
                f"field {CONTENT_FIELD} is not nullable",
            ),
        ],
        ids=["missing", "null"],
    )
    def test_text_non_nullable_insert_rejected(self, row, expected_message):
        """
        target: verify REST insert rejects absent and null non-nullable TEXT values
        method: insert one row that omits TEXT or explicitly sends null
        expected: both requests return the invalid-insert code and a field-specific message
        """
        self.create_collection(nullable=False)
        rsp = self.vector_client.vector_insert({"collectionName": self.name, "data": [row]})
        assert rsp["code"] == 1804, rsp
        assert expected_message in rsp["message"]
