import hashlib
import random

import pytest
from base.testbase import TestBase
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
):
    if content_zh is None and content is not None and len(content) <= 4096:
        content_zh = content
    if content_alt is None and content is not None:
        content_alt = f"alternate text lob payload for {scenario}: {content[:128]}"
    if varchar_text is None:
        varchar_text = content if content is not None and len(content) <= 60000 else f"{scenario} vector database"

    row = {
        ID_FIELD: pk,
        VECTOR_FIELD: vector_for_pk(pk),
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
            "one_mib",
            make_text(1024 * 1024, "0-one-mib"),
            content_zh=None,
            content_alt=make_text(128 * 1024, "0-alt-one-mib"),
            varchar_text="one mib vector database",
        ),
    ]
    return sorted(rows, key=lambda row: row[ID_FIELD])


def expected_payloads(rows):
    return {
        row[ID_FIELD]: {field: payload_meta(row.get(field)) for field in TEXT_FIELDS + [VARCHAR_TEXT_FIELD]}
        for row in rows
    }


def text_field(field_name, analyzer_params):
    return {
        "fieldName": field_name,
        "dataType": "Text",
        "nullable": True,
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
        "indexParams": [
            {
                "fieldName": VECTOR_FIELD,
                "indexName": VECTOR_FIELD,
                "indexType": "FLAT",
                "metricType": "COSINE",
            },
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
        "params": {"consistencyLevel": "Strong"},
    }


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

    def setup_class(self):
        self.collection_name = gen_collection_name(prefix="rest_text_lob")

    @pytest.fixture(scope="class", autouse=True)
    def prepare_text_lob_collection(self, request, init_class_config):
        collection_client, vector_client = self._class_scope_clients()

        def teardown():
            collection_client.collection_drop({"collectionName": self.collection_name})

        request.addfinalizer(teardown)
        self.__class__.shared_rows = build_text_lob_rows()
        self.__class__.shared_expected = expected_payloads(self.shared_rows)
        self.__class__.shared_ids = [row[ID_FIELD] for row in self.shared_rows]

        rsp = collection_client.collection_create(build_collection_payload(self.collection_name))
        assert rsp["code"] == 0, rsp
        collection_client.wait_load_completed(self.collection_name, timeout=120)
        rsp = vector_client.vector_insert({"collectionName": self.collection_name, "data": self.shared_rows})
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == len(self.shared_rows)
        rsp = collection_client.flush(self.collection_name)
        assert rsp["code"] == 0, rsp

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
        method: describe the shared collection and query every deterministic row
        expected: TEXT fields and functions exist and payload checksums match inserted data
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

        for index_name in [CONTENT_SPARSE_FIELD, CONTENT_ZH_SPARSE_FIELD, CONTENT_DEFAULT_SPARSE_FIELD]:
            rsp = self.index_client.index_describe(self.collection_name, index_name)
            assert rsp["code"] == 0, rsp
            index = next(item for item in rsp["data"] if item["indexName"] == index_name)
            assert index["indexType"] == "SPARSE_INVERTED_INDEX"
            assert index["metricType"] == "BM25"

        rows_by_id = self.query_rows(TEXT_FIELDS + [VARCHAR_TEXT_FIELD])
        assert_rows_payload(rows_by_id, self.shared_expected, TEXT_FIELDS + [VARCHAR_TEXT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
    def test_text_lob_dense_search_output_fields(self):
        """
        target: verify dense vector search returns TEXT LOB output fields
        method: search with an exact fixture vector and validate returned TEXT checksums
        expected: every returned content and content_alt payload matches inserted data
        """
        rsp = self.vector_client.vector_search(
            {
                "collectionName": self.collection_name,
                "data": [vector_for_pk(0)],
                "annsField": VECTOR_FIELD,
                "limit": 3,
                "outputFields": [ID_FIELD, CONTENT_FIELD, CONTENT_ALT_FIELD],
            },
            timeout=30,
        )
        assert rsp["code"] == 0, rsp
        assert 0 < len(rsp["data"]) <= 3
        ids = [int(hit[ID_FIELD]) for hit in rsp["data"]]
        assert len(ids) == len(set(ids))
        assert 0 in ids
        for hit in rsp["data"]:
            pk = int(hit[ID_FIELD])
            assert_text_payload(hit.get(CONTENT_FIELD), self.shared_expected[pk][CONTENT_FIELD])
            assert_text_payload(hit.get(CONTENT_ALT_FIELD), self.shared_expected[pk][CONTENT_ALT_FIELD])

    @pytest.mark.tags(CaseLabel.L0)
    def test_text_lob_query_iterator_payloads(self):
        """
        target: verify REST query returns every TEXT LOB row exactly once
        method: page through REST query in batches of three because REST v2 has no query iterator endpoint
        expected: no primary keys are missing or duplicated and every TEXT payload is intact
        """
        batch_size = 3
        rows = []
        for offset in range(0, len(self.shared_rows), batch_size):
            page = self.query_page([CONTENT_FIELD], batch_size, offset)
            assert len(page) == min(batch_size, len(self.shared_rows) - offset)
            rows.extend(page)

        rows_by_id = {int(row[ID_FIELD]): row for row in rows}
        assert len(rows) == len(self.shared_rows)
        assert len(rows_by_id) == len(rows), "REST query pagination returned duplicate primary keys"
        assert_rows_payload(rows_by_id, self.shared_expected, [CONTENT_FIELD])
