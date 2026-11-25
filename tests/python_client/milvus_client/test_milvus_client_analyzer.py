import json
import math
import os
import time
from contextlib import closing
from itertools import product
from pathlib import Path
from typing import Any, Protocol, cast
from zlib import crc32

import pymilvus
import pytest
import requests
from base.client_base import Base
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import text_lob_utils as lob
from common.common_type import CaseLabel
from common.compaction_utils import reload_compaction_outputs
from common.text_generator import generate_text_by_analyzer
from grpc import RpcError
from packaging.version import Version
from pymilvus import DataType, Function, FunctionType, LexicalHighlighter, MilvusClient, MilvusException
from pymilvus.client.utils import check_status
from utils.util_log import test_log as log


class AnalyzerResult(Protocol):
    """Protocol for analyzer result to help with type inference"""

    tokens: list[dict[str, Any]]


class TestMilvusClientAnalyzer(TestMilvusClientV2Base):
    @staticmethod
    def get_expected_jieba_tokens(text, analyzer_params):
        """
        Generate expected tokens using rjieba based on analyzer parameters.
        """
        import rjieba

        tokenizer_config = analyzer_params.get("tokenizer", {})
        if isinstance(tokenizer_config, str):
            tokenizer_config = {}

        # rjieba does not expose jieba-rs dynamic dictionary APIs. Fall back
        # to targeted assertions in custom-dictionary cases.
        if "dict" in tokenizer_config and tokenizer_config["dict"] != ["_default_"]:
            return None

        mode = tokenizer_config.get("mode", "search")
        hmm = tokenizer_config.get("hmm", True)

        if mode == "exact":
            tokens = list(rjieba.cut(text, hmm))
        elif mode == "search":
            tokens = list(rjieba.cut_for_search(text, hmm))
        else:
            tokens = list(rjieba.cut(text, hmm))

        # Filter out empty tokens
        tokens = [token for token in tokens if token.strip()]

        return tokens

    analyzer_params_list = [
        {
            "tokenizer": "standard",
            "filter": [
                {
                    "type": "stop",
                    "stop_words": ["is", "the", "this", "a", "an", "and", "or"],
                }
            ],
        },
        {
            "tokenizer": "jieba",
            "filter": [
                {
                    "type": "stop",
                    "stop_words": ["is", "the", "this", "a", "an", "and", "or", "是", "的", "这", "一个", "和", "或"],
                }
            ],
        },
        {"tokenizer": "icu"},
        # {
        #     "tokenizer": {"type": "lindera", "dict_kind": "ipadic"},
        #     "filter": [
        #         {
        #             "type": "stop",
        #             "stop_words": ["は", "が", "の", "に", "を", "で", "と", "た"],
        #         }
        #     ],
        # },
        # {"tokenizer": {"type": "lindera", "dict_kind": "ko-dic"}},
        # {"tokenizer": {"type": "lindera", "dict_kind": "cc-cedict"}},
    ]

    jieba_custom_analyzer_params_list = [
        # # Test dict parameter with custom dictionary
        {"tokenizer": {"type": "jieba", "dict": ["结巴分词器"], "mode": "exact", "hmm": False}},
        # Test dict parameter with default dict and custom dict
        {"tokenizer": {"type": "jieba", "dict": ["_default_", "结巴分词器"], "mode": "search", "hmm": False}},
        # Test exact mode with hmm enabled
        {"tokenizer": {"type": "jieba", "dict": ["结巴分词器"], "mode": "exact", "hmm": True}},
        # Test search mode with hmm enabled
        {"tokenizer": {"type": "jieba", "dict": ["结巴分词器"], "mode": "search", "hmm": True}},
        # Test with only mode configuration
        {"tokenizer": {"type": "jieba", "mode": "exact"}},
        # Test with only hmm configuration
        {"tokenizer": {"type": "jieba", "hmm": False}},
    ]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("analyzer_params", analyzer_params_list)
    def test_analyzer(self, analyzer_params):
        """
        target: test analyzer
        method: use different analyzer params, then run analyzer to get the tokens
        expected: verify the tokens
        """
        client = self._client()
        text = generate_text_by_analyzer(analyzer_params)
        res, _ = self.run_analyzer(client, text, analyzer_params, with_detail=True, with_hash=True)
        res_2, _ = self.run_analyzer(client, text, analyzer_params, with_detail=True, with_hash=True)

        # Cast to help type inference for gRPC response
        analyzer_res = cast(AnalyzerResult, res)
        analyzer_res_2 = cast(AnalyzerResult, res_2)

        # verify the result are the same when run analyzer twice
        for i in range(len(analyzer_res.tokens)):
            assert analyzer_res.tokens[i]["token"] == analyzer_res_2.tokens[i]["token"]
            assert analyzer_res.tokens[i]["hash"] == analyzer_res_2.tokens[i]["hash"]
            assert analyzer_res.tokens[i]["start_offset"] == analyzer_res_2.tokens[i]["start_offset"]
            assert analyzer_res.tokens[i]["end_offset"] == analyzer_res_2.tokens[i]["end_offset"]
            assert analyzer_res.tokens[i]["position"] == analyzer_res_2.tokens[i]["position"]
            assert analyzer_res.tokens[i]["position_length"] == analyzer_res_2.tokens[i]["position_length"]

        tokens = analyzer_res.tokens
        token_list = [r["token"] for r in tokens]
        # Check tokens are not empty
        assert len(token_list) > 0, "No tokens were generated"

        # Check tokens are related to input text (all token should be a substring of the text)
        assert all(token.lower() in text.lower() for token in token_list), (
            "some of the tokens do not appear in the original text"
        )

        if "filter" in analyzer_params:
            for filter in analyzer_params["filter"]:
                if filter["type"] == "stop":
                    stop_words = filter["stop_words"]
                    assert not any(token in stop_words for token in tokens), "some of the tokens are stop words"

        # Check hash value and detail
        for r in tokens:
            assert isinstance(r["hash"], int)
            assert isinstance(r["start_offset"], int)
            assert isinstance(r["end_offset"], int)
            assert isinstance(r["position"], int)
            assert isinstance(r["position_length"], int)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("analyzer_params", jieba_custom_analyzer_params_list)
    def test_jieba_custom_analyzer(self, analyzer_params):
        """
        target: test jieba analyzer with custom configurations
        method: use different jieba analyzer params with dict, mode, and hmm configurations
        expected: verify the tokens are generated correctly based on configuration
        """
        client = self._client()
        text = "milvus结巴分词器中文测试"
        res, _ = self.run_analyzer(client, text, analyzer_params, with_detail=True)

        analyzer_res = cast(AnalyzerResult, res)
        tokens = analyzer_res.tokens
        token_list = [r["token"] for r in tokens]

        # Check tokens are not empty
        assert len(token_list) > 0, "No tokens were generated"

        # Generate expected tokens using rjieba and compare when the Python
        # binding exposes the required tokenizer configuration.
        expected_tokens = self.get_expected_jieba_tokens(text, analyzer_params)
        if expected_tokens is None:
            custom_words = [
                word
                for word in analyzer_params["tokenizer"].get("dict", [])
                if word not in ("", "_default_", "_extend_default_")
            ]
            assert all(word in token_list for word in custom_words), (
                f"Expected custom words {custom_words}, but got {token_list}"
            )
        else:
            assert sorted(token_list) == sorted(expected_tokens), f"Expected {expected_tokens}, but got {token_list}"

        # Verify token details
        for r in tokens:
            assert isinstance(r["token"], str)
            assert isinstance(r["start_offset"], int)
            assert isinstance(r["end_offset"], int)
            assert isinstance(r["position"], int)
            assert isinstance(r["position_length"], int)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "invalid_analyzer_params",
        [
            {"tokenizer": "invalid_tokenizer"},
            {"tokenizer": 123},
            {"tokenizer": None},
            {"tokenizer": []},
            {"tokenizer": {"type": "invalid_type"}},
            {"tokenizer": {"type": None}},
            {"filter": "invalid_filter"},
            {"filter": [{"type": None}]},
            {"filter": [{"invalid_key": "value"}]},
        ],
    )
    def test_analyzer_with_invalid_params(self, invalid_analyzer_params):
        """
        target: test analyzer with invalid parameters
        method: use invalid analyzer params and expect errors
        expected: analyzer should raise appropriate exceptions
        """
        client = self._client()
        text = "test text for invalid analyzer"

        with pytest.raises(Exception):
            self.run_analyzer(client, text, invalid_analyzer_params)

    @pytest.mark.tags(CaseLabel.L1)
    def test_analyzer_with_empty_params(self):
        """
        target: test analyzer with empty parameters (uses default)
        method: use empty analyzer params
        expected: analyzer should use default configuration and work normally
        """
        client = self._client()
        text = "test text for empty analyzer"

        # Empty params should use default configuration
        res, _ = self.run_analyzer(client, text, {})
        analyzer_res = cast(AnalyzerResult, res)
        assert len(analyzer_res.tokens) > 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "invalid_text",
        [
            None,
            123,
            True,
            False,
        ],
    )
    def test_analyzer_with_invalid_text(self, invalid_text):
        """
        target: test analyzer with invalid text input
        method: use valid analyzer params but invalid text
        expected: analyzer should handle invalid text appropriately
        """
        client = self._client()
        analyzer_params = {"tokenizer": "standard"}

        with pytest.raises(Exception):
            self.run_analyzer(client, invalid_text, analyzer_params)

    @pytest.mark.tags(CaseLabel.L1)
    def test_analyzer_with_empty_text(self):
        """
        target: test analyzer with empty text
        method: use empty text input
        expected: analyzer should return empty tokens
        """
        client = self._client()
        analyzer_params = {"tokenizer": "standard"}

        res, _ = self.run_analyzer(client, "", analyzer_params)
        analyzer_res = cast(AnalyzerResult, res)
        assert len(analyzer_res.tokens) == 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "text_input",
        [
            [],
            {},
            ["list", "of", "strings"],
            {"key": "value"},
        ],
    )
    def test_analyzer_with_structured_text(self, text_input):
        """
        target: test analyzer with structured text input (list/dict)
        method: use list or dict as text input
        expected: analyzer should handle structured input and return tokens
        """
        client = self._client()
        analyzer_params = {"tokenizer": "standard"}

        res, _ = self.run_analyzer(client, text_input, analyzer_params)
        # For structured input, API returns direct list format
        assert isinstance(res, list)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "invalid_jieba_params",
        [
            {"tokenizer": {"type": "jieba", "dict": "not_a_list"}},
            {"tokenizer": {"type": "jieba", "dict": [123, 456]}},
            {"tokenizer": {"type": "jieba", "mode": "invalid_mode"}},
            {"tokenizer": {"type": "jieba", "mode": 123}},
            {"tokenizer": {"type": "jieba", "hmm": "not_boolean"}},
            {"tokenizer": {"type": "jieba", "hmm": 123}},
        ],
    )
    def test_jieba_analyzer_with_invalid_config(self, invalid_jieba_params):
        """
        target: test jieba analyzer with invalid configurations
        method: use jieba analyzer with invalid dict, mode, or hmm values
        expected: analyzer should raise appropriate exceptions
        """
        client = self._client()
        text = "测试文本 for jieba analyzer"

        with pytest.raises(Exception):
            self.run_analyzer(client, text, invalid_jieba_params)

    @pytest.mark.tags(CaseLabel.L1)
    def test_jieba_analyzer_with_empty_dict(self):
        """
        target: test jieba analyzer with empty dictionary
        method: use jieba analyzer with empty dict list
        expected: analyzer should work with empty dict (uses default)
        """
        client = self._client()
        text = "测试文本 for jieba analyzer"
        jieba_params = {"tokenizer": {"type": "jieba", "dict": []}}

        res, _ = self.run_analyzer(client, text, jieba_params)
        analyzer_res = cast(AnalyzerResult, res)
        assert len(analyzer_res.tokens) > 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "invalid_dict_config",
        [
            {"tokenizer": {"type": "jieba", "dict": None}},
            {"tokenizer": {"type": "jieba", "dict": "invalid_string"}},
            {"tokenizer": {"type": "jieba", "dict": 123}},
            {"tokenizer": {"type": "jieba", "dict": True}},
            {"tokenizer": {"type": "jieba", "dict": {"invalid": "dict"}}},
        ],
    )
    def test_jieba_analyzer_with_invalid_dict_values(self, invalid_dict_config):
        """
        target: test jieba analyzer with invalid dict configurations
        method: use jieba analyzer with invalid dict values
        expected: analyzer should raise appropriate exceptions
        """
        client = self._client()
        text = "测试文本 for jieba analyzer"

        with pytest.raises(Exception):
            self.run_analyzer(client, text, invalid_dict_config)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "edge_case_dict_config",
        [
            {"tokenizer": {"type": "jieba", "dict": ["", "valid_word"]}},  # Empty string in list
            {"tokenizer": {"type": "jieba", "dict": ["valid_word", "valid_word"]}},  # Duplicate words
            {"tokenizer": {"type": "jieba", "dict": ["_default_"]}},  # Only default dict
        ],
    )
    def test_jieba_analyzer_with_edge_case_dict_values(self, edge_case_dict_config):
        """
        target: test jieba analyzer with edge case dict configurations
        method: use jieba analyzer with edge case dict values
        expected: analyzer should handle these cases gracefully
        """
        client = self._client()
        text = "测试文本 for jieba analyzer"

        res, _ = self.run_analyzer(client, text, edge_case_dict_config, with_detail=True)
        analyzer_res = cast(AnalyzerResult, res)
        # These should work but might not be recommended usage
        assert len(analyzer_res.tokens) >= 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_jieba_analyzer_with_unknown_param(self):
        """
        target: test jieba analyzer with unknown parameter
        method: use jieba analyzer with invalid parameter name
        expected: analyzer should ignore unknown parameters and work normally
        """
        client = self._client()
        text = "测试文本 for jieba analyzer"
        jieba_params = {"tokenizer": {"type": "jieba", "invalid_param": "value"}}

        res, _ = self.run_analyzer(client, text, jieba_params)
        analyzer_res = cast(AnalyzerResult, res)
        assert len(analyzer_res.tokens) > 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "invalid_filter_params",
        [
            {"tokenizer": "standard", "filter": [{"type": "stop", "stop_words": "not_a_list"}]},
            {"tokenizer": "standard", "filter": [{"type": "stop", "stop_words": [123, 456]}]},
            {"tokenizer": "standard", "filter": [{"type": "invalid_filter_type"}]},
        ],
    )
    def test_analyzer_with_invalid_filter(self, invalid_filter_params):
        """
        target: test analyzer with invalid filter configurations
        method: use analyzer with invalid filter parameters
        expected: analyzer should handle invalid filters appropriately
        """
        client = self._client()
        text = "This is a test text with stop words"

        with pytest.raises(Exception):
            self.run_analyzer(client, text, invalid_filter_params)

    @pytest.mark.tags(CaseLabel.L1)
    def test_analyzer_with_empty_stop_words(self):
        """
        target: test analyzer with empty stop words list
        method: use stop filter with empty stop_words list
        expected: analyzer should work normally with empty stop words (no filtering)
        """
        client = self._client()
        text = "This is a test text with stop words"
        filter_params = {"tokenizer": "standard", "filter": [{"type": "stop", "stop_words": []}]}

        res, _ = self.run_analyzer(client, text, filter_params, with_detail=True)
        analyzer_res = cast(AnalyzerResult, res)
        tokens = analyzer_res.tokens
        token_list = [r["token"] for r in tokens]

        assert len(token_list) > 0
        # With empty stop words, no filtering should occur
        assert "is" in token_list  # Common stop word should still be present

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("with_bm25", [False, True])
    @pytest.mark.parametrize("protocol", ["python", "rest"])
    def test_field_analyzer_without_query_resources(self, with_bm25, protocol):
        """Field analyzers are WAL resources, available before load and after release."""
        client = self._client()
        name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=8)
        schema.add_field(
            "text", DataType.VARCHAR, max_length=1000, enable_analyzer=True, analyzer_params={"type": "standard"}
        )
        if with_bm25:
            schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
            schema.add_function(
                Function(
                    name="bm25",
                    function_type=FunctionType.BM25,
                    input_field_names=["text"],
                    output_field_names=["sparse"],
                )
            )
        self.create_collection(client, name, schema=schema, shards_num=2)

        def analyze():
            if protocol == "python":
                result = client.run_analyzer(
                    "Hello world", collection_name=name, field_name="text", with_detail=True, with_hash=True
                )
                tokens = result.tokens
            else:
                endpoint = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
                response = requests.post(
                    endpoint + "/v2/vectordb/common/run_analyzer",
                    json={
                        "text": ["Hello world"],
                        "collectionName": name,
                        "fieldName": "text",
                        "withDetail": True,
                        "withHash": True,
                    },
                    timeout=30,
                )
                response.raise_for_status()
                body = response.json()
                assert body["code"] == 0, body
                tokens = body["data"]["results"][0]["tokens"]
            assert [token["token"] for token in tokens] == ["hello", "world"]
            assert all(token["hash"] for token in tokens)

        # Create without indexes/load; there is no loaded delegator.
        analyze()
        indexes = client.prepare_index_params()
        indexes.add_index("vector", index_type="FLAT", metric_type="L2")
        if with_bm25:
            indexes.add_index("sparse", index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
        client.create_index(name, indexes)
        client.load_collection(name)
        analyze()
        client.release_collection(name)
        analyze()


def gen_mapping_analyzer_params(*mapping_stages, tokenizer="standard", filters=None, mode=None):
    """Build custom analyzer parameters with one mapping filter per stage."""
    params = {"tokenizer": tokenizer}
    if filters is not None:
        params["filter"] = filters
    if mapping_stages:
        params["char_filter"] = [{"type": "mapping", "mappings": rules} for rules in mapping_stages]
    if mode is not None:
        params["char_filter_offset_mode"] = mode
    return params


def wait_for_text_match(client, name, pk, term, field="text", timeout=60):
    """Wait for the text index to expose a known term before comparing postings from the same batch."""
    pk_filter = f"id == {pk}"
    probe = f"{pk_filter} and text_match({field}, {json.dumps(term, ensure_ascii=False)})"
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if client.query(name, filter=probe, output_fields=["id"], timeout=max(0.1, end - time.monotonic())) == [
            {"id": pk}
        ]:
            return
        time.sleep(0.25)
    raise AssertionError(f"{name}: text index did not expose neutral probe {probe} within {timeout}s")


def wait_for_growing_text_match(client, name, pk, term, field="text", timeout=60):
    deadline = time.monotonic() + timeout
    wait_for_text_match(client, name, pk, term, field, timeout)
    assert not client.query(
        name,
        filter=f"id == {pk}",
        output_fields=["id"],
        ignore_growing=True,
        timeout=max(0.1, deadline - time.monotonic()),
    ), f"{name}: probe {pk} already sealed; growing coverage lost"


def flush_and_wait_for_sealed_segments(client, name, timeout=120, *, sealed_pk=None):
    """Wait for sealed segments to load and, when supplied, a row to become readable there."""
    end = time.monotonic() + timeout
    client.flush(name, timeout=timeout)
    expected, loaded = set(), set()
    while time.monotonic() < end:
        expected = {
            s.segment_id
            for s in client.list_persistent_segments(name, timeout=max(0.1, end - time.monotonic()))
            if s.num_rows > 0 and s.level_name != "L0" and s.state_name in {"Sealed", "Flushed", "Flushing"}
        }
        loaded = {
            s.segment_id
            for s in client.list_loaded_segments(name, timeout=max(0.1, end - time.monotonic()))
            if s.state_name == "Sealed"
        }
        if expected and expected <= loaded:
            if sealed_pk is None or client.query(
                name,
                filter=f"id == {sealed_pk}",
                output_fields=["id"],
                ignore_growing=True,
                timeout=max(0.1, end - time.monotonic()),
            ) == [{"id": sealed_pk}]:
                return
        time.sleep(0.5)
    raise AssertionError(
        f"{name}: sealed handoff timed out, expected segments={expected}, loaded={loaded}, probe PK={sealed_pk}"
    )


def assert_text_query_results(client, name, expr, expected, **kwargs):
    rows = client.query(name, filter=expr, output_fields=["id", "text"], **kwargs)
    assert len(rows) == len(expected), (name, expr, rows)
    assert {row["id"]: row["text"] for row in rows} == expected, (name, expr, rows)


def assert_sparse_hits(client, name, query, expected, **kwargs):
    """Inspect raw hit ordering for ranking and pass through non-default search parameters."""
    kwargs.setdefault("search_params", {"metric_type": "BM25", "params": {"drop_ratio_search": 0}})
    result = client.search(
        name,
        data=[query],
        anns_field="sparse",
        output_fields=["text"],
        consistency_level="Strong",
        **kwargs,
    )
    assert len(result) == 1
    hits = result[0]
    assert len(hits) == len(expected)
    assert {h["id"]: h["entity"]["text"] for h in hits} == expected
    return hits


def assert_bm25_search_results(client, name, query, expected, **kwargs):
    """Verify exact hits and descending, positive BM25 scores. Return scores by PK."""
    kwargs.setdefault("limit", max(10, len(expected) + 1))
    hits = assert_sparse_hits(client, name, query, expected, **kwargs)
    scores = [hit["distance"] for hit in hits]
    assert all(math.isfinite(score) and score > 0 for score in scores), (name, query, scores)
    assert scores == sorted(scores, reverse=True), (name, query, scores)
    return {hit["id"]: hit["distance"] for hit in hits}


def text_indexes(client, sparse_index="SPARSE_INVERTED_INDEX", dense=False, scalar_index=None):
    indexes = client.prepare_index_params()
    # AUTOINDEX accepts only the metric and resolves its build parameters on the server.
    bm25_params = {} if sparse_index == "AUTOINDEX" else {"bm25_k1": 1.5, "bm25_b": 0.75}
    indexes.add_index("sparse", index_type=sparse_index, metric_type="BM25", params=bm25_params)
    if dense:
        indexes.add_index("dense", index_type="FLAT", metric_type="L2")
    if scalar_index:
        params = {"min_gram": 2, "max_gram": 3} if scalar_index == "NGRAM" else {}
        indexes.add_index("text", index_type=scalar_index, params=params)
    return indexes


def create_text_table(
    case,
    client,
    params,
    *,
    dtype=DataType.VARCHAR,
    nullable=False,
    max_length=512,
    dense=False,
    sparse_index="SPARSE_INVERTED_INDEX",
    scalar_index=None,
    name=None,
    defer_index=False,
):
    name = name or cf.gen_collection_name_by_testcase_name(module_index=2)
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    if dtype == DataType.TEXT:
        require_text_storage()
    options = {"max_length": max_length} if dtype == DataType.VARCHAR else {}
    schema.add_field(
        "text", dtype, enable_analyzer=True, enable_match=True, analyzer_params=params, nullable=nullable, **options
    )
    schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
    schema.add_function(Function("bm25", FunctionType.BM25, ["text"], ["sparse"]))
    if dense:
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=2)
    case.create_collection(client, name, schema=schema, num_shards=1, consistency_level="Strong")
    if not defer_index:
        client.create_index(name, text_indexes(client, sparse_index, dense, scalar_index))
        client.load_collection(name)
    return name


def require_text_storage():
    """Default CI uses Storage V3. V2 jobs opt out explicitly before sending any DDL."""
    version = os.environ.get("MILVUS_STORAGE_VERSION", "3")
    assert version in {"2", "3"}, f"MILVUS_STORAGE_VERSION must be 2 or 3, got {version!r}"
    if version == "2":
        pytest.skip("TEXT requires Storage V3; this job explicitly selects Storage V2")


def assert_spans(result, raw, expected, with_hash=True):
    actual = [(t["token"], t["start_offset"], t["end_offset"]) for t in result.tokens]
    assert actual == expected, (raw, actual)
    source = raw.encode()
    for token, (word, start, end) in zip(result.tokens, expected, strict=True):
        assert 0 <= start <= end <= len(source)
        # An empty slice also decodes at an invalid byte boundary. Check each prefix.
        source[:start].decode()
        source[:end].decode()
        if with_hash:
            # RunAnalyzer hashes the first 100 UTF-8 bytes of the emitted term.
            assert token["hash"] == crc32(word.encode()[:100]) % (2**32 - 1)


def corpus_spans(raw, mode):
    """Compute provenance directly from the spaced atoms in the original input."""
    expected = []
    start = 0
    for atom in raw.split():
        end = start + len(atom.encode())
        if atom == "北":
            expected.extend([("x", start, start if mode == "boundary" else end), ("y", start, end)])
        elif atom == "q":
            expected.append(("q", start, end))
        else:
            assert atom == "&"
        start = end + 1
    return expected


def validation_schema(client, params):
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("text", DataType.VARCHAR, max_length=512, enable_analyzer=True, analyzer_params=params)
    schema.add_field("dense", DataType.FLOAT_VECTOR, dim=2)
    return schema


class TestMilvusClientCharFilterAnalyzer(TestMilvusClientV2Base):
    """Add scalar/batch analyzer cases here. Field-bound cases use their own analyzer schema."""

    @pytest.mark.parametrize(
        ("overrides", "raw", "expected"),
        [
            pytest.param({}, "A&B", [("a", 0, 1), ("b", 2, 3)], marks=pytest.mark.tags(CaseLabel.L1), id="absent"),
            pytest.param(
                {"char_filter": []},
                "A&B",
                [("a", 0, 1), ("b", 2, 3)],
                marks=pytest.mark.tags(CaseLabel.L1),
                id="empty-array",
            ),
            pytest.param(
                {"char_filter": [], "char_filter_offset_mode": "boundary"},
                "A&B",
                [("a", 0, 1), ("b", 2, 3)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="empty-array-boundary",
            ),
            pytest.param(
                {"char_filter": [{"type": "mapping", "mappings": []}]},
                "A&B",
                [("a", 0, 1), ("b", 2, 3)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="empty-mapping",
            ),
            pytest.param(
                {"char_filter": [{"type": "mapping", "mappings": ["& => and"]}]},
                "A&B",
                [("aandb", 0, 3)],
                marks=pytest.mark.tags(CaseLabel.L0),
                id="rewrite",
            ),
            pytest.param(
                gen_mapping_analyzer_params(
                    ["中=>x", "😀=>y"], ["xy=>p q"], tokenizer="whitespace", mode="source_span"
                ),
                "中😀",
                [("p", 0, 7), ("q", 0, 7)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="cross-character-source-span",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["中=>x", "😀=>y"], ["xy=>p q"], tokenizer="whitespace", mode="boundary"),
                "中😀",
                [("p", 0, 3), ("q", 3, 7)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="cross-character-boundary",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["ab=>😀 é"], tokenizer="whitespace", mode="source_span"),
                "ab",
                [("😀", 0, 2), ("é", 0, 2)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="multibyte-target-source-span",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["ab=>😀 é"], tokenizer="whitespace", mode="boundary"),
                "ab",
                [("😀", 0, 1), ("é", 1, 2)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="multibyte-target-boundary",
            ),
        ],
    )
    def test_inline_and_field_analysis(self, overrides, raw, expected):
        """
        target: C02/C08 field and inline entry points preserve the same token details.
        method: Analyze the same mappings through both entry points, including multibyte sources and targets.
        expected: Both return the expected hashes, positions and original UTF-8 offsets.
        """
        client = self._client()
        params = {"tokenizer": "standard", "filter": ["lowercase"], **overrides}
        name = create_text_table(self, client, params)
        for entry in ({"analyzer_params": params}, {"collection_name": name, "field_name": "text"}):
            result = client.run_analyzer(raw, with_detail=True, with_hash=True, **entry)
            assert_spans(result, raw, expected)
            assert [(t["position"], t["position_length"]) for t in result.tokens] == [
                (i, 1) for i in range(len(expected))
            ]

    @pytest.mark.parametrize(
        ("params", "raw", "expected"),
        [
            pytest.param(
                gen_mapping_analyzer_params(["中=>x y"]),
                "中",
                [("x", 0, 3), ("y", 0, 3)],
                marks=pytest.mark.tags(CaseLabel.L1),
                id="default-span",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["中=>x y"], mode="source_span"),
                "中",
                [("x", 0, 3), ("y", 0, 3)],
                marks=pytest.mark.tags(CaseLabel.L1),
                id="source-span",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["中=>x y"], mode="boundary"),
                "中",
                [("x", 0, 0), ("y", 0, 3)],
                marks=pytest.mark.tags(CaseLabel.L0),
                id="boundary",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["😀=>smile"]),
                "😀",
                [("smile", 0, 4)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="four-byte-source",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["-=>"], tokenizer="whitespace"),
                "中-文",
                [("中文", 0, 7)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="inner-deletion",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["#=>"]),
                "###",
                [],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="all-deleted",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["#=>"]),
                "a#b",
                [("ab", 0, 3)],
                marks=pytest.mark.tags(CaseLabel.L1),
                id="join",
            ),
            pytest.param(
                gen_mapping_analyzer_params([r"\t=>\u0020"], tokenizer={"type": "char_group", "delimiters": [" "]}),
                "foo\tbar",
                [("foo", 0, 3), ("bar", 4, 7)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="char-group-tab",
            ),
            pytest.param(
                gen_mapping_analyzer_params(
                    ["中=>THE cat"], filters=["lowercase", {"type": "stop", "stop_words": ["the"]}]
                ),
                "中",
                [("cat", 0, 3)],
                marks=pytest.mark.tags(CaseLabel.L1),
                id="stop-after-lowercase",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["A=>Z"], filters=["lowercase"]),
                "A a",
                [("z", 0, 1), ("a", 2, 3)],
                marks=pytest.mark.tags(CaseLabel.L1),
                id="case-sensitive-before-lowercase",
            ),
            pytest.param(
                gen_mapping_analyzer_params([r"-=>\u0020"], tokenizer="whitespace"),
                "a-b",
                [("a", 0, 1), ("b", 2, 3)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="escaped-target-space",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["-=> "], tokenizer="whitespace"),
                "a-b",
                [("ab", 0, 3)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="trim-target-space",
            ),
            pytest.param(
                gen_mapping_analyzer_params([r"\uD83D\uDE00=>smile"]),
                "😀",
                [("smile", 0, 4)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="surrogate-pair",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["aa=>b"]),
                "aa",
                [("b", 0, 2)],
                marks=pytest.mark.tags(CaseLabel.L1),
                id="contraction",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["中=>x y"], ["x=>p q"], tokenizer="whitespace"),
                "起 中 终",
                [("起", 0, 3), ("p", 4, 7), ("q", 4, 7), ("y", 4, 7), ("终", 8, 11)],
                marks=pytest.mark.tags(CaseLabel.L1),
                id="composed-source-span",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["中=>x y"], ["x=>p q"], tokenizer="whitespace", mode="boundary"),
                "起 中 终",
                [("起", 0, 3), ("p", 4, 4), ("q", 4, 4), ("y", 4, 7), ("终", 8, 11)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="composed-boundary",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["b=>", "c=>x y"]),
                "abc",
                [("ax", 0, 3), ("y", 2, 3)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="overlapping-offsets",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["#=>"]),
                "###a###",
                [("a", 3, 4)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="deletion-at-both-ends",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["abc=>x y"]),
                "abc",
                [("x", 0, 3), ("y", 0, 3)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="equal-size-source-span",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["abc=>x y"], mode="boundary"),
                "abc",
                [("x", 0, 1), ("y", 2, 3)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="equal-size-boundary",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["é=>Z"], tokenizer="whitespace", filters=[]),
                "é e\u0301",
                [("Z", 0, 2), ("e\u0301", 3, 6)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="composed-vs-decomposed",
            ),
            pytest.param(
                gen_mapping_analyzer_params(["a=>b=>c"], tokenizer="whitespace"),
                "a=>b",
                [("c", 0, 4)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="last-arrow",
            ),
            pytest.param(
                gen_mapping_analyzer_params([r"\=\>=>arrow"], tokenizer="whitespace"),
                "=>",
                [("arrow", 0, 2)],
                marks=pytest.mark.tags(CaseLabel.L2),
                id="escaped-arrow",
            ),
        ],
    )
    def test_mapping_offsets(self, params, raw, expected):
        """
        target: C05-C08/C18 greedy mappings and tokenizer composition retain source provenance.
        method: Analyze literal cases for expansion, deletion, chaining, tokenizer ordering and escapes.
        expected: Each rewritten token has the specified source-byte interval and canonical hash.
        """
        client = self._client()
        result = client.run_analyzer(raw, analyzer_params=params, with_detail=True, with_hash=True)
        assert_spans(result, raw, expected)

    @pytest.mark.tags(CaseLabel.L2)
    def test_long_mapped_token_hash(self):
        """
        target: C08. Hash expanded tokens at the 100-byte prefix boundary.
        method: Rewrite the last character of three inputs with the whitespace tokenizer.
        expected: Whole tokens and source spans survive when the hash prefix ends in ASCII or inside a UTF-8 character.
        """
        client = self._client()
        texts = ["x" * 99 + "@", "x" * 100 + "@", "x" * 99 + "#"]
        expected_terms = ["x" * 100, "x" * 101, "x" * 99 + "中"]
        params = gen_mapping_analyzer_params(["@=>x", "#=>中"], tokenizer="whitespace")
        results = client.run_analyzer(texts, analyzer_params=params, with_detail=True, with_hash=True)
        for raw, term, result in zip(texts, expected_terms, results, strict=True):
            assert_spans(result, raw, [(term, 0, len(raw.encode()))])

    @pytest.mark.tags(CaseLabel.L1)
    def test_longest_match_and_chaining(self):
        """
        target: C06 a stage chooses its longest source and never reprocesses its own output.
        method: Apply the same rule in the first and second stages over a batch with repeated input.
        expected: Only the second stage turns X into Y and the untouched row keeps its slot.
        """
        client = self._client()
        texts = ["ab a", "keep", "ab a"]
        first = ["a=>Q", "ab=>X", "X=>Y"]
        for stages, word in (([first], "x"), ([first, ["X=>Y"]], "y")):
            result = client.run_analyzer(
                texts,
                analyzer_params=gen_mapping_analyzer_params(*stages, filters=["lowercase"]),
                with_detail=True,
                with_hash=True,
            )
            mapped = [(word, 0, 2), ("q", 3, 4)]
            for row, raw, expected in zip(result, texts, [mapped, [("keep", 0, 4)], mapped], strict=True):
                assert_spans(row, raw, expected)

    @pytest.mark.tags(CaseLabel.L1)
    def test_jieba_offsets_on_mixed_text(self):
        """
        target: C18 character filtering preserves Jieba tokenization and original source spans.
        method: Compare analysis against manually rewritten Chinese and Latin text under the same tokenizer.
        expected: The canonical terms agree and every token points to its original UTF-8 byte range.
        """
        client = self._client()
        tokenizer = {"type": "jieba", "mode": "exact"}
        raw = "中文 中介 @ 测试"
        canonical = raw.replace("@", "token")
        reference = client.run_analyzer(canonical, analyzer_params={"tokenizer": tokenizer})
        actual = client.run_analyzer(
            raw,
            analyzer_params=gen_mapping_analyzer_params(["@=>token"], tokenizer=tokenizer),
            with_detail=True,
            with_hash=True,
        )
        assert [t["token"] for t in actual.tokens] == reference.tokens
        assert_spans(
            actual,
            raw,
            [
                ("中文", 0, 6),
                (" ", 6, 7),
                ("中介", 7, 13),
                (" ", 13, 14),
                ("token", 14, 15),
                (" ", 15, 16),
                ("测试", 16, 22),
            ],
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        ("overrides", "reason"),
        [
            pytest.param({"char_filter": {}}, "should be array", id="not-array"),
            pytest.param({"char_filter": ["mapping"]}, "should be object", id="not-object"),
            pytest.param({"char_filter": [{}]}, "type must be set", id="missing-type"),
            pytest.param({"char_filter": [{"type": 42}]}, "type should be string", id="type-not-string"),
            pytest.param({"char_filter": [{"type": "html_strip"}]}, "unsupported char_filter type", id="unknown-type"),
            pytest.param(
                {"char_filter": [{"type": "mapping", "mappings_path": "rules.txt"}]},
                "must set mappings",
                id="external-rules",
            ),
            pytest.param({"char_filter": [{"type": "mapping"}]}, "must set mappings", id="missing-rules"),
            pytest.param(
                {"char_filter": [{"type": "mapping", "mappings": "a=>b"}]}, "must be array", id="rules-not-array"
            ),
            pytest.param(gen_mapping_analyzer_params([1]), "item must be string", id="rule-not-string"),
            pytest.param(gen_mapping_analyzer_params(["abc"]), "invalid mapping", id="missing-arrow"),
            pytest.param(gen_mapping_analyzer_params(["=>x"]), "source must not be empty", id="empty-source"),
            pytest.param(
                gen_mapping_analyzer_params(["a=>x", r"\u0061=>y"]),
                "source must be unique",
                id="duplicate-unescaped-source",
            ),
            pytest.param(gen_mapping_analyzer_params([r"\uD800=>x"]), "invalid unicode escape", id="lone-surrogate"),
            pytest.param(gen_mapping_analyzer_params([r"\u0000=>x"]), "does not support U+0000", id="nul"),
            pytest.param(gen_mapping_analyzer_params(["a=>b"], mode=3), "should be string", id="mode-not-string"),
            pytest.param(
                gen_mapping_analyzer_params(["a=>b"], mode="other"),
                "unsupported char_filter_offset_mode",
                id="unknown-mode",
            ),
            pytest.param({"char_filter_offset_mode": "boundary"}, "requires char_filter", id="mode-without-filter"),
        ],
    )
    def test_invalid_mapping(self, overrides, reason):
        """
        target: C08 malformed character mappings fail consistently at inline analysis and DDL.
        method: Submit the same invalid JSON to both APIs and reuse the rejected collection name.
        expected: Each error identifies the bad option, and valid DDL succeeds under the same name.
        """
        client = self._client()
        self.check_rejected_config(client, {"tokenizer": "standard", **overrides}, reason)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "options",
        [
            {"char_filter": [{"type": "mapping", "mappings": ["&=>and"]}]},
            {"char_filter": []},
            {"char_filter_offset_mode": "boundary"},
        ],
        ids=["mapping", "empty", "mode"],
    )
    def test_builtin_template_rejects_mapping(self, options):
        """
        target: C04 built-in templates reject custom character-filter options on the new version.
        method: Try each template in RunAnalyzer and collection creation, then retry with a valid template.
        expected: The diagnostic identifies the template restriction and omitting the keys restores old tokens.
        """
        client = self._client()
        self.check_rejected_config(
            client, {"type": "standard", **options}, "cannot be used with build-in analyzer type"
        )
        result = client.run_analyzer("A&B", analyzer_params={"type": "standard"})
        assert result.tokens == ["a", "b"]

    def check_rejected_config(self, client, params, reason):
        """Inline analysis exposes the segcore validation code and DDL exposes the schema validation code."""
        with pytest.raises(MilvusException) as exc:
            client.run_analyzer("A&B", analyzer_params=params)
        assert exc.value.code == 2042, exc.value
        assert reason in str(exc.value)
        name = cf.gen_collection_name_by_testcase_name(module_index=2)
        self.tear_down_collection_names.append(name)
        with pytest.raises(MilvusException) as exc:
            client.create_collection(name, schema=validation_schema(client, params), consistency_level="Strong")
        assert exc.value.code == 1100, exc.value
        assert reason in str(exc.value)
        assert not client.has_collection(name)
        good_params = {"type": "standard"} if "type" in params else {"tokenizer": "standard"}
        client.create_collection(name, schema=validation_schema(client, good_params), consistency_level="Strong")
        assert client.has_collection(name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_field_and_inline_request_are_exclusive(self):
        """
        target: C08 a request cannot select a loaded field and a separate inline analyzer together.
        method: Supply both legal entry points in one RunAnalyzer request.
        expected: The server rejects the ambiguous analyzer selection.
        """
        client = self._client()
        params = gen_mapping_analyzer_params(["&=>and"])
        name = create_text_table(self, client, params)
        with pytest.raises(MilvusException) as exc:
            client.run_analyzer("A&B", analyzer_params=params, collection_name=name, field_name="text")
        assert "can't use analyzer params and (collection,field) in same time" in str(exc.value)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mode", ["source_span", "boundary"])
    def test_exhaustive_short_corpus(self, mode):
        """
        target: C24 empty, repeated, expanded and deleted input survives batch analysis and indexed retrieval.
        method: Enumerate all 40 spaced strings of up to three atoms and replay exact queries before and after flush.
        expected: Ordered byte spans and hashes agree in both APIs, and every matching PK retains its original text.
        """
        client = self._client()
        params = gen_mapping_analyzer_params(["北=>x y", "&=>"], tokenizer="whitespace", mode=mode)
        name = create_text_table(self, client, params)
        texts = [" ".join(atoms) for size in range(4) for atoms in product(("北", "&", "q"), repeat=size)]
        originals = dict(enumerate(texts))
        assert len(originals) == 40

        for entry in ({"analyzer_params": params}, {"collection_name": name, "field_name": "text"}):
            for batch in (texts, texts[::-1], [*texts, texts[1]]):
                for with_hash in (False, True):
                    results = client.run_analyzer(batch, with_detail=True, with_hash=with_hash, **entry)
                    for raw, row in zip(batch, results, strict=True):
                        expected = corpus_spans(raw, mode)
                        assert_spans(row, raw, expected, with_hash)
                        assert [(t["position"], t["position_length"]) for t in row.tokens] == [
                            (i, 1) for i in range(len(expected))
                        ], raw

        north = {pk: raw for pk, raw in originals.items() if "北" in raw}
        q = {pk: raw for pk, raw in originals.items() if "q" in raw}
        both = {pk: original for pk, original in north.items() if pk in q}
        assert (len(north), len(q), len(both)) == (25, 25, 14)
        inserted = client.insert(name, [{"id": pk, "text": raw} for pk, raw in originals.items()])
        assert inserted["insert_count"] == len(originals)
        wait_for_growing_text_match(client, name, 3, "q")
        for sealed in (False, True):
            if sealed:
                flush_and_wait_for_sealed_segments(client, name, sealed_pk=3)
            assert_text_query_results(client, name, 'text_match(text,"北")', north, ignore_growing=sealed)
            assert_text_query_results(client, name, 'phrase_match(text,"x y",0)', north, ignore_growing=sealed)
            assert_text_query_results(
                client, name, 'text_match(text,"x q",minimum_should_match=2)', both, ignore_growing=sealed
            )
            assert_bm25_search_results(client, name, "q", q, limit=len(texts), ignore_growing=sealed)
            assert_text_query_results(client, name, "id >= 0", originals, ignore_growing=sealed)
            if not sealed:
                assert_text_query_results(client, name, "id >= 0", {}, ignore_growing=True)


PLAIN = gen_mapping_analyzer_params(filters=["lowercase"])
MAPPED = gen_mapping_analyzer_params(["&=>and"], filters=["lowercase"])
DOCS = {1: "A&B", 2: "A B", 3: "aandb", 4: "noise", 5: "B A", 6: "aandc"}


def subset(docs, *ids):
    return {pk: docs[pk] for pk in ids}


def insert_docs(client, name, docs, *, dense=False):
    rows = [{"id": pk, "text": text} for pk, text in docs.items()]
    if dense:
        for row in rows:
            row["dense"] = [float(row["id"]), 0.0]
    assert client.insert(name, rows)["insert_count"] == len(rows)


def assert_base_queries(client, name, mapped, *, ignore_growing=False):
    queries = [
        ('text_match(text, "aandb")', [3], [1, 3]),
        ('text_match(text, "A&B")', [1, 2, 5], [1, 3]),
        ('text_match(text, "a")', [1, 2, 5], [2, 5]),
        ('phrase_match(text, "a b", 0)', [1, 2], [2]),
        ('text_match(text, "A&B", minimum_should_match=2)', [1, 2, 5], []),
        ('text_match_fuzzy(text, "aandx", max_edit_distance=1)', [3, 6], [1, 3, 6]),
        ('text_match_fuzzy(text, "A&B", max_edit_distance=0)', [1, 2, 5], [1, 3]),
        ('text_match_fuzzy(text, "A&X", max_edit_distance=1)', [1, 2, 5], [1, 3, 6]),
        ('text == "A&B"', [1], [1]),
        ('text like "A&B%"', [1], [1]),
    ]
    for expr, plain_ids, mapped_ids in queries:
        assert_text_query_results(
            client, name, expr, subset(DOCS, *(mapped_ids if mapped else plain_ids)), ignore_growing=ignore_growing
        )
    for word, plain_ids, mapped_ids in [("aandb", [3], [1, 3]), ("A&B", [1, 2, 5], [1, 3])]:
        assert_bm25_search_results(
            client, name, word, subset(DOCS, *(mapped_ids if mapped else plain_ids)), ignore_growing=ignore_growing
        )


class TestMilvusClientCharFilterQuery(TestMilvusClientV2Base):
    """Use isolated collections for different schemas, handoffs and mutations."""

    @pytest.mark.parametrize(
        "mapped",
        [
            pytest.param(False, marks=pytest.mark.tags(CaseLabel.L1), id="original"),
            pytest.param(True, marks=pytest.mark.tags(CaseLabel.L0), id="mapped"),
        ],
    )
    def test_filter_membership_across_handoff(self, mapped):
        """
        target: C03, C17. Preserve raw comparisons and apply the same analyzer to documents and requests.
        method: 1. Insert six distinguishing rows. 2. Replay filters and searches before/after handoff and reload.
        expected: each stage returns the exact original PK/text pairs for its analyzer configuration.
        """
        client = self._client()
        name = create_text_table(self, client, MAPPED if mapped else PLAIN)
        insert_docs(client, name, DOCS)
        wait_for_growing_text_match(client, name, 4, "noise")
        assert_base_queries(client, name, mapped)
        if mapped:
            for query, distance, ids in [
                ("aandx", 0, []),
                ("aandx", 2, [1, 3, 6]),
                ("aanxx", 1, []),
                ("aanxx", 2, [1, 3, 6]),
            ]:
                assert_text_query_results(
                    client, name, f'text_match_fuzzy(text,"{query}",max_edit_distance={distance})', subset(DOCS, *ids)
                )
            assert_text_query_results(client, name, 'phrase_match(text, "A&B", 0)', subset(DOCS, 1, 3))
        assert_text_query_results(client, name, "id >= 0", {}, ignore_growing=True)
        flush_and_wait_for_sealed_segments(client, name, sealed_pk=4)
        assert_text_query_results(client, name, "id >= 0", DOCS, ignore_growing=True)
        assert_base_queries(client, name, mapped, ignore_growing=True)
        client.release_collection(name)
        client.load_collection(name)
        assert_base_queries(client, name, mapped, ignore_growing=True)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dtype", [DataType.VARCHAR, DataType.TEXT], ids=["varchar", "text"])
    def test_mixed_segments_upsert_and_delete(self, dtype):
        """
        target: C09, C19. Verify row visibility across sealed postings, growing inserts, upsert and deletion.
        method: 1. Seal a batch; copy it into growing. 2. Exclude growing, mutate old PKs, then flush/reload.
        expected: all 12 surviving PKs retain their exact text and only two still match aandb.
        """
        client = self._client()
        name = create_text_table(self, client, MAPPED, dtype=dtype)
        insert_docs(client, name, DOCS)
        flush_and_wait_for_sealed_segments(client, name, sealed_pk=4)
        docs = {**DOCS, **{pk + 100: value for pk, value in DOCS.items()}}
        insert_docs(client, name, subset(docs, *range(101, 107)))
        wait_for_growing_text_match(client, name, 104, "noise")
        expr = 'text_match(text, "aandb")'
        assert_text_query_results(client, name, expr, subset(docs, 1, 3, 101, 103))
        assert_text_query_results(client, name, expr, subset(docs, 1, 3), ignore_growing=True)
        assert_bm25_search_results(client, name, "aandb", subset(docs, 1, 3, 101, 103))
        assert_bm25_search_results(client, name, "aandb", subset(docs, 1, 3), ignore_growing=True)
        docs[1] = "A B"
        docs[107] = "anchor"
        assert client.upsert(name, [{"id": pk, "text": docs[pk]} for pk in (1, 107)])["upsert_count"] == 2
        wait_for_growing_text_match(client, name, 107, "anchor")
        assert client.delete(name, ids=[3])["delete_count"] == 1
        del docs[3]

        def assert_survivors(*, ignore_growing):
            assert_text_query_results(client, name, "id >= 0", docs, ignore_growing=ignore_growing)
            assert_text_query_results(client, name, expr, subset(docs, 101, 103), ignore_growing=ignore_growing)
            assert_text_query_results(
                client, name, 'phrase_match(text, "a b", 0)', subset(docs, 1, 2, 102), ignore_growing=ignore_growing
            )
            assert_bm25_search_results(client, name, "aandb", subset(docs, 101, 103), ignore_growing=ignore_growing)

        assert_survivors(ignore_growing=False)
        assert_text_query_results(client, name, "id >= 100", {}, ignore_growing=True)
        flush_and_wait_for_sealed_segments(client, name, sealed_pk=107)
        client.release_collection(name)
        client.load_collection(name)
        assert_survivors(ignore_growing=True)

    @pytest.mark.tags(CaseLabel.L2)
    def test_compacted_postings(self):
        """
        target: C19. Verify mapped postings after physical compaction.
        method: Flush two batches, delete one match, then reload the outputs of the completed compaction plans.
        expected: all 11 surviving rows keep their original text and only the three matching PKs have postings.
        """
        client = self._client()
        name = create_text_table(self, client, MAPPED)
        client.alter_collection_properties(name, properties={"collection.autocompaction.enabled": "false"})
        insert_docs(client, name, DOCS)
        client.flush(name)
        docs = {pk + 100: text for pk, text in DOCS.items()}
        insert_docs(client, name, docs)
        client.flush(name)
        segments = self.wait_for_compaction_eligible_segments(client, name, minimum_segment_count=2, timeout=180)
        before = {segment.segment_id for segment in segments}
        assert client.delete(name, ids=[1])["delete_count"] == 1
        client.flush(name)
        job = client.compact(name)
        assert job != -1, f"No compaction was scheduled for segments {before}"
        assert self.wait_for_compaction_ready(client, job, timeout=300)
        plans = client.get_compaction_plans(job, timeout=30).plans
        sources = {segment_id for plan in plans for segment_id in plan.sources}
        targets = {plan.target for plan in plans}
        assert sources and sources <= before, (before, plans)
        assert targets and targets.isdisjoint(before), (before, plans)
        reload_compaction_outputs(client, name, (before - sources) | targets, sources)
        survivors = {**DOCS, **docs}
        del survivors[1]
        assert_text_query_results(client, name, "id >= 0", survivors, ignore_growing=True)
        expected = {**subset(DOCS, 3), **subset(docs, 101, 103)}
        assert_text_query_results(client, name, 'text_match(text, "A&B")', expected, ignore_growing=True)
        assert_bm25_search_results(client, name, "aandb", expected, ignore_growing=True)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dtype", [DataType.VARCHAR, DataType.TEXT], ids=["varchar", "text"])
    def test_nullable_and_erased_terms(self, dtype):
        """
        target: C15. Distinguish null, empty and deleted tokens, including a literal term emitted by another rule.
        method: 1. Insert seven distinct rows. 2. Check positive/negated filters, BM25 and raw values in both states.
        expected: only PK 17 has the term erase, only PK 15 has hi, and NULL never satisfies NOT text_match.
        """
        client = self._client()
        rules = gen_mapping_analyzer_params(["#=>", "erase=>", "needle=>erase"], filters=["lowercase"])
        name = create_text_table(self, client, rules, dtype=dtype, nullable=True)
        docs = {11: None, 12: "", 13: " ", 14: "erase", 15: "hi#", 16: "noise", 17: "needle"}
        plain = create_text_table(self, client, PLAIN, dtype=dtype, nullable=True)
        insert_docs(client, plain, docs)
        flush_and_wait_for_sealed_segments(client, plain, sealed_pk=16)
        for word, pk in [("erase", 14), ("needle", 17)]:
            assert_text_query_results(
                client, plain, f'text_match(text, "{word}")', subset(docs, pk), ignore_growing=True
            )
        for word, tokens in [("erase", []), ("needle", ["erase"])]:
            assert client.run_analyzer(word, collection_name=name, field_name="text").tokens == tokens
        insert_docs(client, name, docs)
        wait_for_growing_text_match(client, name, 16, "noise")
        for sealed in (False, True):
            if sealed:
                flush_and_wait_for_sealed_segments(client, name, sealed_pk=16)
            assert_text_query_results(client, name, "id >= 0", docs, ignore_growing=sealed)
            assert_text_query_results(client, name, 'text_match(text, "erase")', {}, ignore_growing=sealed)
            for word, pk in [("hi", 15), ("needle", 17)]:
                expected = subset(docs, pk)
                assert_text_query_results(client, name, f'text_match(text, "{word}")', expected, ignore_growing=sealed)
                assert_text_query_results(
                    client, name, f'phrase_match(text, "{word}", 0)', expected, ignore_growing=sealed
                )
                assert_bm25_search_results(client, name, word, expected, ignore_growing=sealed)
            assert_text_query_results(
                client, name, 'not text_match(text, "hi")', subset(docs, 12, 13, 14, 16, 17), ignore_growing=sealed
            )
            if dtype == DataType.VARCHAR:
                assert_text_query_results(client, name, "text is null", subset(docs, 11), ignore_growing=sealed)
                assert_text_query_results(client, name, 'text == ""', subset(docs, 12), ignore_growing=sealed)
            else:
                for scalar_predicate in ["text is null", 'text == ""', 'text like "%hi%"']:
                    with pytest.raises(MilvusException, match="filter on text field"):
                        client.query(name, filter=scalar_predicate, ignore_growing=sealed)
            if not sealed:
                assert_text_query_results(client, name, "id >= 0", {}, ignore_growing=True)

    @pytest.mark.tags(CaseLabel.L2)
    def test_varchar_original_byte_limit(self):
        """
        target: C15. Enforce max_length on the stored UTF-8 text.
        method: Insert 7- and 8-byte originals, then try two 9-byte originals that respectively expand and contract.
        expected: both rejected inserts report 9 bytes against an 8-byte limit. Only the two legal originals are stored.
        """
        client = self._client()
        params = gen_mapping_analyzer_params(["中=>target word", "压缩词=>x"])
        name = create_text_table(self, client, params, max_length=8)
        docs = {1: "中 中", 2: "中 中!"}
        insert_docs(client, name, docs)
        for pk, raw, tokens in [(3, "中 中!!", ["target", "word", "target", "word"]), (4, "压缩词", ["x"])]:
            assert client.run_analyzer(raw, collection_name=name, field_name="text").tokens == tokens
            with pytest.raises(MilvusException, match=r"\blength: 9, max length: 8\b") as exc:
                client.insert(name, [{"id": pk, "text": raw}])
            assert exc.value.code == 1100, exc.value
            assert "length of varchar field text exceeds max length" in exc.value.message, exc.value
        flush_and_wait_for_sealed_segments(client, name)
        assert_text_query_results(client, name, "id >= 0", docs)
        assert_text_query_results(client, name, 'text_match(text, "target")', docs)

    @pytest.mark.tags(CaseLabel.L1)
    def test_positions_thresholds_and_consumers(self):
        """
        target: C17. Interpret thresholds and phrase gaps after expansion; remove zero-term requests.
        method: 1. Write distinct positions and a query-side deletion sentinel. 2. Query, iterate, search and delete.
        expected: exact PKs distinguish slop 0/1, thresholds 1/2/3, query deletion and correct filter binding.
        """
        client = self._client()
        name = create_text_table(
            self, client, gen_mapping_analyzer_params(["@=>red blue", "dropword=>", "probe=>dropword"]), dense=True
        )
        docs = {21: "@", 22: "red blue", 23: "red x blue", 24: "red", 25: "quiet", 26: "probe"}
        insert_docs(client, name, docs, dense=True)
        flush_and_wait_for_sealed_segments(client, name)
        for k, ids in [(1, [21, 22, 23, 24]), (2, [21, 22, 23]), (3, [])]:
            assert_text_query_results(
                client, name, f'text_match(text, "@", minimum_should_match={k})', subset(docs, *ids)
            )
        terms = client.run_analyzer("@", collection_name=name, field_name="text", with_detail=True).tokens
        assert [t["token"] for t in terms] == ["red", "blue"]
        assert terms[1]["position"] == terms[0]["position"] + 1
        assert [(t["position_length"], t["start_offset"], t["end_offset"]) for t in terms] == [(1, 0, 1), (1, 0, 1)]
        phrase = 'phrase_match(text, "@", 0)'
        assert_text_query_results(client, name, phrase, subset(docs, 21, 22))
        assert_text_query_results(client, name, 'phrase_match(text, "@", 1)', subset(docs, 21, 22, 23))
        for expr in (
            'text_match(text, "dropword")',
            'phrase_match(text, "dropword", 0)',
            'text_match_fuzzy(text, "dropword", max_edit_distance=1)',
        ):
            assert_text_query_results(client, name, expr, {})
        assert_text_query_results(client, name, 'text_match(text, "probe")', subset(docs, 26))
        for expr, ids in [
            ("text_match(text, {q}) and id in [21,25]", [21]),
            ("text_match(text, {q}) or id == 25", [21, 22, 23, 24, 25]),
        ]:
            expected = subset(docs, *ids)
            assert_text_query_results(client, name, expr, expected, filter_params={"q": "@"})
            assert_text_query_results(client, name, expr.replace("{q}", '"@"'), expected)
            result = client.search(
                name,
                [[0.0, 0.0]],
                anns_field="dense",
                filter=expr,
                filter_params={"q": "@"},
                output_fields=["text"],
                limit=len(docs),
            )
            assert len(result) == 1
            assert [(h["id"], h["entity"]["text"]) for h in result[0]] == sorted(expected.items())
        assert_bm25_search_results(client, name, "blue", subset(docs, 21, 22), filter=phrase)
        iterator = client.query_iterator(name, filter=phrase, batch_size=1, output_fields=["id", "text"])
        try:
            rows = []
            while batch := iterator.next():
                assert len(batch) == 1, batch
                rows.append((batch[0]["id"], batch[0]["text"]))
        finally:
            iterator.close()
        assert sorted(rows) == sorted(subset(docs, 21, 22).items())
        client.delete(name, filter=phrase)
        assert_text_query_results(client, name, "id >= 0", subset(docs, 23, 24, 25, 26))

    @pytest.mark.tags(CaseLabel.L2)
    def test_stopword_gaps_and_repeated_terms(self):
        """
        target: C17. Preserve gaps and repeated positions even when offsets coincide.
        method: expand two symbols before the stop filter and check phrase positions and TEXT_MATCH thresholds.
        expected: the repeated phrase matches 45 and 46. Both red clauses match PKs 41 through 46 even with one red
                  occurrence, and a threshold of three matches no rows.
        """
        client = self._client()
        stop = [{"type": "stop", "stop_words": ["the"]}]
        rules = gen_mapping_analyzer_params(["@=>red the blue", "!=>red red"], filters=stop)
        name = create_text_table(self, client, rules)
        reference = create_text_table(self, client, gen_mapping_analyzer_params(filters=stop))
        docs = {41: "@", 42: "red blue", 43: "red x blue", 44: "red", 45: "!", 46: "red red", 47: "quiet"}
        normalized = {pk: {"@": "red the blue", "!": "red red"}.get(raw, raw) for pk, raw in docs.items()}
        for table, data in [(name, docs), (reference, normalized)]:
            insert_docs(client, table, data)
            flush_and_wait_for_sealed_segments(client, table)
        terms = client.run_analyzer("@", collection_name=name, field_name="text", with_detail=True).tokens
        assert [t["token"] for t in terms] == ["red", "blue"]
        assert terms[1]["position"] - terms[0]["position"] == 2
        for q, ids in [("@", [41, 43]), ("red blue", [42]), ("!", [45, 46])]:
            assert_text_query_results(client, name, f'phrase_match(text,"{q}",0)', subset(docs, *ids))
        for table, query, data in [(name, "!", docs), (reference, "red red", normalized)]:
            assert_text_query_results(client, table, 'text_match(text,"quiet")', {47: "quiet"}, ignore_growing=True)
            red_rows = subset(data, 41, 42, 43, 44, 45, 46)
            for minimum, expected in [(1, red_rows), (2, red_rows), (3, {})]:
                expr = f'text_match(text,"{query}",minimum_should_match={minimum})'
                assert_text_query_results(client, table, expr, expected, ignore_growing=True)

    @pytest.mark.tags(CaseLabel.L1)
    def test_field_isolation_and_single_pass(self):
        """
        target: C23. Prevent analyzer leakage between two text fields and recursive application of same-stage rules.
        method: 1. Insert identical raw pairs into differently configured fields. 2. Query each alone and together.
        expected: only the mapped field merges y/z, and only PK 33 satisfies the cross-field predicate.
        """
        client = self._client()
        name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=2)
        for field, config in [("plain", PLAIN), ("mapped", gen_mapping_analyzer_params(["x=>y", "y=>z"]))]:
            schema.add_field(
                field, DataType.VARCHAR, max_length=32, enable_analyzer=True, enable_match=True, analyzer_params=config
            )
        idx = client.prepare_index_params()
        idx.add_index("dense", index_type="FLAT", metric_type="L2")
        self.create_collection(client, name, schema=schema, num_shards=1, consistency_level="Strong")
        client.create_index(name, idx)
        client.load_collection(name)
        docs = {31: "x", 32: "y", 33: "z", 34: "quiet"}
        client.insert(name, [{"id": pk, "plain": v, "mapped": v, "dense": [pk, 0]} for pk, v in docs.items()])
        for field in ["plain", "mapped"]:
            wait_for_growing_text_match(client, name, 34, "quiet", field)
        for state in ["growing", "sealed", "reloaded"]:
            if state == "sealed":
                flush_and_wait_for_sealed_segments(client, name, sealed_pk=34)
            if state == "reloaded":
                client.release_collection(name)
                client.load_collection(name)
            for q, plain_pk, mapped_term, mapped_ids in [
                ("x", 31, "y", [31]),
                ("y", 32, "z", [32, 33]),
                ("z", 33, "z", [32, 33]),
            ]:
                for field, term, ids in [("plain", q, [plain_pk]), ("mapped", mapped_term, mapped_ids)]:
                    assert client.run_analyzer(q, collection_name=name, field_name=field).tokens == [term]
                    rows = client.query(
                        name,
                        filter=f'text_match({field},"{q}")',
                        output_fields=["id", "plain", "mapped"],
                        ignore_growing=state != "growing",
                    )
                    assert sorted((r["id"], r["plain"], r["mapped"]) for r in rows) == [
                        (pk, docs[pk], docs[pk]) for pk in ids
                    ]
            assert client.query(
                name,
                filter='text_match(mapped,"y") and not text_match(plain,"y")',
                output_fields=["id", "plain", "mapped"],
                ignore_growing=state != "growing",
            ) == [{"id": 33, "plain": "z", "mapped": "z"}]
            if state == "growing":
                assert not client.query(name, filter="id >= 0", output_fields=["id"], ignore_growing=True)

    @pytest.mark.tags(CaseLabel.L2)
    def test_rebuild_from_raw_text_and_collection_isolation(self):
        """
        target: C09/C10. Recover old derived data from stored originals and isolate changes to one collection.
        method: 1. Copy mapped raw rows into an original-analyzer schema. 2. Mutate only the mapped source.
        expected: the rebuilt collection retains all six original PK/text pairs and the original query and BM25 hits.
        """
        client = self._client()
        mapped = create_text_table(self, client, MAPPED)
        rebuilt = create_text_table(self, client, PLAIN)
        insert_docs(client, mapped, DOCS)
        flush_and_wait_for_sealed_segments(client, mapped, sealed_pk=4)
        raw_rows = client.query(mapped, filter="id >= 0", output_fields=["id", "text"])
        assert sorted((r["id"], r["text"]) for r in raw_rows) == sorted(DOCS.items())
        assert client.insert(rebuilt, raw_rows)["insert_count"] == len(DOCS)
        flush_and_wait_for_sealed_segments(client, rebuilt, sealed_pk=4)
        assert_base_queries(client, rebuilt, False, ignore_growing=True)
        expr = 'text_match(text, "aandb")'
        client.upsert(mapped, [{"id": 1, "text": "A B"}, {"id": 7, "text": "anchor"}])
        wait_for_growing_text_match(client, mapped, 7, "anchor")
        assert_text_query_results(client, mapped, expr, subset(DOCS, 3))
        assert_bm25_search_results(client, mapped, "aandb", subset(DOCS, 3))
        assert_text_query_results(client, mapped, "id == 7", {}, ignore_growing=True)
        client.delete(mapped, ids=[3])
        flush_and_wait_for_sealed_segments(client, mapped, sealed_pk=7)
        assert_text_query_results(client, mapped, expr, {}, ignore_growing=True)
        assert_bm25_search_results(client, mapped, "aandb", {}, ignore_growing=True)
        assert_text_query_results(client, rebuilt, "id >= 0", DOCS)
        assert_text_query_results(client, rebuilt, expr, subset(DOCS, 3), ignore_growing=True)
        assert_bm25_search_results(client, rebuilt, "aandb", subset(DOCS, 3), ignore_growing=True)

    @staticmethod
    def _assert_field_analyzer(client, name, config, tokens):
        """Check both the stored configuration and the field's tokenization of A&B."""
        field = next(f for f in client.describe_collection(name)["fields"] if f["name"] == "text")
        raw = field["params"]["analyzer_params"]
        assert (json.loads(raw) if isinstance(raw, str) else raw) == config
        assert client.run_analyzer("A&B", collection_name=name, field_name="text").tokens == tokens

    @pytest.mark.parametrize(
        "dependency",
        [
            pytest.param("none", marks=pytest.mark.tags(CaseLabel.L1)),
            pytest.param("match", marks=pytest.mark.tags(CaseLabel.L2)),
            pytest.param("bm25", marks=pytest.mark.tags(CaseLabel.L2)),
            pytest.param("minhash", marks=pytest.mark.tags(CaseLabel.L2)),
        ],
    )
    def test_alter_field_analyzer(self, dependency):
        """
        target: C10. Protect derived data from online analyzer changes and exercise reversible changes on bare fields.
        method: 1. Create a field with exactly one selected dependency. 2. Alter its JSON and reread schema/analysis.
        expected: protected analyzers retain old semantics; a bare field supports A to B, idempotent B, and back to A.
        """
        client = self._client()
        name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(
            "text",
            DataType.VARCHAR,
            max_length=512,
            enable_analyzer=True,
            enable_match=dependency == "match",
            analyzer_params=PLAIN,
        )
        idx = client.prepare_index_params()
        if dependency == "bm25":
            schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
            schema.add_function(Function("bm25", FunctionType.BM25, ["text"], ["sparse"]))
            idx.add_index("sparse", index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
        elif dependency == "minhash":
            schema.add_field("sig", DataType.BINARY_VECTOR, dim=4096)
            schema.add_function(
                Function(
                    "hash",
                    FunctionType.MINHASH,
                    ["text"],
                    ["sig"],
                    params={"num_hashes": 128, "shingle_size": 1, "token_level": "word"},
                )
            )
            idx.add_index("sig", index_type="MINHASH_LSH", metric_type="MHJACCARD", params={"mh_lsh_band": 8})
        else:
            schema.add_field("dense", DataType.FLOAT_VECTOR, dim=2)
            idx.add_index("dense", index_type="FLAT", metric_type="L2")
        self.create_collection(client, name, schema=schema, consistency_level="Strong")
        client.create_index(name, idx)
        client.load_collection(name)
        insert_docs(client, name, DOCS, dense=dependency in {"none", "match"})
        flush_and_wait_for_sealed_segments(client, name)

        self._assert_field_analyzer(client, name, PLAIN, ["a", "b"])
        if dependency == "none":
            mapped = gen_mapping_analyzer_params(["&=>and", "marker=>ready"], filters=["lowercase"])
            for config, words, probe, reload_after in [
                (mapped, ["aandb"], "ready", False),
                (mapped, ["aandb"], "ready", True),
                (PLAIN, ["a", "b"], "marker", True),
            ]:
                client.alter_collection_field(name, "text", {"analyzer_params": json.dumps(config)})
                # The new configuration can become visible to field-bound RunAnalyzer after the DDL returns.
                deadline = time.monotonic() + 30
                while True:
                    observed = client.run_analyzer("marker", collection_name=name, field_name="text", timeout=5).tokens
                    if observed == [probe]:
                        break
                    assert observed in (["marker"], ["ready"]) and time.monotonic() < deadline, (config, observed)
                    time.sleep(0.25)
                self._assert_field_analyzer(client, name, config, words)
                if reload_after:
                    client.release_collection(name)
                    client.load_collection(name)
                    self._assert_field_analyzer(client, name, config, words)
                    assert client.run_analyzer("marker", collection_name=name, field_name="text").tokens == [probe]
        else:
            with pytest.raises(MilvusException) as exc:
                client.alter_collection_field(name, "text", {"analyzer_params": json.dumps(MAPPED)})
            assert exc.value.code == 1100, exc.value
            assert (
                "can not alter analyzer params for field text after text match is enabled "
                "or a BM25/MinHash function depends on it"
            ) in exc.value.message, exc.value
            self._assert_field_analyzer(client, name, PLAIN, ["a", "b"])
            if dependency == "match":
                assert_text_query_results(client, name, 'text_match(text,"A&B")', subset(DOCS, 1, 2, 5))
            elif dependency == "bm25":
                assert_bm25_search_results(client, name, "A&B", subset(DOCS, 1, 2, 5))
            else:
                hits = client.search(name, ["A&B"], anns_field="sig", limit=6)[0]
                assert {h["id"] for h in hits if h["distance"] == 1} == {1, 2, 5}

    @pytest.mark.tags(CaseLabel.L2)
    def test_invalid_mapping_alter_preserves_field_analyzer(self):
        """
        target: C10. Preserve a working analyzer when an otherwise mutable field receives an invalid mapping.
        method: 1. Load a field that maps & to plus. 2. Submit conflicting rules for &. 3. Reload and read the field.
        expected: ALTER rejects the duplicate source with code 1100. The field keeps its original rules and emits aplusb.
        """
        client = self._client()
        name = cf.gen_collection_name_by_testcase_name()
        original = gen_mapping_analyzer_params(["&=>plus"], filters=["lowercase"])
        indexes = client.prepare_index_params()
        indexes.add_index("dense", index_type="FLAT", metric_type="L2")
        self.create_collection(client, name, schema=validation_schema(client, original), index_params=indexes)
        self._assert_field_analyzer(client, name, original, ["aplusb"])

        invalid = gen_mapping_analyzer_params(["&=>and", "&=>or"], filters=["lowercase"])
        with pytest.raises(MilvusException) as exc:
            client.alter_collection_field(name, "text", {"analyzer_params": json.dumps(invalid)})
        assert exc.value.code == 1100, exc.value
        assert 'mapping char_filter source must be unique: "&"' in exc.value.message, exc.value
        self._assert_field_analyzer(client, name, original, ["aplusb"])
        client.release_collection(name)
        client.load_collection(name)
        self._assert_field_analyzer(client, name, original, ["aplusb"])

    @pytest.mark.tags(CaseLabel.L2)
    def test_invalid_operand_types_and_match_disabled(self):
        """
        target: C15. Restrict the text match family to text fields with a matching index.
        method: 1. Declare scalar, JSON, string array and analyzer-only fields. 2. Call each text predicate on them.
        expected: the non-text operands and disabled match field are rejected; analyzer-only still tokenizes.
        """
        client = self._client()
        name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("payload", DataType.JSON)
        schema.add_field("strings", DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=4, max_length=16)
        schema.add_field("text", DataType.VARCHAR, max_length=128, enable_analyzer=True, analyzer_params=MAPPED)
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=2)
        idx = client.prepare_index_params()
        idx.add_index("dense", index_type="FLAT", metric_type="L2")
        self.create_collection(client, name, schema=schema, consistency_level="Strong")
        client.create_index(name, idx)
        client.load_collection(name)
        assert client.run_analyzer("A&B", collection_name=name, field_name="text").tokens == ["aandb"]
        for field in ["id", "payload", "strings", "text"]:
            for pred in [
                f'text_match({field},"A&B")',
                f'phrase_match({field},"A&B",0)',
                f'text_match_fuzzy({field},"A&B",max_edit_distance=1)',
            ]:
                reason = "does not enable match" if field == "text" else "non-string"
                with pytest.raises(MilvusException, match=reason):
                    client.query(name, filter=pred)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "default, control, params, query, matches",
        [
            pytest.param(
                "erase",
                "needle",
                gen_mapping_analyzer_params(["erase=>", "needle=>erase"]),
                "needle",
                [3],
                id="empty-posting",
            ),
            pytest.param("A&B", "A B", MAPPED, "aandb", [1, 2], id="mapped-posting"),
        ],
    )
    def test_varchar_default_postings(self, default, control, params, query, matches):
        """
        target: C15. Index omitted and explicit VARCHAR defaults with the same analyzer.
        method: insert an omitted default, its explicit value, and a distinguishing control.
        expected: stored values stay raw and exact TEXT_MATCH membership follows the mapping.
        """
        client = self._client()
        name = cf.gen_collection_name_by_testcase_name()

        schema = client.create_schema(auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(
            "text",
            DataType.VARCHAR,
            max_length=128,
            nullable=True,
            default_value=default,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=params,
        )
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=2)
        idx = client.prepare_index_params()
        idx.add_index("dense", index_type="FLAT", metric_type="L2")
        self.create_collection(client, name, schema=schema, consistency_level="Strong")
        client.create_index(name, idx)
        client.load_collection(name)
        rows = [
            {"id": 1, "dense": [0, 0]},
            {"id": 2, "text": default, "dense": [1, 0]},
            {"id": 3, "text": control, "dense": [0, 1]},
        ]
        assert client.insert(name, rows)["insert_count"] == len(rows)
        expected = {1: default, 2: default, 3: control}
        wait_for_growing_text_match(client, name, matches[-1], query)
        for sealed in (False, True):
            if sealed:
                flush_and_wait_for_sealed_segments(client, name, sealed_pk=3)
            assert_text_query_results(client, name, "id >= 0", expected, ignore_growing=sealed)
            assert_text_query_results(
                client, name, f'text_match(text,"{query}")', subset(expected, *matches), ignore_growing=sealed
            )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("dependency", ["match", "bm25"])
    def test_match_and_bm25_require_analyzer(self, dependency):
        """
        target: C15. Require an analyzer on VARCHAR fields used by match or BM25.
        method: build the schema with the analyzer disabled and submit it to CreateCollection.
        expected: the server returns code 1100 for the selected dependency and the collection is absent.
        """
        client = self._client()
        name = cf.gen_collection_name_by_testcase_name()
        self.tear_down_collection_names.append(name)
        schema = client.create_schema(auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=2)
        schema.add_field(
            "text", DataType.VARCHAR, max_length=128, enable_match=dependency == "match", enable_analyzer=False
        )
        if dependency == "bm25":
            schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
            schema.add_function(Function("bm25", FunctionType.BM25, ["text"], ["sparse"]))
            reason = "BM25 function input field must set enable_analyzer to true"
        else:
            reason = "field text which has enable_match or is input of BM25 function must also enable_analyzer"
        with pytest.raises(MilvusException) as exc:
            client.create_collection(name, schema=schema, consistency_level="Strong")
        assert exc.value.code == 1100, exc.value
        assert reason in exc.value.message, exc.value
        assert not client.has_collection(name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_text_default_rejected(self):
        """
        target: C15. Reject default values for TEXT where TEXT storage is available.
        method: set A&B as the default on a nullable TEXT field whose analyzer maps & to and.
        expected: the default is rejected and the collection name is still free.
        """
        require_text_storage()
        client = self._client()
        name = cf.gen_collection_name_by_testcase_name()
        self.tear_down_collection_names.append(name)
        schema = client.create_schema(auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=2)
        schema.add_field(
            "text",
            DataType.TEXT,
            nullable=True,
            default_value="A&B",
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=MAPPED,
        )
        with pytest.raises(MilvusException) as exc:
            client.create_collection(name, schema=schema, consistency_level="Strong")
        assert exc.value.code == 1100, exc.value
        reason = "type (Text) of field (text) is not equal to the type(DataType_VarChar) of default_value"
        assert reason in exc.value.message, exc.value
        assert not client.has_collection(name)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("request_kind", ["literal-empty", "mapped-batch"])
    def test_bm25_zero_terms_in_batch(self, request_kind):
        """
        target: C17. Keep a zero-term query in its own result slot, including between two nonempty requests.
        method: 1. Index terms including a mapped literal dropword. 2. Compare the three single searches to a batch.
        expected: nonempty slots match single-query scores and ranking, while zero-term queries return no hits.
        """
        client = self._client()
        name = create_text_table(self, client, gen_mapping_analyzer_params(["dropword=>", "probe=>dropword"]))
        docs = {1: "red", 2: "blue", 3: "red blue", 4: "probe"}
        insert_docs(client, name, docs)
        flush_and_wait_for_sealed_segments(client, name)
        if request_kind == "literal-empty":
            assert_bm25_search_results(client, name, "", {})
            return
        expected = [subset(docs, 1, 3), {}, subset(docs, 2, 3)]
        queries = ["red", "dropword", "blue"]
        search_params = {"metric_type": "BM25", "params": {"drop_ratio_search": 0}}
        singles = [
            assert_bm25_search_results(client, name, query, matches, limit=len(docs), search_params=search_params)
            for query, matches in zip(queries, expected, strict=True)
        ]
        batches = client.search(
            name,
            queries,
            anns_field="sparse",
            limit=len(docs),
            output_fields=["text"],
            consistency_level="Strong",
            search_params=search_params,
        )
        for hits, matches, single in zip(batches, expected, singles, strict=True):
            assert len(hits) == len(matches)
            assert {h["id"]: h["entity"]["text"] for h in hits} == matches
            assert {h["id"]: h["distance"] for h in hits} == pytest.approx(single, rel=1e-4, abs=1e-6)
            scores = [h["distance"] for h in hits]
            assert scores == sorted(scores, reverse=True)


def webui_get(path, *, params=None, timeout=15):
    api = os.environ["MILVUS_WEBUI_API_URL"]
    _, token = Base._connection_params()
    auth = tuple(token.split(":", 1)) if token and ":" in token else None
    response = requests.get(
        f"{api.rstrip('/')}/{path}",
        params=params,
        auth=auth,
        headers={"X-Milvus-Admin-Request": "true"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def configured_threshold(env_name, config_key, default):
    override = lob.int_env(env_name, None)
    if override is not None:
        return override
    if os.environ.get("MILVUS_WEBUI_API_URL"):
        return int(webui_get("_cluster/configs")[config_key.lower()].split("[")[0])
    return default


def indexed_corpus_size():
    threshold = configured_threshold(
        "MILVUS_MIN_SEGMENT_NUM_ROWS_TO_ENABLE_INDEX", "indexCoord.segment.minSegmentNumRowsToEnableIndex", 1024
    )
    return max(2048, threshold)


def assert_index_evidence(client, name, field_name, row_count, record_property):
    """Check sealed index readiness over gRPC and inspect index files when a management URL is configured."""
    deadline = time.monotonic() + 120

    def remaining():
        return max(0.1, deadline - time.monotonic())

    inspect_files = bool(os.environ.get("MILVUS_WEBUI_API_URL"))
    if inspect_files:
        desc = client.describe_collection(name, timeout=remaining())
        collection_id = lob.collection_id_from_desc(desc)
        field_id = lob.field_id_from_desc(desc, field_name)
    while True:
        # Sorting may replace the initially sealed segment while its index is being built.
        by_id = {
            s.segment_id: s.num_rows
            for s in client.list_loaded_segments(name, timeout=remaining())
            if s.num_rows > 0 and s.state_name == "Sealed"
        }
        index = client.describe_index(name, field_name, timeout=remaining())
        ready = sum(by_id.values()) == row_count and index["state"] == "Finished" and index["indexed_rows"] == row_count
        observed = {}
        if ready and not inspect_files:
            break
        if ready:
            segments = webui_get("_qn/segments", params={"collection_id": collection_id}, timeout=min(15, remaining()))
            for segment in segments or []:
                if str(segment.get("collection_id")) != collection_id:
                    continue
                for field in segment.get("index_fields", []):
                    if (
                        str(field.get("field_id")) == field_id
                        and str(field.get("is_loaded")).lower() == "true"
                        and int(field.get("build_id", 0)) > 0
                        and int(field.get("index_size", 0)) > 0
                    ):
                        observed[int(segment["segment_id"])] = (field["build_id"], field["index_size"])
        if ready and by_id.keys() <= observed.keys():
            break
        assert time.monotonic() < deadline, (name, field_name, by_id, index, observed)
        time.sleep(min(1, remaining()))
    record_property(name + "_loaded_segments", str(sorted(by_id.items())))
    if inspect_files:
        record_property(name + "_physical_index", str(observed))


def make_lob_documents(threshold):
    if threshold < 7:
        pytest.skip("the T+1 row needs eight bytes for a UTF-8 prefix, token boundaries and A&B")

    def padding(size):
        return ("pad " * (size // 4 + 1))[:size]

    left = (threshold - 5) // 2
    return {
        1: "A&B " + padding(threshold - 5),
        2: padding(left) + " A&B " + padding(threshold - left - 5),
        3: "北 " + padding(threshold - 7) + " A&B",
        4: "noise " + padding(threshold - 6),
    }


class TestMilvusClientCharFilterConsumers(TestMilvusClientV2Base):
    """Each test owns the field functions and indexes needed by its consumer."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_bm25_frequency_and_offset_modes(self):
        """
        target: preserve BM25 term frequency and score independently of offset mode (C12/C22)
        method: compare A&B and aandb searches on two mapped corpora, a normalized reference and the raw corpus
        expected: tf=2 wins top-1, with a 10/7 score ratio to tf=1 in both mapped tables
        """
        client = self._client()
        raw = {1: "A&B pad pad", 2: "aandb aandb pad", **dict.fromkeys(range(3, 7), "quiet slow fox")}
        normalized = {**raw, 1: "aandb pad pad"}
        scores = {}
        for variant, params, texts in [
            ("plain", PLAIN, raw),
            ("reference", PLAIN, normalized),
            ("source_span", MAPPED, raw),
            ("boundary", gen_mapping_analyzer_params(["&=>and"], filters=["lowercase"], mode="boundary"), raw),
        ]:
            name = create_text_table(self, client, params)
            insert_docs(client, name, texts)
            flush_and_wait_for_sealed_segments(client, name)
            ids = [2] if variant == "plain" else [2, 1]
            hits = assert_sparse_hits(
                client,
                name,
                "aandb",
                {i: texts[i] for i in ids},
                limit=len(texts),
                search_params={"metric_type": "BM25", "params": {"drop_ratio_search": 0}},
            )
            assert [h["id"] for h in hits] == ids
            scores[variant] = {h["id"]: h["distance"] for h in hits}
            assert all(math.isfinite(score) and score > 0 for score in scores[variant].values())
            assert_sparse_hits(client, name, "aandb", {2: texts[2]}, limit=1)
            raw_ids = {"plain": [1], "reference": []}.get(variant, ids)
            raw_scores = assert_bm25_search_results(
                client, name, "A&B", {i: texts[i] for i in raw_ids}, limit=len(texts)
            )
            if variant in ("source_span", "boundary"):
                assert raw_scores == pytest.approx(scores[variant], rel=1e-4, abs=1e-6)
        for mode in ("source_span", "boundary"):
            assert scores[mode] == pytest.approx(scores["reference"], rel=1e-4, abs=1e-6)
            assert scores[mode][2] / scores[mode][1] == pytest.approx(10 / 7, rel=1e-4)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mode", ["source_span", "boundary"])
    def test_lexical_highlight_original_utf8(self, mode):
        """
        target: render lexical hits against original UTF-8 text (C13/C22)
        method: search original characters and their expanded terms, and vary preceding fragment context
        expected: highlights preserve the original emoji, coalesce overlapping spans and retain a zero-width tag
        """
        client = self._client()
        params = gen_mapping_analyzer_params(["😀=>smile", "中=>x y"], mode=mode)
        name = create_text_table(self, client, params)
        texts = {1: "ok 😀 end", 2: "p 中 q"}
        insert_docs(client, name, texts)
        flush_and_wait_for_sealed_segments(client, name)
        x_fragment = "p <em></em>中 q" if mode == "boundary" else "p <em>中</em> q"
        for query, pk, offset, fragment in [
            ("😀", 1, 3, "ok <em>😀</em> end"),
            ("smile", 1, 3, "ok <em>😀</em> end"),
            ("smile", 1, 0, "<em>😀</em> end"),
            ("x", 2, 3, x_fragment),
            ("y", 2, 3, "p <em>中</em> q"),
            ("中", 2, 3, "p <em>中</em> q"),
            ("x y", 2, 3, "p <em>中</em> q"),
        ]:
            highlighter = LexicalHighlighter(
                highlight_search_text=True,
                pre_tags=["<em>"],
                post_tags=["</em>"],
                fragment_offset=offset,
                fragment_size=100,
                num_of_fragments=1,
            )
            hits = assert_sparse_hits(client, name, query, {pk: texts[pk]}, limit=2, highlighter=highlighter)
            assert hits[0]["highlight"]["text"]["fragments"] == [fragment], (mode, query, offset)
        tokens = client.run_analyzer(texts[2], collection_name=name, field_name="text", with_detail=True).tokens
        assert [(t["start_offset"], t["end_offset"]) for t in tokens if t["token"] == "x"] == [
            (2, 2 if mode == "boundary" else 5)
        ]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("level", ["word", "char"])
    def test_minhash_signature_bytes(self, level, record_property):
        """
        target: map words before MinHash word shingling and preserve raw character shingling (C14/C22)
        method: compare signatures for five probe rows, then search both forms using the loaded MINHASH_LSH index
        expected: word search scores both forms at 1. Character search returns its own form first at 1,
                  and character unigrams give abc and cba identical signatures.
        """
        client = self._client()
        raw = {1: "A&B", 2: "aandb", 3: "quiet meadow", 4: "abc", 5: "cba"}
        variants = [("plain", PLAIN, raw), ("mapped", MAPPED, raw)]
        if level == "word":
            variants.append(("reference", PLAIN, {**raw, 1: "aandb"}))
        signatures = {}
        for variant, params, texts in variants:
            name = cf.gen_collection_name_by_testcase_name()
            schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
            schema.add_field("id", DataType.INT64, is_primary=True)
            schema.add_field("text", DataType.VARCHAR, max_length=128, enable_analyzer=True, analyzer_params=params)
            schema.add_field("sig", DataType.BINARY_VECTOR, dim=4096)
            schema.add_function(
                Function(
                    name="minhash",
                    function_type=FunctionType.MINHASH,
                    input_field_names=["text"],
                    output_field_names=["sig"],
                    params={"token_level": level, "num_hashes": 128, "shingle_size": 1, "seed": 1234},
                )
            )
            indexes = client.prepare_index_params()
            indexes.add_index("sig", index_type="MINHASH_LSH", metric_type="MHJACCARD", params={"mh_lsh_band": 8})
            self.create_collection(client, name, schema=schema, num_shards=1, consistency_level="Strong")
            client.create_index(name, indexes)
            client.load_collection(name)
            corpus = texts
            if variant == "mapped":
                # Numeric padding shares no word or character shingles with either query.
                corpus = texts | dict.fromkeys(range(max(texts) + 1, indexed_corpus_size() + 1), "000999777")
            insert_docs(client, name, corpus)
            flush_and_wait_for_sealed_segments(client, name)
            rows = client.get(name, ids=list(texts), output_fields=["id", "sig", "text"])
            assert len(rows) == len(texts) and {r["id"]: r["text"] for r in rows} == texts
            assert all(len(r["sig"]) == 1 and isinstance(r["sig"][0], bytes) for r in rows)
            sigs = {r["id"]: r["sig"][0] for r in rows}
            assert all(len(s) == 512 for s in sigs.values())
            assert (sigs[4] == sigs[5]) == (level == "char"), (level, variant)
            signatures[variant] = sigs
            if variant == "mapped":
                assert client.describe_index(name, "sig")["index_type"] == "MINHASH_LSH"
                assert_index_evidence(client, name, "sig", len(corpus), record_property)
                for query_pk in (1, 2):
                    expected = {1: 1.0, 2: 1.0} if level == "word" else {query_pk: 1.0}
                    result = client.search(
                        name,
                        data=[raw[query_pk]],
                        anns_field="sig",
                        limit=len(expected),
                        ignore_growing=True,
                        search_params={"metric_type": "MHJACCARD", "params": {}},
                    )
                    assert len(result) == 1
                    hits = result[0]
                    assert len(hits) == len(expected)
                    assert {hit["id"]: hit["distance"] for hit in hits} == expected
        assert signatures["plain"][1] != signatures["plain"][2]
        mapped = signatures["mapped"]
        assert mapped[1] != mapped[3]
        if level == "word":
            assert mapped == signatures["reference"] and mapped[1] == mapped[2]
        else:
            assert mapped == signatures["plain"]

    @pytest.mark.parametrize("dtype", [DataType.VARCHAR, DataType.TEXT], ids=["varchar", "text"])
    @pytest.mark.parametrize(
        "index_type",
        [
            pytest.param("SPARSE_INVERTED_INDEX", marks=pytest.mark.tags(CaseLabel.L1)),
            pytest.param("SPARSE_WAND", marks=pytest.mark.tags(CaseLabel.L2)),
            pytest.param("AUTOINDEX", marks=pytest.mark.tags(CaseLabel.L1)),
        ],
    )
    def test_bm25_on_indexed_sealed_segments(self, dtype, index_type, record_property):
        """
        target: search mapped terms on each supported text type and BM25 sparse index (C20)
        method: load index-sized mapped and manually normalized corpora, then query A&B and aandb in each
        expected: both inputs select PKs 1 and 3 with reference scores on mapped tables, while plain A&B selects PK 2
        """
        if dtype == DataType.TEXT:
            require_text_storage()
        client = self._client()
        texts = {1: "A&B", 2: "A B", 3: "aandb", 4: "noise"}
        texts.update(dict.fromkeys(range(5, indexed_corpus_size() + 1), "quiet meadow"))
        normalized = {**texts, 1: "aandb"}
        scores = []
        variants = [(normalized, PLAIN, False), (texts, MAPPED, False)]
        if index_type == "SPARSE_INVERTED_INDEX":
            variants.append((texts, MAPPED, True))
        for data, params, defer in variants:
            name = create_text_table(self, client, params, dtype=dtype, sparse_index=index_type, defer_index=defer)
            insert_docs(client, name, data)
            if defer:
                client.flush(name)
                client.create_index(name, text_indexes(client, sparse_index=index_type))
                client.load_collection(name)
            flush_and_wait_for_sealed_segments(client, name, sealed_pk=3)
            description = client.describe_index(name, "sparse")
            assert description["index_type"] == index_type, description
            if index_type == "AUTOINDEX":
                record_property(name + "_autoindex_description", str(description))
            expected = {i: data[i] for i in (1, 3)}
            assert_index_evidence(client, name, "sparse", len(data), record_property)
            assert_text_query_results(client, name, 'text_match(text,"aandb")', expected, ignore_growing=True)
            normalized_scores = assert_bm25_search_results(client, name, "aandb", expected, ignore_growing=True)
            scores.append(normalized_scores)
            raw_expected = {2: data[2]} if params == PLAIN else expected
            assert_text_query_results(client, name, 'text_match(text,"A&B")', raw_expected, ignore_growing=True)
            raw_scores = assert_bm25_search_results(client, name, "A&B", raw_expected, ignore_growing=True)
            if params == MAPPED:
                assert raw_scores == pytest.approx(normalized_scores, rel=1e-4, abs=1e-6)
        for score in scores[1:]:
            assert score == pytest.approx(scores[0], rel=1e-4, abs=1e-6)

    @pytest.mark.parametrize(
        "scalar_index",
        [
            pytest.param(None, marks=pytest.mark.tags(CaseLabel.L2), id="scan"),
            pytest.param("INVERTED", marks=pytest.mark.tags(CaseLabel.L1), id="inverted"),
            pytest.param("NGRAM", marks=pytest.mark.tags(CaseLabel.L2), id="ngram"),
            pytest.param("BITMAP", marks=pytest.mark.tags(CaseLabel.L2), id="bitmap"),
            pytest.param("TRIE", marks=pytest.mark.tags(CaseLabel.L2), id="trie"),
            pytest.param("AUTOINDEX", marks=pytest.mark.tags(CaseLabel.L2), id="autoindex"),
            pytest.param("STL_SORT", marks=pytest.mark.tags(CaseLabel.L2), id="stl-sort"),
            pytest.param("FMINDEX", marks=pytest.mark.tags(CaseLabel.L2), id="fmindex"),
        ],
    )
    def test_scalar_indexes_preserve_original_text(self, scalar_index, record_property):
        """
        target: compare character-mapped text matches with raw VARCHAR equality and LIKE (C20)
        method: query identical indexed and unindexed data with the raw and rewritten strings
        expected: raw comparisons select distinct original PKs while the mapped term matches both
        """
        client = self._client()
        name = create_text_table(self, client, MAPPED, scalar_index=scalar_index)
        texts = {1: "A&B", 2: "A B", 3: "aandb", 4: "noise"}
        row_count = indexed_corpus_size() if scalar_index else 2048
        texts.update(dict.fromkeys(range(5, row_count + 1), "quiet meadow"))
        insert_docs(client, name, texts)
        flush_and_wait_for_sealed_segments(client, name)
        if scalar_index:
            assert client.describe_index(name, "text")["index_type"] == scalar_index
            assert_index_evidence(client, name, "text", len(texts), record_property)
        for expr, ids in [
            ('text == "A&B"', [1]),
            ('text == "aandb"', [3]),
            ('text like "%A&B%"', [1]),
            ('text like "%aandb%"', [3]),
            ('text_match(text,"A&B")', [1, 3]),
            ('text_match(text,"aandb")', [1, 3]),
        ]:
            assert_text_query_results(client, name, expr, {pk: texts[pk] for pk in ids}, ignore_growing=True)

    @pytest.mark.tags(CaseLabel.L2)
    def test_text_field_rejects_inverted_scalar_index(self):
        """
        target: retain TEXT admission rules for user-created scalar indexes with a mapping (C20)
        method: create a valid TEXT/BM25 schema, then request an explicit INVERTED scalar index
        expected: TEXT rejects this index while its internal text-match index stays usable
        """
        client = self._client()
        name = create_text_table(self, client, MAPPED, dtype=DataType.TEXT)
        indexes = client.prepare_index_params()
        indexes.add_index("text", index_type="INVERTED")
        with pytest.raises(MilvusException, match="TEXT.*index"):
            client.create_index(name, indexes)
        assert "text" not in client.list_indexes(name)
        insert_docs(client, name, {1: "A&B"})
        flush_and_wait_for_sealed_segments(client, name)
        assert_text_query_results(client, name, 'text_match(text,"aandb")', {1: "A&B"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_multi_analyzer_bm25_selects_per_row(self):
        """
        target: select a named mapping independently for each indexed row and each BM25 request (C21)
        method: insert names, aliases and unknown names, then analyze a mixed batch and search with each selector
        expected: mapped and alias return the same four PKs and scores. Omitted and explicit default
                  return only the unnormalized row with the same score.
        """
        client = self._client()
        name = cf.gen_collection_name_by_testcase_name()
        multi = {"by_field": "lang", "analyzers": {"mapped": MAPPED, "default": PLAIN}, "alias": {"m": "mapped"}}
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("lang", DataType.VARCHAR, max_length=16)
        schema.add_field("text", DataType.VARCHAR, max_length=512, enable_analyzer=True, multi_analyzer_params=multi)
        schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_function(
            Function(
                name="bm25", function_type=FunctionType.BM25, input_field_names=["text"], output_field_names=["sparse"]
            )
        )
        self.create_collection(client, name, schema=schema, consistency_level="Strong")
        client.create_index(name, text_indexes(client))
        client.load_collection(name)
        rows = [
            {"id": i, "text": text, "lang": lang}
            for i, (lang, text) in enumerate(
                [("mapped", "A&B"), ("m", "A&B"), ("unknown", "A&B"), ("mapped", "aandb"), ("unknown", "aandb")],
                start=1,
            )
        ]
        assert client.insert(name, rows)["insert_count"] == len(rows)
        flush_and_wait_for_sealed_segments(client, name)
        batch = client.run_analyzer(
            ["A&B"] * 3,
            collection_name=name,
            field_name="text",
            analyzer_names=["mapped", "m", "default"],
            with_detail=True,
        )
        assert [[(t["token"], t["start_offset"], t["end_offset"]) for t in r.tokens] for r in batch] == [
            [("aandb", 0, 3)],
            [("aandb", 0, 3)],
            [("a", 0, 1), ("b", 2, 3)],
        ]
        scores = {}
        for selector in ("mapped", "m", "default", None):
            params = {"metric_type": "BM25"}
            if selector is not None:
                params["analyzer_name"] = selector
            ids = {1, 2, 4, 5} if selector in ("mapped", "m") else {3}
            expected = {row["id"]: row["text"] for row in rows if row["id"] in ids}
            scores[selector] = assert_bm25_search_results(client, name, "A&B", expected, limit=5, search_params=params)
        assert scores["m"] == pytest.approx(scores["mapped"], rel=1e-4, abs=1e-6)
        assert scores[None] == pytest.approx(scores["default"], rel=1e-4, abs=1e-6)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        ("extra", "error"),
        [
            ({"analyzer_params": PLAIN}, "analyzer params should be none"),
            ({"enable_match": True}, "only support for bm25"),
        ],
        ids=["single-and-multi", "multi-and-match"],
    )
    def test_invalid_multi_analyzer_combinations(self, extra, error):
        """
        target: retain multi-analyzer schema restrictions for fields with character mappings (C21)
        method: attempt each conflicting schema and verify the original validation reason
        expected: the server rejects the combination and no collection remains
        """
        client = self._client()
        name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("lang", DataType.VARCHAR, max_length=16)
        schema.add_field(
            "text",
            DataType.VARCHAR,
            max_length=512,
            enable_analyzer=True,
            multi_analyzer_params={"by_field": "lang", "analyzers": {"default": MAPPED}},
            **extra,
        )
        schema.add_field("dense", DataType.FLOAT_VECTOR, dim=2)
        self.tear_down_collection_names.append(name)
        with pytest.raises(MilvusException, match=error):
            client.create_collection(name, schema=schema, consistency_level="Strong")
        assert not client.has_collection(name)

    @pytest.fixture
    def lob_storage_v3(self):
        require_text_storage()

    @pytest.fixture
    def lob_collection(self, lob_storage_v3):
        client = self._client()
        threshold = configured_threshold("MILVUS_TEXT_INLINE_THRESHOLD", "dataNode.text.inlineThreshold", 65536)
        texts = make_lob_documents(threshold)
        assert [len(x.encode()) for x in texts.values()] == [threshold - 1, threshold, threshold + 1, threshold]
        name = create_text_table(self, client, MAPPED, dtype=DataType.TEXT)
        insert_docs(client, name, texts)
        return client, name, texts

    @pytest.mark.tags(CaseLabel.L2)
    def test_lob_payload_and_offsets(self, lob_collection):
        """
        target: retain raw byte offsets and payloads across the TEXT inline/LOB threshold (C16)
        method: read growing, sealed and reloaded data, check text predicates, BM25 and original marker offsets
        expected: exactly three rows match and their offsets span the three original bytes of A&B
        """
        client, name, texts = lob_collection
        wait_for_growing_text_match(client, name, 4, "noise")
        expected = {i: texts[i] for i in (1, 2, 3)}
        for phase in ("growing", "sealed", "reloaded"):
            sealed = phase != "growing"
            if phase == "sealed":
                flush_and_wait_for_sealed_segments(client, name, sealed_pk=4)
            elif phase == "reloaded":
                client.release_collection(name)
                client.load_collection(name)
            assert_text_query_results(client, name, "id >= 0", texts, ignore_growing=sealed)
            assert_text_query_results(client, name, 'text_match(text,"aandb")', expected, ignore_growing=sealed)
            assert_bm25_search_results(client, name, "aandb", expected, ignore_growing=sealed)
            if not sealed:
                assert_text_query_results(client, name, "id >= 0", {}, ignore_growing=True)
        for pk, raw in texts.items():
            tokens = client.run_analyzer(raw, collection_name=name, field_name="text", with_detail=True).tokens
            actual = [(t["start_offset"], t["end_offset"]) for t in tokens if t["token"] == "aandb"]
            start = raw.encode().find(b"A&B")
            assert actual == ([(start, start + 3)] if pk in expected else [])

    @pytest.fixture
    def lob_minio(self, lob_storage_v3, minio_host, minio_bucket):
        if not minio_host.strip():
            pytest.skip("physical LOB inspection disabled by an empty --minio_host")
        try:
            storage = lob.new_minio_client(minio_host)
        except pytest.skip.Exception as exc:
            pytest.fail(f"Cannot initialize the configured MinIO client: {exc}")
        assert storage.bucket_exists(minio_bucket), f"Configured MinIO bucket {minio_bucket!r} does not exist"
        return storage

    @pytest.mark.tags(CaseLabel.L3)
    def test_lob_physical_references(self, lob_minio, lob_collection, minio_bucket):
        """
        target: persist TEXT based on original byte length when mapping increases token length (C16)
        method: inspect Storage V3 references in MinIO on a Parquet deployment or with a compatible Vortex reader
        expected: T-1 is inline, T and T+1 reference existing objects, and every PK returns its exact original bytes
        """
        client, name, texts = lob_collection
        flush_and_wait_for_sealed_segments(client, name, sealed_pk=4)
        segments = client.list_persistent_segments(name)
        seg = lob.select_segment_by_rows(segments, len(texts))
        desc = client.describe_collection(name)
        collection_id = lob.collection_id_from_desc(desc)
        text_field = lob.field_id_from_desc(desc, "text")
        pk_field = lob.field_id_from_desc(desc, "id")
        root_path = os.getenv("MILVUS_MINIO_ROOT_PATH")
        # The Helm chart and the Milvus binary use different default object prefixes.
        roots = [root_path] if root_path is not None else ["file", "files"]
        segment_id = lob.segment_identifier(seg)
        for root in roots:
            base = lob.find_segment_base_path(lob_minio, minio_bucket, root, collection_id, segment_id)
            if base is not None:
                break
        else:
            pytest.fail(
                f"Segment {segment_id} for collection {collection_id} missing from {minio_bucket} under {roots}"
            )
        refs = lob.read_text_reference_rows(
            lob_minio,
            minio_bucket,
            base,
            text_field,
            pk_field,
            texts.keys(),
            text_field_name="text",
            pk_field_name="id",
        )
        assert refs.keys() == texts.keys()
        partition = base.rsplit("/", 1)[0]
        files = lob.list_minio_keys(lob_minio, minio_bucket, lob.object_key(partition, "lobs", text_field, "_data"))
        for pk, raw in texts.items():
            assert lob.classify_text_ref(refs[pk], raw) == ("inline" if pk == 1 else "lob")
            if pk != 1:
                assert lob.lob_object_key(partition, text_field, refs[pk]) in files
        client.release_collection(name)
        client.load_collection(name)
        assert_text_query_results(client, name, "id >= 0", texts, ignore_growing=True)


CHAR_FILTER_UPGRADE_PHASE = os.environ.get("CHAR_FILTER_UPGRADE_PHASE")


@pytest.mark.tags(CaseLabel.L3)
@pytest.mark.skipif(not CHAR_FILTER_UPGRADE_PHASE, reason="requires an externally orchestrated analyzer upgrade phase")
class TestMilvusClientCharFilterUpgrade:
    """Cross-version analyzer checks for an externally upgraded, storage-preserving cluster.

    From tests/python_client, set CHAR_FILTER_UPGRADE_STATE to a new, shared run directory.
    Run the same command once per phase (set PHASE and URI in the CI stage):

        CHAR_FILTER_UPGRADE_PHASE="$PHASE" pytest -n 0 --uri "$URI" \
            milvus_client/test_milvus_client_analyzer.py::TestMilvusClientCharFilterUpgrade

    Use prepare on V0, mixed at each rollout pause, upgraded on V1, rollback on V0 and
    cleanup last. Reuse the state directory and PyMilvus version for these phases.
    At each mixed pause the custom analyzer case inserts fresh PKs in its separate V0 reference
    collection. It records the PKs before each insert. At subsequent phases a batch reported
    as failed may be absent. Visible batches must retain all rows, TEXT_MATCH postings and BM25 hits.
    On V1 it flushes and reloads the reference collection. On both V1 and rollback it
    rechecks the recorded batches on sealed segments.
    The state contains collection IDs, original data, token details and BM25 baselines.
    Each test sends the V1 batch at most once. A retry reconciles the original write
    by PK. Each mixed invocation records its own verdict. Upgraded and rollback retries
    retain the first error per operation in their respective phases.
    Use a fresh prepare run to certify a different build after a failure.
    Pin the images in the deployment job. CHAR_FILTER_EXPECT_COMMIT optionally checks
    the git SHA prefix of the responding Proxy. Record the component images at each
    mixed pause; the inline probe reports only the route exercised by this request.

    The read-only probe-old phase also runs on the legacy 3.0 control. Run probe-new
    under an older supported SDK against V1 with the same state directory. probe-old needs no state file.
    """

    PHASES = {"prepare", "mixed", "upgraded", "rollback", "cleanup", "probe-old", "probe-new"}
    CUSTOM = {"tokenizer": "standard", "filter": ["lowercase"]}
    MAPPING = [{"type": "mapping", "mappings": ["& => and"]}]
    LEGACY_ANALYZERS = {
        "default": None,
        "standard": {"type": "standard"},
        "custom": CUSTOM,
        "stop": {"tokenizer": "standard", "filter": ["lowercase", {"type": "stop", "stop_words": ["noise"]}]},
    }
    HISTORICAL_TEMPLATES = {
        "legacy_mapping": {"type": "standard", "char_filter": MAPPING},
        "legacy_empty": {"type": "standard", "char_filter": []},
        "legacy_mode": {"type": "standard", "char_filter_offset_mode": "boundary"},
    }
    RAW = ["A&B", "A B", "aandb", "noise", "B A", "aandc"]
    PREDICATES = {
        'text_match(text,"aandb")': [3],
        'text_match(text,"A&B")': [1, 2, 5],
        'text_match(text,"a")': [1, 2, 5],
        'phrase_match(text,"a b",0)': [1, 2],
        'text_match_fuzzy(text,"aandx",max_edit_distance=1)': [3, 6],
        'text == "A&B"': [1],
        'text like "A&B%"': [1],
    }

    @pytest.fixture(scope="class")
    def client(self):
        assert CHAR_FILTER_UPGRADE_PHASE in self.PHASES, (
            f"unknown upgrade phase {CHAR_FILTER_UPGRADE_PHASE!r}; choose one of {sorted(self.PHASES)}"
        )
        uri, token = Base._connection_params()
        with closing(
            MilvusClient(uri=uri, token=token or "", dedicated=True, grpc_options={"grpc.enable_retries": 0})
        ) as conn:
            version = conn.get_server_version(detail=True)
            log.info("phase=%s client=%s server=%s", CHAR_FILTER_UPGRADE_PHASE, pymilvus.__version__, version)
            expected_commit = os.environ.get("CHAR_FILTER_EXPECT_COMMIT")
            if expected_commit:
                assert version["git_commit"].startswith(expected_commit), version
            if CHAR_FILTER_UPGRADE_PHASE != "cleanup":
                observed = self.probe(conn, CHAR_FILTER_UPGRADE_PHASE)
                log.info("phase=%s observed analyzer=%s", CHAR_FILTER_UPGRADE_PHASE, observed)
            yield conn

    def probe(self, client, phase):
        assert client.run_analyzer("A&B", analyzer_params=self.CUSTOM).tokens == ["a", "b"]
        try:
            result = client.run_analyzer("A&B", analyzer_params={**self.CUSTOM, "char_filter": self.MAPPING})
        except MilvusException as exc:
            assert "unknown analyzer option key: char_filter" in str(exc), exc
            assert phase in {"prepare", "mixed", "rollback", "probe-old"}, (phase, exc)
            return "legacy"
        assert result.tokens == ["aandb"]
        assert phase in {"mixed", "upgraded", "probe-new"}, phase
        return "mapped"

    @pytest.mark.skipif(CHAR_FILTER_UPGRADE_PHASE == "cleanup", reason="cleanup only")
    def test_char_filter_server_sdk_probe(self, client):
        """
        target: C11, identify the analyzer semantics exposed to the current SDK.
        method: submit identical legacy and mapped inline requests at each endpoint and pause.
        expected: the legacy request succeeds everywhere and the mapped request identifies the routed implementation.
        """
        if CHAR_FILTER_UPGRADE_PHASE == "probe-new":
            reference_sdk = json.loads(self.state_file("custom").read_text())["sdk"]
            assert Version(pymilvus.__version__) < Version(reference_sdk), (pymilvus.__version__, reference_sdk)
        assert client.run_analyzer("A&B", analyzer_params={"type": "standard"}).tokens == ["a", "b"]

    @staticmethod
    def state_file(key):
        directory = os.environ.get("CHAR_FILTER_UPGRADE_STATE")
        assert directory, "set CHAR_FILTER_UPGRADE_STATE to the same run directory in every persistent phase"
        path = Path(directory) / f"{key}.json"
        if CHAR_FILTER_UPGRADE_PHASE == "prepare":
            path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def save_state(path, state):
        pending = path.with_suffix(".tmp")
        pending.write_text(json.dumps(state, ensure_ascii=False, indent=2))
        pending.replace(path)

    def rows(self, offset=0):
        return [{"id": pk + offset, "text": raw} for pk, raw in enumerate(self.RAW, 1)]

    @staticmethod
    def create_collection(client, name, params, owner):
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False, description=owner)
        schema.add_field("id", DataType.INT64, is_primary=True)
        options = {"analyzer_params": params} if params is not None else {}
        schema.add_field("text", DataType.VARCHAR, max_length=512, enable_match=True, enable_analyzer=True, **options)
        schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_function(Function("bm25", FunctionType.BM25, ["text"], ["sparse"]))
        indexes = client.prepare_index_params()
        indexes.add_index("sparse", index_type="SPARSE_INVERTED_INDEX", metric_type="BM25")
        client.create_collection(name, schema=schema, index_params=indexes, consistency_level="Strong", num_shards=1)

    @staticmethod
    def flush_and_reload(client, name):
        client.flush(name, timeout=60)
        client.release_collection(name, timeout=60)
        client.load_collection(name, timeout=60)

    @staticmethod
    def wait_for_sealed_reader(client, name, ids, *, timeout=60, first_retry=None):
        """Wait until every requested PK is visible to sealed-only queries."""
        ids = sorted(ids)
        deadline = time.monotonic() + timeout
        first = True
        last = "no response"
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                rows = client.query(
                    name, filter=f"id in {ids}", output_fields=["id"], ignore_growing=True, timeout=min(8, remaining)
                )
            except MilvusException as exc:
                message = str(exc)
                channel_unavailable = exc.code == 503 and "channel not available" in message
                stale_node = (
                    exc.code == 65535 and "node not match[expectedNodeID=" in message and "][actualNodeID=" in message
                )
                if not (channel_unavailable or stale_node):
                    raise
                last = message
            else:
                returned = sorted(row["id"] for row in rows)
                assert len(set(returned)) == len(returned) and set(returned) <= set(ids), (name, ids, returned)
                if returned == ids:
                    return
                last = f"visible PKs {returned}"
            if first:
                log.info("waiting for sealed PKs %s in %s: %s", ids, name, last)
                if first_retry is not None:
                    first_retry(last)
                first = False
            time.sleep(max(0, min(0.25, deadline - time.monotonic())))
        raise AssertionError(f"{name}: sealed PKs {ids} not ready after {timeout}s; last observation: {last}")

    @staticmethod
    def exact_rows(actual, expected):
        assert len(actual) == len(expected), (actual, expected)
        assert {row["id"]: row["text"] for row in actual} == expected, (actual, expected)

    def expected_rows(self, ids, doubled):
        return {pk + offset: self.RAW[pk - 1] for offset in ((0, 100) if doubled else (0,)) for pk in ids}

    def token_details(self, client, name):
        result = client.run_analyzer(
            self.RAW, collection_name=name, field_name="text", with_hash=True, with_detail=True, timeout=15
        )
        fields = ("token", "start_offset", "end_offset", "position", "position_length", "hash")
        return [[{key: token[key] for key in fields} for token in row.tokens] for row in result]

    def query_snapshot(self, client, name, doubled, sealed):
        predicates = {}
        for expr, ids in {"id >= 0": range(1, 7), **self.PREDICATES}.items():
            rows = client.query(name, filter=expr, output_fields=["id", "text"], ignore_growing=sealed, timeout=15)
            self.exact_rows(rows, self.expected_rows(ids, doubled))
            predicates[expr] = {str(row["id"]): row["text"] for row in rows}
        return predicates

    def score_snapshot(self, client, name, doubled, sealed):
        scores = {}
        for query, ids in [("aandb", [3]), ("A&B", [1, 2, 5])]:
            per_pk = assert_bm25_search_results(
                client,
                name,
                query,
                self.expected_rows(ids, doubled),
                limit=12 if doubled else 6,
                round_decimal=-1,
                ignore_growing=sealed,
                timeout=15,
            )
            scores[query] = {str(pk): score for pk, score in per_pk.items()}
        return scores

    def snapshot(self, client, name, doubled=False):
        return {
            "tokens": self.token_details(client, name),
            "predicates": self.query_snapshot(client, name, doubled, sealed=True),
            "scores": self.score_snapshot(client, name, doubled, sealed=True),
        }

    @staticmethod
    def stored_analyzer(description):
        field = next(field for field in description["fields"] if field["name"] == "text")
        params = field["params"].get("analyzer_params")
        return json.loads(params) if isinstance(params, str) else params

    @staticmethod
    def drop_owned_collection(client, key, table):
        name = table["name"]
        assert name.startswith(f"char_filter_upgrade_{key}_")
        if client.has_collection(name):
            desc = client.describe_collection(name)
            if table["id"] is None:
                # A create RPC can commit before the SDK returns the collection ID.
                assert desc["description"] == table["owner"], f"{name} lacks the original preparation marker"
            else:
                assert desc["collection_id"] == table["id"], (
                    f"{name} has ID {desc['collection_id']}, expected {table['id']}"
                )
            client.drop_collection(name)

    def cleanup_collections(self, client, key, state):
        for entry in ("reference_collection", "mixed_write_collection"):
            if entry in state:
                self.drop_owned_collection(client, key, state[entry])
        self.drop_owned_collection(client, key, state)

    def prepare(self, client, key, params, path):
        assert not path.exists(), f"state already exists: {path}; use a fresh run directory"
        name = cf.gen_unique_str(f"char_filter_upgrade_{key}")
        assert not client.has_collection(name)
        if key in self.HISTORICAL_TEMPLATES:
            assert client.run_analyzer("A&B", analyzer_params=params).tokens == ["a", "b"]
        # Journal the unique name before DDL, then pin the server ID before doing any other work.
        state = {"name": name, "id": None, "owner": cf.gen_unique_str("char_filter_prepare"), "prepared": False}
        self.save_state(path, state)
        try:
            self.create_collection(client, name, params, state["owner"])
            desc = client.describe_collection(name)
            state["id"] = desc["collection_id"]
            assert state["id"]
            self.save_state(path, state)
            stored = self.stored_analyzer(desc)
            assert stored == params, f"V0 persisted analyzer {stored!r}, requested {params!r}"
            state.update(
                stored_params=stored,
                sdk=pymilvus.__version__,
                v0_server=client.get_server_version(detail=True),
                write_status="not_sent",
            )
            client.insert(name, self.rows())
            self.flush_and_reload(client, name)
            self.wait_for_sealed_reader(client, name, [6])
            state["baseline"] = self.snapshot(client, name)
            terms = [[item["token"] for item in row] for row in state["baseline"]["tokens"]]
            assert terms == [
                ["a", "b"],
                ["a", "b"],
                ["aandb"],
                [] if key == "stop" else ["noise"],
                ["b", "a"],
                ["aandc"],
            ]
            # A separate V0 table supplies the BM25 baseline for the changed N and df after the second insert.
            reference = cf.gen_unique_str(f"char_filter_upgrade_{key}_reference")
            assert not client.has_collection(reference)
            table = {"name": reference, "id": None, "owner": state["owner"]}
            state["reference_collection"] = table
            self.save_state(path, state)
            self.create_collection(client, reference, params, table["owner"])
            table["id"] = client.describe_collection(reference)["collection_id"]
            assert table["id"]
            self.save_state(path, state)
            client.insert(reference, self.rows() + self.rows(100))
            self.flush_and_reload(client, reference)
            self.wait_for_sealed_reader(client, reference, [6, 106])
            state["doubled"] = self.snapshot(client, reference, doubled=True)
            if key == "custom":
                table["batches"] = []
                state["mixed_write_collection"] = table
            else:
                self.drop_owned_collection(client, key, table)
            del state["reference_collection"]
            state["prepared"] = True
            self.save_state(path, state)
        except (Exception, KeyboardInterrupt) as exc:
            # A failed preparation has no usable baseline. Keep its first error separate so it can be retried.
            error_path = path.with_suffix(".prepare-error.json")
            if not error_path.exists():
                self.save_state(
                    error_path, {"name": name, "requested_params": params, "error": f"{type(exc).__name__}: {exc}"}
                )
            try:
                self.cleanup_collections(client, key, state)
                path.unlink(missing_ok=True)
            except (MilvusException, RpcError, AssertionError):
                log.exception("Incomplete preparation cleanup for %s; retry cleanup with %s", name, path)
            raise

    @staticmethod
    def insert_once(client, name, rows):
        # Keep the per-row failure indexes in the response and send only one Insert RPC.
        handler = client._get_connection()
        context = client._generate_call_context()
        request = handler._prepare_row_insert_request(name, rows, timeout=15, context=context)
        return handler._stub.Insert(request, timeout=15, metadata=context.to_grpc_metadata())

    def record_insert(self, client, name, rows, record, state, path):
        record["write_status"] = "pending"
        self.save_state(path, state)
        response = self.insert_once(client, name, rows)
        try:
            check_status(response.status)
        except MilvusException:
            if sorted(response.err_index) == list(range(len(rows))):
                record["write_status"] = "reported_error"
                self.save_state(path, state)
            raise
        assert response.insert_cnt == len(rows) and not response.err_index, response
        record["write_status"] = "acknowledged"
        self.save_state(path, state)

    def reconcile_batch(self, client, name, expected, record, state, path):
        """Reconcile a single insert by PK without resending it on a retry."""
        status = record.get("write_status")
        if status is None and "acknowledged" in record:  # Mixed batches from an earlier prepare run.
            status = "acknowledged" if record["acknowledged"] else "pending"
        assert status in {"acknowledged", "observed", "pending", "reported_error", "rejected_before_wal"}, status
        actual = client.get(
            name, ids=list(expected), output_fields=["id", "text"], consistency_level="Strong", timeout=15
        )
        if not actual and status in {"reported_error", "rejected_before_wal"}:
            return False
        if status == "pending":
            assert actual, f"{name}: insert outcome unknown, no ACK and no visible PK for {list(expected)}"
        self.exact_rows(actual, expected)
        if status not in {"acknowledged", "observed"}:
            record["write_status"] = "observed"
            self.save_state(path, state)
        return True

    def insert_mixed_batch(self, client, state, path):
        table = state["mixed_write_collection"]
        name = table["name"]
        rows = self.rows(time.time_ns())
        pks = [row["id"] for row in rows]
        assert_text_query_results(client, name, f"id in {pks}", {}, timeout=15)
        batch = {"pks": pks}
        table["batches"].append(batch)
        self.record_insert(client, name, rows, batch, state, path)

    def check_mixed_batches(self, client, state, path, *, sealed):
        table = state["mixed_write_collection"]
        baseline = state["baseline"]["predicates"]
        for batch in table["batches"]:
            pks = batch["pks"]
            batch_filter = f"id in {pks}"
            expected_rows = dict(zip(pks, self.RAW, strict=True))
            if not self.reconcile_batch(client, table["name"], expected_rows, batch, state, path):
                continue
            if sealed:
                self.wait_for_sealed_reader(client, table["name"], pks)
                assert_text_query_results(
                    client, table["name"], batch_filter, expected_rows, ignore_growing=True, timeout=15
                )
            if not sealed:
                wait_for_text_match(client, table["name"], pks[3], "noise")
            for query in ("A&B", "aandb"):
                expr = f'text_match(text,"{query}")'
                expected = {pks[int(pk) - 1]: raw for pk, raw in baseline[expr].items()}
                assert_text_query_results(
                    client, table["name"], f"({expr}) and {batch_filter}", expected, ignore_growing=sealed, timeout=15
                )
                assert_bm25_search_results(
                    client, table["name"], query, expected, filter=batch_filter, ignore_growing=sealed, timeout=15
                )

    def verify_phase(self, client, state, path, phase, *, probe_writes=False):
        name = state["name"]
        if phase == "mixed":
            errors = {}
            state.setdefault("mixed_pauses", []).append(errors)
            self.save_state(path, state)
        else:
            errors = state.setdefault("compatibility_errors", {}).setdefault(phase, {})

        def check(operation, call):
            try:
                call()
                return True
            except (MilvusException, RpcError, AssertionError) as exc:
                errors.setdefault(operation, f"{type(exc).__name__}: {exc}")
                self.save_state(path, state)
                log.error("phase=%s collection=%s operation=%s failed: %s", phase, name, operation, exc)
                return False

        def verify(label, *, doubled=False, sealed=False):
            reference = state["doubled" if doubled else "baseline"]

            def predicates():
                assert self.query_snapshot(client, name, doubled, sealed) == reference["predicates"]

            def scores():
                actual = self.score_snapshot(client, name, doubled, sealed)
                for query, expected in reference["scores"].items():
                    assert actual[query] == pytest.approx(expected, rel=1e-4, abs=1e-6)

            def tokens():
                assert self.token_details(client, name) == reference["tokens"]

            # Exercise all three entry points even if a historical schema cannot restore its text index.
            query_ok = check(f"{label} query", predicates)
            bm25_ok = check(f"{label} BM25", scores)
            tokens_ok = check(f"{label} field analyzer", tokens)
            return query_ok and bm25_ok and tokens_ok

        def wait(label, doubled=False):
            def remember(sample):
                state.setdefault("first_readiness_retry", {}).setdefault(phase, {}).setdefault(label, sample)
                self.save_state(path, state)

            check(
                f"{label} handoff",
                lambda: self.wait_for_sealed_reader(client, name, [6, 106] if doubled else [6], first_retry=remember),
            )

        def reload(label):
            check(f"{label} release", lambda: client.release_collection(name, timeout=15))
            return check(f"{label} load", lambda: client.load_collection(name, timeout=30))

        if phase == "rollback":
            check("rollback load", lambda: client.load_collection(name, timeout=60))
        known_doubled = state["write_status"] in {"acknowledged", "observed"}
        wait("entry", doubled=phase == "rollback" and known_doubled)
        if probe_writes:
            table = state.get("mixed_write_collection", {})

            def reference_ready():
                assert "batches" in table, "prepare a fresh V0 manifest to track mixed-version writes"
                assert client.describe_collection(table["name"])["collection_id"] == table["id"], table["name"]
                client.load_collection(table["name"], timeout=60)
                self.wait_for_sealed_reader(client, table["name"], [6, 106])

            if check("mixed write collection", reference_ready):
                if phase == "mixed":
                    check("mixed write", lambda: self.insert_mixed_batch(client, state, path))
                check("mixed batch readback", lambda: self.check_mixed_batches(client, state, path, sealed=False))
                if table["batches"] and phase in {"upgraded", "rollback"}:
                    if phase == "rollback" or check(
                        "mixed batch flush and reload", lambda: self.flush_and_reload(client, table["name"])
                    ):
                        check(
                            "sealed mixed batch readback",
                            lambda: self.check_mixed_batches(client, state, path, sealed=True),
                        )
        second_batch = self.rows(100)
        expected = {row["id"]: row["text"] for row in second_batch}

        def reconcile():
            return self.reconcile_batch(client, name, expected, state, state, path)

        if phase == "mixed":
            verify("original", sealed=True)
        elif phase == "upgraded":
            if state["write_status"] == "not_sent":
                verify("original", sealed=True)
                if reload("original cold"):
                    wait("original cold")
                    verify("original cold", sealed=True)
                check("insert", lambda: self.record_insert(client, name, second_batch, state, state, path))

            def check_new_rows():
                assert reconcile(), "the server reported failure for every V1 row and none is visible"

            reconciled = check("second batch PKs", check_new_rows)
            committed = state["write_status"] in {"acknowledged", "observed"}
            # Earlier manifests only recorded successful growing checks and the first error per operation.
            growing_checked = (
                state.get("growing_window_closed")
                or state.get("mixed_verified")
                or any(op == "growing visibility" or op.startswith("mixed old/new ") for op in errors)
            )
            if reconciled and committed and not growing_checked:
                check("growing visibility", lambda: wait_for_growing_text_match(client, name, 106, "aandc"))
                verify("mixed old/new", doubled=True)
            # A retry can only verify sealed data after this point, even if the first checks failed.
            state["growing_window_closed"] = True
            self.save_state(path, state)
            check("flush", lambda: client.flush(name, timeout=60))
            if reload("final cold") and reconciled:
                wait("final sealed", doubled=committed)
                verify("final sealed", doubled=committed, sealed=True)
        elif phase == "rollback":
            if check("second batch PKs", reconcile):
                committed = state["write_status"] in {"acknowledged", "observed"}
                if committed and not known_doubled:
                    wait("rollback reconciled", doubled=True)
                verify("rollback sealed", doubled=committed, sealed=True)
        assert not errors, f"{name} first failures during {phase}: {errors}"

    @pytest.mark.skipif(CHAR_FILTER_UPGRADE_PHASE in {"probe-old", "probe-new"}, reason="capability probe only")
    @pytest.mark.parametrize("key", [*LEGACY_ANALYZERS, *HISTORICAL_TEMPLATES])
    def test_char_filter_persisted_analyzer(self, client, key):
        """
        target: C01/C04/C11, preserve legacy expression results and expose rejected historical templates.
        method: save V0 snapshots, reopen the same collection on each phase, and write a second batch on V1.
        expected: normal schemas retain full tokens, hashes, raw values, hits and BM25 scores through rollback.
        """
        path = self.state_file(key)
        params = (self.LEGACY_ANALYZERS | self.HISTORICAL_TEMPLATES)[key]
        if CHAR_FILTER_UPGRADE_PHASE == "prepare":
            return self.prepare(client, key, params, path)
        if CHAR_FILTER_UPGRADE_PHASE == "cleanup" and not path.exists():
            return
        state = json.loads(path.read_text())
        name = state["name"]
        assert name.startswith(f"char_filter_upgrade_{key}_")
        if CHAR_FILTER_UPGRADE_PHASE == "cleanup":
            self.cleanup_collections(client, key, state)
            state["cleaned"] = True
            self.save_state(path, state)
            return
        assert state.get("prepared", True), f"incomplete V0 preparation: {path}"
        description = client.describe_collection(name)
        assert description["collection_id"] == state["id"], name
        assert self.stored_analyzer(description) == state["stored_params"], name
        assert pymilvus.__version__ == state["sdk"], "use probe-new separately for cross-SDK coverage"
        assert "write_status" in state and "doubled" in state, "upgrade manifest requires a fresh V0 prepare run"
        self.verify_phase(client, state, path, CHAR_FILTER_UPGRADE_PHASE, probe_writes=key == "custom")
