import threading
import time

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import DataType, Function, FunctionType

INDEXED_SEALED_COUNT = 3000
UNINDEXED_SEALED_COUNT = 500
GROWING_COUNT = 500
TOTAL_COUNT = INDEXED_SEALED_COUNT + UNINDEXED_SEALED_COUNT + GROWING_COUNT

INDEXED_TARGET_ID = 0
UNINDEXED_TARGET_ID = INDEXED_SEALED_COUNT
GROWING_TARGET_ID = INDEXED_SEALED_COUNT + UNINDEXED_SEALED_COUNT
TARGET_IDS = {INDEXED_TARGET_ID, UNINDEXED_TARGET_ID, GROWING_TARGET_ID}

BM25_TARGET_TEXTS = {
    INDEXED_TARGET_ID: "中文 向量 数据库",
    UNINDEXED_TARGET_ID: "中文 中文 向量",
    GROWING_TARGET_ID: "中文 中文 中文",
}
BM25_EXPECTED_RANKED_IDS = [GROWING_TARGET_ID, UNINDEXED_TARGET_ID, INDEXED_TARGET_ID]

PINYIN_OUTPUT_MODES = [
    pytest.param(
        {
            "keep_original": True,
            "keep_full_pinyin": True,
            "keep_joined_full_pinyin": False,
            "keep_separate_first_letter": False,
        },
        "zhong",
        ["中文", "zhong", "wen", "测试", "ce", "shi"],
        ["中文", "zhong", "wen"],
        ["zhongwen", "zw"],
        id="full-pinyin",
    ),
    pytest.param(
        {
            "keep_original": True,
            "keep_full_pinyin": False,
            "keep_joined_full_pinyin": True,
            "keep_separate_first_letter": False,
        },
        "zhongwen",
        ["中文", "zhongwen", "测试", "ceshi"],
        ["中文", "zhongwen"],
        ["zhong", "zw"],
        id="joined-pinyin",
    ),
    pytest.param(
        {
            "keep_original": True,
            "keep_full_pinyin": False,
            "keep_joined_full_pinyin": False,
            "keep_separate_first_letter": True,
        },
        "zw",
        ["中文", "zw", "测试", "cs"],
        ["中文", "zw"],
        ["zhong", "zhongwen"],
        id="first-letters",
    ),
]


def pinyin_analyzer(options):
    return {
        "tokenizer": "jieba",
        "filter": [{"type": "pinyin", **options}],
    }


def build_rows(start, count, include_vector, target_texts=None):
    rows = []
    for row_id in range(start, start + count):
        row = {
            "id": row_id,
            "text": (
                (target_texts[row_id] if target_texts is not None else "中文测试")
                if row_id in TARGET_IDS
                else f"向量数据库样本{row_id}"
            ),
        }
        if include_vector:
            row["vector"] = [float(row_id % 2), float((row_id // 2) % 2)]
        rows.append(row)
    return rows


class TestMilvusClientPinyinFilterIndependent(TestMilvusClientV2Base):
    """Independent Pinyin filter cases with per-test analyzer configuration."""

    @staticmethod
    def _assert_exact_ids(rows, expected_ids):
        ids = [row["id"] for row in rows]
        assert len(ids) == len(expected_ids), ids
        assert len(ids) == len(set(ids)), ids
        assert set(ids) == expected_ids, ids

    def _field_analyzer_tokens(self, client, collection_name, text):
        analyzer_result, _ = self.run_analyzer(
            client,
            text,
            analyzer_params=None,
            collection_name=collection_name,
            field_name="text",
        )
        return analyzer_result.tokens

    def _search_text_match_until_ids(
        self,
        client,
        collection_name,
        query_text,
        expected_ids,
        minimum_should_match=None,
        timeout=30,
    ):
        deadline = time.monotonic() + timeout
        rows = []
        match_options = "" if minimum_should_match is None else f", minimum_should_match={minimum_should_match}"
        while time.monotonic() < deadline:
            results, _ = self.search(
                client,
                collection_name,
                data=[[0.0, 0.0]],
                anns_field="vector",
                search_params={"metric_type": "L2", "params": {"nprobe": 64}},
                filter=f'text_match(text, "{query_text}"{match_options})',
                limit=TOTAL_COUNT,
                output_fields=["id", "text"],
            )
            rows = results[0] if results else []
            ids = [row["id"] for row in rows]
            if len(ids) == len(expected_ids) and len(ids) == len(set(ids)) and set(ids) == expected_ids:
                return rows
            threading.Event().wait(1)
        return rows

    def _prepare_mixed_segment_collection(
        self,
        client,
        schema,
        index_params,
        index_name,
        include_vector,
        target_texts=None,
    ):
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            consistency_level="Strong",
        )
        self.alter_collection_properties(
            client,
            collection_name,
            properties={"collection.autocompaction.enabled": "false"},
        )

        indexed_rows = build_rows(0, INDEXED_SEALED_COUNT, include_vector, target_texts)
        self.insert(client, collection_name, data=indexed_rows)
        self.flush(client, collection_name)

        unindexed_rows = build_rows(
            INDEXED_SEALED_COUNT,
            UNINDEXED_SEALED_COUNT,
            include_vector,
            target_texts,
        )
        self.insert(client, collection_name, data=unindexed_rows)
        self.flush(client, collection_name)

        self.create_index(client, collection_name, index_params=index_params)
        assert self.wait_for_index_ready(
            client,
            collection_name,
            index_name=index_name,
            timeout=180,
        )
        self.load_collection(client, collection_name, timeout=180)

        growing_rows = build_rows(
            INDEXED_SEALED_COUNT + UNINDEXED_SEALED_COUNT,
            GROWING_COUNT,
            include_vector,
            target_texts,
        )
        self.insert(client, collection_name, data=growing_rows)
        return collection_name

    def _create_text_match_schema(self, client, analyzer_params):
        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        self.add_field(
            schema,
            "text",
            DataType.VARCHAR,
            max_length=1024,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=analyzer_params,
        )
        self.add_field(schema, "vector", DataType.FLOAT_VECTOR, dim=2)
        return schema

    def _create_text_match_collection(self, client, analyzer_params):
        schema = self._create_text_match_schema(client, analyzer_params)

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 64},
        )
        return self._prepare_mixed_segment_collection(
            client,
            schema,
            index_params,
            index_name="vector",
            include_vector=True,
        )

    def _index_and_load_text_match_collection(self, client, collection_name):
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 64},
        )
        self.create_index(client, collection_name, index_params=index_params)
        assert self.wait_for_index_ready(
            client,
            collection_name,
            index_name="vector",
            timeout=180,
        )
        self.load_collection(client, collection_name, timeout=180)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "options,pinyin_query,expected_tokens,original_query_tokens,disabled_queries",
        PINYIN_OUTPUT_MODES,
    )
    def test_pinyin_filter_text_match_output_modes(
        self,
        options,
        pinyin_query,
        expected_tokens,
        original_query_tokens,
        disabled_queries,
    ):
        """
        target: verify exact Pinyin output modes across indexed, unindexed, and growing search paths
        method: verify analyzer tokens, then vector-search 3000 indexed, 500 unindexed, and 500 growing rows
        expected: enabled tokens match every path while tokens from disabled modes do not match
        """
        client = self._client()
        analyzer_params = pinyin_analyzer(options)

        collection_name = self._create_text_match_collection(client, analyzer_params)
        assert self._field_analyzer_tokens(client, collection_name, "中文测试") == expected_tokens
        assert self._field_analyzer_tokens(client, collection_name, "中文") == original_query_tokens

        pinyin_rows = self._search_text_match_until_ids(
            client,
            collection_name,
            pinyin_query,
            TARGET_IDS,
        )
        self._assert_exact_ids(pinyin_rows, TARGET_IDS)

        original_rows = self._search_text_match_until_ids(
            client,
            collection_name,
            "中文",
            TARGET_IDS,
            minimum_should_match=len(original_query_tokens),
        )
        self._assert_exact_ids(original_rows, TARGET_IDS)

        for disabled_query in disabled_queries:
            disabled_rows = self._search_text_match_until_ids(
                client,
                collection_name,
                disabled_query,
                set(),
            )
            self._assert_exact_ids(disabled_rows, set())

    @pytest.mark.tags(CaseLabel.L1)
    def test_pinyin_filter_analyzer_without_original(self):
        """
        target: verify keep_original=false removes the original Chinese tokens
        method: create a field with the joined-Pinyin analyzer and run the analyzer through that field
        expected: the field configuration emits only joined Pinyin tokens
        """
        client = self._client()
        analyzer_params = pinyin_analyzer(
            {
                "keep_original": False,
                "keep_full_pinyin": False,
                "keep_joined_full_pinyin": True,
                "keep_separate_first_letter": False,
            }
        )

        schema = self._create_text_match_schema(client, analyzer_params)
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            consistency_level="Strong",
        )
        self._index_and_load_text_match_collection(client, collection_name)
        assert self._field_analyzer_tokens(client, collection_name, "中文测试") == ["zhongwen", "ceshi"]

    @pytest.mark.tags(CaseLabel.L1)
    def test_pinyin_filter_bm25_search_joined_and_original(self):
        """
        target: verify joined Pinyin BM25 across indexed, unindexed, and growing segments
        method: build 3000 indexed, 500 unindexed, and 500 growing rows, then search Pinyin and Chinese
        expected: both query forms return the target row from every segment state
        """
        client = self._client()
        analyzer_params = pinyin_analyzer(
            {
                "keep_original": True,
                "keep_full_pinyin": False,
                "keep_joined_full_pinyin": True,
                "keep_separate_first_letter": False,
            }
        )

        schema = self.create_schema(client, auto_id=False, enable_dynamic_field=False)[0]
        self.add_field(schema, "id", DataType.INT64, is_primary=True)
        self.add_field(
            schema,
            "text",
            DataType.VARCHAR,
            max_length=1024,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=analyzer_params,
        )
        self.add_field(schema, "sparse", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_function(
            Function(
                name="text_bm25",
                function_type=FunctionType.BM25,
                input_field_names=["text"],
                output_field_names=["sparse"],
                params={},
            )
        )

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name="sparse",
            index_type="SPARSE_INVERTED_INDEX",
            metric_type="BM25",
            params={},
        )
        collection_name = self._prepare_mixed_segment_collection(
            client,
            schema,
            index_params,
            index_name="sparse",
            include_vector=False,
            target_texts=BM25_TARGET_TEXTS,
        )

        for query_text in ["zhongwen", "中文"]:
            results, _ = self.search(
                client,
                collection_name,
                data=[query_text],
                anns_field="sparse",
                search_params={"metric_type": "BM25", "params": {}},
                limit=len(BM25_EXPECTED_RANKED_IDS),
                output_fields=["id", "text"],
            )
            hits = results[0]
            assert [hit["id"] for hit in hits] == BM25_EXPECTED_RANKED_IDS
            distances = [hit["distance"] for hit in hits]
            assert all(left > right for left, right in zip(distances, distances[1:])), distances
            assert [hit["entity"]["text"] for hit in hits] == [
                BM25_TARGET_TEXTS[row_id] for row_id in BM25_EXPECTED_RANKED_IDS
            ]
