import time

import pytest
from base.testbase import TestBase
from utils.constant import CaseLabel
from utils.utils import gen_collection_name

INDEXED_SEALED_COUNT = 3000
UNINDEXED_SEALED_COUNT = 500
GROWING_COUNT = 500
TOTAL_COUNT = INDEXED_SEALED_COUNT + UNINDEXED_SEALED_COUNT + GROWING_COUNT

TARGET_IDS = {
    0,
    INDEXED_SEALED_COUNT,
    INDEXED_SEALED_COUNT + UNINDEXED_SEALED_COUNT,
}


def pinyin_analyzer_params(keep_original):
    return {
        "tokenizer": "jieba",
        "filter": [
            {
                "type": "pinyin",
                "keep_original": keep_original,
                "keep_full_pinyin": False,
                "keep_joined_full_pinyin": True,
                "keep_separate_first_letter": False,
            }
        ],
    }


def pinyin_collection_payload(collection_name, analyzer_params):
    return {
        "collectionName": collection_name,
        "schema": {
            "autoId": False,
            "enableDynamicField": False,
            "fields": [
                {
                    "fieldName": "id",
                    "dataType": "Int64",
                    "isPrimary": True,
                    "elementTypeParams": {},
                },
                {
                    "fieldName": "text",
                    "dataType": "VarChar",
                    "elementTypeParams": {
                        "max_length": "1024",
                        "enable_analyzer": True,
                        "enable_match": True,
                        "analyzer_params": analyzer_params,
                    },
                },
                {
                    "fieldName": "vector",
                    "dataType": "FloatVector",
                    "elementTypeParams": {"dim": "2"},
                },
            ],
        },
    }


def build_rows(start, count):
    rows = []
    for row_id in range(start, start + count):
        rows.append(
            {
                "id": row_id,
                "text": "中文测试" if row_id in TARGET_IDS else f"向量数据库样本{row_id}",
                "vector": [float(row_id % 2), float((row_id // 2) % 2)],
            }
        )
    return rows


@pytest.mark.tags(CaseLabel.L0)
class TestPinyinFilter(TestBase):
    def _field_analyzer_tokens(self, collection_name, text):
        rsp = self.collection_client.post(
            f"{self.endpoint}/v2/vectordb/common/run_analyzer",
            headers=self.collection_client.update_headers(),
            data={
                "text": [text],
                "collectionName": collection_name,
                "fieldName": "text",
            },
        ).json()
        assert rsp["code"] == 0, rsp
        results = rsp["data"]["results"]
        assert len(results) == 1, results
        return [token["token"] for token in results[0]["tokens"]]

    def _index_and_load_collection(self, collection_name):
        rsp = self.index_client.index_create(
            {
                "collectionName": collection_name,
                "indexParams": [
                    {
                        "fieldName": "vector",
                        "indexName": "vector",
                        "indexType": "IVF_FLAT",
                        "metricType": "L2",
                        "params": {"nlist": 64},
                    }
                ],
            }
        )
        assert rsp["code"] == 0, rsp

        rsp = self.collection_client.collection_load(collection_name=collection_name)
        assert rsp["code"] == 0, rsp
        self.collection_client.wait_load_completed(collection_name, timeout=180)

    def _flush_with_rate_limit_retry(self, collection_name, timeout=30):
        deadline = time.monotonic() + timeout
        rsp = {}
        while time.monotonic() < deadline:
            rsp = self.collection_client.flush(collection_name)
            if rsp["code"] == 0:
                return rsp
            assert rsp["code"] == 1807, rsp
            time.sleep(2)
        return rsp

    def _insert_and_flush(self, collection_name, start, count):
        rsp = self.vector_client.vector_insert(
            {
                "collectionName": collection_name,
                "data": build_rows(start, count),
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == count, rsp

        rsp = self._flush_with_rate_limit_retry(collection_name)
        assert rsp["code"] == 0, rsp

    @staticmethod
    def _assert_exact_ids(rows, expected_ids):
        ids = [int(row["id"]) for row in rows]
        assert len(ids) == len(expected_ids), ids
        assert len(ids) == len(set(ids)), ids
        assert set(ids) == expected_ids, ids

    def _search_until_ids(
        self,
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
            rsp = self.vector_client.vector_search(
                {
                    "collectionName": collection_name,
                    "data": [[0.0, 0.0]],
                    "annsField": "vector",
                    "filter": f'text_match(text, "{query_text}"{match_options})',
                    "outputFields": ["id", "text"],
                    "limit": TOTAL_COUNT,
                    "searchParams": {"metricType": "L2", "params": {"nprobe": 64}},
                },
                timeout=0,
            )
            assert rsp["code"] == 0, rsp
            rows = rsp["data"]
            ids = [int(row["id"]) for row in rows]
            if len(ids) == len(expected_ids) and len(ids) == len(set(ids)) and set(ids) == expected_ids:
                return rows
            time.sleep(1)
        return rows

    def test_pinyin_filter_text_match_across_data_paths(self):
        """
        target: verify exact Pinyin tokens and filtered vector search through RESTful v2
        method: read both field analyzer modes, then search 3000 indexed, 500 unindexed, and 500 growing rows
        expected: exact field tokens preserve the flags, enabled tokens match, and disabled tokens do not
        """
        name = gen_collection_name()
        self.name = name
        analyzer_params = pinyin_analyzer_params(keep_original=True)
        rsp = self.collection_client.collection_create(pinyin_collection_payload(name, analyzer_params))
        assert rsp["code"] == 0, rsp

        without_original_name = gen_collection_name()
        rsp = self.collection_client.collection_create(
            pinyin_collection_payload(
                without_original_name,
                pinyin_analyzer_params(keep_original=False),
            )
        )
        assert rsp["code"] == 0, rsp
        self._index_and_load_collection(without_original_name)
        assert self._field_analyzer_tokens(without_original_name, "中文测试") == ["zhongwen", "ceshi"]

        rsp = self.collection_client.alter_collection_properties(
            name,
            {"collection.autocompaction.enabled": "false"},
        )
        assert rsp["code"] == 0, rsp

        self._insert_and_flush(name, 0, INDEXED_SEALED_COUNT)
        self._insert_and_flush(name, INDEXED_SEALED_COUNT, UNINDEXED_SEALED_COUNT)

        self._index_and_load_collection(name)
        assert self._field_analyzer_tokens(name, "中文测试") == ["中文", "zhongwen", "测试", "ceshi"]
        original_query_tokens = self._field_analyzer_tokens(name, "中文")
        assert original_query_tokens == ["中文", "zhongwen"]

        rsp = self.vector_client.vector_insert(
            {
                "collectionName": name,
                "data": build_rows(
                    INDEXED_SEALED_COUNT + UNINDEXED_SEALED_COUNT,
                    GROWING_COUNT,
                ),
            }
        )
        assert rsp["code"] == 0, rsp
        assert rsp["data"]["insertCount"] == GROWING_COUNT, rsp

        search_cases = [
            ("zhongwen", None, TARGET_IDS),
            ("中文", len(original_query_tokens), TARGET_IDS),
            ("zhong", None, set()),
            ("zw", None, set()),
        ]
        for query_text, minimum_should_match, expected_ids in search_cases:
            rows = self._search_until_ids(
                name,
                query_text,
                expected_ids,
                minimum_should_match=minimum_should_match,
            )
            self._assert_exact_ids(rows, expected_ids)
            if expected_ids:
                assert all(row["text"] == "中文测试" for row in rows)
