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
    def _assert_exact_target_ids(rows):
        ids = [int(row["id"]) for row in rows]
        assert len(ids) == len(TARGET_IDS), ids
        assert len(ids) == len(set(ids)), ids
        assert set(ids) == TARGET_IDS, ids

    def _search_until_target_ids(self, collection_name, query_text, timeout=30):
        deadline = time.monotonic() + timeout
        rows = []
        while time.monotonic() < deadline:
            rsp = self.vector_client.vector_search(
                {
                    "collectionName": collection_name,
                    "data": [[0.0, 0.0]],
                    "annsField": "vector",
                    "filter": f'text_match(text, "{query_text}")',
                    "outputFields": ["id", "text"],
                    "limit": TOTAL_COUNT,
                    "searchParams": {"metricType": "L2", "params": {"nprobe": 64}},
                },
                timeout=0,
            )
            assert rsp["code"] == 0, rsp
            rows = rsp["data"]
            ids = [int(row["id"]) for row in rows]
            if len(ids) == len(TARGET_IDS) and len(ids) == len(set(ids)) and set(ids) == TARGET_IDS:
                return rows
            time.sleep(1)
        return rows

    def test_pinyin_filter_text_match_across_data_paths(self):
        """
        target: verify filtered Pinyin vector search through RESTful v2 across three data paths
        method: vector-search 3000 indexed, 500 unindexed, and 500 growing rows through RESTful v2
        expected: joined Pinyin and original Chinese both return the target row from every path
        """
        name = gen_collection_name()
        self.name = name
        analyzer_params = {
            "tokenizer": "jieba",
            "filter": [
                {
                    "type": "pinyin",
                    "keep_original": True,
                    "keep_full_pinyin": False,
                    "keep_joined_full_pinyin": True,
                    "keep_separate_first_letter": False,
                }
            ],
        }
        create_payload = {
            "collectionName": name,
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
        rsp = self.collection_client.collection_create(create_payload)
        assert rsp["code"] == 0, rsp

        rsp = self.collection_client.alter_collection_properties(
            name,
            {"collection.autocompaction.enabled": "false"},
        )
        assert rsp["code"] == 0, rsp

        self._insert_and_flush(name, 0, INDEXED_SEALED_COUNT)
        self._insert_and_flush(name, INDEXED_SEALED_COUNT, UNINDEXED_SEALED_COUNT)

        rsp = self.index_client.index_create(
            {
                "collectionName": name,
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

        rsp = self.collection_client.collection_load(collection_name=name)
        assert rsp["code"] == 0, rsp
        self.collection_client.wait_load_completed(name, timeout=180)

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

        for query_text in ["zhongwen", "中文"]:
            rows = self._search_until_target_ids(name, query_text)
            self._assert_exact_target_ids(rows)
            assert all(row["text"] == "中文测试" for row in rows)
