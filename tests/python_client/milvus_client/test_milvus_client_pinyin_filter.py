import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import DataType, Function, FunctionType

TEXT_ROWS = [
    {"id": 0, "text": "中文测试", "vector": [0.0, 0.0]},
    {"id": 1, "text": "向量数据库", "vector": [1.0, 0.0]},
    {"id": 2, "text": "机器学习", "vector": [0.0, 1.0]},
]

PINYIN_OUTPUT_MODES = [
    pytest.param(
        {
            "keep_original": True,
            "keep_full_pinyin": True,
            "keep_joined_full_pinyin": False,
            "keep_separate_first_letter": False,
        },
        "zhong",
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
        id="first-letters",
    ),
]


def pinyin_analyzer(options):
    return {
        "tokenizer": "jieba",
        "filter": [{"type": "pinyin", **options}],
    }


class TestMilvusClientPinyinFilterIndependent(TestMilvusClientV2Base):
    """Independent Pinyin filter cases with per-test analyzer configuration."""

    def _create_text_match_collection(self, client, analyzer_params):
        collection_name = cf.gen_collection_name_by_testcase_name()
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

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(field_name="vector", index_type="FLAT", metric_type="L2", params={})
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
        )
        self.insert(
            client,
            collection_name,
            data=TEXT_ROWS,
        )
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return collection_name

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("options,pinyin_query", PINYIN_OUTPUT_MODES)
    def test_pinyin_filter_text_match_output_modes(self, options, pinyin_query):
        """
        target: verify Pinyin output modes through collection indexing and text_match
        method: insert Chinese rows, then query by configured Pinyin form and original Chinese text
        expected: both query forms return only the source Chinese row
        """
        client = self._client()
        collection_name = self._create_text_match_collection(client, pinyin_analyzer(options))

        pinyin_rows, _ = self.query(
            client,
            collection_name,
            filter=f'text_match(text, "{pinyin_query}")',
            output_fields=["id", "text"],
        )
        assert {row["id"] for row in pinyin_rows} == {0}

        original_rows, _ = self.query(
            client,
            collection_name,
            filter='text_match(text, "中文")',
            output_fields=["id", "text"],
        )
        assert {row["id"] for row in original_rows} == {0}

    @pytest.mark.tags(CaseLabel.L1)
    def test_pinyin_filter_bm25_search_joined_and_original(self):
        """
        target: verify joined Pinyin participates in BM25 indexing and search
        method: build a BM25 collection and search the same Chinese row by joined Pinyin and original text
        expected: both query forms rank the source Chinese row first
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
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
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            index_params=index_params,
        )
        bm25_rows = [{"id": row["id"], "text": row["text"]} for row in TEXT_ROWS]
        self.insert(
            client,
            collection_name,
            data=bm25_rows,
        )
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        for query_text in ["zhongwen", "中文"]:
            results, _ = self.search(
                client,
                collection_name,
                data=[query_text],
                anns_field="sparse",
                search_params={"metric_type": "BM25", "params": {}},
                limit=len(bm25_rows),
                output_fields=["id", "text"],
            )
            assert results[0], f"expected BM25 results for query {query_text!r}"
            assert results[0][0]["id"] == 0
            assert results[0][0]["entity"]["text"] == "中文测试"
