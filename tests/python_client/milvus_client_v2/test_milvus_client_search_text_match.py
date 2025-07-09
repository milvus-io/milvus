from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    Collection
)
from utils.util_pymilvus import *
from common.common_type import CaseLabel, CheckTasks
from common import common_type as ct
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_base import TestcaseBase
import random
import pytest
import pandas as pd
from faker import Faker

Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")

# patch faker to generate text with specific distribution
cf.patch_faker_text(fake_en, cf.en_vocabularies_distribution)
cf.patch_faker_text(fake_zh, cf.zh_vocabularies_distribution)

pd.set_option("expand_frame_repr", False)

prefix = "search_collection"
search_num = 10
max_dim = ct.max_dim
min_dim = ct.min_dim
epsilon = ct.epsilon
hybrid_search_epsilon = 0.01
gracefulTime = ct.gracefulTime
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
max_limit = ct.max_limit
default_search_exp = "int64 >= 0"
default_search_string_exp = "varchar >= \"0\""
default_search_mix_exp = "int64 >= 0 && varchar >= \"0\""
default_invaild_string_exp = "varchar >= 0"
default_json_search_exp = "json_field[\"number\"] >= 0"
perfix_expr = 'varchar like "0%"'
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_int64_field_name = ct.default_int64_field_name
default_float_field_name = ct.default_float_field_name
default_bool_field_name = ct.default_bool_field_name
default_string_field_name = ct.default_string_field_name
default_json_field_name = ct.default_json_field_name
default_index_params = ct.default_index
vectors = [[random.random() for _ in range(default_dim)] for _ in range(default_nq)]
uid = "test_search"
nq = 1
epsilon = 0.001
field_name = default_float_vec_field_name
binary_field_name = default_binary_vec_field_name
search_param = {"nprobe": 1}
entity = gen_entities(1, is_normal=True)
entities = gen_entities(default_nb, is_normal=True)
raw_vectors, binary_entities = gen_binary_entities(default_nb)
default_query, _ = gen_search_vectors_params(field_name, entities, default_top_k, nq)
index_name1 = cf.gen_unique_str("float")
index_name2 = cf.gen_unique_str("varhar")
half_nb = ct.default_nb // 2
max_hybrid_search_req_num = ct.max_hybrid_search_req_num


class TestSearchWithTextMatchFilter(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test query text match
    ******************************************************************
    """
    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("enable_inverted_index", [True, False])
    @pytest.mark.parametrize("tokenizer", ["standard"])
    def test_search_with_text_match_filter_normal_en(
        self, tokenizer, enable_inverted_index, enable_partition_key
    ):
        """
        target: test text match normal
        method: 1. enable text match and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="float32_emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        log.info(f"collection {collection_w.describe()}")
        fake = fake_en
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "float32_emb": [random.random() for _ in range(dim)],
                "sparse_emb": cf.gen_sparse_vectors(1, dim=10000)[0],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "float32_emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "sparse_emb",
            {"index_type": "SPARSE_INVERTED_INDEX", "metric_type": "IP"},
        )
        if enable_inverted_index:
            collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        # analyze the croup
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # search with filter single field for one token
        df_split = cf.split_dataframes(df, text_fields, language=language)
        log.info(f"df_split\n{df_split}")
        for ann_field in ["float32_emb", "sparse_emb"]:
            log.info(f"ann_field {ann_field}")
            if ann_field == "float32_emb":
                search_data = [[random.random() for _ in range(dim)]]
            elif ann_field == "sparse_emb":
                search_data = cf.gen_sparse_vectors(1, dim=10000)
            else:
                search_data = [[random.random() for _ in range(dim)]]
            for field in text_fields:
                token = wf_map[field].most_common()[0][0]
                expr = f"text_match({field}, '{token}')"
                manual_result = df_split[
                    df_split.apply(lambda row: token in row[field], axis=1)
                ]
                log.info(f"expr: {expr}, manual_check_result: {len(manual_result)}")
                res_list, _ = collection_w.search(
                    data=search_data,
                    anns_field=ann_field,
                    param={},
                    limit=100,
                    expr=expr, output_fields=["id", field])
                for res in res_list:
                    log.info(f"res len {len(res)} res {res}")
                    assert len(res) > 0
                    for r in res:
                        r = r.to_dict()
                        assert token in r["entity"][field]

            # search with filter single field for multi-token
            for field in text_fields:
                # match top 10 most common words
                top_10_tokens = []
                for word, count in wf_map[field].most_common(10):
                    top_10_tokens.append(word)
                string_of_top_10_words = " ".join(top_10_tokens)
                expr = f"text_match({field}, '{string_of_top_10_words}')"
                log.info(f"expr {expr}")
                res_list, _ = collection_w.search(
                    data=search_data,
                    anns_field=ann_field,
                    param={},
                    limit=100,
                    expr=expr, output_fields=["id", field])
                for res in res_list:
                    log.info(f"res len {len(res)} res {res}")
                    assert len(res) > 0
                    for r in res:
                        r = r.to_dict()
                        assert any([token in r["entity"][field] for token in top_10_tokens])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("enable_inverted_index", [True, False])
    @pytest.mark.parametrize("tokenizer", ["jieba"])
    @pytest.mark.skip(reason="unstable case")
    def test_search_with_text_match_filter_normal_zh(
        self, tokenizer, enable_inverted_index, enable_partition_key
    ):
        """
        target: test text match normal
        method: 1. enable text match and insert data with varchar
                2. get the most common words and query with text match
                3. verify the result
        expected: text match successfully and result is correct
        """
        analyzer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                is_partition_key=enable_partition_key,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
				enable_match=True,
                analyzer_params=analyzer_params,
            ),
            FieldSchema(name="float32_emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        log.info(f"collection {collection_w.describe()}")
        fake = fake_en
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "float32_emb": [random.random() for _ in range(dim)],
                "sparse_emb": cf.gen_sparse_vectors(1, dim=10000)[0],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i : i + batch_size]
                if i + batch_size < len(df)
                else data[i : len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "float32_emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "sparse_emb",
            {"index_type": "SPARSE_INVERTED_INDEX", "metric_type": "IP"},
        )
        if enable_inverted_index:
            collection_w.create_index("word", {"index_type": "INVERTED"})
        collection_w.load()
        # analyze the croup
        text_fields = ["word", "sentence", "paragraph", "text"]
        wf_map = {}
        for field in text_fields:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        # search with filter single field for one token
        df_split = cf.split_dataframes(df, text_fields, language=language)
        log.info(f"df_split\n{df_split}")
        for ann_field in ["float32_emb", "sparse_emb"]:
            log.info(f"ann_field {ann_field}")
            if ann_field == "float32_emb":
                search_data = [[random.random() for _ in range(dim)]]
            elif ann_field == "sparse_emb":
                search_data = cf.gen_sparse_vectors(1,dim=10000)
            else:
                search_data = [[random.random() for _ in range(dim)]]
            for field in text_fields:
                token = wf_map[field].most_common()[0][0]
                expr = f"text_match({field}, '{token}')"
                manual_result = df_split[
                    df_split.apply(lambda row: token in row[field], axis=1)
                ]
                log.info(f"expr: {expr}, manual_check_result: {len(manual_result)}")
                res_list, _ = collection_w.search(
                    data=search_data,
                    anns_field=ann_field,
                    param={},
                    limit=100,
                    expr=expr, output_fields=["id", field])
                for res in res_list:
                    log.info(f"res len {len(res)} res {res}")
                    assert len(res) > 0
                    for r in res:
                        r = r.to_dict()
                        assert token in r["entity"][field]

            # search with filter single field for multi-token
            for field in text_fields:
                # match top 10 most common words
                top_10_tokens = []
                for word, count in wf_map[field].most_common(10):
                    top_10_tokens.append(word)
                string_of_top_10_words = " ".join(top_10_tokens)
                expr = f"text_match({field}, '{string_of_top_10_words}')"
                log.info(f"expr {expr}")
                res_list, _ = collection_w.search(
                    data=search_data,
                    anns_field=ann_field,
                    param={},
                    limit=100,
                    expr=expr, output_fields=["id", field])
                for res in res_list:
                    log.info(f"res len {len(res)} res {res}")
                    assert len(res) > 0
                    for r in res:
                        r = r.to_dict()
                        assert any([token in r["entity"][field] for token in top_10_tokens])
