from pymilvus import DataType
from common.common_type import CaseLabel, CheckTasks
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_v2_base import TestMilvusClientV2Base
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


class TestSearchTextMatchIndependent(TestMilvusClientV2Base):
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
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(
            "word",
            DataType.VARCHAR,
            max_length=65535,
            enable_analyzer=True,
            enable_match=True,
            is_partition_key=enable_partition_key,
            analyzer_params=analyzer_params,
        )
        schema.add_field(
            "sentence",
            DataType.VARCHAR,
            max_length=65535,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=analyzer_params,
        )
        schema.add_field(
            "paragraph",
            DataType.VARCHAR,
            max_length=65535,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=analyzer_params,
        )
        schema.add_field(
            "text",
            DataType.VARCHAR,
            max_length=65535,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=analyzer_params,
        )
        schema.add_field("float32_emb", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("sparse_emb", DataType.SPARSE_FLOAT_VECTOR)
        self.create_collection(client, collection_name, schema=schema)
        desc, _ = self.describe_collection(client, collection_name)
        log.info(f"collection {desc}")
        fake = fake_en
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data_size = 5000
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
            batch = data[i: i + batch_size] if i + batch_size < len(df) else data[i: len(df)]
            self.insert(client, collection_name, data=batch)
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(
            field_name="float32_emb",
            index_type="HNSW",
            metric_type="L2",
            params={"M": 16, "efConstruction": 500},
        )
        idx.add_index(
            field_name="sparse_emb",
            index_type="SPARSE_INVERTED_INDEX",
            metric_type="IP",
        )
        if enable_inverted_index:
            idx.add_index(field_name="word", index_type="INVERTED")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
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
                res_list, _ = self.search(
                    client, collection_name,
                    data=search_data,
                    anns_field=ann_field,
                    search_params={},
                    limit=100,
                    filter=expr,
                    output_fields=["id", field])
                for res in res_list:
                    log.info(f"res len {len(res)} res {res}")
                    assert len(res) > 0
                    for r in res:
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
                res_list, _ = self.search(
                    client, collection_name,
                    data=search_data,
                    anns_field=ann_field,
                    search_params={},
                    limit=100,
                    filter=expr,
                    output_fields=["id", field])
                for res in res_list:
                    log.info(f"res len {len(res)} res {res}")
                    assert len(res) > 0
                    for r in res:
                        assert any([token in r["entity"][field] for token in top_10_tokens])

            # verify Text Match support search by pk
            self.search(
                client, collection_name,
                data=None,
                ids=[1, 2],
                anns_field=ann_field,
                search_params={},
                limit=100,
                filter=expr,
                output_fields=["id", field],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 2, "limit": 100, "enable_milvus_client_api": True})

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
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field(
            "word",
            DataType.VARCHAR,
            max_length=65535,
            enable_analyzer=True,
            enable_match=True,
            is_partition_key=enable_partition_key,
            analyzer_params=analyzer_params,
        )
        schema.add_field(
            "sentence",
            DataType.VARCHAR,
            max_length=65535,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=analyzer_params,
        )
        schema.add_field(
            "paragraph",
            DataType.VARCHAR,
            max_length=65535,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=analyzer_params,
        )
        schema.add_field(
            "text",
            DataType.VARCHAR,
            max_length=65535,
            enable_analyzer=True,
            enable_match=True,
            analyzer_params=analyzer_params,
        )
        schema.add_field("float32_emb", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("sparse_emb", DataType.SPARSE_FLOAT_VECTOR)
        self.create_collection(client, collection_name, schema=schema)
        desc, _ = self.describe_collection(client, collection_name)
        log.info(f"collection {desc}")
        fake = fake_en
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data_size = 5000
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
            batch = data[i: i + batch_size] if i + batch_size < len(df) else data[i: len(df)]
            self.insert(client, collection_name, data=batch)
            self.flush(client, collection_name)
        idx = self.prepare_index_params(client)[0]
        idx.add_index(
            field_name="float32_emb",
            index_type="HNSW",
            metric_type="L2",
            params={"M": 16, "efConstruction": 500},
        )
        idx.add_index(
            field_name="sparse_emb",
            index_type="SPARSE_INVERTED_INDEX",
            metric_type="IP",
        )
        if enable_inverted_index:
            idx.add_index(field_name="word", index_type="INVERTED")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)
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
                res_list, _ = self.search(
                    client, collection_name,
                    data=search_data,
                    anns_field=ann_field,
                    search_params={},
                    limit=100,
                    filter=expr,
                    output_fields=["id", field])
                for res in res_list:
                    log.info(f"res len {len(res)} res {res}")
                    assert len(res) > 0
                    for r in res:
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
                res_list, _ = self.search(
                    client, collection_name,
                    data=search_data,
                    anns_field=ann_field,
                    search_params={},
                    limit=100,
                    filter=expr,
                    output_fields=["id", field])
                for res in res_list:
                    log.info(f"res len {len(res)} res {res}")
                    assert len(res) > 0
                    for r in res:
                        assert any([token in r["entity"][field] for token in top_10_tokens])
