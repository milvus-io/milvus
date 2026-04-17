import pandas as pd
import pytest
from faker import Faker
from pymilvus import DataType

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from utils.util_log import test_log as log

Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")

# patch faker to generate text with specific distribution
cf.patch_faker_text(fake_en, cf.en_vocabularies_distribution)
cf.patch_faker_text(fake_zh, cf.zh_vocabularies_distribution)

pd.set_option("expand_frame_repr", False)


class TestSearchTextMatchIndependent(TestMilvusClientV2Base):
    """Independent tests for text match search with tokenized varchar fields.
    Each test creates its own collection because text_match requires specialized schema
    (enable_analyzer, enable_match, analyzer_params) that varies by tokenizer config.

    Verification approach:
    - Build word frequency map from inserted data using cf.analyze_documents
    - Search with text_match filter using most common tokens
    - Manually assert every returned result contains the matched token(s)
    """

    TEXT_FIELDS = ["word", "sentence", "paragraph", "text"]

    def _setup_text_match_collection(self, client, tokenizer, enable_inverted_index, enable_partition_key):
        """Helper to create collection, insert faker data, build index, and return analysis artifacts.

        Returns: (collection_name, df_split, wf_map, dim)
        """
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"
            fake = fake_en

        analyzer_params = {"tokenizer": tokenizer}
        dim = 128
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = self.create_schema(client)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        for field_name in self.TEXT_FIELDS:
            extra = {}
            if field_name == "word" and enable_partition_key:
                extra["is_partition_key"] = True
            schema.add_field(
                field_name,
                DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                enable_match=True,
                analyzer_params=analyzer_params,
                **extra,
            )
        schema.add_field("float32_emb", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("sparse_emb", DataType.SPARSE_FLOAT_VECTOR)
        self.create_collection(client, collection_name, schema=schema)

        # Generate and insert data
        data_size = 5000
        float_vectors = cf.gen_vectors(data_size, dim)
        sparse_vectors = cf.gen_sparse_vectors(data_size, dim=10000)
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "float32_emb": float_vectors[i],
                "sparse_emb": sparse_vectors[i],
            }
            for i in range(data_size)
        ]
        self.insert(client, collection_name, data=data)
        self.flush(client, collection_name)

        # Build indexes
        idx = self.prepare_index_params(client)[0]
        idx.add_index(
            field_name="float32_emb", index_type="HNSW", metric_type="L2", params={"M": 16, "efConstruction": 500}
        )
        idx.add_index(field_name="sparse_emb", index_type="SPARSE_INVERTED_INDEX", metric_type="IP")
        if enable_inverted_index:
            idx.add_index(field_name="word", index_type="INVERTED")
        self.create_index(client, collection_name, index_params=idx)
        self.load_collection(client, collection_name)

        # Analyze corpus for verification
        df = pd.DataFrame(data)
        wf_map = {}
        for field in self.TEXT_FIELDS:
            wf_map[field] = cf.analyze_documents(df[field].tolist(), language=language)
        df_split = cf.split_dataframes(df, self.TEXT_FIELDS, language=language)

        return collection_name, df_split, wf_map, dim

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("enable_inverted_index", [True, False])
    def test_search_with_text_match_filter_normal_en(self, enable_inverted_index, enable_partition_key):
        """
        target: verify text_match filter with standard tokenizer on English text across dense+sparse ANN
        method: 1. create collection with enable_analyzer+enable_match on 4 varchar fields
                2. insert 5000 rows of faker-generated English text
                3. for each ANN field (float32_emb/sparse_emb) and each text field:
                   a. search with single most-common token → assert token in every result
                   b. search with top-10 tokens → assert any token in every result
                4. verify text_match supports search-by-pk
        expected: all results contain matched token(s); search-by-pk works with text_match filter
        """
        client = self._client()
        collection_name, df_split, wf_map, dim = self._setup_text_match_collection(
            client, "standard", enable_inverted_index, enable_partition_key
        )

        text_fields = self.TEXT_FIELDS
        for ann_field in ["float32_emb", "sparse_emb"]:
            log.info(f"ann_field {ann_field}")
            if ann_field == "float32_emb":
                search_data = cf.gen_vectors(1, dim)
                search_params = {"metric_type": "L2"}
            else:
                search_data = cf.gen_sparse_vectors(1, dim=10000)
                search_params = {"metric_type": "IP"}

            # search with single token per text field
            for field in text_fields:
                token = wf_map[field].most_common()[0][0]
                expr = f"text_match({field}, '{token}')"
                manual_result = df_split[df_split.apply(lambda row, t=token, f=field: t in row[f], axis=1)]
                log.info(f"expr: {expr}, manual_check_result: {len(manual_result)}")
                res_list, _ = self.search(
                    client,
                    collection_name,
                    data=search_data,
                    anns_field=ann_field,
                    search_params=search_params,
                    limit=100,
                    filter=expr,
                    output_fields=["id", field],
                )
                assert len(res_list) >= 1
                assert len(res_list[0]) > 0
                assert len(res_list[0]) <= len(manual_result)
                for res in res_list:
                    log.info(f"res len {len(res)} res {res}")
                    assert len(res) >= 1
                    for r in res:
                        assert token in r["entity"][field]

            # search with multi-token (top 10 most common words) per text field
            for field in text_fields:
                top_10_tokens = [word for word, _ in wf_map[field].most_common(10)]
                string_of_top_10_words = " ".join(top_10_tokens)
                expr = f"text_match({field}, '{string_of_top_10_words}')"
                log.info(f"expr {expr}")
                res_list, _ = self.search(
                    client,
                    collection_name,
                    data=search_data,
                    anns_field=ann_field,
                    search_params=search_params,
                    limit=100,
                    filter=expr,
                    output_fields=["id", field],
                )
                assert len(res_list) >= 1
                assert len(res_list[0]) > 0
                for res in res_list:
                    log.info(f"res len {len(res)} res {res}")
                    assert len(res) >= 1
                    for r in res:
                        assert any(token in r["entity"][field] for token in top_10_tokens)

            # verify text_match supports search-by-pk
            res_list, _ = self.search(
                client,
                collection_name,
                data=None,
                ids=[1, 2],
                anns_field=ann_field,
                search_params=search_params,
                limit=100,
                filter=expr,
                output_fields=["id", field],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 2, "limit": 100, "enable_milvus_client_api": True},
            )
            for res in res_list:
                for r in res:
                    assert any(token in r["entity"][field] for token in top_10_tokens)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_partition_key", [True, False])
    @pytest.mark.parametrize("enable_inverted_index", [True, False])
    @pytest.mark.skip(reason="unstable: jieba tokenization vs Python substring mismatch, see analysis below")
    def test_search_with_text_match_filter_normal_zh(self, enable_inverted_index, enable_partition_key):
        """
        target: verify text_match filter with jieba tokenizer on Chinese text across dense+sparse ANN
        method: 1. create collection with enable_analyzer+enable_match using jieba tokenizer
                2. insert 5000 rows of faker-generated Chinese text
                3. for each ANN field and each text field:
                   a. search with single most-common token → assert token in every result
                   b. search with top-10 tokens → assert any token in every result
                4. verify text_match supports search-by-pk
        expected: all results contain matched token(s); search-by-pk works with text_match filter
        """
        client = self._client()
        collection_name, df_split, wf_map, dim = self._setup_text_match_collection(
            client, "jieba", enable_inverted_index, enable_partition_key
        )

        text_fields = self.TEXT_FIELDS
        for ann_field in ["float32_emb", "sparse_emb"]:
            log.info(f"ann_field {ann_field}")
            if ann_field == "float32_emb":
                search_data = cf.gen_vectors(1, dim)
                search_params = {"metric_type": "L2"}
            else:
                search_data = cf.gen_sparse_vectors(1, dim=10000)
                search_params = {"metric_type": "IP"}

            # search with single token per text field
            for field in text_fields:
                token = wf_map[field].most_common()[0][0]
                expr = f"text_match({field}, '{token}')"
                manual_result = df_split[df_split.apply(lambda row, t=token, f=field: t in row[f], axis=1)]
                log.info(f"expr: {expr}, manual_check_result: {len(manual_result)}")
                res_list, _ = self.search(
                    client,
                    collection_name,
                    data=search_data,
                    anns_field=ann_field,
                    search_params=search_params,
                    limit=100,
                    filter=expr,
                    output_fields=["id", field],
                )
                assert len(res_list) >= 1
                assert len(res_list[0]) > 0
                assert len(res_list[0]) <= len(manual_result)
                for res in res_list:
                    log.info(f"res len {len(res)} res {res}")
                    assert len(res) >= 1
                    for r in res:
                        assert token in r["entity"][field]

            # search with multi-token (top 10 most common words) per text field
            for field in text_fields:
                top_10_tokens = [word for word, _ in wf_map[field].most_common(10)]
                string_of_top_10_words = " ".join(top_10_tokens)
                expr = f"text_match({field}, '{string_of_top_10_words}')"
                log.info(f"expr {expr}")
                res_list, _ = self.search(
                    client,
                    collection_name,
                    data=search_data,
                    anns_field=ann_field,
                    search_params=search_params,
                    limit=100,
                    filter=expr,
                    output_fields=["id", field],
                )
                assert len(res_list) >= 1
                assert len(res_list[0]) > 0
                for res in res_list:
                    log.info(f"res len {len(res)} res {res}")
                    assert len(res) >= 1
                    for r in res:
                        assert any(token in r["entity"][field] for token in top_10_tokens)

            # verify text_match supports search-by-pk
            res_list, _ = self.search(
                client,
                collection_name,
                data=None,
                ids=[1, 2],
                anns_field=ann_field,
                search_params=search_params,
                limit=100,
                filter=expr,
                output_fields=["id", field],
                check_task=CheckTasks.check_search_results,
                check_items={"nq": 2, "limit": 100, "enable_milvus_client_api": True},
            )
            for res in res_list:
                for r in res:
                    assert any(token in r["entity"][field] for token in top_10_tokens)
