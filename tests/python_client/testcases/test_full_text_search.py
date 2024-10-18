from pymilvus import (
    FieldSchema, CollectionSchema, DataType, Function, FunctionType, AnnSearchRequest, WeightedRanker
)
from common.common_type import CaseLabel, CheckTasks
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log
from base.client_base import TestcaseBase

import random
import pytest
import pandas as pd
from faker import Faker
import beir.util
from beir.datasets.data_loader import GenericDataLoader

Faker.seed(19530)
fake_en = Faker("en_US")
fake_zh = Faker("zh_CN")
pd.set_option("expand_frame_repr", False)

prefix = "full_text_search_collection"


@pytest.mark.skip("skip")
class TestCreateCollectionWIthFullTextSearch(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test create collection with full text search
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("tokenizer", ["default", "jieba"])
    def test_create_collection_for_full_text_search(self, tokenizer):
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
            FieldSchema(name="paragraph_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        text_fields = ["text", "paragraph"]
        for field in text_fields:
            bm25_function = Function(
                name=f"{field}_bm25_emb",
                function_type=FunctionType.BM25,
                input_field_names=[field],
                output_field_names=[f"{field}_sparse_emb"],
                params={},
            )
            schema.add_function(bm25_function)
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        res, _ = collection_w.describe()
        assert len(res["functions"]) == len(text_fields)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_create_collection_for_full_text_search_twice_with_same_schema(self, tokenizer):
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
            FieldSchema(name="paragraph_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        text_fields = ["text", "paragraph"]
        for field in text_fields:
            bm25_function = Function(
                name=f"{field}_bm25_emb",
                function_type=FunctionType.BM25,
                input_field_names=[field],
                output_field_names=[f"{field}_sparse_emb"],
                params={},
            )
            schema.add_function(bm25_function)
        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(
            name=c_name, schema=schema
        )
        collection_w = self.init_collection_wrap(
            name=c_name, schema=schema
        )
        res, _ = collection_w.describe()
        assert len(res["functions"]) == len(text_fields)


@pytest.mark.skip("skip")
class TestCreateCollectionWithFullTextSearchNegative(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test create collection with full text search negative
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("tokenizer", ["unsupported"])
    @pytest.mark.xfail(reason="")
    def test_create_collection_for_full_text_search_with_unsupported_tokenizer(self, tokenizer):
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
            FieldSchema(name="paragraph_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        text_fields = ["text", "paragraph"]
        for field in text_fields:
            bm25_function = Function(
                name=f"{field}_bm25_emb",
                function_type=FunctionType.BM25,
                input_field_names=[field],
                output_field_names=[f"{field}_sparse_emb"],
                params={},
            )
            schema.add_function(bm25_function)
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        res, result = collection_w.describe()
        log.info(f"collection describe {res}")
        assert not result, "create collection with unsupported tokenizer should be failed"

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("valid_output", [True, False])
    @pytest.mark.parametrize("valid_input", [True, False])
    def test_create_collection_for_full_text_search_with_invalid_input_output(self, valid_output, valid_input):
        tokenizer_params = {
            "tokenizer": "default",
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
            FieldSchema(name="paragraph_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        if valid_input:
            input_field_names = ["text"]
        else:
            input_field_names = ["invalid_inout"]
        if valid_output:
            output_field_names = ["text_sparse_emb"]
        else:
            output_field_names = ["invalid_output"]

        bm25_function = Function(
            name=f"text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=input_field_names,
            output_field_names=output_field_names,
            params={},
        )
        schema.add_function(bm25_function)
        if (not valid_output) or (not valid_input):
            self.init_collection_wrap(
                name=cf.gen_unique_str(prefix), schema=schema,
                check_task=CheckTasks.err_res,
                check_items={ct.err_code: 1, ct.err_msg: "field not found in collection"}
            )
        else:
            collection_w = self.init_collection_wrap(
                name=cf.gen_unique_str(prefix), schema=schema
            )
            res, result = collection_w.describe()
            log.info(f"collection describe {res}")
            assert result, "create collection with valid input/output should be successful"

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_collection_for_full_text_search_with_field_not_tokenized(self):
        """
        """
        tokenizer_params = {
            "tokenizer": "default",
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=False,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
            FieldSchema(name="paragraph_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        bm25_function = Function(
            name=f"text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={
            },
        )
        schema.add_function(bm25_function)
        check_task = CheckTasks.err_res
        check_items = {ct.err_code: 65535, ct.err_msg: "BM25 function input field must set enable_tokenizer to true"}
        self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema,
            check_task=check_task,
            check_items=check_items
        )


@pytest.mark.skip("skip")
class TestInsertWithFullTextSearch(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("nullable", [False, True])
    @pytest.mark.parametrize("text_lang", ["en", "zh", "de", "hybrid"])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_insert_for_full_text_search_default(self, tokenizer, text_lang, nullable):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                nullable=nullable,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                nullable=nullable,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if text_lang == "zh":
            fake = fake_zh
        elif text_lang == "de":
            fake = Faker("de_DE")
        elif text_lang == "hybrid":
            fake = Faker()

        if nullable:
            data = [
                {
                    "id": i,
                    "word": fake.word().lower(),
                    "sentence": fake.sentence().lower() if random.random() < 0.5 else None,
                    "paragraph": fake.paragraph().lower() if random.random() < 0.5 else None,
                    "text": fake.text().lower(),  # function input should not be None
                    "emb": [random.random() for _ in range(dim)],
                }
                for i in range(data_size)
            ]
        else:
            data = [
                {
                    "id": i,
                    "word": fake.word().lower(),
                    "sentence": fake.sentence().lower(),
                    "paragraph": fake.paragraph().lower(),
                    "text": fake.text().lower(),
                    "emb": [random.random() for _ in range(dim)],
                }
                for i in range(data_size)
            ]
        if text_lang == "hybrid":
            hybrid_data = []
            for i in range(data_size):
                fake = random.choice([fake_en, fake_zh, Faker("de_DE")])
                tmp = {
                    "id": i,
                    "word": fake.word().lower(),
                    "sentence": fake.sentence().lower(),
                    "paragraph": fake.paragraph().lower(),
                    "text": fake.text().lower(),
                    "emb": [random.random() for _ in range(dim)],
                }
                hybrid_data.append(tmp)
            data = hybrid_data + data
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
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": "SPARSE_INVERTED_INDEX",
                "metric_type": "BM25",
                "params": {
                    "drop_ratio_build": 0.3,
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        num_entities = collection_w.num_entities
        res, _ = collection_w.query(
            expr="",
            output_fields=["count(*)"]
        )
        count = res[0]["count(*)"]
        assert len(data) == num_entities
        assert len(data) == count




    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("enable_dynamic_field", [True])
    @pytest.mark.parametrize("nullable", [False])
    @pytest.mark.parametrize("text_lang", ["en"])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_insert_for_full_text_search_enable_dynamic_field(self, tokenizer, text_lang, nullable, enable_dynamic_field):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                nullable=nullable,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                nullable=nullable,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection", enable_dynamic_field=enable_dynamic_field)
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if text_lang == "zh":
            fake = fake_zh
        elif text_lang == "de":
            fake = Faker("de_DE")
        elif text_lang == "hybrid":
            fake = Faker()

        if nullable:
            data = [
                {
                    "id": i,
                    "word": fake.word().lower(),
                    "sentence": fake.sentence().lower() if random.random() < 0.5 else None,
                    "paragraph": fake.paragraph().lower() if random.random() < 0.5 else None,
                    "text": fake.text().lower(),  # function input should not be None
                    "emb": [random.random() for _ in range(dim)],
                    f"dynamic_field_{i}": f"dynamic_value_{i}"
                }
                for i in range(data_size)
            ]
        else:
            data = [
                {
                    "id": i,
                    "word": fake.word().lower(),
                    "sentence": fake.sentence().lower(),
                    "paragraph": fake.paragraph().lower(),
                    "text": fake.text().lower(),
                    "emb": [random.random() for _ in range(dim)],
                    f"dynamic_field_{i}": f"dynamic_value_{i}"
                }
                for i in range(data_size)
            ]
        if text_lang == "hybrid":
            hybrid_data = []
            for i in range(data_size):
                fake = random.choice([fake_en, fake_zh, Faker("de_DE")])
                tmp = {
                    "id": i,
                    "word": fake.word().lower(),
                    "sentence": fake.sentence().lower(),
                    "paragraph": fake.paragraph().lower(),
                    "text": fake.text().lower(),
                    "emb": [random.random() for _ in range(dim)],
                    f"dynamic_field_{i}": f"dynamic_value_{i}"
                }
                hybrid_data.append(tmp)
            data = hybrid_data + data
        # df = pd.DataFrame(data)
        # log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(data), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(data)
                else data[i: len(data)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": "SPARSE_INVERTED_INDEX",
                "metric_type": "BM25",
                "params": {
                    "drop_ratio_build": 0.3,
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        num_entities = collection_w.num_entities
        res, _ = collection_w.query(
            expr="",
            output_fields=["count(*)"]
        )
        count = res[0]["count(*)"]
        assert len(data) == num_entities
        assert len(data) == count




    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("nullable", [True])
    @pytest.mark.parametrize("text_lang", ["en"])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_insert_for_full_text_search_with_dataframe(self, tokenizer, text_lang, nullable):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                nullable=nullable,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                nullable=nullable,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if text_lang == "zh":
            fake = fake_zh
        elif text_lang == "de":
            fake = Faker("de_DE")
        elif text_lang == "hybrid":
            fake = Faker()

        if nullable:
            data = [
                {
                    "id": i,
                    "word": fake.word().lower(),
                    "sentence": fake.sentence().lower() if random.random() < 0.5 else None,
                    "paragraph": fake.paragraph().lower() if random.random() < 0.5 else None,
                    "text": fake.text().lower(),  # function input should not be None
                    "emb": [random.random() for _ in range(dim)],
                }
                for i in range(data_size)
            ]
        else:
            data = [
                {
                    "id": i,
                    "word": fake.word().lower(),
                    "sentence": fake.sentence().lower(),
                    "paragraph": fake.paragraph().lower(),
                    "text": fake.text().lower(),
                    "emb": [random.random() for _ in range(dim)],
                }
                for i in range(data_size)
            ]
        if text_lang == "hybrid":
            hybrid_data = []
            for i in range(data_size):
                fake = random.choice([fake_en, fake_zh, Faker("de_DE")])
                tmp = {
                    "id": i,
                    "word": fake.word().lower(),
                    "sentence": fake.sentence().lower(),
                    "paragraph": fake.paragraph().lower(),
                    "text": fake.text().lower(),
                    "emb": [random.random() for _ in range(dim)],
                }
                hybrid_data.append(tmp)
            data = hybrid_data + data
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(df[i: i + batch_size])
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": "SPARSE_INVERTED_INDEX",
                "metric_type": "BM25",
                "params": {
                    "drop_ratio_build": 0.3,
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        num_entities = collection_w.num_entities
        res, _ = collection_w.query(
            expr="",
            output_fields=["count(*)"]
        )
        count = res[0]["count(*)"]
        assert len(data) == num_entities
        assert len(data) == count




    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("tokenizer", ["default", "jieba"])
    def test_insert_for_full_text_search_with_empty_string(self, tokenizer):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """

        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        log.info(f"collection describe {collection_w.describe()}")
        fake = fake_en
        language = "en"
        if tokenizer == "jieba":
            fake = fake_zh
            language = "zh"
        collection_w.create_index(
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": "SPARSE_INVERTED_INDEX",
                "metric_type": "BM25",
                "params": {
                    "drop_ratio_build": 0.3,
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        data = [
            {
                "id": i,
                "word": fake.word().lower() if random.random() < 0.5 else "",
                "sentence": fake.sentence().lower() if random.random() < 0.5 else "",
                "paragraph": fake.paragraph().lower() if random.random() < 0.5 else "",
                "text": fake.text().lower() if random.random() < 0.5 else "",
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        log.info(f"analyze documents")
        texts = df["text"].to_list()
        word_freq = cf.analyze_documents(texts, language=language)
        tokens = list(word_freq.keys())
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        num_entities = collection_w.num_entities
        # query with count(*)
        res, _ = collection_w.query(
            expr="",
            output_fields=["count(*)"]
        )
        count = res[0]["count(*)"]
        assert len(data) == num_entities
        assert len(data) == count
        # query with expr
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["text_sparse_emb", "text"]
        )
        assert len(res) == len(data)

        # search with text
        nq = 10
        limit = 100
        search_data = [fake.text().lower() + random.choice(tokens) for _ in range(nq)]
        res_list, _ = collection_w.search(
            data=search_data,
            anns_field="text_sparse_emb",
            param={},
            limit=limit,
            output_fields=["id", "text", "text_sparse_emb"])
        assert len(res_list) == nq
        for i in range(nq):
            assert len(res_list[i]) == limit
            search_text = search_data[i]
            log.info(f"res: {res_list[i]}")
            res = res_list[i]
            for j in range(len(res)):
                r = res[j]
                result_text = r.text
                overlap, word_freq_a, word_freq_b = cf.check_token_overlap(search_text, result_text, language=language)
                assert len(
                    overlap) > 0, f"query text: {search_text}, \ntext: {result_text} \n overlap: {overlap} \n word freq a: {word_freq_a} \n word freq b: {word_freq_b}\n result: {r}"


@pytest.mark.skip("skip")
class TestInsertWithFullTextSearchNegative(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("nullable", [True])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_insert_with_full_text_search_with_non_varchar_data(self, tokenizer, nullable):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """

        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                nullable=nullable,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                nullable=nullable,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if tokenizer == "jieba":
            fake = fake_zh

        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower() if random.random() < 0.5 else 1,  # mix some int data
                "emb": [random.random() for _ in range(dim)],
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
                else data[i: len(df)],
                check_task=CheckTasks.err_res,
                check_items={ct.err_code: 1, ct.err_msg: "inconsistent with defined schema"},
            )

@pytest.mark.skip("skip")
class TestUpsertWithFullTextSearch(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("nullable", [False, True])
    @pytest.mark.parametrize("tokenizer", ["default", "jieba"])
    def test_upsert_with_full_text_search(self, tokenizer, nullable):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        if nullable:
            pytest.xfail(reason="nullable field not support yet")

        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                nullable=nullable,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                nullable=nullable,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        if tokenizer == "jieba":
            fake = fake_zh
            language = "zh"

        if nullable:
            data = [
                {
                    "id": i,
                    "sentence": fake.sentence().lower() if random.random() < 0.5 else None,
                    "paragraph": fake.paragraph().lower() if random.random() < 0.5 else None,
                    "text": fake.text().lower(),  # function input should not be None
                    "emb": [random.random() for _ in range(dim)],
                }
                for i in range(data_size)
            ]
        else:
            data = [
                {
                    "id": i,
                    "word": fake.word().lower(),
                    "sentence": fake.sentence().lower(),
                    "paragraph": fake.paragraph().lower(),
                    "text": fake.text().lower(),
                    "emb": [random.random() for _ in range(dim)],
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
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": "SPARSE_INVERTED_INDEX",
                "metric_type": "BM25",
                "params": {
                    "drop_ratio_build": 0.3,
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        num_entities = collection_w.num_entities
        res, _ = collection_w.query(
            expr="",
            output_fields=["count(*)"]
        )
        count = res[0]["count(*)"]
        assert len(data) == num_entities
        assert len(data) == count

        # upsert in half of the data
        upsert_data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size // 2)
        ]
        upsert_data += data[data_size // 2:]
        for i in range(0, len(upsert_data), batch_size):
            collection_w.upsert(
                upsert_data[i: i + batch_size]
                if i + batch_size < len(upsert_data)
                else upsert_data[i: len(upsert_data)]
            )
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["*"]
        )
        upsert_data_map = {}
        for d in upsert_data:
            upsert_data_map[d["id"]] = d
        for r in res:
            _id = r["id"]
            word = r["word"]
            assert word == upsert_data_map[_id]["word"]


@pytest.mark.skip("skip")
class TestUpsertWithFullTextSearchNegative(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("nullable", [False, True])
    @pytest.mark.parametrize("tokenizer", ["default", "jieba"])
    def test_upsert_with_full_text_search(self, tokenizer, nullable):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        if nullable:
            pytest.xfail(reason="nullable field not support yet")

        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                nullable=nullable,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                nullable=nullable,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        language = "en"
        if tokenizer == "jieba":
            fake = fake_zh
            language = "zh"

        if nullable:
            data = [
                {
                    "id": i,
                    "word": fake.word().lower() if random.random() < 0.5 else None,
                    "sentence": fake.sentence().lower() if random.random() < 0.5 else None,
                    "paragraph": fake.paragraph().lower() if random.random() < 0.5 else None,
                    "text": fake.text().lower(),  # function input should not be None
                    "emb": [random.random() for _ in range(dim)],
                }
                for i in range(data_size // 2, data_size)
            ]
        else:
            data = [
                {
                    "id": i,
                    "word": fake.word().lower(),
                    "sentence": fake.sentence().lower(),
                    "paragraph": fake.paragraph().lower(),
                    "text": fake.text().lower(),
                    "emb": [random.random() for _ in range(dim)],
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
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": "SPARSE_INVERTED_INDEX",
                "metric_type": "BM25",
                "params": {
                    "drop_ratio_build": 0.3,
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        num_entities = collection_w.num_entities
        res, _ = collection_w.query(
            expr="",
            output_fields=["count(*)"]
        )
        count = res[0]["count(*)"]
        assert len(data) == num_entities
        assert len(data) == count

        # upsert in half of the data
        upsert_data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size // 2)
        ]
        upsert_data += data[data_size // 2:]
        for i in range(0, len(upsert_data), batch_size):
            collection_w.upsert(
                upsert_data[i: i + batch_size]
                if i + batch_size < len(upsert_data)
                else upsert_data[i: len(upsert_data)]
            )
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["*"]
        )
        upsert_data_map = {}
        for d in upsert_data:
            upsert_data_map[d["id"]] = d
        for r in res:
            _id = r["id"]
            word = r["word"]
            assert word == upsert_data_map[_id]["word"]


@pytest.mark.skip("skip")
class TestDeleteWithFullTextSearch(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_delete_with_full_text_search(self, tokenizer):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if tokenizer == "jieba":
            fake = fake_zh
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
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
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": "SPARSE_INVERTED_INDEX",
                "metric_type": "BM25",
                "params": {
                    "drop_ratio_build": 0.3,
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        num_entities = collection_w.num_entities
        res, _ = collection_w.query(
            expr="",
            output_fields=["count(*)"]
        )
        count = res[0]["count(*)"]
        assert len(data) == num_entities
        assert len(data) == count

        # delete half of the data
        delete_ids = [i for i in range(data_size // 2)]
        collection_w.delete(
            expr=f"id in {delete_ids}"
        )
        res, _ = collection_w.query(
            expr="",
            output_fields=["count(*)"]
        )
        count = res[0]["count(*)"]
        assert count == data_size // 2

        # query with delete expr and get empty result
        res, _ = collection_w.query(
            expr=f"id in {delete_ids}",
            output_fields=["*"]
        )
        assert len(res) == 0

        # search with text has been deleted, not in the result
        search_data = df["text"].to_list()[:data_size // 2]
        res_list, _ = collection_w.search(
            data=search_data,
            anns_field="text_sparse_emb",
            param={},
            limit=100,
            output_fields=["id", "text", "text_sparse_emb"])
        for i in range(len(res_list)):
            query_text = search_data[i]
            result_texts = [r.text for r in res_list[i]]
            assert query_text not in result_texts


@pytest.mark.skip("skip")
class TestDeleteWithFullTextSearchNegative(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_delete_with_full_text_search(self, tokenizer):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if tokenizer == "jieba":
            fake = fake_zh
        data = [
            {
                "id": i,
                "word": fake.word().lower(),
                "sentence": fake.sentence().lower(),
                "paragraph": fake.paragraph().lower(),
                "text": fake.text().lower(),
                "emb": [random.random() for _ in range(dim)],
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
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": "SPARSE_INVERTED_INDEX",
                "metric_type": "BM25",
                "params": {
                    "drop_ratio_build": 0.3,
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        num_entities = collection_w.num_entities
        res, _ = collection_w.query(
            expr="",
            output_fields=["count(*)"]
        )
        count = res[0]["count(*)"]
        assert len(data) == num_entities
        assert len(data) == count

        # delete half of the data
        delete_ids = [i for i in range(data_size // 2)]
        collection_w.delete(
            expr=f"id in {delete_ids}"
        )
        res, _ = collection_w.query(
            expr="",
            output_fields=["count(*)"]
        )
        count = res[0]["count(*)"]
        assert count == data_size // 2

        # query with delete expr and get empty result
        res, _ = collection_w.query(
            expr=f"id in {delete_ids}",
            output_fields=["*"]
        )
        assert len(res) == 0

        # search with text has been deleted, not in the result
        search_data = df["text"].to_list()[:data_size // 2]
        res_list, _ = collection_w.search(
            data=search_data,
            anns_field="text_sparse_emb",
            param={},
            limit=100,
            output_fields=["id", "text", "text_sparse_emb"])
        for i in range(len(res_list)):
            query_text = search_data[i]
            result_texts = [r.text for r in res_list[i]]
            assert query_text not in result_texts


@pytest.mark.skip("skip")
class TestCreateIndexWithFullTextSearch(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test full text search in index creation
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("b", [0.0, 0.5, 1.0])
    @pytest.mark.parametrize("k", [0.0, 1.5, 10 ** 6])
    @pytest.mark.parametrize("index_type", ["SPARSE_INVERTED_INDEX", "SPARSE_WAND"])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_create_full_text_search_index_default(
            self, tokenizer, index_type, k, b
    ):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        empty_percent = 0.0
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                enable_match=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        data = [
            {
                "id": i,
                "word": fake.word().lower() if random.random() >= empty_percent else "",
                "sentence": fake.sentence().lower() if random.random() >= empty_percent else "",
                "paragraph": fake.paragraph().lower() if random.random() >= empty_percent else "",
                "text": fake.text().lower() if random.random() >= empty_percent else "",
                "emb": [random.random() for _ in range(dim)],
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
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": index_type,
                "metric_type": "BM25",
                "params": {
                    "bm25_k1": k,
                    "bm25_b": b,
                }
            }
        )
        # describe index info to verify
        res = collection_w.indexes
        index_info = [r.to_dict() for r in res]
        log.info(f"index info: {index_info}")
        for info in index_info:
            if info["index_name"] == "text_sparse_emb":
                assert info["index_param"]["index_type"] == index_type
                assert info["index_param"]["metric_type"] == "BM25"
                assert info["index_param"]["params"]["bm25_k1"] == k
                assert info["index_param"]["params"]["bm25_b"] == b
                break


@pytest.mark.skip("skip")
class TestCreateIndexWithFullTextSearchNegative(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test full text search in index creation
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("b", [0.5])
    @pytest.mark.parametrize("k", [1.5])
    @pytest.mark.parametrize("index_type", ["HNSW", "INVALID_INDEX_TYPE"])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_create_full_text_search_with_invalid_index_type(
            self, tokenizer, index_type, k, b
    ):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        empty_percent = 0.0
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                enable_match=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        data = [
            {
                "id": i,
                "word": fake.word().lower() if random.random() >= empty_percent else "",
                "sentence": fake.sentence().lower() if random.random() >= empty_percent else "",
                "paragraph": fake.paragraph().lower() if random.random() >= empty_percent else "",
                "text": fake.text().lower() if random.random() >= empty_percent else "",
                "emb": [random.random() for _ in range(dim)],
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
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        error = {"err_code": 1100, "err_msg": "invalid"}
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": index_type,
                "metric_type": "BM25",
                "params": {
                    "bm25_k1": k,
                    "bm25_b": b,
                }
            },
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("b", [0.5])
    @pytest.mark.parametrize("k", [1.5])
    @pytest.mark.parametrize("index_type", ["SPARSE_INVERTED_INDEX"])
    @pytest.mark.parametrize("metric_type", ["COSINE", "L2", "IP"])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_create_full_text_search_index_with_invalid_metric_type(
            self, tokenizer, index_type, metric_type, k, b
    ):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        empty_percent = 0.0
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                enable_match=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        data = [
            {
                "id": i,
                "word": fake.word().lower() if random.random() >= empty_percent else "",
                "sentence": fake.sentence().lower() if random.random() >= empty_percent else "",
                "paragraph": fake.paragraph().lower() if random.random() >= empty_percent else "",
                "text": fake.text().lower() if random.random() >= empty_percent else "",
                "emb": [random.random() for _ in range(dim)],
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
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        error = {ct.err_code: 65535, ct.err_msg: "index metric type of BM25 function output field must be BM25"}
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": index_type,
                "metric_type": metric_type,
                "params": {
                    "bm25_k1": k,
                    "bm25_b": b,
                }
            },
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("b", [0.5])
    @pytest.mark.parametrize("k", [1.5])
    @pytest.mark.parametrize("index_type", ["SPARSE_INVERTED_INDEX"])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_create_index_using_BM25_metric_type_for_non_bm25_output_field(
            self, tokenizer, index_type, k, b
    ):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        empty_percent = 0.0
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                enable_match=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        data = [
            {
                "id": i,
                "word": fake.word().lower() if random.random() >= empty_percent else "",
                "sentence": fake.sentence().lower() if random.random() >= empty_percent else "",
                "paragraph": fake.paragraph().lower() if random.random() >= empty_percent else "",
                "text": fake.text().lower() if random.random() >= empty_percent else "",
                "emb": [random.random() for _ in range(dim)],
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

        error = {ct.err_code: 1100, ct.err_msg: "float vector index does not support metric type: BM25"}
        collection_w.create_index(
            "emb",
            {"index_type": "HNSW", "metric_type": "BM25", "params": {"M": 16, "efConstruction": 500}},
            check_task=CheckTasks.err_res,
            check_items=error
        )

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("b", [-1, 0.5, 2])
    @pytest.mark.parametrize("k", [-1, 1.5])
    @pytest.mark.parametrize("index_type", ["SPARSE_INVERTED_INDEX"])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_create_full_text_search_with_invalid_bm25_params(
            self, tokenizer, index_type, k, b
    ):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        empty_percent = 0.0
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=True,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                enable_match=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        data = [
            {
                "id": i,
                "word": fake.word().lower() if random.random() >= empty_percent else "",
                "sentence": fake.sentence().lower() if random.random() >= empty_percent else "",
                "paragraph": fake.paragraph().lower() if random.random() >= empty_percent else "",
                "text": fake.text().lower() if random.random() >= empty_percent else "",
                "emb": [random.random() for _ in range(dim)],
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
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )

        if k == 1.5 and b == 0.5:
            check_task = None
            error = {}
        else:
            check_task = CheckTasks.err_res
            error = {"err_code": 1100, "err_msg": "invalid"}  # todo, update error code and message
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": index_type,
                "metric_type": "BM25",
                "params": {
                    "bm25_k1": k,
                    "bm25_b": b,
                }
            },
            check_task=check_task,
            check_items=error
        )


@pytest.mark.skip("skip")
class TestSearchWithFullTextSearch(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test search with full text search
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("nq", [10])
    @pytest.mark.parametrize("empty_percent", [0])
    @pytest.mark.parametrize("enable_partition_key", [True])
    @pytest.mark.parametrize("enable_inverted_index", [True])
    @pytest.mark.parametrize("index_type", ["SPARSE_INVERTED_INDEX", "SPARSE_WAND"])
    @pytest.mark.parametrize("expr", ["text_match", "id_range"])
    @pytest.mark.parametrize("tokenizer", ["default"])
    @pytest.mark.parametrize("offset", [10, 0])
    def test_full_text_search_default(
            self, offset, tokenizer, expr, enable_inverted_index, enable_partition_key, empty_percent, index_type, nq
    ):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=enable_partition_key,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                enable_match=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data = [
            {
                "id": i,
                "word": fake.word().lower() if random.random() >= empty_percent else "",
                "sentence": fake.sentence().lower() if random.random() >= empty_percent else "",
                "paragraph": fake.paragraph().lower() if random.random() >= empty_percent else "",
                "text": fake.text().lower() if random.random() >= empty_percent else "",
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        texts = df["text"].to_list()
        word_freq = cf.analyze_documents(texts, language=language)
        tokens = list(word_freq.keys())
        if len(tokens) == 0:
            log.info(f"empty tokens, add a dummy token")
            tokens = ["dummy"]
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": index_type,
                "metric_type": "BM25",
                "params": {
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        if enable_inverted_index:
            collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        limit = 100
        search_data = [fake.text().lower() + " " + random.choice(tokens) for _ in range(nq)]
        if expr == "text_match":
            filter = f"TextMatch(text, '{tokens[0]}')"
            res, _ = collection_w.query(
                expr=filter,
            )
        elif expr == "id_range":
            filter = f"id < {data_size // 2}"
        else:
            filter = ""
        res, _ = collection_w.query(
            expr=filter,
            limit=limit,
        )
        candidates_num = len(res)
        log.info(f"search data: {search_data}")
        # use offset = 0 to get all the results
        full_res_list, _ = collection_w.search(
            data=search_data,
            anns_field="text_sparse_emb",
            expr=filter,
            param={},
            limit=limit + offset,
            offset=0,
            output_fields=["id", "text", "text_sparse_emb"])
        full_res_id_list = []
        for i in range(nq):
            res = full_res_list[i]
            tmp = []
            for r in res:
                tmp.append(r.id)
            full_res_id_list.append(tmp)

        res_list, _ = collection_w.search(
            data=search_data,
            anns_field="text_sparse_emb",
            expr=filter,
            param={},
            limit=limit,
            offset=offset,
            output_fields=["id", "text", "text_sparse_emb"])

        # verify correctness
        for i in range(nq):
            assert 0 < len(res_list[i]) <= min(limit, candidates_num)
            search_text = search_data[i]
            log.info(f"res: {res_list[i]}")
            res = res_list[i]
            for j in range(len(res)):
                r = res[j]
                _id = r.id
                # get the first id of the result in which position is larger than offset
                if j == 0:
                    first_id = _id
                    p = full_res_id_list[i].index(first_id)
                    assert 1.2 * offset >= p >= offset * 0.8
                result_text = r.text
                # verify search result satisfies the filter
                if expr == "text_match":
                    assert tokens[0] in result_text
                if expr == "id_range":
                    assert _id < data_size // 2
                # verify search result has overlap with search text
                overlap, word_freq_a, word_freq_b = cf.check_token_overlap(search_text, result_text, language=language)
                log.info(f"overlap {overlap}")
                assert len(
                    overlap) > 0, f"query text: {search_text}, \ntext: {result_text} \n overlap: {overlap} \n word freq a: {word_freq_a} \n word freq b: {word_freq_b}\n result: {r}"

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("nq", [1])
    @pytest.mark.parametrize("empty_percent", [0])
    @pytest.mark.parametrize("enable_partition_key", [True])
    @pytest.mark.parametrize("enable_inverted_index", [True])
    @pytest.mark.parametrize("index_type", ["SPARSE_INVERTED_INDEX"])
    @pytest.mark.parametrize("expr", [None])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_full_text_search_with_range_search(
            self, tokenizer, expr, enable_inverted_index, enable_partition_key, empty_percent, index_type, nq
    ):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=enable_partition_key,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                enable_match=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data = [
            {
                "id": i,
                "word": fake.word().lower() if random.random() >= empty_percent else "",
                "sentence": fake.sentence().lower() if random.random() >= empty_percent else "",
                "paragraph": fake.paragraph().lower() if random.random() >= empty_percent else "",
                "text": fake.text().lower() if random.random() >= empty_percent else "",
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        texts = df["text"].to_list()
        word_freq = cf.analyze_documents(texts, language=language)
        tokens = list(word_freq.keys())
        if len(tokens) == 0:
            log.info(f"empty tokens, add a dummy token")
            tokens = ["dummy"]
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": index_type,
                "metric_type": "BM25",
                "params": {
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        if enable_inverted_index:
            collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        limit = 1000
        search_data = [fake.text().lower() + random.choice(tokens) for _ in range(nq)]
        log.info(f"search data: {search_data}")
        # get distance with search data
        res_list, _ = collection_w.search(
            data=search_data,
            anns_field="text_sparse_emb",
            param={
            },
            limit=limit,  # get a wider range of search result
            output_fields=["id", "text", "text_sparse_emb"])

        distance_list = []
        for i in range(nq):
            res = res_list[i]
            for j in range(len(res)):
                r = res[j]
                distance = r.distance
                distance_list.append(distance)
        distance_list = sorted(distance_list)
        # get the range of distance 30% ~70%
        low = distance_list[int(len(distance_list) * 0.3)]
        high = distance_list[int(len(distance_list) * 0.7)]

        res_list, _ = collection_w.search(
            data=search_data,
            anns_field="text_sparse_emb",
            param={
                "params": {
                    "radius": low, "range_filter": high
                }
            },
            limit=limit,
            output_fields=["id", "text", "text_sparse_emb"])
        # verify correctness
        for i in range(nq):
            log.info(f"res: {len(res_list[i])}")
            assert len(res_list[i]) < limit  # less than limit, because the range is set
            res = res_list[i]
            for j in range(len(res)):
                r = res[j]
                tmp_distance = r.distance
                assert low <= tmp_distance <= high

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("nq", [1])
    @pytest.mark.parametrize("empty_percent", [0])
    @pytest.mark.parametrize("enable_partition_key", [True])
    @pytest.mark.parametrize("enable_inverted_index", [True])
    @pytest.mark.parametrize("index_type", ["SPARSE_INVERTED_INDEX"])
    @pytest.mark.parametrize("expr", [None])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_full_text_search_with_search_iterator(
            self, tokenizer, expr, enable_inverted_index, enable_partition_key, empty_percent, index_type, nq
    ):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=enable_partition_key,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                enable_match=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data = [
            {
                "id": i,
                "word": fake.word().lower() if random.random() >= empty_percent else "",
                "sentence": fake.sentence().lower() if random.random() >= empty_percent else "",
                "paragraph": fake.paragraph().lower() if random.random() >= empty_percent else "",
                "text": fake.text().lower() if random.random() >= empty_percent else "",
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        log.info(f"dataframe\n{df}")
        texts = df["text"].to_list()
        word_freq = cf.analyze_documents(texts, language=language)
        tokens = list(word_freq.keys())
        if len(tokens) == 0:
            log.info(f"empty tokens, add a dummy token")
            tokens = ["dummy"]
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": index_type,
                "metric_type": "BM25",
                "params": {
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        if enable_inverted_index:
            collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        search_data = [fake.text().lower() + " " + random.choice(tokens) for _ in range(nq)]
        log.info(f"search data: {search_data}")
        # get distance with search data
        batch_size = 100
        limit = batch_size * 10
        iterator, _ = collection_w.search_iterator(
            data=search_data,
            anns_field="text_sparse_emb",
            batch_size=100,
            param={
                "metric_type": "BM25",
            },
            output_fields=["id", "text", "text_sparse_emb"],
            limit=limit
        )
        while True:
            result = iterator.next()
            if not result:
                iterator.close()
                break
            else:
                for r in result:
                    assert len(r) == batch_size


@pytest.mark.skip("skip")
class TestSearchWithFullTextSearchNegative(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test search with full text search
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("empty_percent", [0])
    @pytest.mark.parametrize("enable_partition_key", [True])
    @pytest.mark.parametrize("enable_inverted_index", [True])
    @pytest.mark.parametrize("index_type", ["SPARSE_INVERTED_INDEX", "SPARSE_WAND"])
    @pytest.mark.parametrize("invalid_search_data", ["sparse_vector", "empty_text", "dense_vector"])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_search_for_full_text_search_with_invalid_search_data(
            self, tokenizer, enable_inverted_index, enable_partition_key, empty_percent, index_type, invalid_search_data
    ):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=enable_partition_key,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        if tokenizer == "jieba":
            language = "zh"
            fake = fake_zh
        else:
            language = "en"

        data = [
            {
                "id": i,
                "word": fake.word().lower() if random.random() >= empty_percent else "",
                "sentence": fake.sentence().lower() if random.random() >= empty_percent else "",
                "paragraph": fake.paragraph().lower() if random.random() >= empty_percent else "",
                "text": fake.text().lower() if random.random() >= empty_percent else "",
                "emb": [random.random() for _ in range(dim)],
            }
            for i in range(data_size)
        ]
        df = pd.DataFrame(data)
        corpus = df["text"].to_list()
        log.info(f"dataframe\n{df}")
        texts = df["text"].to_list()
        word_freq = cf.analyze_documents(texts, language=language)
        tokens = list(word_freq.keys())
        if len(tokens) == 0:
            log.info(f"empty tokens, add a dummy token")
            tokens = ["dummy"]
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            collection_w.insert(
                data[i: i + batch_size]
                if i + batch_size < len(df)
                else data[i: len(df)]
            )
            collection_w.flush()
        collection_w.create_index(
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": index_type,
                "metric_type": "BM25",
                "params": {
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        if enable_inverted_index:
            collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        nq = 2
        limit = 100
        if invalid_search_data == "sparse_vector":
            search_data = cf.gen_vectors(nb=nq, dim=1000, vector_data_type="SPARSE_FLOAT_VECTOR")
        elif invalid_search_data == "empty_text":
            search_data = ["" for _ in range(nq)]
        else:
            search_data = cf.gen_vectors(nb=nq, dim=1000, vector_data_type="FLOAT_VECTOR")
        if invalid_search_data == "empty_text":
            res, _ = collection_w.search(
                data=search_data,
                anns_field="text_sparse_emb",
                param={},
                limit=limit,
                output_fields=["id", "text", "text_sparse_emb"],
            )
            assert len(res) == nq
            for r in res:
                assert len(r) == 0
        else:
            error = {ct.err_code: 65535,
                     ct.err_msg: "can't build BM25 IDF for data not varchar"}
            collection_w.search(
                data=search_data,
                anns_field="text_sparse_emb",
                param={},
                limit=limit,
                output_fields=["id", "text", "text_sparse_emb"],
                check_task=CheckTasks.err_res,
                check_items=error
            )


@pytest.mark.skip("skip")
class TestHybridSearchWithFullTextSearch(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test search with full text search
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize("empty_percent", [0])
    @pytest.mark.parametrize("enable_partition_key", [True])
    @pytest.mark.parametrize("enable_inverted_index", [True])
    @pytest.mark.parametrize("index_type", ["SPARSE_INVERTED_INDEX"])
    @pytest.mark.parametrize("tokenizer", ["default"])
    def test_hybrid_search_with_full_text_search(
            self, tokenizer, enable_inverted_index, enable_partition_key, empty_percent, index_type
    ):
        """
        target: test full text search
        method: 1. enable full text search and insert data with varchar
                2. search with text
                3. verify the result
        expected: full text search successfully and result is correct
        """
        tokenizer_params = {
            "tokenizer": tokenizer,
        }
        dim = 128
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="word",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
                is_partition_key=enable_partition_key,
            ),
            FieldSchema(
                name="sentence",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="paragraph",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(
                name="text",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_tokenizer=True,
                enable_match=True,
                tokenizer_params=tokenizer_params,
            ),
            FieldSchema(name="emb", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="text_sparse_emb", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        bm25_function = Function(
            name="text_bm25_emb",
            function_type=FunctionType.BM25,
            input_field_names=["text"],
            output_field_names=["text_sparse_emb"],
            params={},
        )
        schema.add_function(bm25_function)
        data_size = 5000
        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        fake = fake_en
        data = [
            {
                "id": i,
                "word": fake.word().lower() if random.random() >= empty_percent else "",
                "sentence": fake.sentence().lower() if random.random() >= empty_percent else "",
                "paragraph": fake.paragraph().lower() if random.random() >= empty_percent else "",
                "text": fake.text().lower() if random.random() >= empty_percent else "",
                "emb": [random.random() for _ in range(dim)],
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
            "emb",
            {"index_type": "HNSW", "metric_type": "L2", "params": {"M": 16, "efConstruction": 500}},
        )
        collection_w.create_index(
            "text_sparse_emb",
            {
                "index_type": index_type,
                "metric_type": "BM25",
                "params": {
                    "bm25_k1": 1.5,
                    "bm25_b": 0.75,
                }
            }
        )
        if enable_inverted_index:
            collection_w.create_index("text", {"index_type": "INVERTED"})
        collection_w.load()
        nq = 2
        limit = 100
        sparse_search = AnnSearchRequest(
            data=[fake.text().lower() for _ in range(nq)],
            anns_field="text_sparse_emb",
            param={},
            limit=limit,
        )
        dense_search = AnnSearchRequest(
            data=[[random.random() for _ in range(dim)] for _ in range(nq)],
            anns_field="emb",
            param={},
            limit=limit,

        )
        # hybrid search
        res_list, _ = collection_w.hybrid_search(
            reqs=[sparse_search, dense_search],
            rerank=WeightedRanker(0.5, 0.5),
            limit=limit,
            output_fields=["id", "text"]
        )
        assert len(res_list) == nq
        # check the result correctness
        for i in range(nq):
            log.info(f"res length: {len(res_list[i])}")
            assert len(res_list[i]) == limit


@pytest.mark.skip("skip")
class TestSearchWithFullTextSearchBenchmark(TestcaseBase):
    """
    target: test full text search
    method: 1. enable full text search and insert data with varchar
            2. search with text
            3. verify the result
    expected: full text search successfully and result is correct
    """

    @pytest.mark.tags(CaseLabel.L0)
    # @pytest.mark.parametrize("dataset", ["nfcorpus", "nq", "fiqa", "scifact"])
    @pytest.mark.parametrize("index_type", ["SPARSE_INVERTED_INDEX", "SPARSE_WAND"])
    @pytest.mark.parametrize("dataset", ["fiqa"])
    def test_search_with_full_text_search(self, dataset, index_type):
        self._connect()
        BASE_URL = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip"
        data_path = beir.util.download_and_unzip(BASE_URL.format(dataset), out_dir="./tmp/dataset")
        split = "test" if dataset != "msmarco" else "dev"
        corpus, queries, qrels = GenericDataLoader(data_folder=data_path).load(split=split)
        collection_name = cf.gen_unique_str(prefix)
        top_k = 1000
        milvus_full_text_search_result = cf.milvus_full_text_search(collection_name, corpus, queries, qrels,
                                                                    top_k=top_k, index_type=index_type)
        lucene_full_text_search_result = cf.Lucene_full_text_search(corpus, queries, qrels, top_k=top_k)
        log.info(f"result for dataset {dataset}")
        log.info(f"milvus full text search result {milvus_full_text_search_result}")
        log.info(f"lucene full text search result {lucene_full_text_search_result}")
