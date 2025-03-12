import random
import uuid
from pymilvus import (
    FieldSchema,
    CollectionSchema,
    DataType,
    Function,
    FunctionType,
    AnnSearchRequest,
    WeightedRanker,
)
from pymilvus.bulk_writer import BulkFileType, RemoteBulkWriter
from common.common_type import CaseLabel, CheckTasks
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_base import TestcaseBase
import numpy as np
import time
import pytest
import pandas as pd
from faker import Faker
import requests
import os
from numpy import dot
from numpy.linalg import norm

fake_zh = Faker("zh_CN")
fake_jp = Faker("ja_JP")
fake_en = Faker("en_US")

pd.set_option("expand_frame_repr", False)

prefix = "text_embedding_collection"


class TestCreateCollectionWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test create collection with text embedding function
    ******************************************************************
    """

    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_create_collection_with_text_embedding(self, model_name):
        """
        target: test create collection with text embedding function
        method: create collection with text embedding function
        expected: create collection successfully
        """
        dim = 1024  # dimension for bge-m3 model
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="siliconflow",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 1

    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_create_collection_with_text_embedding_twice_with_same_schema(
        self, model_name
    ):
        """
        target: test create collection with text embedding twice with same schema
        method: create collection with text embedding function, then create again
        expected: create collection successfully and create again successfully
        """
        dim = 1024  # dimension for bge-m3 model
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="siliconflow",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name, schema=schema)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 1

    def test_create_collection_with_text_embedding_with_multi_models(self):
        """
        target: test create collection with text embedding twice with same schema
        method: create collection with text embedding function, then create again
        expected: create collection successfully and create again successfully
        """
        bge_dim = 1024
        bce_dim = 768
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
        ]
        model_names = ["BAAI/bge-m3", "netease-youdao/bce-embedding-base_v1"]
        for model_name in model_names:
            field_name = f"dense_{model_name.replace('/', '_').replace('-', '_').replace('.', '_')}"
            dim = bge_dim if "bge" in model_name else bce_dim
            field = FieldSchema(name=field_name, dtype=DataType.FLOAT_VECTOR, dim=dim)
            fields.append(field)

        schema = CollectionSchema(fields=fields, description="test collection")

        for model_name in model_names:
            field_name = f"dense_{model_name.replace('/', '_').replace('-', '_').replace('.', '_')}"
            log.info(f"model_name: {model_name}, field_name: {field_name}")
            text_embedding_function = Function(
                name=f"siliconflow-{model_name}",
                function_type=FunctionType.TEXTEMBEDDING,
                input_field_names=["document"],
                output_field_names=field_name,
                params={
                    "provider": "siliconflow",
                    "model_name": model_name,
                },
            )
            schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)

        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        res, _ = collection_w.describe()
        log.info(f"collection describe: {res}")
        assert len(res["functions"]) == 2
        nb = 10
        data = [{"id": i, "document": fake_en.text()} for i in range(nb)]
        collection_w.insert(data)
        assert collection_w.num_entities == nb
        index_params = {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 48},
        }

        for model_name in model_names:
            field_name = f"dense_{model_name.replace('/', '_').replace('-', '_').replace('.', '_')}"

            collection_w.create_index(field_name=field_name, index_params=index_params)
        collection_w.load()

        for model_name in model_names:
            field_name = f"dense_{model_name.replace('/', '_').replace('-', '_').replace('.', '_')}"
            res, _ = collection_w.query(
                expr="id >= 0",
                output_fields=[field_name],
            )
            for row in res:
                assert (
                    len(row[field_name]) == bge_dim if "bge" in model_name else bce_dim
                )


class TestCreateCollectionWithTextEmbeddingNegative(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test create collection with text embedding negative
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("model_name", ["unsupported_model"])
    def test_create_collection_with_text_embedding_unsupported_model(self, model_name):
        """
        target: test create collection with text embedding with unsupported model
        method: create collection with text embedding function using unsupported model
        expected: create collection failed
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="siliconflow",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        self.init_collection_wrap(
            name=cf.gen_unique_str(prefix),
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 65535, "err_msg": "Unsupported model"},
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_create_collection_with_text_embedding_unmatched_dim(self, model_name):
        """
        target: test create collection with text embedding with unsupported model
        method: create collection with text embedding function using unsupported model
        expected: create collection failed
        """
        dim = 512
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="siliconflow",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        self.init_collection_wrap(
            name=cf.gen_unique_str(prefix),
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={
                "err_code": 65535,
                "err_msg": f"The required embedding dim is [{dim}], but the embedding obtained from the model is [1024]",
            },
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_create_collection_with_text_embedding_invalid_api_key(self, model_name):
        """
        target: test create collection with text embedding with invalid api key
        method: create collection with text embedding function using invalid api key
        expected: create collection failed
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="siliconflow",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
                "api_key": "invalid_api_key",
            },
        )
        schema.add_function(text_embedding_function)

        self.init_collection_wrap(
            name=cf.gen_unique_str(prefix),
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 65535, "err_msg": "Invalid"},
        )


class TestInsertWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test insert with text embedding
    ******************************************************************
    """

    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_insert_with_text_embedding(self, model_name):
        """
        target: test insert data with text embedding
        method: insert data with text embedding function
        expected: insert successfully
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="siliconflow",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data
        nb = 10
        data = [{"id": i, "document": fake_en.text()} for i in range(nb)]

        # insert data
        collection_w.insert(data)
        assert collection_w.num_entities == nb
        # create index
        index_params = {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 48},
        }
        collection_w.create_index(field_name="dense", index_params=index_params)
        collection_w.load()
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["dense"],
        )
        for row in res:
            # For INT8_VECTOR, the data might be returned as a binary array
            # We need to check if there's data, but not necessarily the exact dimension
            if isinstance(row["dense"], bytes):
                # For binary data, just verify it's not empty
                assert len(row["dense"]) > 0, "Vector should not be empty"
            else:
                # For regular vectors, check the exact dimension
                assert len(row["dense"]) == dim


class TestALLProviderWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test all provider with text embedding
    ******************************************************************
    """

    @pytest.mark.parametrize(
        "model_name",
        ["text-embedding-ada-002", "text-embedding-3-small", "text-embedding-3-large"],
    )
    def test_insert_with_openai_text_embedding(self, model_name):
        """
        target: test insert data with text embedding
        method: insert data with text embedding function
        expected: insert successfully
        """
        dim_map = {
            "text-embedding-ada-002": 1536,
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 1024,
        }
        dim = dim_map.get(model_name)
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        params = {
            "provider": "openai",
            "model_name": model_name,
            "user": f"{uuid.uuid4().hex}",
        }
        if model_name == "text-embedding-3-large":
            params["dim"] = dim
        text_embedding_function = Function(
            name="openai",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params=params,
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data
        nb = 10
        data = [{"id": i, "document": fake_en.text()} for i in range(nb)]

        # insert data
        collection_w.insert(data)
        assert collection_w.num_entities == nb
        # create index
        index_params = {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 48},
        }
        collection_w.create_index(field_name="dense", index_params=index_params)
        collection_w.load()
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["dense"],
        )
        for row in res:
            assert len(row["dense"]) == dim

    @pytest.mark.parametrize(
        "model_name", ["text-embedding-v1", "text-embedding-v2", "text-embedding-v3"]
    )
    def test_insert_with_dashscope_text_embedding(self, model_name):
        """
        target: test insert data with text embedding
        method: insert data with text embedding function
        expected: insert successfully
        """
        dim_map = {
            "text-embedding-v1": 1536,
            "text-embedding-v2": 1536,
            "text-embedding-v3": 768,
        }
        dim = dim_map.get(model_name)
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="dashscope",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "dashscope",
                "model_name": model_name,
                "dim": dim,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data
        nb = 10
        data = [{"id": i, "document": fake_en.text()} for i in range(nb)]

        # insert data
        collection_w.insert(data)
        assert collection_w.num_entities == nb
        # create index
        index_params = {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 48},
        }
        collection_w.create_index(field_name="dense", index_params=index_params)
        collection_w.load()
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["dense"],
        )
        for row in res:
            assert len(row["dense"]) == dim

    @pytest.mark.parametrize(
        "model_name,dim",
        [
            ("amazon.titan-embed-text-v2:0", 1024),
            ("amazon.titan-embed-text-v2:0", 512),
            ("amazon.titan-embed-text-v2:0", 256),
        ],
    )
    def test_insert_with_bedrock_text_embedding(self, model_name, dim):
        """
        target: test insert data with text embedding
        method: insert data with text embedding function
        expected: insert successfully
        """
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        # Set up parameters for Bedrock embedding
        params = {
            "provider": "bedrock",
            "model_name": model_name,
            "normalize": True,
            "dim": dim,
        }

        text_embedding_function = Function(
            name="bedrock_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params=params,
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data
        nb = 10
        data = [{"id": i, "document": fake_en.text()} for i in range(nb)]

        # insert data
        collection_w.insert(data)
        assert collection_w.num_entities == nb
        # create index
        index_params = {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 48},
        }
        collection_w.create_index(field_name="dense", index_params=index_params)
        collection_w.load()
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["dense"],
        )
        for row in res:
            assert len(row["dense"]) == dim

    @pytest.mark.parametrize(
        "model_name", ["text-embedding-005", "text-multilingual-embedding-002"]
    )
    def test_insert_with_vertexai_text_embedding(self, model_name):
        """
        target: test insert data with text embedding
        method: insert data with text embedding function
        expected: insert successfully
        """
        # Both models use 768 dimensions by default
        dim = 768
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        # Set up parameters for Vertex AI embedding
        params = {
            "provider": "vertexai",
            "model_name": model_name,
            "projectid": "test-410709",
            "location": "us-central1",
        }

        text_embedding_function = Function(
            name="vertexai_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params=params,
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data
        nb = 10
        data = [{"id": i, "document": fake_en.text()} for i in range(nb)]

        # insert data
        collection_w.insert(data)
        assert collection_w.num_entities == nb
        # create index
        index_params = {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 48},
        }
        collection_w.create_index(field_name="dense", index_params=index_params)
        collection_w.load()
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["dense"],
        )
        for row in res:
            assert len(row["dense"]) == dim

    @pytest.mark.parametrize(
        "model_name",
        [
            "voyage-3-large",  # 1024 (default), 256, 512, 2048
            "voyage-3",  # 1024
            "voyage-3-lite",  # 512
            "voyage-code-3",  # 1024 (default), 256, 512, 2048
            "voyage-finance-2",  # 1024
            "voyage-law-2",  # 1024
            "voyage-code-2",  # 1536
        ],
    )
    def test_insert_with_voyageai_text_embedding(self, model_name):
        """
        target: test insert data with text embedding
        method: insert data with text embedding function
        expected: insert successfully
        """
        dim_map = {
            "voyage-3-large": 2048,
            "voyage-3": 1024,
            "voyage-3-lite": 512,
            "voyage-code-3": 2048,
            "voyage-finance-2": 1024,
            "voyage-law-2": 1024,
            "voyage-code-2": 1536,
        }
        dim = dim_map.get(model_name)
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="voyageai_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "voyageai",
                "model_name": model_name,
                "dim": dim,
                "api_key": "pa-c18f3MzGnJUYpxCxp9pWuzp9l-wQZ_lXfS1ZKzV7IGL",
            },
        )
        schema.add_function(text_embedding_function)

        # insert data with retry mechanism to handle rate limit (429 errors)
        max_retries = 5
        retry_delay = 20  # seconds between retries
        for retry_count in range(max_retries):
            try:
                collection_w = self.init_collection_wrap(
                    name=cf.gen_unique_str(prefix),
                    schema=schema,
                    check_task=CheckTasks.check_nothing,
                )

                # prepare data
                nb = 1
                data = [{"id": i, "document": fake_en.text()} for i in range(nb)]
                res, result = collection_w.insert(
                    data, check_task=CheckTasks.check_nothing
                )
                if result:
                    assert collection_w.num_entities == nb
                    break  # Success, exit retry loop
                else:
                    # Insert failed, raise exception to trigger retry
                    # res is already an Error object, so we can directly raise it
                    raise Exception(str(res))
            except Exception as e:
                error_msg = str(e)
                if (
                    "429 Too Many Requests" in error_msg
                    or "'NoneType' object has no attribute" in error_msg
                ) and retry_count < max_retries - 1:
                    log.info(
                        f"Rate limit exceeded, retrying in {retry_delay} seconds... (Attempt {retry_count + 1}/{max_retries})"
                    )
                    time.sleep(retry_delay)
                    # Increase delay for next retry (exponential backoff)
                    retry_delay *= 1.5
                else:
                    # If it's not a rate limit error or we've exhausted retries, re-raise
                    raise
        # create index
        index_params = {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 48},
        }
        collection_w.create_index(field_name="dense", index_params=index_params)
        collection_w.load()
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["dense"],
        )
        for row in res:
            assert len(row["dense"]) == dim

    @pytest.mark.parametrize(
        "model_name",
        [
            "embed-english-v3.0",  # 1024
            "embed-multilingual-v3.0",  # 1024
            "embed-english-light-v3.0",  # 384
            "embed-multilingual-light-v3.0",  # 384
            "embed-english-v2.0",  # 4096
            "embed-english-light-v2.0",  # 1024
            "embed-multilingual-v2.0",  # 768
        ],
    )
    def test_insert_with_cohere_text_embedding(self, model_name):
        """
        target: test insert data with text embedding
        method: insert data with text embedding function
        expected: insert successfully
        """
        dim_map = {
            "embed-english-v3.0": 1024,
            "embed-multilingual-v3.0": 1024,
            "embed-english-light-v3.0": 384,
            "embed-multilingual-light-v3.0": 384,
            "embed-english-v2.0": 4096,
            "embed-english-light-v2.0": 1024,
            "embed-multilingual-v2.0": 768,
        }
        dim = dim_map.get(model_name)
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="cohere_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "cohere",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data
        nb = 10
        data = [{"id": i, "document": fake_en.text()} for i in range(nb)]

        # insert data
        collection_w.insert(data)
        assert collection_w.num_entities == nb
        # create index
        index_params = {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 48},
        }
        collection_w.create_index(field_name="dense", index_params=index_params)
        collection_w.load()
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["dense"],
        )
        for row in res:
            assert len(row["dense"]) == dim

    @pytest.mark.parametrize(
        "model_name",
        [
            "BAAI/bge-large-zh-v1.5",  # 1024
            "BAAI/bge-large-en-v1.5",  # 1024
            "netease-youdao/bce-embedding-base_v1",  # 768
            "BAAI/bge-m3",  # 1024
            "Pro/BAAI/bge-m3",  # 1024
        ],
    )
    def test_insert_with_siliconflow_text_embedding(self, model_name):
        """
        target: test insert data with text embedding
        method: insert data with text embedding function
        expected: insert successfully
        """
        dim_map = {
            "BAAI/bge-large-zh-v1.5": 1024,
            "BAAI/bge-large-en-v1.5": 1024,
            "netease-youdao/bce-embedding-base_v1": 768,
            "BAAI/bge-m3": 1024,
            "Pro/BAAI/bge-m3": 1024,
        }
        dim = dim_map.get(model_name)
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="siliconflow_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data
        nb = 10
        data = [{"id": i, "document": fake_en.text()} for i in range(nb)]

        # insert data
        collection_w.insert(data)
        assert collection_w.num_entities == nb
        # create index
        index_params = {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 48},
        }
        collection_w.create_index(field_name="dense", index_params=index_params)
        collection_w.load()
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["dense"],
        )
        for row in res:
            assert len(row["dense"]) == dim

    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_insert_with_tei_text_embedding(self, model_name, tei_endpoint):
        """
        target: test insert data with text embedding
        method: insert data with text embedding function
        expected: insert successfully
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "tei",
                "model_name": model_name,
                "tei_url": tei_endpoint,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data
        nb = 10
        data = [{"id": i, "document": fake_en.text()} for i in range(nb)]

        # insert data
        collection_w.insert(data)
        assert collection_w.num_entities == nb
        # create index
        index_params = {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 48},
        }
        collection_w.create_index(field_name="dense", index_params=index_params)
        collection_w.load()
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["dense"],
        )
        for row in res:
            assert len(row["dense"]) == dim

    @pytest.mark.parametrize(
        "provider, model_name, dim",
        [
            ("cohere", "embed-english-v3.0", 1024),
            ("cohere", "embed-multilingual-v3.0", 1024),
            ("cohere", "embed-english-light-v3.0", 384),
            ("cohere", "embed-multilingual-light-v3.0", 384),
            ("voyageai", "voyage-3-large", 1024),
            ("voyageai", "voyage-code-3", 1024),
        ],
    )
    def test_insert_with_int8_text_embedding(self, provider, model_name, dim):
        """
        target: test insert data with text embedding
        method: insert data with text embedding function
        expected: insert successfully
        """
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.INT8_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name=f"{provider}_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": provider,
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data
        nb = 10
        data = [{"id": i, "document": fake_en.text()} for i in range(nb)]

        # insert data
        collection_w.insert(data)
        assert collection_w.num_entities == nb
        # create index
        index_params = {
            "index_type": "HNSW",
            "metric_type": "COSINE",
            "params": {"M": 48},
        }
        collection_w.create_index(field_name="dense", index_params=index_params)
        collection_w.load()
        res, _ = collection_w.query(
            expr="id >= 0",
            output_fields=["dense"],
        )
        for row in res:
            assert len(row["dense"]) == dim


class TestSearchWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test search with text embedding
    ******************************************************************
    """

    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_search_with_text_embedding(self, model_name):
        """
        target: test search with text embedding
        method: search with text embedding function
        expected: search successfully
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="siliconflow",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data
        nb = 10
        data = [{"id": i, "document": fake_en.text()} for i in range(nb)]

        # insert data
        collection_w.insert(data)
        assert collection_w.num_entities == nb

        # create index
        index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "COSINE",
            "params": {},
        }
        collection_w.create_index("dense", index_params)
        collection_w.load()

        # search
        search_params = {"metric_type": "COSINE", "params": {}}
        nq = 1
        limit = 10
        res, _ = collection_w.search(
            data=[fake_en.text() for _ in range(nq)],
            anns_field="dense",
            param=search_params,
            limit=10,
            output_fields=["document"],
        )
        assert len(res) == nq
        for hits in res:
            assert len(hits) == limit


class TestInsertWithTextEmbeddingNegative(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test insert with text embedding negative
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_insert_with_text_embedding_empty_document(self, model_name):
        """
        target: test insert data with empty document
        method: insert data with empty document
        expected: insert failed
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="siliconflow",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data with empty document
        empty_data = [{"id": 1, "document": ""}]
        normal_data = [{"id": 2, "document": fake_en.text()}]
        data = empty_data + normal_data

        collection_w.insert(
            data,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 65535, "err_msg": "The parameter is invalid"},
        )
        assert collection_w.num_entities == 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_insert_with_text_embedding_long_document(self, model_name):
        """
        target: test insert data with long document
        method: insert data with long document
        expected: insert failed
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="siliconflow",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data with empty document
        empty_data = [{"id": 1, "document": fake_en.word() * 10000}]
        normal_data = [{"id": 2, "document": fake_en.text()}]
        data = empty_data + normal_data

        collection_w.insert(
            data,
            check_task=CheckTasks.err_res,
            check_items={
                "err_code": 65535,
                "err_msg": "input must have less than 8192 tokens",
            },
        )
        assert collection_w.num_entities == 0


class TestEmbeddingAccuracy(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test embedding accuracy
    ******************************************************************
    """

    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_embedding_accuracy(self, model_name):
        """
        target: test embedding accuracy compared with provider API
        method: 1. generate embedding using Milvus
                2. generate embedding using provider API directly
                3. compare the results
        expected: embeddings should be identical within float precision
        """
        # Connect to Milvus
        self._connect()

        # Test document
        test_document = fake_en.text()

        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="siliconflow",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        batch_size = 10
        # Insert data
        data = [{"id": i, "document": test_document} for i in range(batch_size)]
        collection_w.insert(data)

        # Create index and load collection
        index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "COSINE",
            "params": {},
        }
        collection_w.create_index("dense", index_params)
        collection_w.load()

        # Query the document and get the embedding from Milvus
        res, _ = collection_w.query(expr="id >= 0", output_fields=["document", "dense"])

        assert len(res) == batch_size

        # Get API key from environment variable
        api_key = os.getenv("SILICONFLOW_API_KEY")
        if not api_key:
            assert False, "SILICONFLOW_API_KEY environment variable not set"

        # API endpoint for SiliconFlow
        url = "https://api.siliconflow.cn/v1/embeddings"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        # Test all vectors instead of just the first one
        similarities = []
        for i, item in enumerate(res):
            milvus_embedding = item["dense"]
            retrieved_document = item["document"]
            assert retrieved_document == test_document

            # Call SiliconFlow API directly to get embedding
            payload = {
                "model": model_name,
                "input": retrieved_document,
                "encoding_format": "float",
            }

            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()  # Raise exception for HTTP errors

            # Extract embedding from response
            api_embedding = response.json()["data"][0]["embedding"]

            # Compare embeddings
            assert (
                api_embedding is not None
            ), f"Failed to get embedding from SiliconFlow API for item {i}"
            assert len(milvus_embedding) == len(
                api_embedding
            ), f"Embedding dimensions don't match for item {i}"

            # Calculate cosine similarity
            cosine_sim = dot(milvus_embedding, api_embedding) / (
                norm(milvus_embedding) * norm(api_embedding)
            )
            similarities.append(cosine_sim)

            # Log the similarity for debugging
            log.info(
                f"Item {i}: Cosine similarity between Milvus and SiliconFlow API embeddings: {cosine_sim}"
            )

            # Embeddings should be nearly identical (allowing for minor floating point differences)
            assert (
                cosine_sim > 0.999
            ), f"Embeddings are not similar enough for item {i}: {cosine_sim}"

        # Log summary statistics
        avg_similarity = sum(similarities) / len(similarities)
        min_similarity = min(similarities)
        max_similarity = max(similarities)
        log.info(
            f"Summary - Average similarity: {avg_similarity}, Min: {min_similarity}, Max: {max_similarity}"
        )
        query_text = fake_en.text()
        text_search_res, _ = collection_w.search(
            data=[query_text],
            anns_field="dense",
            param={},
            output_fields=["document"],
            limit=10,
        )
        query_embedding = requests.post(
            url,
            json={"model": model_name, "input": query_text, "encoding_format": "float"},
            headers=headers,
        ).json()["data"][0]["embedding"]
        vector_search_res, _ = collection_w.search(
            data=[query_embedding],
            anns_field="dense",
            param={},
            output_fields=["document"],
            limit=10,
        )
        for i in range(len(text_search_res)):
            for j in range(len(text_search_res[i])):
                assert text_search_res[i][j].entity.get(
                    "document"
                ) == vector_search_res[i][j].entity.get("document")


class TestMultiLanguageSupport(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test multi-language support
    ******************************************************************
    """

    def test_multi_language_semantic(self):
        """
        target: test semantic similarity of embeddings across different languages
        method: 1. Test similar sentences in same language
                2. Test same meaning sentences in different languages
        expected: 1. Similar sentences in same language should have high similarity
                 2. Same meaning in different languages should have high similarity
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="text_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": "BAAI/bge-m3",
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # Same sentence in different languages (relevant texts)
        relevant_texts = [
            "我喜欢中国美食",  # Chinese: I love Chinese food
            "I love Chinese food",  # English
            "私は中華料理が大好きです",  # Japanese: I love Chinese food
        ]

        # Different sentence in different languages (irrelevant texts)
        irrelevant_texts = [
            "意大利面很好吃",  # Chinese: Italian pasta is delicious
            "Italian pasta is delicious",  # English
            "イタリアンパスタは美味しいです",  # Japanese: Italian pasta is delicious
        ]

        # Insert all texts
        data = [
            {"id": i, "document": text}
            for i, text in enumerate(relevant_texts + irrelevant_texts)
        ]
        collection_w.insert(data)

        # Create index and load
        index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "COSINE",
            "params": {},
        }
        collection_w.create_index("dense", index_params)
        collection_w.load()

        # Search parameters
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}}

        for q_text in relevant_texts:
            # Search with text
            res, _ = collection_w.search(
                data=[q_text],
                anns_field="dense",
                param=search_params,
                limit=len(relevant_texts) + len(irrelevant_texts),  # Get all results
                output_fields=["document"],
            )

            # Verify results
            assert len(res) == 1  # One search query

            # Get all result texts with their scores
            results = [(hit.entity.get("document"), hit.score) for hit in res[0]]
            log.info(f"data {q_text}, Search results: {results}")

            # Verify that all translations of the same sentence are ranked higher
            relevant_scores = [
                score for text, score in results if text in relevant_texts
            ]
            irrelevant_scores = [
                score for text, score in results if text in irrelevant_texts
            ]

            # Check each relevant text score is higher than any irrelevant text score
            min_relevant_score = min(relevant_scores)
            max_irrelevant_score = max(irrelevant_scores) if irrelevant_scores else 0

            # All translations should be found with high similarity
            assert min_relevant_score > max_irrelevant_score, (
                f"Some irrelevant texts ranked higher than relevant ones. \n"
                f"Relevant texts (scores): {relevant_scores}\n"
                f"Irrelevant texts (scores): {irrelevant_scores}"
            )


class TestMultiProviderSearch(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test multi-provider search
    ******************************************************************
    """

    def test_multi_provider_search(self):
        """
        target: test search with multiple embedding providers
        method: 1. create collection with multiple embedding functions
                2. insert data
                3. search with different providers
        expected: search results should be relevant for each provider
        """
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="openai_dense", dtype=DataType.FLOAT_VECTOR, dim=1536),
            FieldSchema(name="bge_dense", dtype=DataType.FLOAT_VECTOR, dim=1024),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        # Add OpenAI embedding function
        openai_function = Function(
            name="openai_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="openai_dense",
            params={
                "provider": "openai",
                "model_name": "text-embedding-ada-002",
            },
        )
        schema.add_function(openai_function)
        #
        # Add BGE embedding function
        bge_function = Function(
            name="bge_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="bge_dense",
            params={
                "provider": "siliconflow",
                "model_name": "BAAI/bge-m3",
            },
        )
        schema.add_function(bge_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # insert data
        nb = 10
        data = [{"id": i, "document": f"This is test document {i}"} for i in range(nb)]
        collection_w.insert(data)

        # create indexes and load
        for field in ["openai_dense", "bge_dense"]:
            index_params = {
                "index_type": "AUTOINDEX",
                "metric_type": "COSINE",
                "params": {},
            }
            collection_w.create_index(field, index_params)
        collection_w.load()

        # search with both providers
        search_params = {"metric_type": "COSINE", "params": {}}
        for field in ["openai_dense", "bge_dense"]:
            res, _ = collection_w.search(
                data=["test document"],
                anns_field=field,
                param=search_params,
                limit=10,
                output_fields=["document"],
            )
            assert len(res) == 1
            assert len(res[0]) == 10


class TestUpsertWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test upsert with text embedding
    ******************************************************************
    """

    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_upsert_text_field(self, model_name):
        """
        target: test upsert text field updates embedding
        method: 1. insert data
                2. upsert text field
                3. verify embedding is updated
        expected: embedding should be updated after text field is updated
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="text_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        # create index and load
        index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "COSINE",
            "params": {},
        }
        collection_w.create_index("dense", index_params)
        collection_w.load()

        # insert initial data
        old_text = "This is the original text"
        data = [{"id": 1, "document": old_text}]
        collection_w.insert(data)

        # get original embedding
        res, _ = collection_w.query(expr="id == 1", output_fields=["dense"])
        old_embedding = res[0]["dense"]

        # upsert with new text
        new_text = "This is the updated text"
        upsert_data = [{"id": 1, "document": new_text}]
        collection_w.upsert(upsert_data)

        # get new embedding
        res, _ = collection_w.query(expr="id == 1", output_fields=["dense"])
        new_embedding = res[0]["dense"]

        # verify embeddings are different
        assert not np.allclose(old_embedding, new_embedding)
        # caculate cosine similarity
        sim = np.dot(old_embedding, new_embedding) / (
            np.linalg.norm(old_embedding) * np.linalg.norm(new_embedding)
        )
        log.info(f"cosine similarity: {sim}")
        assert sim < 0.99


class TestDeleteWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test delete with text embedding
    ******************************************************************
    """

    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_delete_and_search(self, model_name):
        """
        target: test deleted text cannot be searched
        method: 1. insert data
                2. delete some data
                3. verify deleted data cannot be searched
        expected: deleted data should not appear in search results
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="text_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # insert data
        nb = 3
        data = [{"id": i, "document": f"This is test document {i}"} for i in range(nb)]
        collection_w.insert(data)

        # create index and load
        index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "COSINE",
            "params": {},
        }
        collection_w.create_index("dense", index_params)
        collection_w.load()

        # delete document 1
        collection_w.delete("id in [1]")

        # search and verify document 1 is not in results
        search_params = {"metric_type": "COSINE", "params": {"nprobe": 10}}
        res, _ = collection_w.search(
            data=["test document 1"],
            anns_field="dense",
            param=search_params,
            limit=3,
            output_fields=["document", "id"],
        )
        assert len(res) == 1
        for hit in res[0]:
            assert hit.entity.get("id") != 1


class TestImportWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test import with text embedding
    ******************************************************************
    """

    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    @pytest.mark.parametrize("file_format", ["json", "parquet", "numpy"])
    def test_import_without_embedding(self, model_name, minio_host, file_format):
        """
        target: test import data without embedding
        method: 1. create collection
                2. import data without embedding field
                3. verify embeddings are generated
        expected: embeddings should be generated after import
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="text_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # prepare import data without embedding
        nb = 1000
        if file_format == "json":
            file_type = BulkFileType.JSON
        elif file_format == "numpy":
            file_type = BulkFileType.NUMPY
        else:
            file_type = BulkFileType.PARQUET
        with RemoteBulkWriter(
            schema=schema,
            remote_path="bulk_data",
            connect_param=RemoteBulkWriter.ConnectParam(
                bucket_name="milvus-bucket",
                endpoint=f"{minio_host}:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
            ),
            file_type=file_type,
        ) as remote_writer:
            for i in range(nb):
                row = {"id": i, "document": f"This is test document {i}"}
                remote_writer.append_row(row)
            remote_writer.commit()
            files = remote_writer.batch_files
        # import data
        for f in files:
            t0 = time.time()
            task_id, _ = self.utility_wrap.do_bulk_insert(
                collection_name=c_name, files=f
            )
            log.info(f"bulk insert task ids:{task_id}")
            success, states = self.utility_wrap.wait_for_bulk_insert_tasks_completed(
                task_ids=[task_id], timeout=300
            )
            tt = time.time() - t0
            log.info(f"bulk insert state:{success} in {tt} with states:{states}")
            assert success
        num_entities = collection_w.num_entities
        log.info(f" collection entities: {num_entities}")
        assert num_entities == nb

        # create index and load
        index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "COSINE",
            "params": {},
        }
        collection_w.create_index("dense", index_params)
        collection_w.load()
        # verify embeddings are generated
        res, _ = collection_w.query(expr="id >= 0", output_fields=["dense"])
        assert len(res) == nb
        for r in res:
            assert "dense" in r
            assert len(r["dense"]) == dim


class TestHybridSearch(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test hybrid search
    ******************************************************************
    """

    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    def test_hybrid_search(self, model_name):
        """
        target: test hybrid search with text embedding and BM25
        method: 1. create collection with text embedding and BM25 functions
                2. insert data
                3. perform hybrid search
        expected: search results should combine vector similarity and text relevance
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name="document",
                dtype=DataType.VARCHAR,
                max_length=65535,
                enable_analyzer=True,
                analyzer_params={"tokenizer": "standard"},
            ),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="sparse", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        # Add text embedding function
        text_embedding_function = Function(
            name="text_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        # Add BM25 function
        bm25_function = Function(
            name="bm25",
            function_type=FunctionType.BM25,
            input_field_names=["document"],
            output_field_names="sparse",
            params={},
        )
        schema.add_function(bm25_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # insert test data
        data_size = 1000
        data = [{"id": i, "document": fake_en.text()} for i in range(data_size)]

        for batch in range(0, data_size, 100):
            collection_w.insert(data[batch : batch + 100])

        # create index and load
        dense_index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "COSINE",
            "params": {},
        }
        sparse_index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "BM25",
            "params": {},
        }
        collection_w.create_index("dense", dense_index_params)
        collection_w.create_index("sparse", sparse_index_params)
        collection_w.load()
        nq = 2
        limit = 100
        dense_text_search = AnnSearchRequest(
            data=[fake_en.text().lower() for _ in range(nq)],
            anns_field="dense",
            param={},
            limit=limit,
        )
        dense_vector_search = AnnSearchRequest(
            data=[[random.random() for _ in range(dim)] for _ in range(nq)],
            anns_field="dense",
            param={},
            limit=limit,
        )
        full_text_search = AnnSearchRequest(
            data=[fake_en.text().lower() for _ in range(nq)],
            anns_field="sparse",
            param={},
            limit=limit,
        )
        # hybrid search
        res_list, _ = collection_w.hybrid_search(
            reqs=[dense_text_search, dense_vector_search, full_text_search],
            rerank=WeightedRanker(0.5, 0.5, 0.5),
            limit=limit,
            output_fields=["id", "document"],
        )
        assert len(res_list) == nq
        # check the result correctness
        for i in range(nq):
            log.info(f"res length: {len(res_list[i])}")
            assert len(res_list[i]) == limit


class TestMultiVectorSearch(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test multi-vector search
    ******************************************************************
    """

    def test_multi_vector_search(self):
        """
        target: test search with multiple embedding vectors
        method: 1. create collection with multiple embedding functions
                2. insert data
                3. perform weighted search across multiple vectors
        expected: search results should reflect combined similarity scores
        """
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="openai_dense", dtype=DataType.FLOAT_VECTOR, dim=1536),
            FieldSchema(name="bge_dense", dtype=DataType.FLOAT_VECTOR, dim=1024),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        # Add OpenAI embedding function
        openai_function = Function(
            name="openai_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="openai_dense",
            params={
                "provider": "openai",
                "model_name": "text-embedding-ada-002",
            },
        )
        schema.add_function(openai_function)

        # Add BGE embedding function
        bge_function = Function(
            name="bge_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="bge_dense",
            params={
                "provider": "siliconflow",
                "model_name": "BAAI/bge-m3",
            },
        )
        schema.add_function(bge_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # insert data
        data_size = 3000
        batch_size = 100
        data = [{"id": i, "document": fake_en.text()} for i in range(data_size)]
        for batch in range(0, data_size, batch_size):
            collection_w.insert(data[batch : batch + batch_size])

        # create indexes and load
        for field in ["openai_dense", "bge_dense"]:
            index_params = {
                "index_type": "AUTOINDEX",
                "metric_type": "COSINE",
                "params": {},
            }
            collection_w.create_index(field, index_params)
        collection_w.load()

        # perform multi-vector search
        search_params = {"metric_type": "COSINE", "params": {}}
        nq = 10
        limit = 100
        query_text = [fake_en.text() for i in range(nq)]

        # search with OpenAI embedding
        openai_res, _ = collection_w.search(
            data=query_text,
            anns_field="openai_dense",
            param=search_params,
            limit=limit,
            output_fields=["document"],
        )

        # search with BGE embedding
        bge_res, _ = collection_w.search(
            data=query_text,
            anns_field="bge_dense",
            param=search_params,
            limit=limit,
            output_fields=["document"],
        )

        # verify both searches return results
        assert len(openai_res) == nq
        assert len(bge_res) == nq
        assert len(openai_res[0]) == limit
        assert len(bge_res[0]) == limit


class TestSearchWithTextEmbeddingNegative(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test search with text embedding negative
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("model_name", ["BAAI/bge-m3"])
    @pytest.mark.parametrize("query", ["", "hello world" * 8192])
    def test_search_with_text_embedding_negative_query(self, model_name, query):
        """
        target: test search with empty query or long query
        method: search with empty query
        expected: search failed
        """
        dim = 1024
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        text_embedding_function = Function(
            name="siliconflow",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "siliconflow",
                "model_name": model_name,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data
        nb = 3
        data = [{"id": i, "document": f"This is test document {i}"} for i in range(nb)]

        # insert data
        collection_w.insert(data)
        assert collection_w.num_entities == nb

        # create index
        index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "COSINE",
            "params": {},
        }
        collection_w.create_index("dense", index_params)
        collection_w.load()

        # search with empty query should fail
        search_params = {"metric_type": "COSINE", "params": {}}
        collection_w.search(
            data=[query],
            anns_field="dense",
            param=search_params,
            limit=3,
            output_fields=["document"],
            check_task=CheckTasks.err_res,
            check_items={"err_code": 65535, "err_msg": "Call service faild"},
        )


class TestInsertPerformanceWithTextEmbeddingFunction(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test insert performance with text embedding function
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_performance_with_text_embedding_function(self, tei_endpoint):
        """
        target: test insert performance with text embedding function for all providers and models
        method: 1. Test performance with different token counts
               2. Test performance across all providers and models
               3. Test with batch size = 1
        expected: Performance metrics are collected and compared for all models
        """
        self._connect()
        import time
        import pandas as pd
        from pymilvus import (
            Collection,
            DataType,
            FieldSchema,
            CollectionSchema,
            utility,
            Function,
            FunctionType,
        )

        # Define all providers and their models with dimensions
        providers_models = {
            "openai": [
                {"name": "text-embedding-ada-002", "dim": 1536},
                {"name": "text-embedding-3-small", "dim": 1536},
                {"name": "text-embedding-3-large", "dim": 3072},
            ],
            # "azure_openai": [
            #     {"name": "text-embedding-ada-002", "dim": 1536},
            #     {"name": "text-embedding-3-small", "dim": 1536},
            #     {"name": "text-embedding-3-large", "dim": 3072}
            # ],
            "dashscope": [
                {"name": "text-embedding-v1", "dim": 1536},
                {"name": "text-embedding-v2", "dim": 1536},
                {"name": "text-embedding-v3", "dim": 1024},
            ],
            # "bedrock": [
            #     {"name": "amazon.titan-embed-text-v2:0", "dim": 1024, "params": {"regin": "us-east-2"}}
            # ],
            # "vertexai": [
            #     {"name": "text-embedding-005", "dim": 768, "params": {"projectid": "zilliz-test-410709"}},
            #     {"name": "text-multilingual-embedding-002", "dim": 768, "params": {"projectid": "zilliz-test-410709"}}
            # ],
            # "voyageai": [
            #     {"name": "voyage-3-large", "dim": 1024},
            #     {"name": "voyage-3", "dim": 1024},
            #     {"name": "voyage-3-lite", "dim": 512},
            #     {"name": "voyage-code-3", "dim": 1024},
            #     {"name": "voyage-finance-2", "dim": 1024},
            #     {"name": "voyage-law-2", "dim": 1024},
            #     {"name": "voyage-code-2", "dim": 1536}
            # ],
            "cohere": [
                {"name": "embed-english-v3.0", "dim": 1024},
                {"name": "embed-multilingual-v3.0", "dim": 1024},
                {"name": "embed-english-light-v3.0", "dim": 384},
                {"name": "embed-multilingual-light-v3.0", "dim": 384},
                {"name": "embed-english-v2.0", "dim": 4096},
                {"name": "embed-english-light-v2.0", "dim": 1024},
                {"name": "embed-multilingual-v2.0", "dim": 768},
            ],
            #
            "siliconflow": [
                {"name": "BAAI/bge-large-zh-v1.5", "dim": 1024},
                {"name": "BAAI/bge-large-en-v1.5", "dim": 1024},
                {"name": "netease-youdao/bce-embedding-base_v1", "dim": 768},
                {"name": "BAAI/bge-m3", "dim": 1024},
                {"name": "Pro/BAAI/bge-m3", "dim": 1024},
            ],
            "tei": [
                {
                    "name": "tei",
                    "dim": 1024,
                    "params": {"provider": "TEI", "endpoint": tei_endpoint},
                }
            ],
        }

        # Generate text with simple fake words for precise token control
        def generate_fake_text(token_count):
            # Generate text with the specified number of tokens
            return " ".join([fake_en.word() for i in range(int(token_count * 0.8))])

        # Define token count variations
        token_variations = [
            {"name": "256_tokens", "text": generate_fake_text(256), "tokens": 256},
            {"name": "512_tokens", "text": generate_fake_text(512), "tokens": 512},
            {"name": "1024_tokens", "text": generate_fake_text(1024), "tokens": 1024},
            {"name": "2048_tokens", "text": generate_fake_text(2048), "tokens": 2048},
            {"name": "4096_tokens", "text": generate_fake_text(4096), "tokens": 4096},
            {"name": "8192_tokens", "text": generate_fake_text(8192), "tokens": 8192},
        ]

        # Prepare results dataframe
        results = []

        # Test each provider and model
        for provider, models in providers_models.items():
            for model in models:
                model_name = model["name"]
                dim = model["dim"]

                # Create collection with appropriate dimension
                schema = CollectionSchema(
                    [
                        FieldSchema("id", DataType.INT64, is_primary=True),
                        FieldSchema("text", DataType.VARCHAR, max_length=65535),
                        FieldSchema("embedding", DataType.FLOAT_VECTOR, dim=dim),
                    ]
                )
                # Configure text embedding function
                params = {"provider": provider, "model_name": model_name}

                # Add additional parameters
                if "params" in model:
                    params.update(model["params"])
                log.info(f"params: {params}")
                text_embedding_function = Function(
                    name=f"{provider}_{model_name.replace('/', '_')}_func",
                    function_type=FunctionType.TEXTEMBEDDING,
                    input_field_names=["text"],
                    output_field_names="embedding",
                    params=params,
                )
                schema.add_function(text_embedding_function)
                # Process special characters in model name
                model_name_safe = (
                    model_name.replace("/", "_")
                    .replace(".", "_")
                    .replace(":", "_")
                    .replace("-", "_")
                )

                # Create collection name
                collection_name = f"test_text_embedding_perf_{provider}_{model_name_safe}_{int(time.time())}"

                try:
                    collection = Collection(collection_name, schema)
                    # Test with different token counts
                    for token_var in token_variations:
                        test_text = token_var["text"]
                        token_count = token_var["tokens"]
                        token_name = token_var["name"]

                        # Measure latency with batch size = 1
                        try:
                            data = [
                                {
                                    "id": 0,
                                    "text": test_text,
                                }
                            ]
                            start_time = time.time()
                            collection.insert(data)
                            latency = time.time() - start_time

                            # Add concurrent test for 256 tokens only to avoid excessive API calls
                            if token_name == "256_tokens":
                                import concurrent.futures

                                # Function to run in parallel
                                def concurrent_insert(i, request_id):
                                    try:
                                        data = {
                                            "id": 0,
                                            "text": test_text,
                                        }
                                        start = time.time()
                                        collection.insert(data)
                                        end = time.time()
                                        return {"success": True, "latency": end - start}
                                    except Exception as e:
                                        return {"success": False, "error": str(e)}

                                # Define concurrency levels to test
                                concurrency_levels = [1, 2, 5, 10, 20, 50]
                                rate_limit_detected = False
                                best_qps = 0
                                best_concurrency = 1
                                previous_success_rate = 100

                                print(
                                    f"\n{provider} - {model_name} - Concurrency scaling test:"
                                )

                                # Test each concurrency level
                                for concurrency in concurrency_levels:
                                    if rate_limit_detected:
                                        break

                                    # Run concurrent test
                                    concurrent_results = []
                                    concurrent_start = time.time()

                                    with concurrent.futures.ThreadPoolExecutor(
                                        max_workers=concurrency
                                    ) as executor:
                                        future_to_idx = {
                                            executor.submit(
                                                concurrent_insert, i % concurrency, i
                                            ): i
                                            for i in range(concurrency)
                                        }
                                        for future in concurrent.futures.as_completed(
                                            future_to_idx
                                        ):
                                            idx = future_to_idx[future]
                                            try:
                                                result = future.result()
                                                result["idx"] = idx
                                                concurrent_results.append(result)
                                            except Exception as e:
                                                concurrent_results.append(
                                                    {
                                                        "idx": idx,
                                                        "success": False,
                                                        "error": str(e),
                                                    }
                                                )

                                    concurrent_end = time.time()
                                    concurrent_total_time = (
                                        concurrent_end - concurrent_start
                                    )

                                    # Calculate concurrent metrics
                                    successful = [
                                        r
                                        for r in concurrent_results
                                        if r.get("success", False)
                                    ]
                                    success_rate = (
                                        len(successful) / concurrency
                                        if concurrency > 0
                                        else 0
                                    )
                                    success_rate_pct = success_rate * 100
                                    avg_latency = (
                                        sum(r.get("latency", 0) for r in successful)
                                        / len(successful)
                                        if successful
                                        else 0
                                    )

                                    # Calculate QPS (Queries Per Second)
                                    qps = (
                                        len(successful) / concurrent_total_time
                                        if concurrent_total_time > 0
                                        else 0
                                    )

                                    # Check if this is the best QPS so far
                                    if (
                                        qps > best_qps and success_rate_pct >= 90
                                    ):  # Only consider if success rate is good
                                        best_qps = qps
                                        best_concurrency = concurrency

                                    # Check if we've hit a rate limit (success rate dropped significantly)
                                    if (
                                        previous_success_rate > 90
                                        and success_rate_pct < 70
                                    ):
                                        rate_limit_detected = True
                                        print(
                                            f"  Rate limit detected at concurrency {concurrency} (QPS: {qps:.2f})"
                                        )

                                    previous_success_rate = success_rate_pct

                                    # Collect error messages
                                    error_messages = [
                                        r.get("error", "")
                                        for r in concurrent_results
                                        if not r.get("success", False)
                                    ]
                                    error_message = (
                                        "; ".join(set(error_messages))
                                        if error_messages
                                        else ""
                                    )

                                    # Record results
                                    results.append(
                                        {
                                            "provider": provider,
                                            "model": model_name,
                                            "token_count": token_count,
                                            "token_name": token_name,
                                            "test_type": "concurrent",
                                            "concurrent_count": concurrency,
                                            "total_time": concurrent_total_time,
                                            "avg_latency": avg_latency,
                                            "qps": qps,
                                            "success_rate": success_rate_pct,
                                            "fail_rate": 100.0 - success_rate_pct,
                                            "error_message": error_message,
                                            "rate_limit_detected": rate_limit_detected,
                                            "status": "success",
                                        }
                                    )

                                    print(
                                        f"  Concurrency {concurrency}: QPS={qps:.2f}, Success={success_rate_pct:.1f}%, Avg Latency={avg_latency:.3f}s"
                                    )

                                    # Add a small delay between tests to avoid immediate rate limiting
                                    time.sleep(1)

                                # Record best QPS results
                                if best_qps > 0:
                                    print(
                                        f"  Best performance: {best_qps:.2f} QPS at concurrency {best_concurrency}"
                                    )
                                    results.append(
                                        {
                                            "provider": provider,
                                            "model": model_name,
                                            "token_count": token_count,
                                            "token_name": token_name,
                                            "test_type": "best_performance",
                                            "best_qps": best_qps,
                                            "best_concurrency": best_concurrency,
                                            "status": "success",
                                        }
                                    )

                            # Record results
                            results.append(
                                {
                                    "provider": provider,
                                    "model": model_name,
                                    "token_count": token_count,
                                    "token_name": token_name,
                                    "latency": latency,
                                    "tokens_per_second": token_count / latency,
                                    "test_type": "single",  # Add test_type field
                                    "status": "success",
                                }
                            )

                            print(
                                f"{provider} - {model_name} - {token_name} ({token_count} tokens): {latency:.3f}s"
                            )

                        except Exception as e:
                            print(
                                f"Error testing {provider} - {model_name} with {token_count} tokens: {str(e)}"
                            )
                            results.append(
                                {
                                    "provider": provider,
                                    "model": model_name,
                                    "token_count": token_count,
                                    "token_name": token_name,
                                    "latency": None,
                                    "tokens_per_second": None,
                                    "test_type": "single",  # Add test_type field
                                    "status": f"error: {str(e)}",
                                }
                            )

                except Exception as e:
                    print(f"Error setting up {provider} - {model_name}: {str(e)}")
                    results.append(
                        {
                            "provider": provider,
                            "model": model_name,
                            "token_count": "N/A",
                            "token_name": "N/A",
                            "latency": None,
                            "tokens_per_second": None,
                            "test_type": "setup",  # Add test_type field
                            "status": f"setup error: {str(e)}",
                        }
                    )

                # Cleanup
                utility.drop_collection(collection_name)

        # Convert results to DataFrame for analysis
        df = pd.DataFrame(results)
        if not df.empty:
            # Create a new DataFrame for generating more intuitive tabular data
            performance_table = []

            # Process single token test results - check if test_type field exists
            # First add test_type field (if it doesn't exist)
            if "test_type" not in df.columns:
                df["test_type"] = "single"  # Default to single test

            single_tests = df[
                (df["status"] == "success")
                & (~df["test_type"].isin(["concurrent", "best_performance"]))
            ]
            for _, row in single_tests.iterrows():
                performance_table.append(
                    {
                        "Provider": row["provider"],
                        "Model Name": row["model"],
                        "Text Token": row["token_count"],
                        "Batch Size": 1,
                        "Concurrent": 1,
                        "Latency (avg)": row["latency"],
                        "Latency (min)": row["latency"],
                        "Latency (max)": row["latency"],
                        "QPS": 1 / row["latency"] if row["latency"] > 0 else 0,
                        "Success Rate": 100.0,
                        "Fail Rate": 0.0,
                        "Error Message": "",
                        "Rate Limit": "No",
                        "Token Limit": "No",
                    }
                )

            concurrent_tests = df[
                (df["status"] == "success") & (df["test_type"] == "concurrent")
            ]
            for _, row in concurrent_tests.iterrows():
                performance_table.append(
                    {
                        "Provider": row["provider"],
                        "Model Name": row["model"],
                        "Text Token": row["token_count"],
                        "Batch Size": 1,
                        "Concurrent": row["concurrent_count"],
                        "Latency (avg)": row["avg_latency"],
                        "Latency (min)": row["avg_latency"],
                        "Latency (max)": row["avg_latency"],
                        "QPS": row["qps"],
                        "Success Rate": row["success_rate"],
                        "Fail Rate": 100.0 - row["success_rate"],
                        "Error Message": "",
                        "Rate Limit": "Yes"
                        if row.get("rate_limit_detected", False)
                        else "No",
                        "Token Limit": "No",
                    }
                )

            error_tests = df[df["status"].str.contains("error")]
            for _, row in error_tests.iterrows():
                error_msg = row["status"].replace("error: ", "")
                token_limit = (
                    "Yes"
                    if "input must have less than 512 tokens" in error_msg
                    else "No"
                )

                performance_table.append(
                    {
                        "Provider": row["provider"],
                        "Model Name": row["model"],
                        "Text Token": row["token_count"],
                        "Batch Size": 1,
                        "Concurrent": 1,
                        "Latency (avg)": None,
                        "Latency (min)": None,
                        "Latency (max)": None,
                        "QPS": 0,
                        "Success Rate": 0.0,
                        "Fail Rate": 100.0,
                        "Error Message": error_msg,
                        "Rate Limit": "No",
                        "Token Limit": token_limit,
                    }
                )

            performance_df = pd.DataFrame(performance_table)

            performance_df = performance_df.sort_values(
                by=["Provider", "Model Name", "Text Token", "Concurrent"]
            )

            print("\nDetailed Performance Results:")
            pd.set_option("display.max_rows", None)
            pd.set_option("display.max_columns", None)
            pd.set_option("display.width", 1000)
            print(performance_df.to_string(index=False))

            import os
            from datetime import datetime

            results_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "results"
            )
            os.makedirs(results_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = os.path.join(
                results_dir, f"embedding_performance_{timestamp}.csv"
            )

            performance_df.to_csv(csv_filename, index=False)

            if "status" in df.columns:
                provider_summary = (
                    df[df["status"] == "success"]
                    .groupby("provider")["latency"]
                    .agg(["mean", "min", "max"])
                )
                summary_csv = os.path.join(
                    results_dir, f"provider_summary_{timestamp}.csv"
                )
                provider_summary.to_csv(summary_csv)

            print(f"\nResults saved to: {csv_filename}")

            print("\nPerformance Summary by Provider:")
            provider_summary = (
                df[df["status"] == "success"]
                .groupby("provider")["latency"]
                .agg(["mean", "min", "max"])
            )
            print(provider_summary)

            print("\nPerformance Summary by Model:")
            model_summary = (
                df[df["status"] == "success"]
                .groupby(["provider", "model"])["latency"]
                .agg(["mean", "min", "max"])
            )
            print(model_summary)
        else:
            print("No successful tests completed")
