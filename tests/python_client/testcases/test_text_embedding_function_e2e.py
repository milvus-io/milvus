# fmt: off
import random

import numpy as np
import pandas as pd
import pytest
from base.client_base import TestcaseBase
from common import common_func as cf
from common.common_type import CaseLabel, CheckTasks
from common.mock_tei_server import MockTEIServer, get_docker_host, get_local_ip
from faker import Faker
from pymilvus import (
    AnnSearchRequest,
    CollectionSchema,
    DataType,
    FieldSchema,
    Function,
    FunctionType,
    WeightedRanker,
)
from utils.util_log import test_log as log

fake_zh = Faker("zh_CN")
fake_jp = Faker("ja_JP")
fake_en = Faker("en_US")

pd.set_option("expand_frame_repr", False)

prefix = "text_embedding_collection"


# TEI: https://github.com/huggingface/text-embeddings-inference
# model id:BAAI/bge-base-en-v1.5
# dim: 768


@pytest.mark.tags(CaseLabel.L1)
class TestCreateCollectionWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test create collection with text embedding function
    ******************************************************************
    """

    def test_create_collection_with_text_embedding(self, tei_endpoint):
        """
        target: test create collection with text embedding function
        method: create collection with text embedding function
        expected: create collection successfully
        """
        dim = 768
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
                "provider": "TEI",
                "endpoint": tei_endpoint,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 1

    def test_create_collection_with_text_embedding_twice_with_same_schema(self, tei_endpoint):
        """
        target: test create collection with text embedding twice with same schema
        method: create collection with text embedding function, then create again
        expected: create collection successfully and create again successfully
        """
        dim = 768
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
                "provider": "TEI",
                "endpoint": tei_endpoint,
            },
        )
        schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name, schema=schema)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 1


@pytest.mark.tags(CaseLabel.L1)
class TestCreateCollectionWithTextEmbeddingNegative(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test create collection with text embedding negative
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_collection_with_text_embedding_unsupported_endpoint(self):
        """
        target: test create collection with text embedding with unsupported model
        method: create collection with text embedding function using unsupported model
        expected: create collection failed
        """
        dim = 768
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
                "provider": "TEI",
                "endpoint": "http://unsupported_endpoint",
            },
        )
        schema.add_function(text_embedding_function)

        self.init_collection_wrap(
            name=cf.gen_unique_str(prefix),
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 65535, "err_msg": "unsupported_endpoint"},
        )

    def test_create_collection_with_text_embedding_unmatched_dim(self, tei_endpoint):
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
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "TEI",
                "endpoint": tei_endpoint,
            },
        )
        schema.add_function(text_embedding_function)

        self.init_collection_wrap(
            name=cf.gen_unique_str(prefix),
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items={
                "err_code": 65535,
                "err_msg": f"the required embedding dim is [{dim}], but the embedding obtained from the model is [768]",
            },
        )


@pytest.mark.tags(CaseLabel.L0)
class TestInsertWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test insert with text embedding
    ******************************************************************
    """

    def test_insert_with_text_embedding(self, tei_endpoint):
        """
        target: test insert data with text embedding
        method: insert data with text embedding function
        expected: insert successfully
        """
        dim = 768
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
                "provider": "TEI",
                "endpoint": tei_endpoint,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)

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

    @pytest.mark.parametrize("truncate", [True, False])
    @pytest.mark.parametrize("truncation_direction", ["Left", "Right"])
    def test_insert_with_text_embedding_truncate(self, tei_endpoint, truncate, truncation_direction):
        """
        target: test insert data with text embedding
        method: insert data with text embedding function
        expected: insert successfully
        """
        dim = 768
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
                "provider": "TEI",
                "endpoint": tei_endpoint,
                "truncate": truncate,
                "truncation_direction": truncation_direction,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)

        # prepare data
        left = " ".join([fake_en.word() for _ in range(512)])
        right = " ".join([fake_en.word() for _ in range(512)])
        data = [{"id": 0, "document": left + " " + right}, {"id": 1, "document": left}, {"id": 2, "document": right}]
        res, result = collection_w.insert(data, check_task=CheckTasks.check_nothing)

        if not truncate:
            assert result is False
            print("truncate is False, should insert failed")
            return

        assert collection_w.num_entities == len(data)
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
        # compare similarity between left and right using cosine similarity
        import numpy as np

        # Calculate cosine similarity: cos(θ) = A·B / (||A|| * ||B||)
        # when direction is left, right part is reversed
        similarity_left = np.dot(res[0]["dense"], res[1]["dense"]) / (
            np.linalg.norm(res[0]["dense"]) * np.linalg.norm(res[1]["dense"])
        )
        # when direction is right, left part is reversed
        similarity_right = np.dot(res[0]["dense"], res[2]["dense"]) / (
            np.linalg.norm(res[0]["dense"]) * np.linalg.norm(res[2]["dense"])
        )
        if truncation_direction == "Left":
            assert similarity_left < similarity_right
        else:
            assert similarity_left > similarity_right


@pytest.mark.tags(CaseLabel.L2)
class TestInsertWithTextEmbeddingNegative(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test insert with text embedding negative
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip("not support empty document now")
    def test_insert_with_text_embedding_empty_document(self, tei_endpoint):
        """
        target: test insert data with empty document
        method: insert data with empty document
        expected: insert failed
        """
        dim = 768
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
                "provider": "TEI",
                "endpoint": tei_endpoint,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)

        # prepare data with empty document
        empty_data = [{"id": 1, "document": ""}]
        normal_data = [{"id": 2, "document": fake_en.text()}]
        data = empty_data + normal_data

        collection_w.insert(
            data,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 65535, "err_msg": "cannot be empty"},
        )
        assert collection_w.num_entities == 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip("TODO")
    def test_insert_with_text_embedding_long_document(self, tei_endpoint):
        """
        target: test insert data with long document
        method: insert data with long document
        expected: insert failed
        """
        dim = 768
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
                "provider": "TEI",
                "endpoint": tei_endpoint,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)

        # prepare data with empty document
        long_data = [{"id": 1, "document": " ".join([fake_en.word() for _ in range(8192)])}]
        normal_data = [{"id": 2, "document": fake_en.text()}]
        data = long_data + normal_data

        collection_w.insert(
            data,
            check_task=CheckTasks.err_res,
            check_items={
                "err_code": 65535,
                "err_msg": "call service failed",
            },
        )
        assert collection_w.num_entities == 0


@pytest.mark.tags(CaseLabel.L1)
class TestUpsertWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test upsert with text embedding
    ******************************************************************
    """

    def test_upsert_text_field(self, tei_endpoint):
        """
        target: test upsert text field updates embedding
        method: 1. insert data
                2. upsert text field
                3. verify embedding is updated
        expected: embedding should be updated after text field is updated
        """
        dim = 768
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
                "provider": "TEI",
                "endpoint": tei_endpoint,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)
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
        sim = np.dot(old_embedding, new_embedding) / (np.linalg.norm(old_embedding) * np.linalg.norm(new_embedding))
        log.info(f"cosine similarity: {sim}")
        assert sim < 0.99


@pytest.mark.tags(CaseLabel.L1)
class TestDeleteWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test delete with text embedding
    ******************************************************************
    """

    def test_delete_and_search(self, tei_endpoint):
        """
        target: test deleted text cannot be searched
        method: 1. insert data
                2. delete some data
                3. verify deleted data cannot be searched
        expected: deleted data should not appear in search results
        """
        dim = 768
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
                "provider": "TEI",
                "endpoint": tei_endpoint,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)

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


@pytest.mark.tags(CaseLabel.L0)
class TestSearchWithTextEmbedding(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test search with text embedding
    ******************************************************************
    """

    def test_search_with_text_embedding(self, tei_endpoint):
        """
        target: test search with text embedding
        method: search with text embedding function
        expected: search successfully
        """
        dim = 768
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
                "provider": "TEI",
                "endpoint": tei_endpoint,
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)

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


@pytest.mark.tags(CaseLabel.L1)
class TestSearchWithTextEmbeddingNegative(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test search with text embedding negative
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("query", ["empty_query", "long_query"])
    @pytest.mark.skip("not support empty query now")
    def test_search_with_text_embedding_negative_query(self, query, tei_endpoint):
        """
        target: test search with empty query or long query
        method: search with empty query
        expected: search failed
        """
        if query == "empty_query":
            query = ""
        if query == "long_query":
            query = " ".join([fake_en.word() for _ in range(8192)])
        dim = 768
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
            params={"provider": "TEI", "endpoint": tei_endpoint},
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)

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

        # search with empty query should fail
        search_params = {"metric_type": "COSINE", "params": {}}
        collection_w.search(
            data=[query],
            anns_field="dense",
            param=search_params,
            limit=3,
            output_fields=["document"],
            check_task=CheckTasks.err_res,
            check_items={"err_code": 65535, "err_msg": "call service failed"},
        )


@pytest.mark.tags(CaseLabel.L1)
class TestHybridSearch(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test hybrid search
    ******************************************************************
    """

    def test_hybrid_search(self, tei_endpoint):
        """
        target: test hybrid search with text embedding and BM25
        method: 1. create collection with text embedding and BM25 functions
                2. insert data
                3. perform hybrid search
        expected: search results should combine vector similarity and text relevance
        """
        dim = 768
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
            params={"provider": "TEI", "endpoint": tei_endpoint},
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

        collection_w = self.init_collection_wrap(name=cf.gen_unique_str(prefix), schema=schema)

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


@pytest.mark.tags(CaseLabel.L1)
class TestTextEmbeddingFunctionCURD(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test add/alter/drop collection function APIs
    ******************************************************************
    """

    # ============ deprecated add/drop_collection_function RPCs (rejected) ============
    # A function is coupled to its output field: BM25/MinHash are added via
    # add_function_field, TextEmbedding is defined at collection creation, and a
    # function is always dropped together with its output field (drop_function_field).
    # The legacy attach (add_collection_function) / detach (drop_collection_function)
    # RPCs are therefore rejected.

    def test_add_collection_function_rejected(self, tei_endpoint):
        """
        target: add_collection_function (legacy attach RPC) is rejected
        method: create a collection, then call add_collection_function
        expected: rejected - no longer supported (use add_function_field, or define
                  a TextEmbedding function at collection creation)
        """
        self._connect()
        dim = 768
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name, schema=schema)

        embedding_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint},
        )
        try:
            self.client.add_collection_function(collection_name=c_name, function=embedding_function)
            assert False, "Expected exception: add_collection_function is no longer supported"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert "no longer supported" in str(e)

    def test_drop_collection_function_rejected(self, tei_endpoint):
        """
        target: drop_collection_function (legacy detach RPC) is rejected
        method: create a collection with a TextEmbedding function, call drop_collection_function
        expected: rejected - detaching a function without dropping its output field is
                  not supported (use drop_function_field)
        """
        self._connect()
        dim = 768
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
            params={"provider": "TEI", "endpoint": tei_endpoint},
        )
        schema.add_function(text_embedding_function)
        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name, schema=schema)

        try:
            self.client.drop_collection_function(collection_name=c_name, function_name="tei")
            assert False, "Expected exception: drop_collection_function is no longer supported"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert "not supported" in str(e)

    # ==================== alter_collection_function tests ====================

    def test_alter_collection_function_change_endpoint(self, tei_endpoint):
        """
        target: test alter function to change endpoint
        method: create collection with function, alter function endpoint
        expected: endpoint changed successfully
        """
        self._connect()
        dim = 768
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
            params={"provider": "TEI", "endpoint": tei_endpoint},
        )
        schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # Alter function with same endpoint (just testing the API works)
        new_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint},
        )
        self.client.alter_collection_function(collection_name=c_name, function_name="tei", function=new_function)

        # Verify function still exists and params are correct
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 1
        func = res["functions"][0]
        assert func["name"] == "tei"
        assert func["params"]["provider"] == "TEI"
        assert func["params"]["endpoint"] == tei_endpoint

    def test_alter_collection_function_change_params(self, tei_endpoint):
        """
        target: test altering semantic params (truncate/truncation_direction) is rejected
        method: create collection with function, try to alter truncate params
        expected: rejected - truncate/truncation_direction change the embedding of
                  over-length inputs, so they are immutable (altering them would mix
                  vector semantics in the same output field)
        """
        self._connect()
        dim = 768
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
            params={"provider": "TEI", "endpoint": tei_endpoint},
        )
        schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # truncate / truncation_direction are semantic params (immutable): altering them
        # is rejected.
        new_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint, "truncate": True, "truncation_direction": "Left"},
        )
        try:
            self.client.alter_collection_function(collection_name=c_name, function_name="tei", function=new_function)
            assert False, "Expected exception: truncate/truncation_direction are immutable"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert "cannot be altered" in str(e)

        # function params unchanged
        res, _ = collection_w.describe()
        func = res["functions"][0]
        assert "truncate" not in func["params"]

    def test_alter_collection_function_verify_crud(self, tei_endpoint):
        """
        target: test the function keeps serving all CRUD after a rejected immutable-param alter
        method: create collection with function, insert data, attempt an immutable-param
                (truncate) alter which is rejected, then verify all CRUD operations
        expected: alter is rejected and the intact function keeps serving CRUD
        """
        self._connect()
        dim = 768
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
            params={"provider": "TEI", "endpoint": tei_endpoint},
        )
        schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # === INSERT before alter ===
        data1 = [{"id": i, "document": f"Document before alter {i}"} for i in range(5)]
        collection_w.insert(data1)

        # Create index and load
        index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "COSINE",
            "params": {},
        }
        collection_w.create_index("dense", index_params)
        collection_w.load()

        # Get embedding before alter for comparison
        res_before, _ = collection_w.query(expr="id == 0", output_fields=["dense"])
        embedding_before_alter = res_before[0]["dense"]

        # === ALTER FUNCTION: truncate is a semantic param (immutable) -> rejected;
        # the function stays intact and CRUD keeps working below ===
        new_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint, "truncate": True},
        )
        try:
            self.client.alter_collection_function(collection_name=c_name, function_name="tei", function=new_function)
            assert False, "Expected exception: truncate is immutable"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert "cannot be altered" in str(e)

        # === INSERT after alter ===
        data2 = [{"id": i + 5, "document": f"Document after alter {i}"} for i in range(5)]
        collection_w.insert(data2)
        assert collection_w.num_entities == 10

        # === QUERY - verify all data accessible ===
        res, _ = collection_w.query(expr="id >= 0", output_fields=["dense", "document"])
        assert len(res) == 10
        for row in res:
            assert len(row["dense"]) == dim

        # === SEARCH with text ===
        search_params = {"metric_type": "COSINE", "params": {}}
        res, _ = collection_w.search(
            data=["Document after alter"],
            anns_field="dense",
            param=search_params,
            limit=10,
            output_fields=["document"],
        )
        assert len(res[0]) == 10

        # === UPSERT - update existing record after alter ===
        upsert_data = [{"id": 0, "document": "Completely new document after alter"}]
        collection_w.upsert(upsert_data)

        res_after_upsert, _ = collection_w.query(expr="id == 0", output_fields=["dense"])
        embedding_after_upsert = res_after_upsert[0]["dense"]

        # Verify embedding changed
        assert not np.allclose(embedding_before_alter, embedding_after_upsert)

        # === UPSERT - insert new record after alter ===
        upsert_new = [{"id": 100, "document": "Brand new document via upsert after alter"}]
        collection_w.upsert(upsert_new)
        count_res, _ = collection_w.query(expr="", output_fields=["count(*)"])
        assert count_res[0]["count(*)"] == 11

        res, _ = collection_w.query(expr="id == 100", output_fields=["dense"])
        assert len(res[0]["dense"]) == dim

        # === DELETE after alter ===
        collection_w.delete("id in [1, 2]")

        # Verify deleted records not in search results
        res, _ = collection_w.search(
            data=["Document before alter 1"],
            anns_field="dense",
            param=search_params,
            limit=10,
            output_fields=["id"],
        )
        for hit in res[0]:
            assert hit.entity.get("id") not in {1, 2}

        # Verify count
        res, _ = collection_w.query(expr="id >= 0", output_fields=["id"])
        assert len(res) == 9  # 10 + 1 - 2

    # ==================== alter_collection_function L3 tests ====================

    @pytest.mark.tags(CaseLabel.L3)
    def test_alter_collection_function_change_to_different_endpoint(self, tei_endpoint, tei_endpoint_2):
        """
        target: test altering to a different endpoint is rejected (endpoint immutable)
        method: create with endpoint1, insert, attempt to alter to endpoint2 (rejected)
        expected: rejected; the function keeps using endpoint1 and serving CRUD
        """
        self._connect()
        dim = 768
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
            params={"provider": "TEI", "endpoint": tei_endpoint},
        )
        schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # Insert data with original endpoint
        data1 = [{"id": i, "document": f"Document with endpoint1 {i}"} for i in range(3)]
        collection_w.insert(data1)

        # Create index and load
        index_params = {"index_type": "AUTOINDEX", "metric_type": "COSINE", "params": {}}
        collection_w.create_index("dense", index_params)
        collection_w.load()

        # endpoint is immutable: altering to a different endpoint is rejected.
        new_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint_2},
        )
        try:
            self.client.alter_collection_function(collection_name=c_name, function_name="tei", function=new_function)
            assert False, "Expected exception: endpoint is immutable"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert "cannot be altered" in str(e)

        # function still uses the original endpoint and keeps serving CRUD
        data2 = [{"id": i + 10, "document": f"Document with endpoint1 again {i}"} for i in range(3)]
        collection_w.insert(data2)
        assert collection_w.num_entities == 6

    @pytest.mark.tags(CaseLabel.L3)
    def test_alter_function_when_other_function_is_invalid(self, host):
        """
        target: test alter function succeeds even when another function in collection is invalid
        method:
            1. create collection with 2 text embedding functions using mock TEI servers
            2. make one mock server return errors (simulate service becoming unavailable)
            3. alter the other valid function
        expected: alter should succeed (only validates the target function, not all functions)
        issue: https://github.com/milvus-io/milvus/issues/46949
        pr: https://github.com/milvus-io/milvus/pull/46984

        NOTE: This test requires the Milvus server to be able to access the local mock TEI server.
        - localhost/127.0.0.1: uses host.docker.internal for Docker containers
        - Remote host: skipped (network may not be reachable)
        """
        # Skip if Milvus is on remote host (network may not be reachable)
        local_ip = get_local_ip()
        docker_host = get_docker_host()
        if host not in ["localhost", "127.0.0.1", local_ip, docker_host]:
            pytest.skip(
                f"Skipping: Milvus host ({host}) may not be able to access local mock server. "
                "Run this test with Milvus on localhost or in the same network."
            )

        self._connect()
        dim = 768

        # Start two mock TEI servers with external access enabled
        # host='0.0.0.0' binds to all interfaces, external_host='docker' uses host.docker.internal
        mock_server_1 = MockTEIServer(dim=dim, host="0.0.0.0", external_host="docker")
        mock_server_2 = MockTEIServer(dim=dim, host="0.0.0.0", external_host="docker")

        try:
            endpoint_1 = mock_server_1.start()
            endpoint_2 = mock_server_2.start()
            log.info(f"Mock TEI servers started at: {endpoint_1}, {endpoint_2}")

            fields = [
                FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
                FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=65535),
                FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=65535),
                FieldSchema(name="title_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
                FieldSchema(name="content_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
            ]
            schema = CollectionSchema(fields=fields, description="test collection")

            # Add two text embedding functions using different mock servers
            title_embedding = Function(
                name="title_embedding",
                function_type=FunctionType.TEXTEMBEDDING,
                input_field_names=["title"],
                output_field_names="title_vector",
                params={"provider": "TEI", "endpoint": endpoint_1},
            )
            schema.add_function(title_embedding)

            content_embedding = Function(
                name="content_embedding",
                function_type=FunctionType.TEXTEMBEDDING,
                input_field_names=["content"],
                output_field_names="content_vector",
                params={"provider": "TEI", "endpoint": endpoint_2},
            )
            schema.add_function(content_embedding)

            c_name = cf.gen_unique_str(prefix)
            collection_w = self.init_collection_wrap(name=c_name, schema=schema)

            # Verify both functions exist
            res, _ = collection_w.describe()
            assert len(res["functions"]) == 2

            # Make server_1 return errors (simulate "Model integration is not active")
            mock_server_1.set_error_mode(enabled=True, status_code=400, message="Model integration is not active")

            # Now title_embedding function is invalid, but we should still be able to
            # alter content_embedding. Alter a whitelisted connection param (timeout_ms),
            # since semantic params (truncate/endpoint/...) are immutable.
            new_content_embedding = Function(
                name="content_embedding",
                function_type=FunctionType.TEXTEMBEDDING,
                input_field_names=["content"],
                output_field_names="content_vector",
                params={"provider": "TEI", "endpoint": endpoint_2, "timeout_ms": "30000"},
            )

            # This should succeed: altering the target function must not be blocked by
            # the other function being invalid (validate only the target function).
            self.client.alter_collection_function(
                collection_name=c_name, function_name="content_embedding", function=new_content_embedding
            )

            # alter succeeded (did not raise) and both functions are still present
            res, _ = collection_w.describe()
            assert len(res["functions"]) == 2

            log.info("Successfully altered content_embedding function while title_embedding is invalid")

        finally:
            mock_server_1.stop()
            mock_server_2.stop()

    @pytest.mark.tags(CaseLabel.L3)
    def test_alter_invalid_function_to_valid_endpoint(self, host):
        """
        target: test that repointing a broken function to a new endpoint is rejected
        method:
            1. create collection with function using mock TEI server
            2. make mock server return errors (function becomes invalid)
            3. try to alter the function to a new valid endpoint
        expected: rejected - endpoint is the TEI model identity and immutable. This
                  supersedes the earlier #46984 repoint flow; a broken endpoint must be
                  fixed at the infra level, or the function dropped and re-added.
        issue: https://github.com/milvus-io/milvus/issues/46949

        NOTE: This test requires the Milvus server to be able to access the local mock TEI server.
        - localhost/127.0.0.1: uses host.docker.internal for Docker containers
        - Remote host: skipped (network may not be reachable)
        """
        # Skip if Milvus is on remote host (network may not be reachable)
        local_ip = get_local_ip()
        docker_host = get_docker_host()
        if host not in ["localhost", "127.0.0.1", local_ip, docker_host]:
            pytest.skip(
                f"Skipping: Milvus host ({host}) may not be able to access local mock server. "
                "Run this test with Milvus on localhost or in the same network."
            )

        self._connect()
        dim = 768

        # Start mock servers with external access enabled
        mock_server = MockTEIServer(dim=dim, host="0.0.0.0", external_host="docker")
        backup_server = MockTEIServer(dim=dim, host="0.0.0.0", external_host="docker")

        try:
            endpoint = mock_server.start()
            backup_endpoint = backup_server.start()
            log.info(f"Mock TEI servers started at: {endpoint}, {backup_endpoint}")

            fields = [
                FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
                FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
                FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
            ]
            schema = CollectionSchema(fields=fields, description="test collection")

            text_embedding = Function(
                name="tei",
                function_type=FunctionType.TEXTEMBEDDING,
                input_field_names=["document"],
                output_field_names="dense",
                params={"provider": "TEI", "endpoint": endpoint},
            )
            schema.add_function(text_embedding)

            c_name = cf.gen_unique_str(prefix)
            collection_w = self.init_collection_wrap(name=c_name, schema=schema)

            # Insert some data while function is working
            data = [{"id": i, "document": f"Document {i}"} for i in range(3)]
            collection_w.insert(data)

            # Create index and load
            index_params = {"index_type": "AUTOINDEX", "metric_type": "COSINE", "params": {}}
            collection_w.create_index("dense", index_params)
            collection_w.load()

            # Simulate the original endpoint becoming unavailable
            mock_server.set_error_mode(enabled=True, status_code=400, message="Model integration is not active")

            # endpoint is immutable: repointing to a different endpoint is rejected,
            # even to recover a broken function (supersedes #46984).
            new_function = Function(
                name="tei",
                function_type=FunctionType.TEXTEMBEDDING,
                input_field_names=["document"],
                output_field_names="dense",
                params={"provider": "TEI", "endpoint": backup_endpoint},
            )
            try:
                self.client.alter_collection_function(collection_name=c_name, function_name="tei", function=new_function)
                assert False, "Expected exception: endpoint is immutable"
            except Exception as e:
                log.info(f"Expected error: {e}")
                assert "cannot be altered" in str(e)

            # function still points at the original endpoint (unchanged)
            res, _ = collection_w.describe()
            func = res["functions"][0]
            assert func["params"]["endpoint"] == endpoint

        finally:
            mock_server.stop()
            backup_server.stop()


    def test_alter_collection_function_nonexistent_collection(self, tei_endpoint):
        """
        target: test alter function on nonexistent collection
        method: call alter_collection_function on collection that doesn't exist
        expected: error with collection not found (code=100)
        """
        self._connect()
        new_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint},
        )

        try:
            self.client.alter_collection_function(
                collection_name="nonexistent_collection_12345", function_name="tei", function=new_function
            )
            assert False, "Expected exception for nonexistent collection"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert e.code == 100
            assert "collection not found" in str(e)

    def test_alter_collection_function_nonexistent_function(self, tei_endpoint):
        """
        target: test alter function that doesn't exist
        method: create collection without function, try to alter non-existent function
        expected: error indicating function not found (code=1100)
        """
        self._connect()
        dim = 768
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name, schema=schema)

        new_function = Function(
            name="nonexistent_function",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint},
        )

        try:
            self.client.alter_collection_function(
                collection_name=c_name, function_name="nonexistent_function", function=new_function
            )
            assert False, "Expected exception for nonexistent function"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert e.code == 1100
            assert "not found" in str(e)

    def test_alter_collection_function_invalid_new_endpoint(self, tei_endpoint):
        """
        target: test altering the endpoint is rejected (endpoint is immutable)
        method: create collection with valid function, try to alter to a different endpoint
        expected: rejected before any connect attempt - for TEI the endpoint is the
                  model's identity, so it is immutable
        """
        self._connect()
        dim = 768
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
            params={"provider": "TEI", "endpoint": tei_endpoint},
        )
        schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name, schema=schema)

        # endpoint is immutable, so altering it (even to an invalid one) is rejected
        # before any connect attempt.
        new_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": "http://invalid_endpoint_12345"},
        )

        try:
            self.client.alter_collection_function(collection_name=c_name, function_name="tei", function=new_function)
            assert False, "Expected exception: endpoint is immutable"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert "cannot be altered" in str(e)
