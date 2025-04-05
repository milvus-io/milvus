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
from common.common_type import CaseLabel, CheckTasks
from common import common_func as cf
from utils.util_log import test_log as log
from base.client_base import TestcaseBase
import numpy as np
import pytest
import pandas as pd
from faker import Faker

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
            }
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 1

    def test_create_collection_with_text_embedding_twice_with_same_schema(
            self, tei_endpoint
    ):
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
                "err_msg": f"The required embedding dim is [{dim}], but the embedding obtained from the model is [768]",
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
                "truncation_direction": truncation_direction
            },
        )
        schema.add_function(text_embedding_function)

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data
        left = " ".join([fake_en.word() for _ in range(512)])
        right = " ".join([fake_en.word() for _ in range(512)])
        data = [
            {
                "id": 0,
                "document": left + " " + right
            },
            {
                "id": 1,
                "document": left
            },
            {
                "id": 2,
                "document": right
            }]
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
                    np.linalg.norm(res[0]["dense"]) * np.linalg.norm(res[1]["dense"]))
        # when direction is right, left part is reversed
        similarity_right = np.dot(res[0]["dense"], res[2]["dense"]) / (
                    np.linalg.norm(res[0]["dense"]) * np.linalg.norm(res[2]["dense"]))
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

        collection_w = self.init_collection_wrap(
            name=cf.gen_unique_str(prefix), schema=schema
        )

        # prepare data with empty document
        long_data = [{"id": 1, "document": " ".join([fake_en.word() for _ in range(8192)])}]
        normal_data = [{"id": 2, "document": fake_en.text()}]
        data = long_data + normal_data

        collection_w.insert(
            data,
            check_task=CheckTasks.err_res,
            check_items={
                "err_code": 65535,
                "err_msg": "Call service faild",
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
            params={
                "provider": "TEI",
                "endpoint": tei_endpoint
            }
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
            params={
                "provider": "TEI",
                "endpoint": tei_endpoint
            }
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
            collection_w.insert(data[batch: batch + 100])

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
