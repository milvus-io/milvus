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
from common.mock_tei_server import MockTEIServer, get_local_ip, get_docker_host

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


@pytest.mark.tags(CaseLabel.L1)
class TestTextEmbeddingFunctionCURD(TestcaseBase):
    """
    ******************************************************************
      The following cases are used to test add/alter/drop collection function APIs
    ******************************************************************
    """

    # ==================== add_collection_function positive tests ====================

    def test_add_collection_function_text_embedding(self, tei_endpoint):
        """
        target: test add text embedding function to existing collection
        method: create collection without function, then add function via API
        expected: function added successfully, describe shows 1 function
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
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # Verify no functions initially
        res, _ = collection_w.describe()
        assert len(res.get("functions", [])) == 0

        # Create and add function
        embedding_function = Function(
            name="text_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        self.client.add_collection_function(
            collection_name=c_name,
            function=embedding_function
        )

        # Verify function is added
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 1
        assert res["functions"][0]["name"] == "text_embedding"

    def test_add_collection_function_then_crud(self, tei_endpoint):
        """
        target: test that added function works for all CRUD operations
        method: create collection without function, add function, then verify insert/query/search/upsert/delete
        expected: all CRUD operations work correctly with dynamically added function
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
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # Add function
        embedding_function = Function(
            name="text_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        self.client.add_collection_function(
            collection_name=c_name,
            function=embedding_function
        )

        # === INSERT ===
        nb = 10
        data = [{"id": i, "document": f"This is document number {i}"} for i in range(nb)]
        collection_w.insert(data)
        assert collection_w.num_entities == nb

        # Create index and load
        index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "COSINE",
            "params": {},
        }
        collection_w.create_index("dense", index_params)
        collection_w.load()

        # === QUERY ===
        res, _ = collection_w.query(expr="id >= 0", output_fields=["dense", "document"])
        assert len(res) == nb
        for row in res:
            assert len(row["dense"]) == dim

        # === SEARCH with text ===
        search_params = {"metric_type": "COSINE", "params": {}}
        res, _ = collection_w.search(
            data=["document number 5"],
            anns_field="dense",
            param=search_params,
            limit=5,
            output_fields=["document"],
        )
        assert len(res) == 1
        assert len(res[0]) == 5

        # === UPSERT - update existing record ===
        old_res, _ = collection_w.query(expr="id == 0", output_fields=["dense"])
        old_embedding = old_res[0]["dense"]

        upsert_data = [{"id": 0, "document": "This is a completely different updated text"}]
        collection_w.upsert(upsert_data)

        new_res, _ = collection_w.query(expr="id == 0", output_fields=["dense"])
        new_embedding = new_res[0]["dense"]

        # Verify embedding changed after upsert
        assert not np.allclose(old_embedding, new_embedding)

        # === UPSERT - insert new record ===
        upsert_new_data = [{"id": 100, "document": "This is a brand new document"}]
        collection_w.upsert(upsert_new_data)
        count_res, _ = collection_w.query(expr="", output_fields=["count(*)"])
        assert count_res[0]["count(*)"] == nb + 1

        # Verify new record has vector
        res, _ = collection_w.query(expr="id == 100", output_fields=["dense"])
        assert len(res) == 1
        assert len(res[0]["dense"]) == dim

        # === DELETE ===
        collection_w.delete("id in [1, 2, 3]")

        # Verify deleted records are not searchable
        res, _ = collection_w.search(
            data=["document number 1"],
            anns_field="dense",
            param=search_params,
            limit=10,
            output_fields=["id"],
        )
        deleted_ids = {1, 2, 3}
        for hit in res[0]:
            assert hit.entity.get("id") not in deleted_ids

        # Verify count decreased
        res, _ = collection_w.query(expr="id >= 0", output_fields=["id"])
        assert len(res) == nb + 1 - 3  # original + 1 upserted - 3 deleted

    def test_add_collection_function_multiple_text_embedding(self, tei_endpoint):
        """
        target: test add multiple text embedding functions to different output fields
        method: create collection with two vector fields, add text_embedding function to each
        expected: both functions added successfully
        """
        self._connect()
        dim = 768
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="title_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="content_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # Add text embedding function for title
        title_embedding_function = Function(
            name="title_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["title"],
            output_field_names="title_vector",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        self.client.add_collection_function(
            collection_name=c_name,
            function=title_embedding_function
        )

        # Add text embedding function for content
        content_embedding_function = Function(
            name="content_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["content"],
            output_field_names="content_vector",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        self.client.add_collection_function(
            collection_name=c_name,
            function=content_embedding_function
        )

        # Verify both functions are added
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 2
        function_names = [f["name"] for f in res["functions"]]
        assert "title_embedding" in function_names
        assert "content_embedding" in function_names

        # Verify CRUD works with both functions
        # Insert
        nb = 5
        data = [{"id": i, "title": fake_en.sentence(), "content": fake_en.text()} for i in range(nb)]
        collection_w.insert(data)
        assert collection_w.num_entities == nb

        # Create index and load
        index_params = {"index_type": "AUTOINDEX", "metric_type": "COSINE", "params": {}}
        collection_w.create_index("title_vector", index_params)
        collection_w.create_index("content_vector", index_params)
        collection_w.load()

        # Query - verify vectors are generated
        res, _ = collection_w.query(expr="id >= 0", output_fields=["title_vector", "content_vector"])
        for row in res:
            assert len(row["title_vector"]) == dim
            assert len(row["content_vector"]) == dim

        # Search on both vector fields
        search_params = {"metric_type": "COSINE", "params": {}}
        res, _ = collection_w.search(
            data=[fake_en.sentence()],
            anns_field="title_vector",
            param=search_params,
            limit=3,
        )
        assert len(res[0]) == 3

        res, _ = collection_w.search(
            data=[fake_en.text()],
            anns_field="content_vector",
            param=search_params,
            limit=3,
        )
        assert len(res[0]) == 3

    # ==================== alter_collection_function positive tests ====================

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
            params={"provider": "TEI", "endpoint": tei_endpoint}
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
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        self.client.alter_collection_function(
            collection_name=c_name,
            function_name="tei",
            function=new_function
        )

        # Verify function still exists and params are correct
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 1
        func = res["functions"][0]
        assert func["name"] == "tei"
        assert func["params"]["provider"] == "TEI"
        assert func["params"]["endpoint"] == tei_endpoint

    def test_alter_collection_function_change_params(self, tei_endpoint):
        """
        target: test alter function parameters (truncate settings)
        method: create collection with function, alter truncate params
        expected: params changed successfully
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
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # Alter function with new truncate params
        new_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "TEI",
                "endpoint": tei_endpoint,
                "truncate": True,
                "truncation_direction": "Left"
            }
        )
        self.client.alter_collection_function(
            collection_name=c_name,
            function_name="tei",
            function=new_function
        )

        # Verify function params are updated correctly
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 1
        func = res["functions"][0]
        assert func["name"] == "tei"
        assert func["params"]["provider"] == "TEI"
        assert func["params"]["endpoint"] == tei_endpoint
        # Note: params values are returned as strings
        assert func["params"]["truncate"] == "True"
        assert func["params"]["truncation_direction"] == "Left"

    def test_alter_collection_function_verify_crud(self, tei_endpoint):
        """
        target: test altered function works correctly for all CRUD operations
        method: create collection with function, insert data, alter function, verify all CRUD operations
        expected: all CRUD operations continue to work after function alteration
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
            params={"provider": "TEI", "endpoint": tei_endpoint}
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

        # === ALTER FUNCTION ===
        new_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={
                "provider": "TEI",
                "endpoint": tei_endpoint,
                "truncate": True
            }
        )
        self.client.alter_collection_function(
            collection_name=c_name,
            function_name="tei",
            function=new_function
        )

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
        target: test alter function to use a different valid endpoint
        method: create collection with function using endpoint1, alter to endpoint2, verify CRUD
        expected: function works with new endpoint after alteration
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
            params={"provider": "TEI", "endpoint": tei_endpoint}
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

        # Alter to use different endpoint
        new_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint_2}
        )
        self.client.alter_collection_function(
            collection_name=c_name,
            function_name="tei",
            function=new_function
        )

        # Insert data with new endpoint
        data2 = [{"id": i + 10, "document": f"Document with endpoint2 {i}"} for i in range(3)]
        collection_w.insert(data2)
        assert collection_w.num_entities == 6

        # Search should work
        search_params = {"metric_type": "COSINE", "params": {}}
        res, _ = collection_w.search(
            data=["Document with endpoint2"],
            anns_field="dense",
            param=search_params,
            limit=6,
            output_fields=["document"],
        )
        assert len(res[0]) == 6

        # Upsert should work with new endpoint
        upsert_data = [{"id": 0, "document": "Updated document with new endpoint"}]
        collection_w.upsert(upsert_data)

        res, _ = collection_w.query(expr="id == 0", output_fields=["document"])
        assert "Updated document" in res[0]["document"]

    @pytest.mark.tags(CaseLabel.L1)
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
        if host not in ['localhost', '127.0.0.1', local_ip, docker_host]:
            pytest.skip(f"Skipping: Milvus host ({host}) may not be able to access local mock server. "
                       "Run this test with Milvus on localhost or in the same network.")

        self._connect()
        dim = 768

        # Start two mock TEI servers with external access enabled
        # host='0.0.0.0' binds to all interfaces, external_host='docker' uses host.docker.internal
        mock_server_1 = MockTEIServer(dim=dim, host='0.0.0.0', external_host='docker')
        mock_server_2 = MockTEIServer(dim=dim, host='0.0.0.0', external_host='docker')

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
                params={"provider": "TEI", "endpoint": endpoint_1}
            )
            schema.add_function(title_embedding)

            content_embedding = Function(
                name="content_embedding",
                function_type=FunctionType.TEXTEMBEDDING,
                input_field_names=["content"],
                output_field_names="content_vector",
                params={"provider": "TEI", "endpoint": endpoint_2}
            )
            schema.add_function(content_embedding)

            c_name = cf.gen_unique_str(prefix)
            collection_w = self.init_collection_wrap(name=c_name, schema=schema)

            # Verify both functions exist
            res, _ = collection_w.describe()
            assert len(res["functions"]) == 2

            # Make server_1 return errors (simulate "Model integration is not active")
            mock_server_1.set_error_mode(
                enabled=True,
                status_code=400,
                message="Model integration is not active"
            )

            # Now title_embedding function is invalid, but we should still be able to
            # alter content_embedding function (PR #46984 fix)
            new_content_embedding = Function(
                name="content_embedding",
                function_type=FunctionType.TEXTEMBEDDING,
                input_field_names=["content"],
                output_field_names="content_vector",
                params={
                    "provider": "TEI",
                    "endpoint": endpoint_2,
                    "truncate": True
                }
            )

            # This should succeed after PR #46984 fix
            # Before the fix, this would fail because it validated ALL functions
            self.client.alter_collection_function(
                collection_name=c_name,
                function_name="content_embedding",
                function=new_content_embedding
            )

            # Verify function params are updated
            res, _ = collection_w.describe()
            content_func = next(f for f in res["functions"] if f["name"] == "content_embedding")
            assert content_func["params"]["truncate"] == str(True)

            log.info("Successfully altered content_embedding function while title_embedding is invalid")

        finally:
            mock_server_1.stop()
            mock_server_2.stop()

    @pytest.mark.tags(CaseLabel.L1)
    def test_alter_invalid_function_to_valid_endpoint(self, host):
        """
        target: test user can fix an invalid function by altering it to a valid endpoint
        method:
            1. create collection with function using mock TEI server
            2. make mock server return errors (function becomes invalid)
            3. start a new valid server and alter function to use it
        expected: alter should succeed, allowing user to fix the broken function
        issue: https://github.com/milvus-io/milvus/issues/46949
        pr: https://github.com/milvus-io/milvus/pull/46984

        NOTE: This test requires the Milvus server to be able to access the local mock TEI server.
        - localhost/127.0.0.1: uses host.docker.internal for Docker containers
        - Remote host: skipped (network may not be reachable)
        """
        # Skip if Milvus is on remote host (network may not be reachable)
        local_ip = get_local_ip()
        docker_host = get_docker_host()
        if host not in ['localhost', '127.0.0.1', local_ip, docker_host]:
            pytest.skip(f"Skipping: Milvus host ({host}) may not be able to access local mock server. "
                       "Run this test with Milvus on localhost or in the same network.")

        self._connect()
        dim = 768

        # Start mock servers with external access enabled
        mock_server = MockTEIServer(dim=dim, host='0.0.0.0', external_host='docker')
        backup_server = MockTEIServer(dim=dim, host='0.0.0.0', external_host='docker')

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
                params={"provider": "TEI", "endpoint": endpoint}
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
            mock_server.set_error_mode(
                enabled=True,
                status_code=400,
                message="Model integration is not active"
            )

            # User wants to fix the function by switching to backup endpoint
            # This is the exact scenario from issue #46949
            new_function = Function(
                name="tei",
                function_type=FunctionType.TEXTEMBEDDING,
                input_field_names=["document"],
                output_field_names="dense",
                params={"provider": "TEI", "endpoint": backup_endpoint}
            )

            # After PR #46984, this should succeed
            self.client.alter_collection_function(
                collection_name=c_name,
                function_name="tei",
                function=new_function
            )

            # Verify function is updated
            res, _ = collection_w.describe()
            func = res["functions"][0]
            assert func["params"]["endpoint"] == backup_endpoint

            # Verify the function works with new endpoint
            new_data = [{"id": 10, "document": "New document after fix"}]
            collection_w.insert(new_data)
            assert collection_w.num_entities == 4

            # Search should work
            search_params = {"metric_type": "COSINE", "params": {}}
            res, _ = collection_w.search(
                data=["New document"],
                anns_field="dense",
                param=search_params,
                limit=4,
            )
            assert len(res[0]) == 4

            log.info("Successfully fixed invalid function by altering to valid endpoint")

        finally:
            mock_server.stop()
            backup_server.stop()

    # ==================== drop_collection_function positive tests ====================

    def test_drop_collection_function_verify_crud(self, tei_endpoint):
        """
        target: test CRUD behavior changes after dropping function
        method: create collection with function, insert data, drop function, verify CRUD behavior
        expected: after drop, insert requires manual vector, existing data still queryable/searchable
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
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # === INSERT with function (auto-generate vector) ===
        data_with_func = [{"id": i, "document": f"Document with function {i}"} for i in range(5)]
        collection_w.insert(data_with_func)
        assert collection_w.num_entities == 5

        # Create index and load
        index_params = {
            "index_type": "AUTOINDEX",
            "metric_type": "COSINE",
            "params": {},
        }
        collection_w.create_index("dense", index_params)
        collection_w.load()

        # Verify vectors are generated
        res, _ = collection_w.query(expr="id >= 0", output_fields=["dense"])
        for row in res:
            assert len(row["dense"]) == dim

        # === DROP FUNCTION ===
        self.client.drop_collection_function(
            collection_name=c_name,
            function_name="tei"
        )

        # Verify function is removed
        res, _ = collection_w.describe()
        assert len(res.get("functions", [])) == 0

        # === QUERY - existing data still accessible ===
        res, _ = collection_w.query(expr="id >= 0", output_fields=["dense", "document"])
        assert len(res) == 5
        for row in res:
            assert len(row["dense"]) == dim

        # === SEARCH - existing data still searchable with vector ===
        search_params = {"metric_type": "COSINE", "params": {}}
        search_vector = [[random.random() for _ in range(dim)]]
        res, _ = collection_w.search(
            data=search_vector,
            anns_field="dense",
            param=search_params,
            limit=5,
            output_fields=["document"],
        )
        assert len(res[0]) == 5

        # === INSERT after drop - must provide vector manually ===
        manual_vector = [random.random() for _ in range(dim)]
        data_manual = [{"id": 10, "document": "Manual vector document", "dense": manual_vector}]
        collection_w.insert(data_manual)
        assert collection_w.num_entities == 6

        # Verify manual insert succeeded
        res, _ = collection_w.query(expr="id == 10", output_fields=["dense"])
        assert len(res) == 1
        assert len(res[0]["dense"]) == dim

        # === INSERT after drop without vector - should fail ===
        data_no_vector = [{"id": 11, "document": "No vector document"}]
        collection_w.insert(
            data_no_vector,
            check_task=CheckTasks.err_res,
            check_items={"err_code": 65535, "err_msg": ""},
        )

        # === UPSERT after drop - must provide vector manually ===
        upsert_vector = [random.random() for _ in range(dim)]
        upsert_data = [{"id": 0, "document": "Updated via upsert", "dense": upsert_vector}]
        collection_w.upsert(upsert_data)

        res, _ = collection_w.query(expr="id == 0", output_fields=["dense"])
        # Verify vector is updated to manual one
        assert np.allclose(res[0]["dense"], upsert_vector, rtol=1e-5)

        # === DELETE after drop - still works ===
        collection_w.delete("id in [1, 2]")
        res, _ = collection_w.query(expr="id >= 0", output_fields=["id"])
        assert len(res) == 4  # 6 - 2

    def test_drop_collection_function_one_of_multiple(self, tei_endpoint):
        """
        target: test drop one function when multiple text embedding functions exist
        method: create collection with two text_embedding functions, drop one, verify CRUD
        expected: only specified function is dropped, other still works for CRUD
        """
        self._connect()
        dim = 768
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="content", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="title_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="content_vector", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        # Add two text embedding functions
        title_embedding = Function(
            name="title_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["title"],
            output_field_names="title_vector",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        schema.add_function(title_embedding)

        content_embedding = Function(
            name="content_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["content"],
            output_field_names="content_vector",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        schema.add_function(content_embedding)

        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # Verify both functions exist
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 2

        # === INSERT with both functions ===
        data = [{"id": i, "title": f"Title {i}", "content": f"Content {i}"} for i in range(3)]
        collection_w.insert(data)
        assert collection_w.num_entities == 3

        # Create indexes and load
        index_params = {"index_type": "AUTOINDEX", "metric_type": "COSINE", "params": {}}
        collection_w.create_index("title_vector", index_params)
        collection_w.create_index("content_vector", index_params)
        collection_w.load()

        # Verify both vectors generated
        res, _ = collection_w.query(expr="id >= 0", output_fields=["title_vector", "content_vector"])
        for row in res:
            assert len(row["title_vector"]) == dim
            assert len(row["content_vector"]) == dim

        # === DROP one function (title_embedding) ===
        self.client.drop_collection_function(
            collection_name=c_name,
            function_name="title_embedding"
        )

        # Verify only content_embedding remains
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 1
        assert res["functions"][0]["name"] == "content_embedding"

        # === INSERT after drop - content_vector auto-generated, title_vector manual ===
        manual_title_vector = [random.random() for _ in range(dim)]
        data_after_drop = [{
            "id": 10,
            "title": "New title",
            "content": "New content",
            "title_vector": manual_title_vector
        }]
        collection_w.insert(data_after_drop)
        assert collection_w.num_entities == 4

        # Verify vectors
        res, _ = collection_w.query(expr="id == 10", output_fields=["title_vector", "content_vector"])
        assert np.allclose(res[0]["title_vector"], manual_title_vector, rtol=1e-5)
        assert len(res[0]["content_vector"]) == dim

        # === SEARCH on both fields still works ===
        search_params = {"metric_type": "COSINE", "params": {}}
        # Search title_vector with manual vector
        res, _ = collection_w.search(
            data=[manual_title_vector],
            anns_field="title_vector",
            param=search_params,
            limit=4,
        )
        assert len(res[0]) == 4

        # Search content_vector with text (function still active)
        res, _ = collection_w.search(
            data=["New content"],
            anns_field="content_vector",
            param=search_params,
            limit=4,
        )
        assert len(res[0]) == 4

        # === UPSERT - content function still works ===
        upsert_title_vector = [random.random() for _ in range(dim)]
        upsert_data = [{
            "id": 0,
            "title": "Updated title",
            "content": "Updated content",
            "title_vector": upsert_title_vector
        }]
        collection_w.upsert(upsert_data)

        res, _ = collection_w.query(expr="id == 0", output_fields=["title_vector", "content_vector"])
        assert np.allclose(res[0]["title_vector"], upsert_title_vector, rtol=1e-5)

        # === DELETE still works ===
        collection_w.delete("id == 1")
        res, _ = collection_w.query(expr="id >= 0", output_fields=["id"])
        assert len(res) == 3

    def test_drop_collection_function_then_add_again(self, tei_endpoint):
        """
        target: test can re-add function after dropping
        method: create collection with function, drop it, add function again
        expected: function can be re-added after drop
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
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)
        collection_w = self.init_collection_wrap(name=c_name, schema=schema)

        # Drop function
        self.client.drop_collection_function(
            collection_name=c_name,
            function_name="tei"
        )

        # Verify function is removed
        res, _ = collection_w.describe()
        assert len(res.get("functions", [])) == 0

        # Add function again
        new_function = Function(
            name="text_embedding_v2",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        self.client.add_collection_function(
            collection_name=c_name,
            function=new_function
        )

        # Verify function is added
        res, _ = collection_w.describe()
        assert len(res["functions"]) == 1
        assert res["functions"][0]["name"] == "text_embedding_v2"


@pytest.mark.tags(CaseLabel.L2)
class TestTextEmbeddingFunctionCURDNegative(TestcaseBase):
    """
    ******************************************************************
      The following cases are negative tests for add/alter/drop collection function APIs
    ******************************************************************
    """

    # ==================== add_collection_function negative tests ====================

    def test_add_collection_function_nonexistent_collection(self, tei_endpoint):
        """
        target: test add function to nonexistent collection
        method: call add_collection_function on collection that doesn't exist
        expected: error with collection not found (code=100)
        """
        self._connect()
        embedding_function = Function(
            name="text_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )

        try:
            self.client.add_collection_function(
                collection_name="nonexistent_collection_12345",
                function=embedding_function
            )
            assert False, "Expected exception for nonexistent collection"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert e.code == 100
            assert "collection not found" in str(e)

    def test_add_collection_function_duplicate_name(self, tei_endpoint):
        """
        target: test add function with duplicate name
        method: create collection with function, try to add another function with same name
        expected: error indicating duplicate function name (code=65535)
        """
        self._connect()
        dim = 768
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")

        # Add function to schema first
        text_embedding_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name, schema=schema)

        # Try to add another function with same name
        duplicate_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )

        try:
            self.client.add_collection_function(
                collection_name=c_name,
                function=duplicate_function
            )
            assert False, "Expected exception for duplicate function name"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert e.code == 65535
            assert "duplicate function name" in str(e)

    def test_add_collection_function_missing_input_field(self, tei_endpoint):
        """
        target: test add function with input field that doesn't exist
        method: add function referencing non-existent input field
        expected: error indicating input field not found (code=65535)
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

        # Create function with non-existent input field
        embedding_function = Function(
            name="text_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["nonexistent_field"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )

        try:
            self.client.add_collection_function(
                collection_name=c_name,
                function=embedding_function
            )
            assert False, "Expected exception for missing input field"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert e.code == 65535
            assert "function input field not found" in str(e)

    def test_add_collection_function_missing_output_field(self, tei_endpoint):
        """
        target: test add function with output field that doesn't exist
        method: add function referencing non-existent output field
        expected: error indicating output field not found (code=65535)
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

        # Create function with non-existent output field
        embedding_function = Function(
            name="text_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="nonexistent_vector_field",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )

        try:
            self.client.add_collection_function(
                collection_name=c_name,
                function=embedding_function
            )
            assert False, "Expected exception for missing output field"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert e.code == 65535
            assert "function output field not found" in str(e)

    def test_add_collection_function_dim_mismatch(self, tei_endpoint):
        """
        target: test add function with dimension mismatch
        method: create collection with vector field dim=512, add function for model that outputs dim=768
        expected: error indicating dimension mismatch (code=65535)
        """
        self._connect()
        dim = 512  # Mismatched dimension (TEI model outputs 768)
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(name="document", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="dense", dtype=DataType.FLOAT_VECTOR, dim=dim),
        ]
        schema = CollectionSchema(fields=fields, description="test collection")
        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name, schema=schema)

        # Create function (model outputs 768 dim, but field is 512)
        embedding_function = Function(
            name="text_embedding",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )

        try:
            self.client.add_collection_function(
                collection_name=c_name,
                function=embedding_function
            )
            assert False, "Expected exception for dimension mismatch"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert e.code == 65535
            assert "embedding dim" in str(e)

    # ==================== alter_collection_function negative tests ====================

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
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )

        try:
            self.client.alter_collection_function(
                collection_name="nonexistent_collection_12345",
                function_name="tei",
                function=new_function
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
        expected: error indicating function not found (code=65535)
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
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )

        try:
            self.client.alter_collection_function(
                collection_name=c_name,
                function_name="nonexistent_function",
                function=new_function
            )
            assert False, "Expected exception for nonexistent function"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert e.code == 65535
            assert "not found" in str(e)

    def test_alter_collection_function_invalid_new_endpoint(self, tei_endpoint):
        """
        target: test alter function with invalid endpoint
        method: create collection with valid function, alter to use invalid endpoint
        expected: error indicating endpoint unreachable (code=65535)
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
            params={"provider": "TEI", "endpoint": tei_endpoint}
        )
        schema.add_function(text_embedding_function)

        c_name = cf.gen_unique_str(prefix)
        self.init_collection_wrap(name=c_name, schema=schema)

        # Try to alter with invalid endpoint
        new_function = Function(
            name="tei",
            function_type=FunctionType.TEXTEMBEDDING,
            input_field_names=["document"],
            output_field_names="dense",
            params={"provider": "TEI", "endpoint": "http://invalid_endpoint_12345"}
        )

        try:
            self.client.alter_collection_function(
                collection_name=c_name,
                function_name="tei",
                function=new_function
            )
            assert False, "Expected exception for invalid endpoint"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert e.code == 65535
            assert "Check function" in str(e) and "failed" in str(e)

    # ==================== drop_collection_function negative tests ====================

    def test_drop_collection_function_nonexistent_collection(self):
        """
        target: test drop function from nonexistent collection
        method: call drop_collection_function on collection that doesn't exist
        expected: error with collection not found (code=100)
        """
        self._connect()

        try:
            self.client.drop_collection_function(
                collection_name="nonexistent_collection_12345",
                function_name="tei"
            )
            assert False, "Expected exception for nonexistent collection"
        except Exception as e:
            log.info(f"Expected error: {e}")
            assert e.code == 100
            assert "collection not found" in str(e)

    def test_drop_collection_function_nonexistent_function(self):
        """
        target: test drop function that doesn't exist
        method: create collection without function, try to drop non-existent function
        expected: no error (idempotent delete behavior)
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

        # Drop nonexistent function should not raise error (idempotent)
        self.client.drop_collection_function(
            collection_name=c_name,
            function_name="nonexistent_function"
        )
        log.info("Drop nonexistent function succeeded (idempotent behavior)")

    def test_drop_collection_function_empty_name(self):
        """
        target: test drop function with empty name
        method: call drop_collection_function with function_name=""
        expected: no error (idempotent delete behavior)
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

        # Drop with empty name should not raise error (idempotent)
        self.client.drop_collection_function(
            collection_name=c_name,
            function_name=""
        )
        log.info("Drop with empty function name succeeded (idempotent behavior)")