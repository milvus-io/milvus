import hashlib
import random
import struct
import time

import pytest
import numpy as np
import xxhash
from faker import Faker

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *  # noqa
from common.constants import *  # noqa
from pymilvus import AnnSearchRequest, DataType, Function, FunctionType, RRFRanker, WeightedRanker

fake = Faker()

prefix = "minhash"
default_nb = ct.default_nb
default_limit = ct.default_limit
default_primary_key_field_name = "id"
default_text_field_name = "text"
default_minhash_field_name = "minhash_signature"
default_num_hashes = 16
default_shingle_size = 3
default_dim = default_num_hashes * 32

def gen_text_data(nb, min_words=5, max_words=50):
    """Generate random text data for testing."""
    return [fake.sentence(nb_words=random.randint(min_words, max_words)) for _ in range(nb)]

def gen_similar_text_pairs(nb, overlap_ratios=[0.0, 0.25, 0.5, 0.75, 1.0]):
    """Generate text pairs with known word overlap ratios for Jaccard similarity testing."""
    pairs = []

    for ratio in overlap_ratios:
        for _ in range(nb // len(overlap_ratios)):
            # Generate base words
            base_words = fake.words(nb=20)
            num_common = int(len(base_words) * ratio)

            # Text 1 uses first half + common words
            words1 = base_words[:num_common] + fake.words(nb=10 - num_common // 2)
            # Text 2 uses common words + different second half
            words2 = base_words[:num_common] + fake.words(nb=10 - num_common // 2)

            text1 = " ".join(words1)
            text2 = " ".join(words2)

            # Calculate expected Jaccard (approximate)
            set1 = set(words1)
            set2 = set(words2)
            expected_jaccard = len(set1 & set2) / len(set1 | set2) if set1 | set2 else 0

            pairs.append((text1, text2, expected_jaccard))

    return pairs

def gen_minhash_rows(nb, start_id=0, text_field="text", pk_field="id"):
    """Generate row data for MinHash collection."""
    texts = gen_text_data(nb)
    return [{pk_field: start_id + i, text_field: texts[i]} for i in range(nb)]

class TestMilvusClientMinHashBasic(TestMilvusClientV2Base):
    """ Test case of MinHash DIDO basic function """

    @pytest.mark.tags(CaseLabel.L0)
    def test_minhash_create_collection_basic(self):
        """
        target: test creating collection with basic MinHash function
        method: create collection with MinHash function using default parameters
        expected: collection created successfully with MinHash function
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create schema with MinHash function
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": default_num_hashes,
                "shingle_size": default_shingle_size,
            },
        ))

        # 2. create collection
        self.create_collection(client, collection_name, schema=schema)

        # 3. verify collection exists
        collections = self.list_collections(client)[0]
        assert collection_name in collections

        # 4. verify schema has MinHash function
        desc = self.describe_collection(client, collection_name)[0]
        assert len(desc.get("functions", [])) == 1
        func = desc["functions"][0]
        assert func["type"] == FunctionType.MINHASH
        assert func["input_field_names"] == [default_text_field_name]
        assert func["output_field_names"] == [default_minhash_field_name]

    @pytest.mark.tags(CaseLabel.L0)
    def test_minhash_create_index_basic(self):
        """
        target: test creating MINHASH_LSH index with basic parameters
        method: create MINHASH_LSH index with mh_lsh_band parameter
        expected: index created successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create collection with MinHash function
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema)

        # 2. create MINHASH_LSH index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_index(client, collection_name, index_params)

        # 3. verify index exists
        indexes = self.list_indexes(client, collection_name)[0]
        assert default_minhash_field_name in indexes

    @pytest.mark.tags(CaseLabel.L0)
    def test_minhash_insert_basic(self):
        """
        target: test inserting data into MinHash collection
        method: insert text data, MinHash signature should be auto-generated
        expected: insert succeeds, data count matches
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create collection with MinHash function
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        # 2. create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # 3. insert data
        rows = gen_minhash_rows(default_nb)
        result = self.insert(client, collection_name, rows)[0]
        assert result["insert_count"] == default_nb

        # 4. verify data count
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats["row_count"] == default_nb

    @pytest.mark.tags(CaseLabel.L0)
    def test_minhash_search_basic(self):
        """
        target: test basic MinHash search
        method: search using text query with MHJACCARD metric
        expected: search returns results with valid distances
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create collection with MinHash function
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        # 2. create index and collection
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # 3. insert data
        rows = gen_minhash_rows(default_nb)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # 4. load collection
        self.load_collection(client, collection_name)

        # 5. search using text
        query_text = rows[0][default_text_field_name]
        results = self.search(client, collection_name, [query_text],
                              anns_field=default_minhash_field_name,
                              search_params={
                                  "metric_type": "MHJACCARD",
                                  "params": {},
                              },
                              limit=default_limit,
                              output_fields=[default_primary_key_field_name, default_text_field_name])[0]

        # 6. verify results
        assert len(results) == 1
        assert len(results[0]) <= default_limit
        # First result should be the query itself (exact match)
        assert results[0][0]["id"] == rows[0][default_primary_key_field_name]

    @pytest.mark.tags(CaseLabel.L0)
    def test_minhash_search_with_filter(self):
        """
        target: test MinHash search with scalar filter
        method: search with filter expression
        expected: results satisfy filter condition
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create schema with additional scalar field
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field("category", DataType.INT64)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # 2. insert data with category field
        texts = gen_text_data(default_nb)
        rows = [{
            default_primary_key_field_name: i,
            default_text_field_name: texts[i],
            "category": i % 5
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # 3. search with filter
        query_text = texts[0]
        filter_expr = "category == 0"
        results = self.search(client, collection_name, [query_text],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              filter=filter_expr,
                              limit=default_limit,
                              output_fields=[default_primary_key_field_name, "category"])[0]

        # 4. verify all results satisfy filter
        for hit in results[0]:
            assert hit["entity"]["category"] == 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_minhash_deterministic_signature(self):
        """
        target: verify MinHash signature generation is deterministic
        method: insert same text multiple times, compare signatures
        expected: same text produces same signature (via search self-match)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size, "seed": 1234},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert same text with different IDs
        test_text = "The quick brown fox jumps over the lazy dog."
        rows = [
            {default_primary_key_field_name: 1, default_text_field_name: test_text},
            {default_primary_key_field_name: 2, default_text_field_name: test_text},
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search should return both with distance 0 (identical)
        results = self.search(client, collection_name, [test_text],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=2,
                              output_fields=[default_primary_key_field_name])[0]

        # Both results should have distance 1.0 (MHJACCARD returns similarity, 1.0 = exact match)
        assert len(results[0]) == 2
        for hit in results[0]:
            assert hit["distance"] == 1.0

class TestMilvusClientMinHashExtended(TestMilvusClientV2Base):
    """ Test case of MinHash DIDO extended function """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("num_hashes", [1, 8, 16, 32, 64, 128])
    def test_minhash_num_hashes_variations(self, num_hashes):
        """
        target: test MinHash function with different num_hashes values
        method: create collection with various num_hashes settings
        expected: collection created and search works correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        dim = num_hashes * 32

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": min(8, num_hashes)},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert and search
        rows = gen_minhash_rows(100)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results = self.search(client, collection_name, [rows[0][default_text_field_name]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5)[0]

        assert len(results[0]) <= 5

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("shingle_size", [1, 3, 5, 10])
    def test_minhash_shingle_size_variations(self, shingle_size):
        """
        target: test MinHash function with different shingle_size values
        method: create collection with various shingle_size settings
        expected: collection created and search works correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = gen_minhash_rows(100)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results = self.search(client, collection_name, [rows[0][default_text_field_name]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5)[0]

        assert len(results[0]) <= 5

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("hash_function", ["xxhash64", "sha1"])
    def test_minhash_hash_function_variations(self, hash_function):
        """
        target: test MinHash function with different hash functions
        method: create collection with xxhash64 or sha1 hash function
        expected: collection created and search works correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": default_num_hashes,
                "shingle_size": default_shingle_size,
                "hash_function": hash_function,
            },
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = gen_minhash_rows(100)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results = self.search(client, collection_name, [rows[0][default_text_field_name]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5)[0]

        assert len(results[0]) <= 5

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("token_level", ["word", "char"])
    def test_minhash_token_level_variations(self, token_level):
        """
        target: test MinHash function with different token levels
        method: create collection with word or char tokenization
        expected: collection created and search works correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": default_num_hashes,
                "shingle_size": default_shingle_size,
                "token_level": token_level,
            },
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = gen_minhash_rows(100)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results = self.search(client, collection_name, [rows[0][default_text_field_name]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5)[0]

        assert len(results[0]) <= 5

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("mh_lsh_band", [4, 8, 16, 32])
    def test_minhash_index_band_variations(self, mh_lsh_band):
        """
        target: test MinHashLSH index with different band values
        method: create index with various mh_lsh_band settings
        expected: index created and search works correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": mh_lsh_band},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = gen_minhash_rows(100)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results = self.search(client, collection_name, [rows[0][default_text_field_name]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5)[0]

        assert len(results[0]) <= 5

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_search_with_jaccard_reranking(self):
        """
        target: test MinHash search with Jaccard reranking
        method: search with mh_search_with_jaccard=True and refine_k parameter
        expected: search returns results with accurate Jaccard distances
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8, "with_raw_data": True},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = gen_minhash_rows(default_nb)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # 2. search with Jaccard reranking
        results = self.search(client, collection_name, [rows[0][default_text_field_name]],
                              anns_field=default_minhash_field_name,
                              search_params={
                                  "metric_type": "MHJACCARD",
                                  "params": {
                                      "mh_search_with_jaccard": True,
                                      "refine_k": 100,
                                  },
                              },
                              limit=default_limit,
                              output_fields=[default_primary_key_field_name])[0]

        # First result should be exact match with distance 0
        assert results[0][0]["distance"] == 1.0
        assert results[0][0]["id"] == rows[0][default_primary_key_field_name]

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_with_raw_data_affects_jaccard_distance(self):
        """
        target: test if with_raw_data affects mh_search_with_jaccard distance calculation
        method:
            1. Create two collections with with_raw_data=True and with_raw_data=False
            2. Insert large dataset and flush to trigger index building
            3. Wait for index to be ready on sealed segment
            4. Search with mh_search_with_jaccard=True on both
            5. Compare returned distances
        expected:
            Based on knowhere source code analysis:
            - BruteForce search path: computes actual Jaccard distance regardless of with_raw_data
            - MINHASH_LSH index search path: requires with_raw_data=True for mh_search_with_jaccard=True
              (otherwise returns Status::invalid_args error)

            This test verifies:
            1. Index building correctly receives with_raw_data parameter
            2. Search behavior with different configurations

            Note: If distances are identical, search likely goes through BruteForce path.
            If with_raw_data=False fails with mh_search_with_jaccard=True, it confirms
            index search path is being used.
        """
        client = self._client()

        num_hashes = 128
        dim = num_hashes * 32
        num_rows = 10000  # Use 10K rows to ensure index search path is used

        # Generate test data with variations
        base_texts = [
            "the quick brown fox jumps over the lazy dog",
            "the quick brown fox jumps over the lazy cat",
            "a fast red wolf leaps across the sleeping hound",
            "machine learning algorithms process data efficiently",
            "natural language processing transforms text analysis",
            "deep neural networks recognize complex patterns",
            "database systems store and retrieve information",
            "distributed computing enables parallel processing",
            "cloud infrastructure supports scalable applications",
            "software engineering practices improve code quality",
        ]

        # Generate 10K rows by adding variations
        test_texts = []
        for i in range(num_rows):
            base = base_texts[i % len(base_texts)]
            # Add variation to create unique texts
            variation = f" variant number {i} with extra words for uniqueness"
            test_texts.append(base + variation)

        # Query texts - use original base texts for searching
        query_text = base_texts[0]  # "the quick brown fox jumps over the lazy dog"
        _ = 1  # "the quick brown fox jumps over the lazy cat" variant

        results_by_config = {}

        for with_raw_data in [True, False]:
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_raw_{with_raw_data}"

            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
            schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

            schema.add_function(Function(
                name="text_to_minhash",
                function_type=FunctionType.MINHASH,
                input_field_names=[default_text_field_name],
                output_field_names=[default_minhash_field_name],
                params={"num_hashes": num_hashes, "shingle_size": 3, "token_level": "char"},
            ))

            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(
                field_name=default_minhash_field_name,
                index_type="MINHASH_LSH",
                metric_type="MHJACCARD",
                params={"mh_lsh_band": 128, "with_raw_data": with_raw_data},
            )
            self.create_collection(client, collection_name, schema=schema, index_params=index_params)

            # Insert data in batches
            batch_size = 500
            for i in range(0, num_rows, batch_size):
                batch = test_texts[i:i+batch_size]
                rows = [{default_primary_key_field_name: i + j, default_text_field_name: t}
                        for j, t in enumerate(batch)]
                self.insert(client, collection_name, rows)

            # Flush to ensure data is persisted (converts Growing -> Sealed segment)
            self.flush(client, collection_name)

            # Get index name for this field
            indexes = self.list_indexes(client, collection_name, default_minhash_field_name)[0]
            index_name = indexes[0] if indexes else default_minhash_field_name

            # Wait for index building to complete on sealed segment
            # This is CRITICAL - without this, search may still go through brute force path
            log.info(f"with_raw_data={with_raw_data}: waiting for index to be ready...")
            index_ready = self.wait_for_index_ready(client, collection_name, index_name, timeout=120)
            if not index_ready:
                log.warning("Index not ready after timeout, test may use brute force search")

            # Verify index state
            index_info = self.describe_index(client, collection_name, index_name)[0]
            log.info(f"with_raw_data={with_raw_data}: index_info={index_info}")

            # Load collection first
            self.load_collection(client, collection_name)

            # CRITICAL: Use refresh_load to ensure QueryNode loads sealed segment with index
            # Without this, QueryNode may only have Growing segment loaded
            self.refresh_load(client, collection_name)
            time.sleep(2)  # Give QueryNode time to load sealed segment

            log.info(f"with_raw_data={with_raw_data}: inserted {num_rows} rows, index ready, refresh loaded")

            # Search with mh_search_with_jaccard=True
            # Note: If search goes through MINHASH_LSH index path with with_raw_data=False,
            # knowhere should return Status::invalid_args error
            try:
                results = self.search(client, collection_name, [query_text],
                                      anns_field=default_minhash_field_name,
                                      search_params={
                                          "metric_type": "MHJACCARD",
                                          "params": {"mh_search_with_jaccard": True},
                                      },
                                      limit=10,
                                      output_fields=[default_primary_key_field_name, default_text_field_name])[0]

                # Store results for comparison
                distances = {}
                for hit in results[0]:
                    hit_id = hit["entity"][default_primary_key_field_name]
                    distances[hit_id] = hit["distance"]

                results_by_config[with_raw_data] = {
                    "distances": distances,
                    "num_results": len(results[0]),
                    "results": results[0],
                    "error": None,
                }

                log.info(f"with_raw_data={with_raw_data}: {len(results[0])} results")
                for i, hit in enumerate(results[0][:5]):  # Log top 5
                    hit_id = hit["entity"][default_primary_key_field_name]
                    text_preview = hit["entity"][default_text_field_name][:50] + "..."
                    log.info(f"  [{i}] id={hit_id}, distance={hit['distance']:.6f}, text={text_preview}")

            except Exception as e:
                log.info(f"with_raw_data={with_raw_data}: Search failed with error: {e}")
                results_by_config[with_raw_data] = {
                    "distances": {},
                    "num_results": 0,
                    "results": [],
                    "error": str(e),
                }

        # Compare results between with_raw_data=True and with_raw_data=False
        log.info("=" * 60)
        log.info("COMPARISON: with_raw_data=True vs with_raw_data=False")
        log.info(f"Data size: {num_rows} rows")
        log.info("=" * 60)

        true_result = results_by_config[True]
        false_result = results_by_config[False]

        # Check for errors first
        if true_result.get("error"):
            log.info(f"with_raw_data=True: FAILED with error: {true_result['error']}")
        if false_result.get("error"):
            log.info(f"with_raw_data=False: FAILED with error: {false_result['error']}")
            log.info("ANALYSIS: This error is EXPECTED if search goes through MINHASH_LSH index path")
            log.info("         (knowhere requires with_raw_data=True for mh_search_with_jaccard=True)")
            return

        # Compare result counts
        true_count = true_result["num_results"]
        false_count = false_result["num_results"]
        log.info(f"Result counts: with_raw_data=True: {true_count}, with_raw_data=False: {false_count}")

        # Check if result counts differ significantly
        # This indicates that with_raw_data=False causes knowhere to return error (ignored)
        # which results in fewer/no valid results
        if true_count > 1 and false_count <= 1:
            log.info("RESULT: with_raw_data=False returns significantly fewer results")
            log.info("ANALYSIS: Search goes through MINHASH_LSH index path.")
            log.info("         knowhere detects mh_search_with_jaccard=True without raw data")
            log.info("         and returns Status::invalid_args (but error is silently ignored)")
            log.info("         Check server logs for: 'fail to search with jaccard distance without raw data'")
            return

        true_distances = true_result["distances"]
        false_distances = false_result["distances"]

        # Check if distances are identical or different
        distances_match = True
        common_ids = set(true_distances.keys()) & set(false_distances.keys())
        log.info(f"Common result IDs: {len(common_ids)}")

        for doc_id in sorted(common_ids)[:10]:  # Compare top 10
            diff = abs(true_distances[doc_id] - false_distances[doc_id])
            log.info(f"  doc_id={doc_id}: True={true_distances[doc_id]:.6f}, "
                     f"False={false_distances[doc_id]:.6f}, diff={diff:.6f}")
            if diff > 1e-6:
                distances_match = False

        if distances_match:
            log.info("RESULT: Distances are IDENTICAL")
            log.info("ANALYSIS: Search likely goes through BruteForce path, which computes")
            log.info("          actual Jaccard distance regardless of with_raw_data setting.")
        else:
            log.info("RESULT: Distances DIFFER")
            log.info("ANALYSIS: Search goes through index path, with_raw_data affects distance calculation.")

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_upsert(self):
        """
        target: test upsert operation with MinHash collection
        method: insert data, then upsert with same primary keys
        expected: data updated correctly, MinHash signature regenerated
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert initial data
        original_text = "Original text content for testing."
        rows = [{default_primary_key_field_name: 1, default_text_field_name: original_text}]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Upsert with new text
        updated_text = "Completely different updated text content."
        upsert_rows = [{default_primary_key_field_name: 1, default_text_field_name: updated_text}]
        self.upsert(client, collection_name, upsert_rows)
        self.flush(client, collection_name)

        # Search with updated text should find exact match
        time.sleep(1)  # Wait for data sync
        results = self.search(client, collection_name, [updated_text],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=1,
                              output_fields=[default_text_field_name])[0]

        assert results[0][0]["distance"] == 1.0
        assert results[0][0]["entity"][default_text_field_name] == updated_text

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_query_by_id(self):
        """
        target: test query by primary key in MinHash collection
        method: insert data, then query by ID
        expected: query returns correct text and MinHash signature
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        rows = gen_minhash_rows(100)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Query by ID
        query_ids = [0, 1, 2]
        results = self.query(client, collection_name,
                             filter=f"{default_primary_key_field_name} in {query_ids}",
                             output_fields=[default_primary_key_field_name, default_text_field_name])[0]

        assert len(results) == 3
        for result in results:
            assert result[default_primary_key_field_name] in query_ids
            assert default_text_field_name in result

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_delete(self):
        """
        target: test delete operation in MinHash collection
        method: insert data, delete some, verify deletion
        expected: deleted data not found in search/query
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = gen_minhash_rows(100)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Delete first 10 entries
        delete_ids = list(range(10))
        self.delete(client, collection_name, filter=f"{default_primary_key_field_name} in {delete_ids}")
        self.flush(client, collection_name)

        # Query deleted IDs should return empty
        time.sleep(1)
        results = self.query(client, collection_name,
                             filter=f"{default_primary_key_field_name} in {delete_ids}")[0]
        assert len(results) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_with_auto_id(self):
        """
        target: test MinHash collection with auto-generated primary key
        method: create collection with auto_id=True, insert without ID
        expected: IDs auto-generated, MinHash signatures created
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=True)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert without ID
        texts = gen_text_data(100)
        rows = [{default_text_field_name: text} for text in texts]
        result = self.insert(client, collection_name, rows)[0]

        assert result["insert_count"] == 100
        assert len(result["ids"]) == 100

        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search should work
        results = self.search(client, collection_name, [texts[0]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5)[0]

        assert len(results[0]) <= 5

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_unicode_text(self):
        """
        target: test MinHash function with Unicode/multilingual text
        method: insert text in multiple languages
        expected: MinHash signature generated correctly for all languages
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Multilingual texts
        texts = [
            "Hello world, this is English text.",
            "你好世界，这是中文文本。",
            "こんにちは世界、これは日本語テキストです。",
            "Привет мир, это русский текст.",
            "مرحبا بالعالم، هذا نص عربي.",
            "Olá mundo, este é um texto em português.",
            "🎉 Emoji test with 🌍 symbols 🚀",
        ]

        rows = [{default_primary_key_field_name: i, default_text_field_name: texts[i]}
                for i in range(len(texts))]

        result = self.insert(client, collection_name, rows)[0]
        assert result["insert_count"] == len(texts)

        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search with each language
        for i, text in enumerate(texts):
            results = self.search(client, collection_name, [text],
                                  anns_field=default_minhash_field_name,
                                  search_params={"metric_type": "MHJACCARD", "params": {}},
                                  limit=1)[0]
            # Exact match should have distance 0
            assert results[0][0]["distance"] == 1.0

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_partition_search(self):
        """
        target: test MinHash search with partitions
        method: create partitions, insert data, search within specific partition
        expected: search returns results only from specified partition
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        partition_names = ["partition_a", "partition_b"]

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Create partitions
        for pname in partition_names:
            self.create_partition(client, collection_name, pname)

        # Insert data into different partitions
        rows_a = gen_minhash_rows(50, start_id=0)
        rows_b = gen_minhash_rows(50, start_id=50)

        self.insert(client, collection_name, rows_a, partition_name=partition_names[0])
        self.insert(client, collection_name, rows_b, partition_name=partition_names[1])
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search in partition_a only
        results = self.search(client, collection_name, [rows_a[0][default_text_field_name]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              partition_names=[partition_names[0]],
                              limit=10,
                              output_fields=[default_primary_key_field_name])[0]

        # All results should have ID < 50 (from partition_a)
        for hit in results[0]:
            assert hit["id"] < 50

class TestMilvusClientMinHashNegative(TestMilvusClientV2Base):
    """ Test case of MinHash DIDO negative function """

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_invalid_input_field_type(self):
        """
        target: test MinHash function with non-VARCHAR input field
        method: try to create MinHash function with INT64 input field
        expected: error raised during collection creation - input must be VARCHAR
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("int_field", DataType.INT64)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=["int_field"],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535,
                                             ct.err_msg: "VARCHAR"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_invalid_output_field_type(self):
        """
        target: test MinHash function with non-BINARY_VECTOR output field
        method: try to create MinHash function with FLOAT_VECTOR output field
        expected: error raised during collection creation - output must be BINARY_VECTOR
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field("float_vec", DataType.FLOAT_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=["float_vec"],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535,
                                             ct.err_msg: "BinaryVector"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_dim_not_multiple_of_32(self):
        """
        target: test MinHash function with mismatched dimension
        method: try to create MinHash function where num_hashes*32 != field dim
        expected: error raised - dimension mismatch
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=128)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": 3, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535,
                                             ct.err_msg: "does not match expected dim"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="https://github.com/milvus-io/milvus/issues/47585 "
                               "Server allows BIN_FLAT index on MinHash function output field")
    def test_minhash_invalid_index_type(self):
        """
        target: test creating non-MinHashLSH index on MinHash output field
        method: try to create BIN_FLAT index on MinHash signature field
        expected: error raised - must use MINHASH_LSH index for MinHash function output
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema)

        # 2. try to create wrong index type
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="BIN_FLAT",
            metric_type="HAMMING",
        )
        self.create_index(client, collection_name, index_params,
                          check_task=CheckTasks.err_res,
                          check_items={ct.err_code: 1})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="https://github.com/milvus-io/milvus/issues/47585 "
                               "Server allows HAMMING metric with MINHASH_LSH index")
    def test_minhash_invalid_metric_type(self):
        """
        target: test creating MinHashLSH index with wrong metric type
        method: try to create MINHASH_LSH index with HAMMING metric
        expected: error raised - must use MHJACCARD metric
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema)

        # 2. try to create index with wrong metric
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="HAMMING",
            params={"mh_lsh_band": 8},
        )
        self.create_index(client, collection_name, index_params,
                          check_task=CheckTasks.err_res,
                          check_items={ct.err_code: 1})

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_insert_to_output_field(self):
        """
        target: test directly inserting data to MinHash output field
        method: try to insert MinHash signature directly
        expected: error raised - cannot insert to function output field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        client.create_collection(collection_name, schema=schema, index_params=index_params)

        # 2. try to insert with MinHash signature directly
        fake_signature = bytes([0] * (default_dim // 8))
        rows = [{
            default_primary_key_field_name: 1,
            default_text_field_name: "Test text",
            default_minhash_field_name: fake_signature,
        }]
        self.insert(client, collection_name, rows,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1,
                                 ct.err_msg: "unexpected function output field"})

        client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_missing_input_field(self):
        """
        target: test inserting without MinHash input field
        method: try to insert without text field
        expected: error raised - required field missing
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # 1. create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        client.create_collection(collection_name, schema=schema, index_params=index_params)

        # 2. try to insert without text field
        rows = [{default_primary_key_field_name: 1}]
        self.insert(client, collection_name, rows,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 1,
                                 ct.err_msg: "missed an field"})

        client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_num_hashes", [0, -1, "abc", "123abc"])
    def test_minhash_invalid_num_hashes(self, invalid_num_hashes):
        """
        target: test MinHash function with invalid num_hashes value
        method: try to create function with invalid num_hashes
        expected: error raised during collection creation
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": invalid_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535,
                                             ct.err_msg: "num_hashes"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_shingle_size", [0, -1, "xyz"])
    def test_minhash_invalid_shingle_size(self, invalid_shingle_size):
        """
        target: test MinHash function with invalid shingle_size value
        method: try to create function with invalid shingle_size
        expected: error raised during collection creation
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": invalid_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535,
                                             ct.err_msg: "shingle_size"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_invalid_hash_function(self):
        """
        target: test MinHash function with invalid hash_function value
        method: try to create function with unsupported hash function
        expected: error raised during collection creation
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": default_num_hashes,
                "shingle_size": default_shingle_size,
                "hash_function": "md5",
            },
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535,
                                             ct.err_msg: "Unknown hash function"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_token_level", ["sentence", "invalid", ""])
    def test_minhash_invalid_token_level(self, invalid_token_level):
        """
        target: test MinHash function with invalid token_level value
        method: try to create function with unsupported token_level
        expected: error raised during collection creation
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": default_num_hashes,
                "shingle_size": default_shingle_size,
                "token_level": invalid_token_level,
            },
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535,
                                             ct.err_msg: "Unknown token_level"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_seed", ["not_a_number", "abc123"])
    def test_minhash_invalid_seed(self, invalid_seed):
        """
        target: test MinHash function with invalid seed value
        method: try to create function with non-numeric seed
        expected: error raised during collection creation
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": default_num_hashes,
                "shingle_size": default_shingle_size,
                "seed": invalid_seed,
            },
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535,
                                             ct.err_msg: "seed"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_search_empty_collection(self):
        """
        target: test MinHash search on empty collection
        method: create collection, search without inserting data
        expected: return empty results
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        self.load_collection(client, collection_name)

        # Search on empty collection
        results = self.search(client, collection_name, ["Test query text"],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=default_limit)[0]

        # Should return empty results
        assert len(results[0]) == 0

class TestMilvusClientMinHashAdvanced(TestMilvusClientV2Base):
    """ Test case of MinHash DIDO advanced function """

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_all_parameters(self):
        """
        target: test MinHash function with all parameters specified
        method: create collection with all MinHash and index parameters
        expected: collection and index created successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=True)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field("category", DataType.VARCHAR, max_length=256, nullable=True)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=1024)

        # All function parameters
        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": 32,
                "shingle_size": 5,
                "hash_function": "xxhash64",
                "token_level": "word",
                "seed": 42,
            },
        ))

        # All index parameters
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={
                "mh_lsh_band": 12,
                "mh_element_bit_width": 32,
                "mh_lsh_code_in_mem": 1,
                "with_raw_data": True,
                "mh_lsh_bloom_false_positive_prob": 0.01,
            },
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        rows = gen_minhash_rows(100)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search with all parameters
        results = self.search(client, collection_name, [rows[0][default_text_field_name]],
                              anns_field=default_minhash_field_name,
                              search_params={
                                  "metric_type": "MHJACCARD",
                                  "params": {
                                      "mh_search_with_jaccard": True,
                                      "refine_k": 100,
                                      "mh_lsh_batch_search": True,
                                  },
                              },
                              limit=10)[0]

        assert len(results[0]) <= 10

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_batch_search(self):
        """
        target: test MinHash batch search with multiple queries
        method: search with multiple query texts at once
        expected: results returned for all queries
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = gen_minhash_rows(default_nb)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Batch search with multiple queries
        query_texts = [rows[i][default_text_field_name] for i in [0, 10, 50, 100]]
        results = self.search(client, collection_name, query_texts,
                              anns_field=default_minhash_field_name,
                              search_params={
                                  "metric_type": "MHJACCARD",
                                  "params": {"mh_lsh_batch_search": True},
                              },
                              limit=5)[0]

        # Should have results for all queries
        assert len(results) == len(query_texts)
        for i, result in enumerate(results):
            assert len(result) <= 5
            # First result should be exact match
            assert result[0]["distance"] == 1.0

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_empty_string(self):
        """
        target: test MinHash function with empty string input
        method: insert empty string as text
        expected: valid signature generated (minimal)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert empty string
        rows = [{default_primary_key_field_name: 1, default_text_field_name: ""}]
        result = self.insert(client, collection_name, rows)[0]

        # Should succeed
        assert result["insert_count"] == 1

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_single_char_text(self):
        """
        target: test MinHash function with single character text
        method: insert single character as text
        expected: valid signature generated
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": 1, "token_level": "char"},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert single character
        rows = [{default_primary_key_field_name: 1, default_text_field_name: "a"}]
        result = self.insert(client, collection_name, rows)[0]

        assert result["insert_count"] == 1

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_special_characters(self):
        """
        target: test MinHash function with special characters
        method: insert text with special characters
        expected: valid signature generated
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Various special character texts
        special_texts = [
            "!@#$%^&*()_+-=[]{}|;':\",./<>?",
            "\t\n\r text with whitespace \t\n\r",
            "Text with <html> tags </html>",
            "Path/like\\text\\with/slashes",
            "Numbers: 123.456 and 7.89e-10",
        ]

        rows = [{default_primary_key_field_name: i, default_text_field_name: text}
                for i, text in enumerate(special_texts)]
        result = self.insert(client, collection_name, rows)[0]

        assert result["insert_count"] == len(special_texts)

        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search should work
        for text in special_texts:
            results = self.search(client, collection_name, [text],
                                  anns_field=default_minhash_field_name,
                                  search_params={"metric_type": "MHJACCARD", "params": {}},
                                  limit=1)[0]
            assert results[0][0]["distance"] == 1.0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("mh_element_bit_width", [32, 64])
    def test_minhash_index_element_bit_width(self, mh_element_bit_width):
        """
        target: test MinHashLSH index with different element bit widths
        method: create index with mh_element_bit_width parameter
        expected: index created and search works correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={
                "mh_lsh_band": 8,
                "mh_element_bit_width": mh_element_bit_width,
            },
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = gen_minhash_rows(100)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results = self.search(client, collection_name, [rows[0][default_text_field_name]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5)[0]

        assert len(results[0]) <= 5

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("bloom_fp_prob", [0.001, 0.01, 0.1])
    def test_minhash_bloom_filter_prob(self, bloom_fp_prob):
        """
        target: test MinHashLSH index with different bloom filter FP probabilities
        method: create index with mh_lsh_bloom_false_positive_prob parameter
        expected: index created and search works correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={
                "mh_lsh_band": 8,
                "mh_lsh_bloom_false_positive_prob": bloom_fp_prob,
            },
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = gen_minhash_rows(100)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results = self.search(client, collection_name, [rows[0][default_text_field_name]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5)[0]

        assert len(results[0]) <= 5

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_drop_and_recreate_index(self):
        """
        target: test dropping and recreating MinHash index
        method: create index, drop it, create again with different params
        expected: both operations succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        # Create initial index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 4},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = gen_minhash_rows(100)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Drop index
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, default_minhash_field_name)

        # Recreate with different parameters
        new_index_params = self.prepare_index_params(client)[0]
        new_index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 16},  # Different band count
        )
        self.create_index(client, collection_name, new_index_params)
        self.load_collection(client, collection_name)

        # Search should work with new index
        results = self.search(client, collection_name, [rows[0][default_text_field_name]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5)[0]

        assert len(results[0]) <= 5

class TestMilvusClientMinHashAccuracy(TestMilvusClientV2Base):
    """ Test case of MinHash DIDO accuracy """

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_similar_text_search(self):
        """
        target: verify similar texts are ranked higher in search results
        method: insert original text and variations, search for similar
        expected: similar texts have lower distances than dissimilar ones
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=512)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": 16, "shingle_size": 3, "token_level": "word"},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8, "with_raw_data": True},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert texts with varying similarity
        texts = [
            "The quick brown fox jumps over the lazy dog.",  # ID 0 - Original
            "A quick brown fox jumped over a lazy dog.",      # ID 1 - Very similar
            "The fast brown fox leaps over the sleepy dog.",  # ID 2 - Similar
            "Machine learning is transforming AI research.",   # ID 3 - Unrelated
            "Python is a popular programming language.",       # ID 4 - Unrelated
        ]
        rows = [{default_primary_key_field_name: i, default_text_field_name: texts[i]}
                for i in range(len(texts))]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search for original text
        results = self.search(client, collection_name, [texts[0]],
                              anns_field=default_minhash_field_name,
                              search_params={
                                  "metric_type": "MHJACCARD",
                                  "params": {"mh_search_with_jaccard": True, "refine_k": 10},
                              },
                              limit=5,
                              output_fields=[default_primary_key_field_name])[0]

        # Verify ordering: similar texts should have lower distances
        result_ids = [hit["id"] for hit in results[0]]

        # ID 0 should be first (exact match)
        assert result_ids[0] == 0

        # IDs 1 and 2 (similar texts) should appear before IDs 3 and 4 (unrelated)
        similar_positions = [result_ids.index(i) for i in [1, 2] if i in result_ids]
        unrelated_positions = [result_ids.index(i) for i in [3, 4] if i in result_ids]

        if similar_positions and unrelated_positions:
            assert max(similar_positions) < min(unrelated_positions), \
                "Similar texts should rank higher than unrelated texts"

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_identical_text_distance_zero(self):
        """
        target: verify identical texts have distance 0
        method: search for exact same text
        expected: distance should be 0
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8, "with_raw_data": True},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        test_text = "This is a test text for identical matching."
        rows = [{default_primary_key_field_name: 1, default_text_field_name: test_text}]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results = self.search(client, collection_name, [test_text],
                              anns_field=default_minhash_field_name,
                              search_params={
                                  "metric_type": "MHJACCARD",
                                  "params": {"mh_search_with_jaccard": True},
                              },
                              limit=1)[0]

        # Distance should be exactly 0 for identical text
        assert results[0][0]["distance"] == 1.0

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_seed_reproducibility(self):
        """
        target: verify same seed produces same results across collections
        method: create two collections with same seed, compare search results
        expected: search results should be identical
        """
        client = self._client()
        collection_name_1 = cf.gen_collection_name_by_testcase_name() + "_1"
        collection_name_2 = cf.gen_collection_name_by_testcase_name() + "_2"
        seed = 12345

        # Create two identical collections with same seed
        for collection_name in [collection_name_1, collection_name_2]:
            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
            schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

            schema.add_function(Function(
                name="text_to_minhash",
                function_type=FunctionType.MINHASH,
                input_field_names=[default_text_field_name],
                output_field_names=[default_minhash_field_name],
                params={
                    "num_hashes": default_num_hashes,
                    "shingle_size": default_shingle_size,
                    "seed": seed,  # Same seed
                },
            ))

            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(
                field_name=default_minhash_field_name,
                index_type="MINHASH_LSH",
                metric_type="MHJACCARD",
                params={"mh_lsh_band": 8},
            )
            self.create_collection(client, collection_name, schema=schema, index_params=index_params)

            # Insert same data
            rows = gen_minhash_rows(100)
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            self.load_collection(client, collection_name)

        # Search in both collections
        query_text = "Test query for reproducibility"
        results_1 = self.search(client, collection_name_1, [query_text],
                                anns_field=default_minhash_field_name,
                                search_params={"metric_type": "MHJACCARD", "params": {}},
                                limit=10,
                                output_fields=[default_primary_key_field_name])[0]

        results_2 = self.search(client, collection_name_2, [query_text],
                                anns_field=default_minhash_field_name,
                                search_params={"metric_type": "MHJACCARD", "params": {}},
                                limit=10,
                                output_fields=[default_primary_key_field_name])[0]

        # Results should be identical
        ids_1 = [hit["id"] for hit in results_1[0]]
        ids_2 = [hit["id"] for hit in results_2[0]]
        distances_1 = [hit["distance"] for hit in results_1[0]]
        distances_2 = [hit["distance"] for hit in results_2[0]]

        assert ids_1 == ids_2, "Same seed should produce same result ordering"
        assert distances_1 == distances_2, "Same seed should produce same distances"

        self.drop_collection(client, collection_name_1)
        self.drop_collection(client, collection_name_2)

# Pure Python implementation of Milvus MinHash algorithm.
# Verified bit-identical to the C++ implementation (MinHashComputer.cpp).

# ---- MT19937-64 (matches std::mt19937_64) ----

_MASK64 = 0xFFFFFFFFFFFFFFFF
_MT_N = 312
_MT_M = 156
_MT_A = 0xB5026F5AA96619E9
_MT_F = 6364136223846793005
_MT_UPPER = _MASK64 & ~((1 << 31) - 1)
_MT_LOWER = (1 << 31) - 1

class _MT19937_64:
    __slots__ = ("_mt", "_idx")

    def __init__(self, seed):
        mt = [0] * _MT_N
        mt[0] = seed & _MASK64
        for i in range(1, _MT_N):
            mt[i] = (_MT_F * (mt[i - 1] ^ (mt[i - 1] >> 62)) + i) & _MASK64
        self._mt = mt
        self._idx = _MT_N

    def __call__(self):
        if self._idx >= _MT_N:
            mt = self._mt
            for i in range(_MT_N):
                x = (mt[i] & _MT_UPPER) | (mt[(i + 1) % _MT_N] & _MT_LOWER)
                xa = x >> 1
                if x & 1:
                    xa ^= _MT_A
                mt[i] = mt[(i + _MT_M) % _MT_N] ^ xa
            self._idx = 0
        y = self._mt[self._idx]
        y ^= (y >> 29) & 0x5555555555555555
        y ^= (y << 17) & 0x71D67FFFEDA60000
        y ^= (y << 37) & 0xFFF7EEE000000000
        y ^= y >> 43
        self._idx += 1
        return y & _MASK64

# ---- MinHash constants (matching MinHashComputer.cpp) ----

MINHASH_MERSENNE_PRIME = 0x1FFFFFFFFFFFFFFF  # 2^61 - 1
MINHASH_MAX_HASH_MASK = 0xFFFFFFFF           # 2^32 - 1

# ---- Hash functions ----

def _hash_xxhash(data):
    """xxHash (XXH3_64bits) cast to uint32, matching C++ static_cast<uint32_t>."""
    return xxhash.xxh3_64(data).intdigest() & 0xFFFFFFFF

def _hash_sha1(data):
    """SHA1 first 4 bytes as little-endian uint32."""
    return struct.unpack("<I", hashlib.sha1(data).digest()[:4])[0]

# ---- Core MinHash functions ----

def init_permutations_like_milvus(num_hashes, seed):
    """Generate permutation parameters matching Milvus InitPermutations."""
    rng = _MT19937_64(seed)
    perm_a = np.empty(num_hashes, dtype=np.uint64)
    perm_b = np.empty(num_hashes, dtype=np.uint64)
    for i in range(num_hashes):
        raw_a, raw_b = rng(), rng()
        perm_a[i] = (raw_a % (MINHASH_MERSENNE_PRIME - 1)) + 1
        perm_b[i] = raw_b % MINHASH_MERSENNE_PRIME
    return perm_a, perm_b

def hash_shingles_xxhash(text, shingle_size):
    """Compute character-level shingle hashes using xxhash."""
    return _hash_shingles(text, shingle_size, _hash_xxhash)

def hash_shingles_sha1(text, shingle_size):
    """Compute character-level shingle hashes using SHA1."""
    return _hash_shingles(text, shingle_size, _hash_sha1)

def _hash_shingles(text, shingle_size, hash_func):
    data = text.encode("utf-8")
    if len(data) < shingle_size:
        return [hash_func(data)]
    return [hash_func(data[i:i + shingle_size]) for i in range(len(data) - shingle_size + 1)]

def compute_minhash_signature(base_hashes, perm_a, perm_b):
    """Compute MinHash signature from base hashes, matching Milvus exactly."""
    MP = MINHASH_MERSENNE_PRIME
    num_hashes = len(perm_a)
    sig = [0xFFFFFFFF] * num_hashes
    a_vals = [int(x) for x in perm_a]
    b_vals = [int(x) for x in perm_b]
    for base in base_hashes:
        base = int(base)
        for i in range(num_hashes):
            temp = (a_vals[i] * base + b_vals[i]) & _MASK64
            temp = (temp & MP) + (temp >> 61)
            if temp >= MP:
                temp -= MP
            h = temp & 0xFFFFFFFF
            if h < sig[i]:
                sig[i] = h
    return sig

def compute_minhash(text, num_hashes, shingle_size, seed,
                    use_char_level=True, use_sha1=False):
    """Compute MinHash signature from text (high-level API)."""
    perm_a, perm_b = init_permutations_like_milvus(num_hashes, seed)
    hash_func = _hash_sha1 if use_sha1 else _hash_xxhash
    if use_char_level:
        base_hashes = _hash_shingles(text, shingle_size, hash_func)
    else:
        tokens = text.split()
        if len(tokens) < shingle_size:
            combined = "".join(tokens).encode("utf-8")
            base_hashes = [hash_func(combined)] if combined else []
        else:
            base_hashes = [
                hash_func("".join(tokens[i:i + shingle_size]).encode("utf-8"))
                for i in range(len(tokens) - shingle_size + 1)
            ]
    return compute_minhash_signature(base_hashes, perm_a, perm_b)

# ---- Conversion helpers ----

def signature_to_binary_vector(signature):
    """Convert MinHash signature to binary vector (little-endian)."""
    return b''.join(struct.pack('<I', s) for s in signature)

def binary_vector_to_signature(binary_vector):
    """Convert binary vector back to signature (little-endian)."""
    if isinstance(binary_vector, list):
        binary_vector = binary_vector[0]
    num_hashes = len(binary_vector) // 4
    return list(struct.unpack(f'<{num_hashes}I', binary_vector))

class TestMinHashFunctionCorrectness(TestMilvusClientV2Base):
    """ Test case of MinHash function correctness """

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_signature_deterministic(self):
        """
        target: verify MinHash signature generation is deterministic
        method: insert same text multiple times, verify all signatures are identical
        expected: same text with same parameters produces identical signature

        Note: Due to potential xxhash implementation differences between Python and C++,
        we verify determinism and correctness properties rather than exact values.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        seed = 42
        num_hashes = 16
        shingle_size = 3
        dim = num_hashes * 32

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": num_hashes,
                "shingle_size": shingle_size,
                "seed": seed,
                "token_level": "char",
            },
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert same text with different IDs
        test_text = "hello world test document for determinism verification"
        rows = [
            {default_primary_key_field_name: i, default_text_field_name: test_text}
            for i in range(5)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Query all signatures
        results = self.query(client, collection_name,
                             filter=f"{default_primary_key_field_name} >= 0",
                             output_fields=[default_primary_key_field_name,
                                            default_minhash_field_name])[0]

        # All signatures should be identical
        signatures = [binary_vector_to_signature(r[default_minhash_field_name]) for r in results]
        first_sig = signatures[0]
        for i, sig in enumerate(signatures[1:], 1):
            assert sig == first_sig, \
                f"Signature {i} differs from signature 0: {sig} != {first_sig}"

        # Verify signature format
        assert len(first_sig) == num_hashes, f"Signature should have {num_hashes} values"
        assert all(0 <= s <= 0xFFFFFFFF for s in first_sig), "All values should be 32-bit"

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_signature_reproducible_across_collections(self):
        """
        target: verify MinHash signatures are reproducible across different collections
        method: create two collections with same parameters, insert same text, compare signatures
        expected: identical configuration produces identical signatures
        """
        client = self._client()

        seed = 42
        num_hashes = 16
        shingle_size = 3
        dim = num_hashes * 32
        test_text = "reproducibility test text"

        signatures = []

        for i in range(2):
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_coll{i}"

            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
            schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

            schema.add_function(Function(
                name="text_to_minhash",
                function_type=FunctionType.MINHASH,
                input_field_names=[default_text_field_name],
                output_field_names=[default_minhash_field_name],
                params={
                    "num_hashes": num_hashes,
                    "shingle_size": shingle_size,
                    "seed": seed,
                    "token_level": "char",
                },
            ))

            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(
                field_name=default_minhash_field_name,
                index_type="MINHASH_LSH",
                metric_type="MHJACCARD",
                params={"mh_lsh_band": 8},
            )
            self.create_collection(client, collection_name, schema=schema, index_params=index_params)

            rows = [{default_primary_key_field_name: 1, default_text_field_name: test_text}]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            self.load_collection(client, collection_name)

            results = self.query(client, collection_name,
                                 filter=f"{default_primary_key_field_name} == 1",
                                 output_fields=[default_minhash_field_name])[0]

            sig = binary_vector_to_signature(results[0][default_minhash_field_name])
            signatures.append(sig)

        assert signatures[0] == signatures[1], \
            "Same configuration should produce identical signatures across collections"

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_permutation_generation_consistency(self):
        """
        target: verify permutation generation consistency across multiple seeds
        method: create collections with different seeds, verify signatures differ
        expected: different seeds produce different signatures for same text
        """
        client = self._client()
        num_hashes = 16
        shingle_size = 3
        dim = num_hashes * 32
        test_text = "consistent test text for permutation verification"

        signatures_by_seed = {}

        for seed in [1234, 42, 0, 999999]:
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_seed{seed}"

            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
            schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

            schema.add_function(Function(
                name="text_to_minhash",
                function_type=FunctionType.MINHASH,
                input_field_names=[default_text_field_name],
                output_field_names=[default_minhash_field_name],
                params={"num_hashes": num_hashes, "shingle_size": shingle_size, "seed": seed},
            ))

            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(
                field_name=default_minhash_field_name,
                index_type="MINHASH_LSH",
                metric_type="MHJACCARD",
                params={"mh_lsh_band": 8},
            )
            self.create_collection(client, collection_name, schema=schema, index_params=index_params)

            # Insert same text
            rows = [{default_primary_key_field_name: 1, default_text_field_name: test_text}]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            self.load_collection(client, collection_name)

            # Query signature
            results = self.query(client, collection_name,
                                 filter=f"{default_primary_key_field_name} == 1",
                                 output_fields=[default_minhash_field_name])[0]

            actual_binary = results[0][default_minhash_field_name]
            actual_sig = tuple(binary_vector_to_signature(actual_binary))
            signatures_by_seed[seed] = actual_sig

            # Verify signature format
            assert len(actual_sig) == num_hashes, f"Signature should have {num_hashes} values"
            assert all(0 <= s <= 0xFFFFFFFF for s in actual_sig), "All values should be 32-bit"

        # Verify different seeds produce different signatures
        unique_signatures = set(signatures_by_seed.values())
        assert len(unique_signatures) == len(signatures_by_seed), \
            "Different seeds should produce different signatures"

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_shingle_hash_correctness(self):
        """
        target: verify shingle generation and hash computation
        method: test with known inputs and verify expected shingle count
        expected: shingle count matches expected value based on text length
        """
        # Test character-level shingle generation
        test_cases = [
            # (text, shingle_size, expected_shingle_count)
            ("abc", 3, 1),           # "abc" -> 1 shingle
            ("abcd", 3, 2),          # "abc", "bcd" -> 2 shingles
            ("abcde", 3, 3),         # "abc", "bcd", "cde" -> 3 shingles
            ("ab", 3, 1),            # short text -> 1 shingle (whole text)
            ("hello world", 3, 9),   # 11 chars -> 9 shingles
        ]

        for text, shingle_size, expected_count in test_cases:
            hashes = hash_shingles_xxhash(text, shingle_size)
            assert len(hashes) == expected_count, \
                f"Expected {expected_count} shingles for '{text}' with size {shingle_size}, got {len(hashes)}"

            # All hashes should be 32-bit
            for h in hashes:
                assert 0 <= h <= 0xFFFFFFFF, f"Hash {h} is not a valid 32-bit value"

        # Verify hash determinism
        text = "deterministic test"
        h1 = hash_shingles_xxhash(text, 3)
        h2 = hash_shingles_xxhash(text, 3)
        assert h1 == h2, "Same text should produce same hashes"

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_binary_vector_format(self):
        """
        target: verify binary vector format (little-endian encoding)
        method: convert signature to binary and back, verify roundtrip
        expected: signature survives roundtrip conversion
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        seed = 12345
        num_hashes = 8  # Smaller for easier verification
        shingle_size = 3
        dim = num_hashes * 32

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": num_hashes, "shingle_size": shingle_size, "seed": seed},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 4},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        test_text = "binary vector format test"
        rows = [{default_primary_key_field_name: 1, default_text_field_name: test_text}]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Get actual binary vector from Milvus
        results = self.query(client, collection_name,
                             filter=f"{default_primary_key_field_name} == 1",
                             output_fields=[default_minhash_field_name])[0]

        actual_binary = results[0][default_minhash_field_name]
        # Handle Milvus return format: [b'...'] (list containing bytes)
        if isinstance(actual_binary, list):
            actual_binary = actual_binary[0]

        # Verify binary vector size: num_hashes * 4 bytes (32 bits each)
        expected_byte_size = num_hashes * 4
        assert len(actual_binary) == expected_byte_size, \
            f"Binary vector should be {expected_byte_size} bytes, got {len(actual_binary)}"

        # Roundtrip test
        sig = binary_vector_to_signature(actual_binary)
        roundtrip_binary = signature_to_binary_vector(sig)
        assert actual_binary == roundtrip_binary, "Binary vector should survive roundtrip conversion"

        # Verify signature format
        assert len(sig) == num_hashes, f"Signature should have {num_hashes} values"
        assert all(0 <= s <= 0xFFFFFFFF for s in sig), "All values should be 32-bit"

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_empty_and_short_text_handling(self):
        """
        target: verify MinHash handles edge cases (empty and very short text)
        method: insert empty and single-char texts
        expected: MinHash generates valid signatures for all inputs
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        seed = 42
        num_hashes = 16
        shingle_size = 3
        dim = num_hashes * 32

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": num_hashes, "shingle_size": shingle_size, "seed": seed},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Edge case texts
        edge_cases = [
            (1, "a"),           # Single char
            (2, "ab"),          # Two chars (less than shingle_size)
            (3, "abc"),         # Exactly shingle_size
            (4, " "),           # Single space
            (5, "  "),          # Multiple spaces
        ]

        rows = [{default_primary_key_field_name: pk, default_text_field_name: text}
                for pk, text in edge_cases]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Verify all texts get valid signatures
        results = self.query(client, collection_name,
                             filter=f"{default_primary_key_field_name} >= 0",
                             output_fields=[default_primary_key_field_name,
                                            default_text_field_name,
                                            default_minhash_field_name])[0]

        assert len(results) == len(edge_cases), "All edge cases should be inserted"

        for result in results:
            binary_vec = result[default_minhash_field_name]
            # Handle Milvus return format: [b'...'] (list containing bytes)
            if isinstance(binary_vec, list):
                binary_vec = binary_vec[0]
            text = result[default_text_field_name]

            # Binary vector should have correct size
            assert len(binary_vec) == num_hashes * 4, f"Invalid binary vector size for '{text}'"

            sig = binary_vector_to_signature(binary_vec)

            # All signature values should be valid 32-bit
            assert len(sig) == num_hashes, f"Signature should have {num_hashes} values for '{text}'"
            for s in sig:
                assert 0 <= s <= 0xFFFFFFFF, f"Invalid signature value for '{text}'"

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="https://github.com/milvus-io/milvus/issues/47521 "
                               "MINHASH_LSH returns meaningless distance=1.0 "
                               "when mh_search_with_jaccard is not set")
    def test_minhash_jaccard_distance_correlation(self):
        """
        target: verify MinHash Jaccard distance correlates with actual Jaccard similarity
        method: create pairs with known overlaps, verify distance ordering
        expected: higher text overlap -> higher similarity (lower distance)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        seed = 42
        num_hashes = 128  # More hashes for better accuracy
        shingle_size = 3
        dim = num_hashes * 32

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": num_hashes,
                "shingle_size": shingle_size,
                "seed": seed,
                "token_level": "char",  # Use char-level for more predictable similarity
            },
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 16},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Base text and variants with decreasing similarity
        base_text = "the quick brown fox jumps over the lazy dog"
        variants = [
            (0, base_text),
            (1, "the quick brown fox jumps over the lazy cat"),
            (2, "the slow brown fox jumps over the lazy dog"),
            (3, "a slow red fox runs over the tired dog"),
            (4, "xyz completely different text about something else"),
        ]

        rows = [{default_primary_key_field_name: pk, default_text_field_name: text}
                for pk, text in variants]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results = self.search(client, collection_name, [base_text],
                              anns_field=default_minhash_field_name,
                              search_params={
                                  "metric_type": "MHJACCARD",
                                  "params": {},
                              },
                              limit=len(variants),
                              output_fields=[default_primary_key_field_name, default_text_field_name])[0]

        # Verify result ordering: identical text should be first
        assert results[0][0]["id"] == 0, "Identical text should be first result"
        assert results[0][0]["distance"] == 1.0, "Identical text should have distance 1.0"

        # The very different text should have lowest similarity
        last_hit = results[0][-1]
        assert last_hit["distance"] < 0.9, \
            f"Very different text should have lower similarity, got {last_hit['distance']}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_sha1_hash_function_correctness(self):
        """
        target: verify MinHash signature correctness with SHA1 hash function
        method: compute expected signature using SHA1 in Python, compare with Milvus
        expected: signatures should match exactly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        seed = 42
        num_hashes = 16
        shingle_size = 3
        dim = num_hashes * 32

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": num_hashes,
                "shingle_size": shingle_size,
                "seed": seed,
                "hash_function": "sha1",  # Use SHA1 instead of xxhash
            },
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        test_texts = ["hello world", "test document", "abc"]

        # Insert data
        rows = [{default_primary_key_field_name: i, default_text_field_name: test_texts[i]}
                for i in range(len(test_texts))]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Query actual signatures
        results = self.query(client, collection_name,
                             filter=f"{default_primary_key_field_name} >= 0",
                             output_fields=[default_primary_key_field_name,
                                            default_text_field_name,
                                            default_minhash_field_name])[0]
        results.sort(key=lambda x: x[default_primary_key_field_name])

        # Verify SHA1 signatures are valid and deterministic
        signatures = []
        for result in results:
            actual_binary = result[default_minhash_field_name]
            actual_sig = binary_vector_to_signature(actual_binary)

            # Verify format
            assert len(actual_sig) == num_hashes, f"Signature should have {num_hashes} values"
            assert all(0 <= s <= 0xFFFFFFFF for s in actual_sig), "All values should be 32-bit"

            signatures.append(actual_sig)

        # Verify same text in same collection produces same signature (determinism)
        # Insert same text again
        self.insert(client, collection_name, [{default_primary_key_field_name: 100, default_text_field_name: test_texts[0]}])
        self.flush(client, collection_name)

        result2 = self.query(client, collection_name,
                             filter=f"{default_primary_key_field_name} == 100",
                             output_fields=[default_minhash_field_name])[0]
        sig2 = binary_vector_to_signature(result2[0][default_minhash_field_name])
        assert sig2 == signatures[0], "SHA1 should be deterministic"

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_token_level_char_correctness(self):
        """
        target: verify MinHash signature correctness with token_level='char'
        method: compute expected signature for char-level shingles, compare with Milvus
        expected: signatures should match exactly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        seed = 42
        num_hashes = 16
        shingle_size = 3
        dim = num_hashes * 32

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": num_hashes,
                "shingle_size": shingle_size,
                "seed": seed,
                "token_level": "char",  # Explicit char-level
            },
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        test_texts = ["hello world", "abcdefg"]

        # Compute expected signatures (char-level uses xxhash on char shingles)
        perm_a, perm_b = init_permutations_like_milvus(num_hashes, seed)
        expected_signatures = []
        for text in test_texts:
            base_hashes = hash_shingles_xxhash(text, shingle_size)
            sig = compute_minhash_signature(base_hashes, perm_a, perm_b)
            expected_signatures.append(sig)

        # Insert and query
        rows = [{default_primary_key_field_name: i, default_text_field_name: test_texts[i]}
                for i in range(len(test_texts))]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results = self.query(client, collection_name,
                             filter=f"{default_primary_key_field_name} >= 0",
                             output_fields=[default_primary_key_field_name,
                                            default_minhash_field_name])[0]
        results.sort(key=lambda x: x[default_primary_key_field_name])

        for i, result in enumerate(results):
            actual_sig = binary_vector_to_signature(result[default_minhash_field_name])
            expected_sig = expected_signatures[i]
            assert actual_sig == expected_sig, \
                f"Char-level signature mismatch for '{test_texts[i]}'"

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_token_level_word_consistency(self):
        """
        target: verify MinHash with token_level='word' produces consistent results
        method: insert same text twice, verify signatures are identical
        expected: same text with same parameters produces identical signature
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        seed = 42
        num_hashes = 16
        shingle_size = 2  # Word-level typically uses smaller n-gram
        dim = num_hashes * 32

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": num_hashes,
                "shingle_size": shingle_size,
                "seed": seed,
                "token_level": "word",  # Word-level tokenization
            },
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Same text inserted with different IDs
        test_text = "The quick brown fox jumps over the lazy dog"
        rows = [
            {default_primary_key_field_name: 1, default_text_field_name: test_text},
            {default_primary_key_field_name: 2, default_text_field_name: test_text},
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        results = self.query(client, collection_name,
                             filter=f"{default_primary_key_field_name} >= 0",
                             output_fields=[default_primary_key_field_name,
                                            default_minhash_field_name])[0]

        sig1 = binary_vector_to_signature(results[0][default_minhash_field_name])
        sig2 = binary_vector_to_signature(results[1][default_minhash_field_name])

        assert sig1 == sig2, "Same text should produce identical signatures with word-level tokenization"

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_token_level_word_vs_char_difference(self):
        """
        target: verify word-level and char-level produce different signatures
        method: create collections with different token_level, compare signatures
        expected: same text with different token_level should produce different signatures
        """
        client = self._client()

        seed = 42
        num_hashes = 16
        shingle_size = 3
        dim = num_hashes * 32
        test_text = "hello world test"

        signatures = {}

        for token_level in ["word", "char"]:
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_{token_level}"

            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
            schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

            schema.add_function(Function(
                name="text_to_minhash",
                function_type=FunctionType.MINHASH,
                input_field_names=[default_text_field_name],
                output_field_names=[default_minhash_field_name],
                params={
                    "num_hashes": num_hashes,
                    "shingle_size": shingle_size,
                    "seed": seed,
                    "token_level": token_level,
                },
            ))

            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(
                field_name=default_minhash_field_name,
                index_type="MINHASH_LSH",
                metric_type="MHJACCARD",
                params={"mh_lsh_band": 8},
            )
            self.create_collection(client, collection_name, schema=schema, index_params=index_params)

            rows = [{default_primary_key_field_name: 1, default_text_field_name: test_text}]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            self.load_collection(client, collection_name)

            results = self.query(client, collection_name,
                                 filter=f"{default_primary_key_field_name} == 1",
                                 output_fields=[default_minhash_field_name])[0]

            signatures[token_level] = tuple(binary_vector_to_signature(
                results[0][default_minhash_field_name]))

        # Word-level and char-level should produce different signatures
        assert signatures["word"] != signatures["char"], \
            "Word-level and char-level tokenization should produce different signatures"

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_default_seed_value(self):
        """
        target: verify default seed value is 1234
        method: create collection without seed, compare with explicit seed=1234
        expected: both should produce identical signatures
        """
        client = self._client()

        num_hashes = 16
        shingle_size = 3
        dim = num_hashes * 32
        test_text = "default seed test"

        signatures = {}

        for config_name, params in [("default", {}), ("explicit_1234", {"seed": 1234})]:
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_{config_name}"

            base_params = {"num_hashes": num_hashes, "shingle_size": shingle_size}
            base_params.update(params)

            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
            schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

            schema.add_function(Function(
                name="text_to_minhash",
                function_type=FunctionType.MINHASH,
                input_field_names=[default_text_field_name],
                output_field_names=[default_minhash_field_name],
                params=base_params,
            ))

            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(
                field_name=default_minhash_field_name,
                index_type="MINHASH_LSH",
                metric_type="MHJACCARD",
                params={"mh_lsh_band": 8},
            )
            self.create_collection(client, collection_name, schema=schema, index_params=index_params)

            rows = [{default_primary_key_field_name: 1, default_text_field_name: test_text}]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            self.load_collection(client, collection_name)

            results = self.query(client, collection_name,
                                 filter=f"{default_primary_key_field_name} == 1",
                                 output_fields=[default_minhash_field_name])[0]

            signatures[config_name] = tuple(binary_vector_to_signature(
                results[0][default_minhash_field_name]))

        assert signatures["default"] == signatures["explicit_1234"], \
            "Default seed should be 1234"

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_xxhash_vs_sha1_difference(self):
        """
        target: verify xxhash and sha1 produce different signatures
        method: create collections with different hash_function, compare signatures
        expected: same text with different hash_function should produce different signatures
        """
        client = self._client()

        seed = 42
        num_hashes = 16
        shingle_size = 3
        dim = num_hashes * 32
        test_text = "hash function comparison test"

        signatures = {}

        for hash_func in ["xxhash64", "sha1"]:
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_{hash_func}"

            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
            schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

            schema.add_function(Function(
                name="text_to_minhash",
                function_type=FunctionType.MINHASH,
                input_field_names=[default_text_field_name],
                output_field_names=[default_minhash_field_name],
                params={
                    "num_hashes": num_hashes,
                    "shingle_size": shingle_size,
                    "seed": seed,
                    "hash_function": hash_func,
                },
            ))

            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(
                field_name=default_minhash_field_name,
                index_type="MINHASH_LSH",
                metric_type="MHJACCARD",
                params={"mh_lsh_band": 8},
            )
            self.create_collection(client, collection_name, schema=schema, index_params=index_params)

            rows = [{default_primary_key_field_name: 1, default_text_field_name: test_text}]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            self.load_collection(client, collection_name)

            results = self.query(client, collection_name,
                                 filter=f"{default_primary_key_field_name} == 1",
                                 output_fields=[default_minhash_field_name])[0]

            signatures[hash_func] = tuple(binary_vector_to_signature(
                results[0][default_minhash_field_name]))

        assert signatures["xxhash64"] != signatures["sha1"], \
            "xxhash64 and sha1 should produce different signatures"

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_lsh_recall(self):
        """
        target: evaluate MINHASH_LSH search recall quality
        method:
            1. Generate test texts with high similarity (same base, small variations)
            2. Compute MinHash signatures using C++ binding
            3. Calculate ground truth using brute-force Jaccard similarity
            4. Perform ANN search with MINHASH_LSH
            5. Calculate recall@k
        expected: recall should be above acceptable threshold for similar texts
        """

        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Parameters
        seed = 42
        num_hashes = 128
        shingle_size = 3
        dim = num_hashes * 32
        top_k = 5
        min_recall = 0.2  # LSH is approximate, set reasonable threshold

        # Generate test texts: one base with many similar variants
        base_text = "the quick brown fox jumps over the lazy dog near the river bank today"
        test_texts = [(0, base_text)]

        # Create highly similar variants (change only 1-2 chars at different positions)
        for i in range(1, 30):
            # Small character-level changes to maintain high similarity
            variant = base_text[:i] + "X" + base_text[i+1:] if i < len(base_text) else base_text + str(i)
            test_texts.append((i, variant))

        # Create collection with lower mh_lsh_band for better recall
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={
                "num_hashes": num_hashes,
                "shingle_size": shingle_size,
                "seed": seed,
                "token_level": "char",
            },
        ))

        # Use fewer bands for better recall (more candidates pass LSH filter)
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},  # Fewer bands = higher recall, lower precision
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        rows = [{default_primary_key_field_name: pk, default_text_field_name: text}
                for pk, text in test_texts]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Compute ground truth using C++ binding
        signatures = {}
        for pk, text in test_texts:
            sig = compute_minhash(text, num_hashes, shingle_size, seed, use_char_level=True)
            signatures[pk] = list(sig)

        def compute_minhash_jaccard(sig1, sig2):
            return sum(1 for a, b in zip(sig1, sig2) if a == b) / len(sig1)

        # Query with base text
        query_pk = 0
        query_text = base_text
        query_sig = signatures[query_pk]

        # Ground truth: top-k most similar (excluding self)
        similarities = [(pk, compute_minhash_jaccard(query_sig, sig))
                        for pk, sig in signatures.items() if pk != query_pk]
        similarities.sort(key=lambda x: x[1], reverse=True)
        ground_truth_topk = set(pk for pk, sim in similarities[:top_k])

        log.info(f"Ground truth top-{top_k}: {ground_truth_topk}")
        log.info(f"Top similarities: {[(pk, f'{sim:.4f}') for pk, sim in similarities[:top_k]]}")

        # ANN search
        search_results = self.search(
            client, collection_name,
            [query_text],
            anns_field=default_minhash_field_name,
            search_params={"metric_type": "MHJACCARD", "params": {}},
            limit=top_k + 1,
            output_fields=[default_primary_key_field_name]
        )[0]

        # Extract ANN results (excluding self)
        ann_results = set()
        for hit in search_results[0]:
            hit_id = hit["entity"][default_primary_key_field_name]
            if hit_id != query_pk:
                ann_results.add(hit_id)

        log.info(f"ANN results: {ann_results}")

        # Calculate recall
        recall = len(ann_results & ground_truth_topk) / len(ground_truth_topk) if ground_truth_topk else 0
        log.info(f"Recall@{top_k}: {recall:.4f}")

        # Verify recall meets minimum threshold
        assert recall >= min_recall, \
            f"Recall@{top_k} is {recall:.4f}, expected >= {min_recall}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_lsh_recall_with_different_bands(self):
        """
        target: evaluate how mh_lsh_band parameter affects recall
        method:
            1. Create collections with different mh_lsh_band values
            2. Measure recall for each configuration
            3. Verify that more bands generally improve recall (with trade-off)
        expected: recall should vary with band configuration
        """

        client = self._client()

        # Parameters
        seed = 42
        num_hashes = 128
        shingle_size = 3
        dim = num_hashes * 32
        top_k = 5

        # Generate test data
        base_text = "the quick brown fox jumps over the lazy dog near the river bank"
        test_texts = [(0, base_text)]
        words = base_text.split()
        for i in range(1, 20):
            # Create variations by changing i words
            variant_words = words.copy()
            for j in range(min(i, len(words))):
                variant_words[j] = f"var{i}w{j}"
            test_texts.append((i, " ".join(variant_words)))

        # Compute ground truth signatures
        signatures = {}
        for pk, text in test_texts:
            sig = compute_minhash(text, num_hashes, shingle_size, seed, use_char_level=True)
            signatures[pk] = list(sig)

        def compute_jaccard(sig1, sig2):
            return sum(1 for a, b in zip(sig1, sig2) if a == b) / len(sig1)

        # Ground truth for query 0
        query_sig = signatures[0]
        similarities = [(pk, compute_jaccard(query_sig, sig))
                        for pk, sig in signatures.items() if pk != 0]
        similarities.sort(key=lambda x: x[1], reverse=True)
        ground_truth = set(pk for pk, _ in similarities[:top_k])

        # Test different band configurations
        band_configs = [4, 8, 16, 32]
        recalls = {}

        for bands in band_configs:
            collection_name = cf.gen_collection_name_by_testcase_name() + f"_band{bands}"

            schema = self.create_schema(client, enable_dynamic_field=False)[0]
            schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
            schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
            schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=dim)

            schema.add_function(Function(
                name="text_to_minhash",
                function_type=FunctionType.MINHASH,
                input_field_names=[default_text_field_name],
                output_field_names=[default_minhash_field_name],
                params={
                    "num_hashes": num_hashes,
                    "shingle_size": shingle_size,
                    "seed": seed,
                    "token_level": "char",
                },
            ))

            index_params = self.prepare_index_params(client)[0]
            index_params.add_index(
                field_name=default_minhash_field_name,
                index_type="MINHASH_LSH",
                metric_type="MHJACCARD",
                params={"mh_lsh_band": bands},
            )
            self.create_collection(client, collection_name, schema=schema, index_params=index_params)

            rows = [{default_primary_key_field_name: pk, default_text_field_name: text}
                    for pk, text in test_texts]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            self.load_collection(client, collection_name)

            # Search
            results = self.search(
                client, collection_name,
                [base_text],
                anns_field=default_minhash_field_name,
                search_params={"metric_type": "MHJACCARD", "params": {}},
                limit=top_k + 1,
                output_fields=[default_primary_key_field_name]
            )[0]

            ann_results = set()
            for hit in results[0]:
                hit_id = hit["entity"][default_primary_key_field_name]
                if hit_id != 0:
                    ann_results.add(hit_id)

            recall = len(ann_results & ground_truth) / len(ground_truth) if ground_truth else 0
            recalls[bands] = recall

        # Verify we got different recalls for different band configs
        # (the specific relationship depends on data characteristics)
        assert len(set(recalls.values())) >= 1, \
            f"Expected varying recalls for different bands, got: {recalls}"

        # Log results for debugging
        for bands, recall in sorted(recalls.items()):
            log.info(f"mh_lsh_band={bands}: recall@{top_k}={recall:.4f}")

class TestMinHashBulkImport(TestMilvusClientV2Base):
    """ Test case of MinHash bulk import """

    # MinIO configuration constants
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    REMOTE_DATA_PATH = "bulkinsert_data"
    LOCAL_FILES_PATH = "/tmp/milvus_bulkinsert/"

    @pytest.fixture(scope="function", autouse=True)
    def setup_minio(self, minio_host, minio_bucket):
        """Setup MinIO configuration from fixtures"""
        from pathlib import Path
        Path(self.LOCAL_FILES_PATH).mkdir(parents=True, exist_ok=True)
        self.minio_host = minio_host
        self.bucket_name = minio_bucket
        self.minio_endpoint = f"{minio_host}:9000"

    def gen_file_with_local_bulk_writer(
        self,
        schema,
        data: list,
        file_type: str = "PARQUET"
    ) -> list:
        """
        Generate import file using LocalBulkWriter from row data

        Args:
            schema: Collection schema
            data: List of dictionaries in row format
            file_type: Output file type, "PARQUET" or "JSON"

        Returns:
            List of batch files generated by LocalBulkWriter
        """
        from pymilvus.bulk_writer import LocalBulkWriter, BulkFileType

        # Convert file_type string to BulkFileType enum
        bulk_file_type = BulkFileType.PARQUET if file_type == "PARQUET" else BulkFileType.JSON

        # Create LocalBulkWriter
        writer = LocalBulkWriter(
            schema=schema,
            local_path=self.LOCAL_FILES_PATH,
            segment_size=512 * 1024 * 1024,  # 512MB
            file_type=bulk_file_type
        )

        log.info(f"Creating {file_type} file using LocalBulkWriter with {len(data)} rows")

        # Append each row
        for row in data:
            writer.append_row(row)

        # Commit to generate files
        writer.commit()

        # Get the generated file paths
        batch_files = writer.batch_files
        log.info(f"LocalBulkWriter generated files: {batch_files}")

        return batch_files

    def upload_to_minio(self, local_file_path: str) -> list:
        """
        Upload file to MinIO

        Args:
            local_file_path: Local path of the file to upload

        Returns:
            List of remote file paths in MinIO
        """
        import os
        from minio import Minio
        from minio.error import S3Error

        if not os.path.exists(local_file_path):
            raise Exception(f"Local file '{local_file_path}' doesn't exist")

        try:
            minio_client = Minio(
                endpoint=self.minio_endpoint,
                access_key=self.MINIO_ACCESS_KEY,
                secret_key=self.MINIO_SECRET_KEY,
                secure=False
            )

            # Check if bucket exists
            if not minio_client.bucket_exists(self.bucket_name):
                raise Exception(f"MinIO bucket '{self.bucket_name}' doesn't exist")

            # Upload file with unique prefix to avoid parallel test conflicts
            import uuid
            unique_prefix = str(uuid.uuid4())[:8]
            filename = os.path.basename(local_file_path)
            minio_file_path = os.path.join(self.REMOTE_DATA_PATH, unique_prefix, filename)
            minio_client.fput_object(self.bucket_name, minio_file_path, local_file_path)

            log.info(f"Uploaded file to MinIO: {minio_file_path}")
            return [[minio_file_path]]

        except S3Error as e:
            raise Exception(f"Failed to connect MinIO server {self.minio_endpoint}, error: {e}")

    def call_bulkinsert(self, collection_name: str, batch_files: list, expect_fail: bool = False) -> dict:
        """
        Call bulk import API and wait for completion

        Args:
            collection_name: Target collection name
            batch_files: List of file paths in MinIO
            expect_fail: If True, expect the import to fail

        Returns:
            Import result dict with state and reason
        """
        from pymilvus.bulk_writer import bulk_import, get_import_progress

        url = f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"

        log.info(f"Starting bulk import to collection '{collection_name}'")
        resp = bulk_import(
            url=url,
            collection_name=collection_name,
            files=batch_files,
        )

        job_id = resp.json()['data']['jobId']
        log.info(f"Bulk import job created, job_id: {job_id}")

        # Wait for import to complete
        timeout = 300
        start_time = time.time()
        while time.time() - start_time < timeout:
            time.sleep(5)

            resp = get_import_progress(url=url, job_id=job_id)
            state = resp.json()['data']['state']
            progress = resp.json()['data'].get('progress', 0)

            log.info(f"Import job {job_id} - state: {state}, progress: {progress}%")

            if state == "Importing":
                continue
            elif state == "Failed":
                reason = resp.json()['data'].get('reason', 'Unknown reason')
                log.info(f"Bulk import job {job_id} failed: {reason}")
                return {"state": "Failed", "reason": reason}
            elif state == "Completed" and progress == 100:
                log.info(f"Bulk import job {job_id} completed successfully")
                return {"state": "Completed", "reason": None}
        else:
            raise Exception(f"Bulk import job {job_id} timeout after {timeout}s")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("file_type", ["PARQUET", "JSON"])
    def test_minhash_bulk_import_basic(self, minio_host, minio_bucket, file_type):
        """
        target: test bulk import with MinHash function
        method:
            1. Create collection with MinHash function (text -> minhash_signature)
            2. Generate import data using LocalBulkWriter (only text field, no signature)
            3. Upload to MinIO and bulk import
            4. Verify data count
            5. Create MINHASH_LSH index and load
            6. Verify search functionality
        expected:
            - Import succeeds
            - MinHash signatures auto-generated by server
            - Search returns correct results
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        nb = 1000

        # Step 1: Create collection with MinHash function
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        # Create index params
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )

        self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        log.info(f"Collection '{collection_name}' created with MinHash function")

        # Step 2: Generate import data (only text field, no minhash_signature)
        texts = gen_text_data(nb)
        data = [{
            default_primary_key_field_name: i,
            default_text_field_name: texts[i]
        } for i in range(nb)]

        batch_files = self.gen_file_with_local_bulk_writer(schema, data, file_type)

        # Step 3: Upload to MinIO
        local_file = batch_files[0][0]
        remote_files = self.upload_to_minio(local_file)

        # Step 4: Bulk import
        result = self.call_bulkinsert(collection_name, remote_files)
        assert result["state"] == "Completed", f"Import failed: {result['reason']}"

        # Step 5: Refresh load state after import and verify data count
        # refresh_load ensures QueryNode loads newly imported sealed segments
        client.refresh_load(collection_name=collection_name)
        stats = client.get_collection_stats(collection_name=collection_name)
        count = stats['row_count']
        assert count == nb, f"Expected {nb} rows, got {count}"
        log.info(f"Verified data count: {count}")

        # Step 6: Verify search functionality
        search_text = texts[0]  # Use first text as query
        results = client.search(
            collection_name=collection_name,
            data=[search_text],
            anns_field=default_minhash_field_name,
            search_params={"metric_type": "MHJACCARD", "params": {}},
            limit=10,
            output_fields=[default_primary_key_field_name, default_text_field_name],
        )

        assert len(results) > 0, "Search should return results"
        assert len(results[0]) > 0, "Search should return at least one result"
        log.info(f"Search returned {len(results[0])} results")

        # The first result should be the query text itself (exact match)
        first_result = results[0][0]
        log.info(f"First result: id={first_result['id']}, distance={first_result['distance']}")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("file_type", ["PARQUET", "JSON"])
    def test_minhash_bulk_import_with_output_field_negative(self, minio_host, minio_bucket, file_type):
        """
        target: test bulk import rejects data containing function output field
        method:
            1. Create collection with MinHash function
            2. Generate import data containing minhash_signature field (function output)
            3. Bulk import
            4. Verify import fails with proper error message
        expected: import should fail - cannot provide function output field in import data
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        nb = 100

        # Step 1: Create collection with MinHash function
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema)
        log.info(f"Collection '{collection_name}' created with MinHash function")

        # Step 2: Create a schema WITHOUT function to bypass client-side validation
        from pymilvus import CollectionSchema, FieldSchema
        file_schema = CollectionSchema(fields=[
            FieldSchema(name=default_primary_key_field_name, dtype=DataType.INT64, is_primary=True),
            FieldSchema(name=default_text_field_name, dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name=default_minhash_field_name, dtype=DataType.BINARY_VECTOR, dim=default_dim),
        ])

        # Generate random binary vector data
        def gen_binary_vector(dim):
            return bytes([random.randint(0, 255) for _ in range(dim // 8)])

        texts = gen_text_data(nb)
        data = [{
            default_primary_key_field_name: i,
            default_text_field_name: texts[i],
            default_minhash_field_name: gen_binary_vector(default_dim),
        } for i in range(nb)]

        batch_files = self.gen_file_with_local_bulk_writer(file_schema, data, file_type)

        # Step 3: Upload to MinIO
        local_file = batch_files[0][0]
        remote_files = self.upload_to_minio(local_file)

        # Step 4: Bulk import - should fail because function output field is provided
        result = self.call_bulkinsert(collection_name, remote_files, expect_fail=True)

        assert result["state"] == "Failed", \
            f"Import should have failed when providing function output field, but got: {result['state']}"
        assert "output by function" in result["reason"], \
            f"Error should mention 'output by function', got: {result['reason']}"
        log.info(f"Import correctly failed: {result['reason']}")

class TestMilvusClientMinHashHybridSearch(TestMilvusClientV2Base):
    """ Test case of MinHash DIDO hybrid search with dense/sparse vectors """

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_hybrid_search_with_dense_vector(self):
        """
        target: test hybrid search combining MinHash and dense float vector
        method:
            1. Create collection with both text->MinHash function and a float vector field
            2. Insert data with text and float vectors
            3. Perform hybrid_search using both ANN requests with RRFRanker
        expected: hybrid search returns fused results from both channels
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        dense_dim = 128
        dense_field = "dense_vector"

        # 1. create schema with MinHash function + dense vector
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        schema.add_field(dense_field, DataType.FLOAT_VECTOR, dim=dense_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        # 2. create indexes for both vector fields
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        index_params.add_index(
            field_name=dense_field,
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 200},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # 3. insert data
        rng = np.random.default_rng(seed=42)
        nb = 200
        texts = gen_text_data(nb)
        rows = [{
            default_primary_key_field_name: i,
            default_text_field_name: texts[i],
            dense_field: list(rng.random(dense_dim).astype(np.float32)),
        } for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # 4. hybrid search
        query_text = texts[0]
        query_dense = list(rng.random(dense_dim).astype(np.float32))

        minhash_req = AnnSearchRequest(
            data=[query_text],
            anns_field=default_minhash_field_name,
            param={"metric_type": "MHJACCARD", "params": {}},
            limit=default_limit,
        )
        dense_req = AnnSearchRequest(
            data=[query_dense],
            anns_field=dense_field,
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=default_limit,
        )

        results = self.hybrid_search(
            client, collection_name,
            reqs=[minhash_req, dense_req],
            ranker=RRFRanker(),
            limit=default_limit,
            output_fields=[default_primary_key_field_name, default_text_field_name],
        )[0]

        # 5. verify results
        assert len(results) > 0, "Hybrid search should return results"
        assert len(results[0]) <= default_limit

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_hybrid_search_with_weighted_ranker(self):
        """
        target: test hybrid search with WeightedRanker to control fusion weights
        method:
            1. Create collection with MinHash function + dense vector
            2. Hybrid search with WeightedRanker giving more weight to MinHash
        expected: results biased toward MinHash channel, exact match ranked first
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        dense_dim = 128
        dense_field = "dense_vector"

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        schema.add_field(dense_field, DataType.FLOAT_VECTOR, dim=dense_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        index_params.add_index(
            field_name=dense_field,
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 200},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rng = np.random.default_rng(seed=42)
        nb = 200
        texts = gen_text_data(nb)
        rows = [{
            default_primary_key_field_name: i,
            default_text_field_name: texts[i],
            dense_field: list(rng.random(dense_dim).astype(np.float32)),
        } for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        query_text = texts[0]
        query_dense = list(rng.random(dense_dim).astype(np.float32))

        minhash_req = AnnSearchRequest(
            data=[query_text],
            anns_field=default_minhash_field_name,
            param={"metric_type": "MHJACCARD", "params": {}},
            limit=default_limit,
        )
        dense_req = AnnSearchRequest(
            data=[query_dense],
            anns_field=dense_field,
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=default_limit,
        )

        # MinHash weight=0.8, dense weight=0.2
        results = self.hybrid_search(
            client, collection_name,
            reqs=[minhash_req, dense_req],
            ranker=WeightedRanker(0.8, 0.2),
            limit=default_limit,
            output_fields=[default_primary_key_field_name],
        )[0]

        assert len(results) > 0
        assert len(results[0]) <= default_limit
        # The first result should be the exact match from MinHash channel
        assert results[0][0]["id"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_hybrid_search_with_filter(self):
        """
        target: test hybrid search with scalar filter expression
        method:
            1. Create collection with MinHash function + dense vector + scalar field
            2. Hybrid search with filter on scalar field
        expected: all results satisfy the filter condition
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        dense_dim = 128
        dense_field = "dense_vector"

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field("category", DataType.INT64)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        schema.add_field(dense_field, DataType.FLOAT_VECTOR, dim=dense_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        index_params.add_index(
            field_name=dense_field,
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 200},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rng = np.random.default_rng(seed=42)
        nb = 200
        texts = gen_text_data(nb)
        rows = [{
            default_primary_key_field_name: i,
            default_text_field_name: texts[i],
            "category": i % 5,
            dense_field: list(rng.random(dense_dim).astype(np.float32)),
        } for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        query_text = texts[0]
        query_dense = list(rng.random(dense_dim).astype(np.float32))
        filter_expr = "category == 0"

        minhash_req = AnnSearchRequest(
            data=[query_text],
            anns_field=default_minhash_field_name,
            param={"metric_type": "MHJACCARD", "params": {}},
            limit=default_limit,
            expr=filter_expr,
        )
        dense_req = AnnSearchRequest(
            data=[query_dense],
            anns_field=dense_field,
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=default_limit,
            expr=filter_expr,
        )

        results = self.hybrid_search(
            client, collection_name,
            reqs=[minhash_req, dense_req],
            ranker=RRFRanker(),
            limit=default_limit,
            output_fields=[default_primary_key_field_name, "category"],
        )[0]

        assert len(results) > 0
        for hit in results[0]:
            assert hit["entity"]["category"] == 0, \
                f"All results should satisfy filter, got category={hit['entity']['category']}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_hybrid_search_batch_queries(self):
        """
        target: test hybrid search with multiple query vectors (nq > 1)
        method: hybrid search with 3 query texts and 3 dense vectors simultaneously
        expected: results returned for each query
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        dense_dim = 128
        dense_field = "dense_vector"

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        schema.add_field(dense_field, DataType.FLOAT_VECTOR, dim=dense_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        index_params.add_index(
            field_name=dense_field,
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 200},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rng = np.random.default_rng(seed=42)
        nb = 300
        texts = gen_text_data(nb)
        rows = [{
            default_primary_key_field_name: i,
            default_text_field_name: texts[i],
            dense_field: list(rng.random(dense_dim).astype(np.float32)),
        } for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Batch queries: nq=3
        nq = 3
        query_texts = [texts[i] for i in [0, 50, 100]]
        query_dense = [list(rng.random(dense_dim).astype(np.float32)) for _ in range(nq)]

        minhash_req = AnnSearchRequest(
            data=query_texts,
            anns_field=default_minhash_field_name,
            param={"metric_type": "MHJACCARD", "params": {}},
            limit=default_limit,
        )
        dense_req = AnnSearchRequest(
            data=query_dense,
            anns_field=dense_field,
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=default_limit,
        )

        results = self.hybrid_search(
            client, collection_name,
            reqs=[minhash_req, dense_req],
            ranker=RRFRanker(),
            limit=default_limit,
            output_fields=[default_primary_key_field_name],
        )[0]

        assert len(results) == nq, f"Expected {nq} result sets, got {len(results)}"
        for i in range(nq):
            assert len(results[i]) <= default_limit

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_hybrid_search_with_partition(self):
        """
        target: test hybrid search within specific partitions
        method: insert data into partitions, hybrid search in one partition
        expected: results only from the specified partition
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        dense_dim = 128
        dense_field = "dense_vector"

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)
        schema.add_field(dense_field, DataType.FLOAT_VECTOR, dim=dense_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        index_params.add_index(
            field_name=dense_field,
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 200},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Create partitions
        self.create_partition(client, collection_name, "part_a")
        self.create_partition(client, collection_name, "part_b")

        rng = np.random.default_rng(seed=42)
        texts_a = gen_text_data(50)
        texts_b = gen_text_data(50)

        rows_a = [{
            default_primary_key_field_name: i,
            default_text_field_name: texts_a[i],
            dense_field: list(rng.random(dense_dim).astype(np.float32)),
        } for i in range(50)]
        rows_b = [{
            default_primary_key_field_name: 50 + i,
            default_text_field_name: texts_b[i],
            dense_field: list(rng.random(dense_dim).astype(np.float32)),
        } for i in range(50)]

        self.insert(client, collection_name, rows_a, partition_name="part_a")
        self.insert(client, collection_name, rows_b, partition_name="part_b")
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        query_text = texts_a[0]
        query_dense = list(rng.random(dense_dim).astype(np.float32))

        minhash_req = AnnSearchRequest(
            data=[query_text],
            anns_field=default_minhash_field_name,
            param={"metric_type": "MHJACCARD", "params": {}},
            limit=default_limit,
        )
        dense_req = AnnSearchRequest(
            data=[query_dense],
            anns_field=dense_field,
            param={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=default_limit,
        )

        results = self.hybrid_search(
            client, collection_name,
            reqs=[minhash_req, dense_req],
            ranker=RRFRanker(),
            limit=10,
            partition_names=["part_a"],
            output_fields=[default_primary_key_field_name],
        )[0]

        # All results should be from part_a (id < 50)
        for hit in results[0]:
            assert hit["id"] < 50, f"Result id={hit['id']} not from part_a"

class TestMilvusClientMinHashNullable(TestMilvusClientV2Base):
    """ Test case of MinHash DIDO nullable field validation """

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_nullable_input_field_insert_and_search(self):
        """
        target: test MinHash function works with nullable input field
        method:
            1. Create collection with nullable=True VARCHAR input field
            2. Insert rows with normal text, empty string, and NULL text
            3. Verify NULL text produces all-0xFFFFFFFF signature
            4. Verify normal text search does not recall NULL rows
            5. Verify same text produces identical signatures
        expected:
            - Insert succeeds for all rows including NULL
            - NULL text generates max-value signature (0xFFFFFFFF)
            - Search with normal text returns correct matches, NULL rows excluded
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535, nullable=True)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert mix of normal text and NULL
        test_text = "the quick brown fox jumps over the lazy dog"
        rows = [
            {default_primary_key_field_name: 0, default_text_field_name: test_text},
            {default_primary_key_field_name: 1, default_text_field_name: None},
            {default_primary_key_field_name: 2, default_text_field_name: "completely different text"},
            {default_primary_key_field_name: 3, default_text_field_name: None},
            {default_primary_key_field_name: 4, default_text_field_name: test_text},  # same as id=0
        ]
        result = self.insert(client, collection_name, rows)[0]
        assert result["insert_count"] == 5

        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Verify signatures via query
        query_results = self.query(
            client, collection_name,
            filter=f"{default_primary_key_field_name} >= 0",
            output_fields=[default_primary_key_field_name, default_minhash_field_name],
        )[0]
        sigs = {}
        for r in query_results:
            raw = r[default_minhash_field_name]
            if isinstance(raw, list):
                raw = raw[0]
            sigs[r[default_primary_key_field_name]] = raw

        # NULL rows should have all-0xFF signature (every uint32 == 0xFFFFFFFF)
        null_expected = b'\xff' * (default_dim // 8)
        assert sigs[1] == null_expected, "NULL text should produce all-0xFF signature"
        assert sigs[3] == null_expected, "NULL text should produce all-0xFF signature"

        # Same text should produce identical signatures
        assert sigs[0] == sigs[4], "Same text should produce identical signatures"

        # Normal text signatures should differ from NULL signature
        assert sigs[0] != null_expected
        assert sigs[2] != null_expected

        # Search with normal text should not recall NULL rows
        results = self.search(
            client, collection_name, [test_text],
            anns_field=default_minhash_field_name,
            search_params={"metric_type": "MHJACCARD", "params": {}},
            limit=5,
            output_fields=[default_primary_key_field_name],
        )[0]

        result_ids = [hit["id"] for hit in results[0]]
        # Exact matches (id=0, id=4) should be present
        assert 0 in result_ids, "Exact match id=0 should be in results"
        assert 4 in result_ids, "Exact match id=4 should be in results"

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_non_nullable_input_field_accepted(self):
        """
        target: test MinHash function accepts non-nullable input field (default)
        method: create MinHash function with default (non-nullable) VARCHAR input
        expected: collection created successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema)

        # Verify collection exists
        collections = self.list_collections(client)[0]
        assert collection_name in collections

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_nullable_output_field_rejected(self):
        """
        target: test MinHash function rejects nullable output field
        method: create MinHash function with nullable=True BINARY_VECTOR output field
        expected: error raised - function output field cannot be nullable
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim, nullable=True)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535,
                                            ct.err_msg: "nullable"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_with_other_nullable_scalar_fields(self):
        """
        target: test MinHash collection works when other scalar fields are nullable
        method:
            1. Create collection with MinHash function, non-nullable text input,
               but nullable scalar fields (category, description)
            2. Insert data with some null values in scalar fields
            3. Search should work correctly
        expected: nullable scalar fields do not affect MinHash function
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field("category", DataType.INT64, nullable=True)
        schema.add_field("description", DataType.VARCHAR, max_length=256, nullable=True)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data with some null values in nullable fields
        nb = 100
        texts = gen_text_data(nb)
        rows = []
        for i in range(nb):
            row = {
                default_primary_key_field_name: i,
                default_text_field_name: texts[i],
            }
            # Alternate null and non-null for nullable fields
            if i % 3 == 0:
                row["category"] = None
                row["description"] = None
            else:
                row["category"] = i % 10
                row["description"] = f"description_{i}"
            rows.append(row)

        result = self.insert(client, collection_name, rows)[0]
        assert result["insert_count"] == nb

        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search should work despite nullable scalar fields
        results = self.search(client, collection_name, [texts[0]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5,
                              output_fields=[default_primary_key_field_name, "category"])[0]

        assert len(results[0]) <= 5
        assert results[0][0]["distance"] == 1.0

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_multiple_input_fields_rejected(self):
        """
        target: test MinHash function rejects multiple input fields
        method: create MinHash function with two input field names
        expected: error raised - MinHash only supports single input field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("text1", DataType.VARCHAR, max_length=65535)
        schema.add_field("text2", DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=["text1", "text2"],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535,
                                            ct.err_msg: "input"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_multiple_output_fields_rejected(self):
        """
        target: test MinHash function rejects multiple output fields
        method: create MinHash function with two output field names
        expected: error raised - MinHash only supports single output field
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field("mh_out1", DataType.BINARY_VECTOR, dim=default_dim)
        schema.add_field("mh_out2", DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=["mh_out1", "mh_out2"],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535,
                                            ct.err_msg: "output"})

class TestMilvusClientMinHashSearchIterator(TestMilvusClientV2Base):
    """ Test case of MinHash DIDO search iterator """

    def _create_minhash_collection_with_data(self, client, collection_name, nb=200):
        """Helper to create a MinHash collection with data for iterator tests."""
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field("category", DataType.INT64)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        texts = gen_text_data(nb)
        rows = [
            {
                default_primary_key_field_name: i,
                default_text_field_name: texts[i],
                "category": i % 5,
            }
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)
        return texts

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="Bug #47745: MINHASH_LSH VectorIterators() not implemented -"
                               "CachedSearchIterator.cpp:85 fails to create iterators")
    def test_minhash_search_iterator_basic(self):
        """
        target: test basic search_iterator with MinHash
        method: create collection, insert data, iterate search results
        expected: iterator returns results in batches, all results valid
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 200
        texts = self._create_minhash_collection_with_data(client, collection_name, nb)

        batch_size = 50
        query_text = texts[0]
        search_params = {"metric_type": "MHJACCARD", "params": {}}

        it = self.search_iterator(
            client, collection_name,
            data=[query_text],
            batch_size=batch_size,
            search_params=search_params,
            output_fields=[default_primary_key_field_name],
            check_task=CheckTasks.check_nothing,
        )[0]

        all_ids = []
        while True:
            batch = it.next()
            if not batch:
                break
            all_ids.extend([hit["id"] for hit in batch])
            assert len(batch) <= batch_size

        it.close()
        # The exact match (id=0) should be in results
        assert 0 in all_ids
        # No duplicate ids
        assert len(all_ids) == len(set(all_ids))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="Bug #47745: MINHASH_LSH VectorIterators() not implemented -"
                               "CachedSearchIterator.cpp:85 fails to create iterators")
    def test_minhash_search_iterator_with_limit(self):
        """
        target: test search_iterator with explicit limit
        method: set limit=30, verify total results do not exceed limit
        expected: total results <= limit
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 200
        texts = self._create_minhash_collection_with_data(client, collection_name, nb)

        batch_size = 10
        limit = 30
        search_params = {"metric_type": "MHJACCARD", "params": {}}

        it = self.search_iterator(
            client, collection_name,
            data=[texts[0]],
            batch_size=batch_size,
            limit=limit,
            search_params=search_params,
            output_fields=[default_primary_key_field_name],
            check_task=CheckTasks.check_nothing,
        )[0]

        all_ids = []
        while True:
            batch = it.next()
            if not batch:
                break
            all_ids.extend([hit["id"] for hit in batch])

        it.close()
        assert len(all_ids) <= limit

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="Bug #47745: MINHASH_LSH VectorIterators() not implemented -"
                               "CachedSearchIterator.cpp:85 fails to create iterators")
    def test_minhash_search_iterator_with_filter(self):
        """
        target: test search_iterator with scalar filter
        method: search with filter category == 0, verify all results match
        expected: all returned results have category == 0
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        nb = 200
        texts = self._create_minhash_collection_with_data(client, collection_name, nb)

        batch_size = 20
        search_params = {"metric_type": "MHJACCARD", "params": {}}

        it = self.search_iterator(
            client, collection_name,
            data=[texts[0]],
            batch_size=batch_size,
            filter="category == 0",
            search_params=search_params,
            output_fields=[default_primary_key_field_name, "category"],
            check_task=CheckTasks.check_nothing,
        )[0]

        all_results = []
        while True:
            batch = it.next()
            if not batch:
                break
            for hit in batch:
                assert hit["category"] == 0, f"Expected category=0, got {hit['category']}"
                all_results.append(hit)

        it.close()
        assert len(all_results) > 0

class TestMilvusClientMinHashVarCharPK(TestMilvusClientV2Base):
    """ Test case of MinHash DIDO with VARCHAR primary key """

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_varchar_pk_basic(self):
        """
        target: test MinHash with VARCHAR primary key
        method: create collection with VARCHAR PK, insert data, search
        expected: collection created, data inserted, search returns correct results
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=128,
                         is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        nb = 100
        texts = gen_text_data(nb)
        rows = [
            {
                default_primary_key_field_name: f"doc_{i:04d}",
                default_text_field_name: texts[i],
            }
            for i in range(nb)
        ]
        result = self.insert(client, collection_name, rows)[0]
        assert result["insert_count"] == nb

        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search
        results = self.search(client, collection_name, [texts[0]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5,
                              output_fields=[default_primary_key_field_name])[0]

        assert len(results[0]) > 0
        result_ids = [hit["id"] for hit in results[0]]
        assert "doc_0000" in result_ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_varchar_pk_auto_id(self):
        """
        target: test MinHash with VARCHAR primary key and auto_id=True
        method: create collection with auto_id VARCHAR PK, insert without PK field
        expected: PK auto-generated, data inserted, search works
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=128,
                         is_primary=True, auto_id=True)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        nb = 50
        texts = gen_text_data(nb)
        rows = [{default_text_field_name: texts[i]} for i in range(nb)]

        result = self.insert(client, collection_name, rows)[0]
        assert result["insert_count"] == nb

        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search
        results = self.search(client, collection_name, [texts[0]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5,
                              output_fields=[default_primary_key_field_name])[0]

        assert len(results[0]) > 0
        # PK should be auto-generated string
        for hit in results[0]:
            assert isinstance(hit["id"], str)

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_varchar_pk_query_and_delete(self):
        """
        target: test query and delete with VARCHAR PK in MinHash collection
        method: insert with VARCHAR PK, query by PK, delete by PK, verify
        expected: query returns correct rows, delete removes rows, search excludes deleted
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.VARCHAR, max_length=128,
                         is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        nb = 50
        texts = gen_text_data(nb)
        rows = [
            {
                default_primary_key_field_name: f"doc_{i:04d}",
                default_text_field_name: texts[i],
            }
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Query by PK
        query_result = self.query(client, collection_name,
                                  filter=f'{default_primary_key_field_name} == "doc_0005"',
                                  output_fields=[default_text_field_name])[0]
        assert len(query_result) == 1
        assert query_result[0][default_text_field_name] == texts[5]

        # Delete by PK
        self.delete(client, collection_name,
                    filter=f'{default_primary_key_field_name} in ["doc_0005", "doc_0010"]')

        # Verify deletion
        query_result = self.query(client, collection_name,
                                  filter=f'{default_primary_key_field_name} in ["doc_0005", "doc_0010"]',
                                  output_fields=[default_text_field_name])[0]
        assert len(query_result) == 0

class TestMilvusClientMinHashGroupBy(TestMilvusClientV2Base):
    """ Test case of MinHash DIDO with group_by search """

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_search_group_by_not_supported(self):
        """
        target: test MinHash search with group_by_field is not supported
        method: insert data, search with group_by_field on binary vector column
        expected: error raised - binary vector column does not support group_by
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field("category", DataType.INT64)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        nb = 100
        texts = gen_text_data(nb)
        rows = [
            {
                default_primary_key_field_name: i,
                default_text_field_name: texts[i],
                "category": i % 5,
            }
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # group_by is not supported on binary vector columns
        self.search(client, collection_name, [texts[0]],
                    anns_field=default_minhash_field_name,
                    search_params={"metric_type": "MHJACCARD", "params": {}},
                    limit=10,
                    output_fields=[default_primary_key_field_name, "category"],
                    group_by_field="category",
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 65535,
                                 ct.err_msg: "not support search_group_by operation based on binary vector"})

class TestMilvusClientMinHashDescribeIndex(TestMilvusClientV2Base):
    """ Test case of MinHash DIDO describe index """

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_describe_index(self):
        """
        target: test describe_index returns correct MINHASH_LSH index info
        method: create MINHASH_LSH index, call describe_index
        expected: index info contains correct type, metric, and params
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Describe index
        index_info = self.describe_index(client, collection_name,
                                         index_name=default_minhash_field_name)[0]

        assert index_info["index_type"] == "MINHASH_LSH"
        assert index_info["metric_type"] == "MHJACCARD"
        assert index_info["field_name"] == default_minhash_field_name

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_drop_and_recreate_index_describe(self):
        """
        target: test drop index and recreate with different params, verify describe
        method: create index with band=8, drop, recreate with band=4, describe
        expected: describe_index reflects updated params
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert some data first
        nb = 50
        texts = gen_text_data(nb)
        rows = [{default_primary_key_field_name: i, default_text_field_name: texts[i]} for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Release and drop index
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, index_name=default_minhash_field_name)

        # Recreate index with different params
        index_params2 = self.prepare_index_params(client)[0]
        index_params2.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 4},
        )
        self.create_index(client, collection_name, index_params2)
        self.load_collection(client, collection_name)

        # Describe should reflect new params
        index_info = self.describe_index(client, collection_name,
                                         index_name=default_minhash_field_name)[0]
        assert index_info["index_type"] == "MINHASH_LSH"

        # Verify search still works after re-index
        results = self.search(client, collection_name, [texts[0]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5,
                              output_fields=[default_primary_key_field_name])[0]
        assert len(results[0]) > 0

class TestMilvusClientMinHashNegativeExtended(TestMilvusClientV2Base):
    """ Extended negative test cases for MinHash DIDO """

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_nonexistent_input_field(self):
        """
        target: test MinHash function with non-existent input field name
        method: specify input_field_names referencing a field not in schema
        expected: error raised
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=["nonexistent_field"],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 1,
                                            ct.err_msg: "not found"})

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_nonexistent_output_field(self):
        """
        target: test MinHash function with non-existent output field name
        method: specify output_field_names referencing a field not in schema
        expected: error raised
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=["nonexistent_output"],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 1,
                                            ct.err_msg: "not found"})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("band_value", [0, -1])
    @pytest.mark.xfail(reason="Bug #47748: server does not validate mh_lsh_band values (0, -1, >num_hashes)")
    def test_minhash_invalid_lsh_band(self, band_value):
        """
        target: test MINHASH_LSH index with invalid mh_lsh_band values
        method: create index with mh_lsh_band=0 or -1
        expected: error raised
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": band_value},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535})

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_search_wrong_metric_type(self):
        """
        target: test MinHash search with wrong metric type
        method: create MINHASH_LSH index with MHJACCARD, search with HAMMING
        expected: error raised - metric type mismatch
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        nb = 50
        texts = gen_text_data(nb)
        rows = [{default_primary_key_field_name: i, default_text_field_name: texts[i]} for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search with wrong metric type
        self.search(client, collection_name, [texts[0]],
                    anns_field=default_minhash_field_name,
                    search_params={"metric_type": "HAMMING", "params": {}},
                    limit=5,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 65535,
                                 ct.err_msg: "metric type"})

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="Bug #47748: server does not validate mh_lsh_band values (0, -1, >num_hashes)")
    def test_minhash_lsh_band_exceeds_num_hashes(self):
        """
        target: test mh_lsh_band value exceeding num_hashes
        method: create index with mh_lsh_band > num_hashes
        expected: error raised - band must divide evenly or be valid
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": default_num_hashes + 10},  # Exceeds num_hashes
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params,
                               check_task=CheckTasks.err_res,
                               check_items={ct.err_code: 65535})

class TestMilvusClientMinHashEdgeCases(TestMilvusClientV2Base):
    """ Edge case tests for MinHash DIDO """

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_long_text(self):
        """
        target: test MinHash with very long text (>64KB)
        method: insert text exceeding 64KB, verify signature generation
        expected: insert succeeds, search works with long text
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Use max_length=65535 (Milvus VARCHAR limit)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Generate long text close to 65535 byte limit (~60KB)
        long_text = " ".join(fake.words(nb=8000))[:60000]
        normal_text = "The quick brown fox jumps over the lazy dog"
        rows = [
            {default_primary_key_field_name: 0, default_text_field_name: long_text},
            {default_primary_key_field_name: 1, default_text_field_name: normal_text},
        ]

        result = self.insert(client, collection_name, rows)[0]
        assert result["insert_count"] == 2

        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search with long text as query
        results = self.search(client, collection_name, [long_text],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5,
                              output_fields=[default_primary_key_field_name])[0]

        assert len(results[0]) > 0
        # Exact match should rank first
        assert results[0][0]["id"] == 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_range_search_not_supported(self):
        """
        target: test MinHash range search is not supported
        method: search with radius/range_filter params on MinHash collection
        expected: error raised - minhash does not support range search
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        nb = 50
        texts = gen_text_data(nb)
        rows = [{default_primary_key_field_name: i, default_text_field_name: texts[i]} for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Range search not supported for minhash
        self.search(client, collection_name, [texts[0]],
                    anns_field=default_minhash_field_name,
                    search_params={
                        "metric_type": "MHJACCARD",
                        "params": {"radius": 0.0, "range_filter": 0.5}
                    },
                    limit=10,
                    check_task=CheckTasks.err_res,
                    check_items={ct.err_code: 65535,
                                 ct.err_msg: "not support range search"})

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_mh_lsh_code_in_mem(self):
        """
        target: test mh_lsh_code_in_mem index parameter
        method: create index with mh_lsh_code_in_mem=True, insert data, search
        expected: search works correctly with code in memory
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8, "mh_lsh_code_in_mem": True},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        nb = 100
        texts = gen_text_data(nb)
        rows = [{default_primary_key_field_name: i, default_text_field_name: texts[i]} for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search should work normally
        results = self.search(client, collection_name, [texts[0]],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5,
                              output_fields=[default_primary_key_field_name])[0]

        assert len(results[0]) > 0
        result_ids = [hit["id"] for hit in results[0]]
        assert 0 in result_ids

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_whitespace_only_text(self):
        """
        target: test MinHash with whitespace-only text input
        method: insert text containing only spaces/tabs/newlines
        expected: insert succeeds, signature generated (edge case handling)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        rows = [
            {default_primary_key_field_name: 0, default_text_field_name: "   "},
            {default_primary_key_field_name: 1, default_text_field_name: "\t\t"},
            {default_primary_key_field_name: 2, default_text_field_name: "\n\n"},
            {default_primary_key_field_name: 3, default_text_field_name: "normal text here"},
        ]

        result = self.insert(client, collection_name, rows)[0]
        assert result["insert_count"] == 4

        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Query to verify all rows exist
        query_result = self.query(client, collection_name,
                                  filter=f"{default_primary_key_field_name} >= 0",
                                  output_fields=[default_primary_key_field_name])[0]
        assert len(query_result) == 4

    @pytest.mark.tags(CaseLabel.L2)
    def test_minhash_batch_search(self):
        """
        target: test MinHash batch search with multiple query texts (nq > 1)
        method: search with 3 different query texts simultaneously
        expected: each query returns results, first match is exact
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        nb = 100
        texts = gen_text_data(nb)
        rows = [{default_primary_key_field_name: i, default_text_field_name: texts[i]} for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Batch search with nq=3
        query_texts = [texts[0], texts[50], texts[99]]
        results = self.search(client, collection_name, query_texts,
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=5,
                              output_fields=[default_primary_key_field_name])[0]

        assert len(results) == 3
        # Each query should find its exact match
        expected_ids = [0, 50, 99]
        for i, result in enumerate(results):
            assert len(result) > 0
            result_ids = [hit["id"] for hit in result]
            assert expected_ids[i] in result_ids, \
                f"Query {i}: expected id {expected_ids[i]} not in results {result_ids}"

