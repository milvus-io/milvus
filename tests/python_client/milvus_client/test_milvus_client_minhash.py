"""
MinHash DIDO Function Test Suite

This module contains comprehensive tests for MinHash DIDO (Data-In-Data-Out) Function,
which automatically converts text (VarChar/String) into MinHash signature vectors (BinaryVector)
for approximate nearest neighbor search based on Jaccard similarity.

Test Categories:
- L0: Basic functionality tests (core features, must pass)
- L1: Extended functionality tests (common use cases)
- L2: Advanced functionality tests (edge cases, combinations)

Key Components:
- MinHash Function: Configurable hash functions and shingles for text-to-signature conversion
- MinHashLSH Index: Locality-Sensitive Hashing index optimized for MinHash vectors
- MHJACCARD Metric: MinHash-specific Jaccard distance calculation
"""

import random
import time

import pytest
import numpy as np
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import DataType
from pymilvus.orm.schema import Function
from pymilvus.client.types import FunctionType


# ============================================================================
# Constants
# ============================================================================
prefix = "minhash"
default_text_field_name = "text"
default_minhash_field_name = "minhash_signature"
default_primary_key_field_name = "id"
default_nb = 1000
default_dim = 512  # num_hashes=16 -> dim=16*32=512
default_limit = 10
default_num_hashes = 16
default_shingle_size = 3


# ============================================================================
# Helper Functions
# ============================================================================
def gen_text_data(nb, min_words=5, max_words=50):
    """Generate random text data for testing."""
    from faker import Faker
    fake = Faker()
    return [fake.sentence(nb_words=random.randint(min_words, max_words)) for _ in range(nb)]


def gen_similar_text_pairs(nb, overlap_ratios=[0.0, 0.25, 0.5, 0.75, 1.0]):
    """
    Generate text pairs with known word overlap ratios for Jaccard similarity testing.

    Returns:
        List of tuples: [(text1, text2, expected_jaccard), ...]
    """
    from faker import Faker
    fake = Faker()
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


# ============================================================================
# Basic Functionality Test Cases
# ============================================================================
class TestMilvusClientMinHashBasic(TestMilvusClientV2Base):
    """
    Basic Test Cases for MinHash DIDO Function.
    These are critical path tests that must pass for basic functionality.
    """

    @pytest.mark.tags(CaseLabel.L0)
    def test_minhash_create_collection_basic(self):
        """
        target: test creating collection with basic MinHash function
        method: create collection with MinHash function using default parameters
        expected: collection created successfully with MinHash function
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with MinHash function
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        # Add MinHash function
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

        # Create collection
        self.create_collection(client, collection_name, schema=schema)

        # Verify collection exists
        collections = self.list_collections(client)[0]
        assert collection_name in collections

        # Verify schema has MinHash function
        desc = self.describe_collection(client, collection_name)[0]
        assert len(desc.get("functions", [])) == 1
        func = desc["functions"][0]
        assert func["type"] == FunctionType.MINHASH
        assert func["input_field_names"] == [default_text_field_name]
        assert func["output_field_names"] == [default_minhash_field_name]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_minhash_create_index_basic(self):
        """
        target: test creating MINHASH_LSH index with basic parameters
        method: create MINHASH_LSH index with mh_lsh_band parameter
        expected: index created successfully
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

        self.create_collection(client, collection_name, schema=schema)

        # Create MINHASH_LSH index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_index(client, collection_name, index_params)

        # Verify index exists
        indexes = self.list_indexes(client, collection_name)[0]
        assert default_minhash_field_name in indexes

        self.drop_collection(client, collection_name)

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

        # Create index
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data (only text, MinHash signature auto-generated)
        rows = gen_minhash_rows(default_nb)
        result = self.insert(client, collection_name, rows)[0]
        assert result["insert_count"] == default_nb

        # Verify data count
        self.flush(client, collection_name)
        stats = self.get_collection_stats(client, collection_name)[0]
        assert stats["row_count"] == default_nb

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_minhash_search_basic(self):
        """
        target: test basic MinHash search
        method: search using text query with MHJACCARD metric
        expected: search returns results with valid distances
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

        # Create index and collection
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="MHJACCARD",
            params={"mh_lsh_band": 8},
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        rows = gen_minhash_rows(default_nb)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Load collection
        self.load_collection(client, collection_name)

        # Search using text
        query_text = rows[0][default_text_field_name]
        results = self.search(client, collection_name, [query_text],
                              anns_field=default_minhash_field_name,
                              search_params={
                                  "metric_type": "MHJACCARD",
                                  "params": {},
                              },
                              limit=default_limit,
                              output_fields=[default_primary_key_field_name, default_text_field_name])[0]

        # Verify results
        assert len(results) == 1  # One query
        assert len(results[0]) <= default_limit
        # First result should be the query itself (exact match)
        assert results[0][0]["id"] == rows[0][default_primary_key_field_name]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_minhash_search_with_filter(self):
        """
        target: test MinHash search with scalar filter
        method: search with filter expression
        expected: results satisfy filter condition
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Create schema with additional scalar field
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

        # Insert data with category field
        texts = gen_text_data(default_nb)
        rows = [{
            default_primary_key_field_name: i,
            default_text_field_name: texts[i],
            "category": i % 5  # 5 categories
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search with filter
        query_text = texts[0]
        filter_expr = "category == 0"
        results = self.search(client, collection_name, [query_text],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              filter=filter_expr,
                              limit=default_limit,
                              output_fields=[default_primary_key_field_name, "category"])[0]

        # Verify all results satisfy filter
        for hit in results[0]:
            assert hit["entity"]["category"] == 0

        self.drop_collection(client, collection_name)

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
            assert hit["distance"] == 1.0  # MHJACCARD similarity: 1.0 = identical

        self.drop_collection(client, collection_name)


# ============================================================================
# Extended Functionality Test Cases
# ============================================================================
class TestMilvusClientMinHashExtended(TestMilvusClientV2Base):
    """
    Extended Test Cases for MinHash DIDO Function.
    These cover common use cases and parameter combinations.
    """

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
        dim = num_hashes * 32  # dim = num_hashes * 32

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

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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

        # Search with Jaccard reranking
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
        assert results[0][0]["distance"] == 1.0  # MHJACCARD returns similarity
        assert results[0][0]["id"] == rows[0][default_primary_key_field_name]

        self.drop_collection(client, collection_name)

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

        assert results[0][0]["distance"] == 1.0  # MHJACCARD returns similarity
        assert results[0][0]["entity"][default_text_field_name] == updated_text

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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
            assert results[0][0]["distance"] == 1.0  # MHJACCARD returns similarity

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)


# ============================================================================
# Negative Test Cases - Error Handling
# ============================================================================
class TestMilvusClientMinHashNegative(TestMilvusClientV2Base):
    """
    Negative Test Cases for MinHash DIDO Function.
    These test error handling and invalid inputs.
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_invalid_input_field_type(self):
        """
        target: test MinHash function with non-VARCHAR input field
        method: try to create MinHash function with INT64 input field
        expected: error raised during collection creation - input must be VARCHAR
        """
        from pymilvus.exceptions import ParamError, MilvusException

        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("int_field", DataType.INT64)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=default_dim)

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=["int_field"],  # Invalid - not VARCHAR
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        # PyMilvus validates client-side, raises ParamError
        with pytest.raises((ParamError, MilvusException)) as exc_info:
            client.create_collection(collection_name, schema=schema)

        error_msg = str(exc_info.value).lower()
        assert "varchar" in error_msg or "type" in error_msg or "string" in error_msg

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_invalid_output_field_type(self):
        """
        target: test MinHash function with non-BINARY_VECTOR output field
        method: try to create MinHash function with FLOAT_VECTOR output field
        expected: error raised during collection creation - output must be BINARY_VECTOR
        """
        from pymilvus.exceptions import ParamError, MilvusException

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
            output_field_names=["float_vec"],  # Invalid - not BINARY_VECTOR
            params={"num_hashes": default_num_hashes, "shingle_size": default_shingle_size},
        ))

        # PyMilvus validates client-side, raises ParamError
        with pytest.raises((ParamError, MilvusException)) as exc_info:
            client.create_collection(collection_name, schema=schema)

        error_msg = str(exc_info.value).lower()
        assert "binary" in error_msg or "type" in error_msg or "vector" in error_msg

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_dim_not_multiple_of_32(self):
        """
        target: test MinHash function with mismatched dimension
        method: try to create MinHash function where num_hashes*32 != field dim
        expected: error raised - dimension mismatch
        """
        from pymilvus.exceptions import ParamError, MilvusException

        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(default_primary_key_field_name, DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field(default_text_field_name, DataType.VARCHAR, max_length=65535)
        schema.add_field(default_minhash_field_name, DataType.BINARY_VECTOR, dim=128)  # 128 bits = 4 * 32

        schema.add_function(Function(
            name="text_to_minhash",
            function_type=FunctionType.MINHASH,
            input_field_names=[default_text_field_name],
            output_field_names=[default_minhash_field_name],
            params={"num_hashes": 3, "shingle_size": default_shingle_size},  # 3*32=96 != 128
        ))

        # PyMilvus validates client-side, raises ParamError for dimension mismatch
        with pytest.raises((ParamError, MilvusException)) as exc_info:
            client.create_collection(collection_name, schema=schema)

        error_msg = str(exc_info.value).lower()
        assert "dim" in error_msg or "mismatch" in error_msg or "num_hashes" in error_msg

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="Server bug: allows BIN_FLAT index on MinHash function output field")
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

        # Try to create wrong index type
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="BIN_FLAT",  # Invalid - should be MINHASH_LSH
            metric_type="HAMMING",
        )

        # Error should occur during index creation
        self.create_index(client, collection_name, index_params,
                          check_task=CheckTasks.err_res,
                          check_items={"err_code": 1})

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="Server bug: allows HAMMING metric with MINHASH_LSH index")
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

        # Try to create index with wrong metric
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(
            field_name=default_minhash_field_name,
            index_type="MINHASH_LSH",
            metric_type="HAMMING",  # Invalid - should be MHJACCARD
            params={"mh_lsh_band": 8},
        )

        # Error should occur during index creation
        self.create_index(client, collection_name, index_params,
                          check_task=CheckTasks.err_res,
                          check_items={"err_code": 1})

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_insert_to_output_field(self):
        """
        target: test directly inserting data to MinHash output field
        method: try to insert MinHash signature directly
        expected: error raised - cannot insert to function output field
        """
        from pymilvus.exceptions import ParamError, MilvusException

        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Ensure collection doesn't exist
        if client.has_collection(collection_name):
            client.drop_collection(collection_name)

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

        # Try to insert with MinHash signature directly
        fake_signature = bytes([0] * (default_dim // 8))
        rows = [{
            default_primary_key_field_name: 1,
            default_text_field_name: "Test text",
            default_minhash_field_name: fake_signature,  # Should not be allowed
        }]

        # Error should occur during insert - cannot provide function output field
        with pytest.raises((ParamError, MilvusException)):
            client.insert(collection_name, rows)

        client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_missing_input_field(self):
        """
        target: test inserting without MinHash input field
        method: try to insert without text field
        expected: error raised - required field missing
        """
        from pymilvus.exceptions import ParamError, MilvusException

        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Ensure collection doesn't exist
        if client.has_collection(collection_name):
            client.drop_collection(collection_name)

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

        # Try to insert without text field
        rows = [{default_primary_key_field_name: 1}]  # Missing text field

        # Error should occur during insert - missing required field
        with pytest.raises((ParamError, MilvusException)):
            client.insert(collection_name, rows)

        client.drop_collection(collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_num_hashes", [0, -1])
    # Note: "abc" removed - causes server panic (bug: Param num_hashes:abc is not a number)
    # TODO: Add back after server bug is fixed
    def test_minhash_invalid_num_hashes(self, invalid_num_hashes):
        """
        target: test MinHash function with invalid num_hashes value
        method: try to create function with invalid num_hashes
        expected: error raised during collection creation
        """
        from pymilvus.exceptions import ParamError, MilvusException

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

        # PyMilvus or server validates num_hashes, raises error
        with pytest.raises((ParamError, MilvusException)):
            client.create_collection(collection_name, schema=schema)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("invalid_shingle_size", [0, -1])
    @pytest.mark.xfail(reason="Server bug: shingle_size parameter not validated - accepts invalid values")
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

        # Error should occur during collection creation (server-side validation)
        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={"err_code": 1})

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="Server bug: hash_function parameter not validated - accepts invalid values like 'md5'")
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
                "hash_function": "md5",  # Invalid - only xxhash64 and sha1 supported
            },
        ))

        # Error should occur during collection creation (server-side validation)
        self.create_collection(client, collection_name, schema=schema,
                               check_task=CheckTasks.err_res,
                               check_items={"err_code": 1})

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

        self.drop_collection(client, collection_name)


# ============================================================================
# Advanced Functionality Test Cases
# ============================================================================
class TestMilvusClientMinHashAdvanced(TestMilvusClientV2Base):
    """
    Advanced Test Cases for MinHash DIDO Function.
    These cover edge cases, complex combinations, and advanced features.
    """

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

        self.drop_collection(client, collection_name)

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
            assert result[0]["distance"] == 1.0  # MHJACCARD returns similarity

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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
            assert results[0][0]["distance"] == 1.0  # MHJACCARD returns similarity

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)


# ============================================================================
# Accuracy Test Cases
# ============================================================================
class TestMilvusClientMinHashAccuracy(TestMilvusClientV2Base):
    """
    Accuracy Test Cases for MinHash DIDO Function.
    These verify the correctness of MinHash similarity estimation.
    """

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

        self.drop_collection(client, collection_name)

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
        assert results[0][0]["distance"] == 1.0  # MHJACCARD returns similarity

        self.drop_collection(client, collection_name)

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


# ============================================================================
# MinHash Function Correctness Test Helpers
# ============================================================================
# These helper functions reproduce Milvus's MinHash algorithm in Python
# to verify the correctness of the generated signatures.

# Constants matching Milvus implementation
MINHASH_MERSENNE_PRIME = (1 << 61) - 1  # 2^61 - 1
MINHASH_MAX_HASH_MASK = (1 << 32) - 1    # 2^32 - 1


def init_permutations_like_milvus(num_hashes: int, seed: int):
    """
    Reproduce Milvus C++ InitPermutations function.

    Milvus uses std::mt19937_64 which is compatible with numpy's MT19937
    when using the Generator interface.

    Args:
        num_hashes: Number of hash functions
        seed: Random seed

    Returns:
        tuple: (perm_a, perm_b) arrays of uint64
    """
    # numpy.random.Generator with MT19937 is compatible with C++ std::mt19937_64
    rng = np.random.Generator(np.random.MT19937(seed))

    perm_a = np.zeros(num_hashes, dtype=np.uint64)
    perm_b = np.zeros(num_hashes, dtype=np.uint64)

    for i in range(num_hashes):
        raw_a = rng.integers(0, 2**64, dtype=np.uint64)
        raw_b = rng.integers(0, 2**64, dtype=np.uint64)
        perm_a[i] = (int(raw_a) % (MINHASH_MERSENNE_PRIME - 1)) + 1
        perm_b[i] = int(raw_b) % MINHASH_MERSENNE_PRIME

    return perm_a, perm_b


def hash_shingles_xxhash(text: str, shingle_size: int) -> list:
    """
    Compute character-level shingle hashes using xxhash.

    This reproduces Milvus's shingle hashing for char-level shingles.

    Args:
        text: Input text
        shingle_size: Size of character n-grams

    Returns:
        List of 32-bit hash values
    """
    import xxhash

    if len(text) < shingle_size:
        # For short texts, hash the entire text
        return [xxhash.xxh3_64(text.encode('utf-8')).intdigest() & 0xFFFFFFFF]

    hashes = []
    for i in range(len(text) - shingle_size + 1):
        shingle = text[i:i + shingle_size]
        h = xxhash.xxh3_64(shingle.encode('utf-8')).intdigest() & 0xFFFFFFFF
        hashes.append(h)
    return hashes


def hash_shingles_sha1(text: str, shingle_size: int) -> list:
    """
    Compute character-level shingle hashes using SHA1.

    This reproduces Milvus's shingle hashing when hash_function='sha1'.
    Milvus takes the first 4 bytes of SHA1 digest as little-endian uint32.

    Args:
        text: Input text
        shingle_size: Size of character n-grams

    Returns:
        List of 32-bit hash values
    """
    import hashlib

    if len(text) < shingle_size:
        # For short texts, hash the entire text
        digest = hashlib.sha1(text.encode('utf-8')).digest()
        # Take first 4 bytes as little-endian uint32
        return [digest[0] | (digest[1] << 8) | (digest[2] << 16) | (digest[3] << 24)]

    hashes = []
    for i in range(len(text) - shingle_size + 1):
        shingle = text[i:i + shingle_size]
        digest = hashlib.sha1(shingle.encode('utf-8')).digest()
        # Take first 4 bytes as little-endian uint32 (matching Milvus C++ code)
        h = digest[0] | (digest[1] << 8) | (digest[2] << 16) | (digest[3] << 24)
        hashes.append(h)
    return hashes


def compute_minhash_signature(base_hashes: list, perm_a: np.ndarray, perm_b: np.ndarray) -> list:
    """
    Compute MinHash signature from base hashes.

    This reproduces Milvus's linear_and_find_min_native function.

    Args:
        base_hashes: List of base hash values (from shingles)
        perm_a: Permutation parameters a
        perm_b: Permutation parameters b

    Returns:
        List of uint32 signature values
    """
    num_hashes = len(perm_a)
    signature = [0xFFFFFFFF] * num_hashes

    for h in base_hashes:
        for i in range(num_hashes):
            # Match Milvus: temp = perm_a * base + perm_b
            temp = int(perm_a[i]) * h + int(perm_b[i])
            # Fast Mersenne modulo
            low = temp & MINHASH_MERSENNE_PRIME
            high = temp >> 61
            temp = low + high
            if temp >= MINHASH_MERSENNE_PRIME:
                temp -= MINHASH_MERSENNE_PRIME
            permuted = temp & MINHASH_MAX_HASH_MASK
            if permuted < signature[i]:
                signature[i] = permuted

    return signature


def signature_to_binary_vector(signature: list) -> bytes:
    """
    Convert MinHash signature to binary vector (little-endian).

    Args:
        signature: List of uint32 signature values

    Returns:
        bytes: Binary vector
    """
    import struct
    return b''.join(struct.pack('<I', s) for s in signature)


def binary_vector_to_signature(binary_vector: bytes) -> list:
    """
    Convert binary vector back to signature (little-endian).

    Args:
        binary_vector: Binary vector bytes

    Returns:
        List of uint32 signature values
    """
    import struct
    num_hashes = len(binary_vector) // 4
    return list(struct.unpack(f'<{num_hashes}I', binary_vector))


# ============================================================================
# MinHash Function Correctness Test Cases
# ============================================================================
class TestMinHashFunctionCorrectness(TestMilvusClientV2Base):
    """
    Test cases to verify the correctness of MinHash function output.

    These tests directly compare Milvus MinHash signatures against
    Python-computed expected values using the same algorithm.
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_minhash_signature_matches_expected(self):
        """
        target: verify MinHash signature matches Python-computed expected value
        method: compute expected signature in Python, compare with Milvus output
        expected: signatures should match exactly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        # Fixed parameters for reproducibility
        seed = 42
        num_hashes = 16
        shingle_size = 3
        dim = num_hashes * 32

        # Create collection with MinHash function
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

        # Test texts
        test_texts = [
            "hello world",
            "The quick brown fox",
            "abcdefghij",
        ]

        # Compute expected signatures in Python
        perm_a, perm_b = init_permutations_like_milvus(num_hashes, seed)
        expected_signatures = []
        for text in test_texts:
            base_hashes = hash_shingles_xxhash(text, shingle_size)
            sig = compute_minhash_signature(base_hashes, perm_a, perm_b)
            expected_signatures.append(sig)

        # Insert data into Milvus
        rows = [{default_primary_key_field_name: i, default_text_field_name: test_texts[i]}
                for i in range(len(test_texts))]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Query to retrieve actual signatures
        results = self.query(client, collection_name,
                             filter=f"{default_primary_key_field_name} >= 0",
                             output_fields=[default_primary_key_field_name,
                                            default_text_field_name,
                                            default_minhash_field_name])[0]

        # Sort by ID for consistent comparison
        results.sort(key=lambda x: x[default_primary_key_field_name])

        # Compare signatures
        for i, result in enumerate(results):
            actual_binary = result[default_minhash_field_name]
            actual_sig = binary_vector_to_signature(actual_binary)
            expected_sig = expected_signatures[i]

            # Log for debugging if mismatch
            if actual_sig != expected_sig:
                print(f"Text: {test_texts[i]}")
                print(f"Expected signature: {expected_sig}")
                print(f"Actual signature: {actual_sig}")

            assert actual_sig == expected_sig, \
                f"Signature mismatch for text '{test_texts[i]}': expected {expected_sig}, got {actual_sig}"

        self.drop_collection(client, collection_name)

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

            # Verify against Python computation
            perm_a, perm_b = init_permutations_like_milvus(num_hashes, seed)
            base_hashes = hash_shingles_xxhash(test_text, shingle_size)
            expected_sig = tuple(compute_minhash_signature(base_hashes, perm_a, perm_b))

            assert actual_sig == expected_sig, \
                f"Signature mismatch for seed {seed}: expected {expected_sig}, got {actual_sig}"

            self.drop_collection(client, collection_name)

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

        # Verify binary vector size: num_hashes * 4 bytes (32 bits each)
        expected_byte_size = num_hashes * 4
        assert len(actual_binary) == expected_byte_size, \
            f"Binary vector should be {expected_byte_size} bytes, got {len(actual_binary)}"

        # Roundtrip test
        sig = binary_vector_to_signature(actual_binary)
        roundtrip_binary = signature_to_binary_vector(sig)
        assert actual_binary == roundtrip_binary, "Binary vector should survive roundtrip conversion"

        # Verify against Python computation
        perm_a, perm_b = init_permutations_like_milvus(num_hashes, seed)
        base_hashes = hash_shingles_xxhash(test_text, shingle_size)
        expected_sig = compute_minhash_signature(base_hashes, perm_a, perm_b)
        expected_binary = signature_to_binary_vector(expected_sig)

        assert actual_binary == expected_binary, \
            f"Binary vector mismatch: expected {expected_binary.hex()}, got {actual_binary.hex()}"

        self.drop_collection(client, collection_name)

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

        perm_a, perm_b = init_permutations_like_milvus(num_hashes, seed)

        for result in results:
            binary_vec = result[default_minhash_field_name]
            text = result[default_text_field_name]

            # Binary vector should have correct size
            assert len(binary_vec) == num_hashes * 4, f"Invalid binary vector size for '{text}'"

            sig = binary_vector_to_signature(binary_vec)

            # All signature values should be valid 32-bit
            for s in sig:
                assert 0 <= s <= 0xFFFFFFFF, f"Invalid signature value for '{text}'"

            # Verify against Python computation
            base_hashes = hash_shingles_xxhash(text, shingle_size)
            expected_sig = compute_minhash_signature(base_hashes, perm_a, perm_b)

            assert sig == expected_sig, \
                f"Signature mismatch for edge case '{text}': expected {expected_sig}, got {sig}"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
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
            params={"num_hashes": num_hashes, "shingle_size": shingle_size, "seed": seed},
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
            (0, base_text),                                              # Identical
            (1, "the quick brown fox jumps over the lazy cat"),          # 1 word changed
            (2, "the slow brown fox jumps over the lazy dog"),           # 1 word changed
            (3, "a slow red fox runs over the tired dog"),               # Multiple changes
            (4, "completely different text about something else"),       # Very different
        ]

        rows = [{default_primary_key_field_name: pk, default_text_field_name: text}
                for pk, text in variants]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search with base text
        results = self.search(client, collection_name, [base_text],
                              anns_field=default_minhash_field_name,
                              search_params={"metric_type": "MHJACCARD", "params": {}},
                              limit=len(variants),
                              output_fields=[default_primary_key_field_name, default_text_field_name])[0]

        # Verify result ordering: identical text should be first
        assert results[0][0]["id"] == 0, "Identical text should be first result"
        assert results[0][0]["distance"] == 1.0, "Identical text should have distance 1.0"

        # Verify distances are monotonically decreasing (more similar = higher score)
        distances = [hit["distance"] for hit in results[0]]
        # Note: MHJACCARD returns similarity (1.0 = identical), not distance
        # So higher values mean more similar

        # The very different text should have lowest similarity
        last_hit = results[0][-1]
        assert last_hit["distance"] < 0.9, \
            f"Very different text should have lower similarity, got {last_hit['distance']}"

        self.drop_collection(client, collection_name)

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

        # Compute expected signatures using SHA1
        perm_a, perm_b = init_permutations_like_milvus(num_hashes, seed)
        expected_signatures = []
        for text in test_texts:
            base_hashes = hash_shingles_sha1(text, shingle_size)
            sig = compute_minhash_signature(base_hashes, perm_a, perm_b)
            expected_signatures.append(sig)

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

        # Compare
        for i, result in enumerate(results):
            actual_binary = result[default_minhash_field_name]
            actual_sig = binary_vector_to_signature(actual_binary)
            expected_sig = expected_signatures[i]

            if actual_sig != expected_sig:
                print(f"SHA1 mismatch for text '{test_texts[i]}'")
                print(f"Expected: {expected_sig}")
                print(f"Actual: {actual_sig}")

            assert actual_sig == expected_sig, \
                f"SHA1 signature mismatch for '{test_texts[i]}'"

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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

        self.drop_collection(client, collection_name)

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

            self.drop_collection(client, collection_name)

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

            self.drop_collection(client, collection_name)

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

            self.drop_collection(client, collection_name)

        assert signatures["xxhash64"] != signatures["sha1"], \
            "xxhash64 and sha1 should produce different signatures"
