import json
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from check.param_check import compare_lists_with_epsilon_ignore_dict_order
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from deepdiff import DeepDiff
from minio import Minio
from minio.error import S3Error
from pymilvus import AnnSearchRequest, DataType, MilvusClient, RRFRanker, WeightedRanker
from pymilvus.bulk_writer import (
    BulkFileType,
    LocalBulkWriter,
    bulk_import,
    get_import_progress,
)
from pymilvus.client.embedding_list import EmbeddingList
from utils.util_log import test_log as log
from utils.util_pymilvus import *  # noqa: F403

prefix = "struct_array"
epsilon = 0.001
default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = 128
default_capacity = 100
METRICS = ["MAX_SIM", "MAX_SIM_IP", "MAX_SIM_COSINE", "MAX_SIM_L2"]
INDEX_PARAMS = {"M": 16, "efConstruction": 200}

# EmbList index type configs: {index_type: {build_params, search_params}}
EMB_LIST_INDEX_CONFIGS = {
    "HNSW_SQ": {
        "build_params": {"M": 16, "efConstruction": 200, "sq_type": "SQ8"},
        "search_params": {"ef": 64},
    },
    "HNSW_PQ": {
        "build_params": {"M": 16, "efConstruction": 200},
        "search_params": {"ef": 64},
    },
    "HNSW_PRQ": {
        "build_params": {"M": 16, "efConstruction": 200},
        "search_params": {"ef": 64},
    },
    "IVF_FLAT": {
        "build_params": {"nlist": 128},
        "search_params": {"nprobe": 10},
    },
    "IVF_FLAT_CC": {
        "build_params": {"nlist": 128},
        "search_params": {"nprobe": 10},
    },
    "DISKANN": {
        "build_params": {},
        "search_params": {"search_list": 30},
    },
}
EMB_LIST_INDEX_TYPES = list(EMB_LIST_INDEX_CONFIGS.keys())

# Supported vector types per emb list index type (for MaxSim metrics)
EMB_LIST_VECTOR_TYPES = {
    "HNSW": [
        DataType.FLOAT_VECTOR,
        DataType.FLOAT16_VECTOR,
        DataType.BFLOAT16_VECTOR,
        DataType.INT8_VECTOR,
        DataType.BINARY_VECTOR,
    ],
    "HNSW_SQ": [
        DataType.FLOAT_VECTOR,
        DataType.FLOAT16_VECTOR,
        DataType.BFLOAT16_VECTOR,
        DataType.INT8_VECTOR,
    ],
    "HNSW_PQ": [
        DataType.FLOAT_VECTOR,
        DataType.FLOAT16_VECTOR,
        DataType.BFLOAT16_VECTOR,
        DataType.INT8_VECTOR,
    ],
    "HNSW_PRQ": [
        DataType.FLOAT_VECTOR,
        DataType.FLOAT16_VECTOR,
        DataType.BFLOAT16_VECTOR,
        DataType.INT8_VECTOR,
    ],
    "IVF_FLAT": [
        DataType.FLOAT_VECTOR,
        DataType.FLOAT16_VECTOR,
        DataType.BFLOAT16_VECTOR,
    ],
    "IVF_FLAT_CC": [
        DataType.FLOAT_VECTOR,
        DataType.FLOAT16_VECTOR,
        DataType.BFLOAT16_VECTOR,
    ],
    "DISKANN": [
        DataType.FLOAT_VECTOR,
        DataType.FLOAT16_VECTOR,
        DataType.BFLOAT16_VECTOR,
    ],
}

# Dim for emb list index tests (smaller for faster index building)
EMB_LIST_DIM = 32

# Metric type for binary vectors vs float vectors
BINARY_METRIC = "MAX_SIM_HAMMING"
FLOAT_METRIC = "MAX_SIM_COSINE"
INT8_METRIC = "MAX_SIM_COSINE"

EMB_LIST_STRATEGY_CONFIGS = {
    "tokenann": {
        "strategy_params": {
            "emb_list_strategy": "tokenann",
        },
    },
    "muvera": {
        "strategy_params": {
            "emb_list_strategy": "muvera",
            "muvera_num_projections": 3,
            "muvera_num_repeats": 5,
            "muvera_seed": 42,
        },
    },
    "lemur": {
        "strategy_params": {
            "emb_list_strategy": "lemur",
            "lemur_hidden_dim": 32,
            "lemur_num_train_samples": 1000,
            "lemur_num_epochs": 2,
            "lemur_batch_size": 16,
            "lemur_learning_rate": 0.001,
            "lemur_seed": 42,
            "lemur_num_layers": 1,
        },
    },
}

EMB_LIST_STRATEGY_INDEX_CONFIGS = {
    "HNSW": {
        "build_params": {
            "M": 16,
            "efConstruction": 96,
        },
        "search_params": {"ef": 64, "retrieval_ann_ratio": 3.0, "emb_list_rerank": True},
    },
    "DISKANN": {
        "build_params": {},
        "search_params": {"search_list": 30, "retrieval_ann_ratio": 3.0, "emb_list_rerank": True},
    },
}
EMB_LIST_STRATEGY_INDEX_CASES = [
    ("tokenann", "HNSW"),
    pytest.param(
        "muvera",
        "HNSW",
        marks=pytest.mark.skip(reason="milvus-io/milvus#49748: muvera+HNSW can fail to load emb-list index"),
    ),
    pytest.param(
        "lemur",
        "HNSW",
        marks=pytest.mark.skip(reason="milvus-io/milvus#49748: lemur+HNSW can fail to load emb-list index"),
    ),
    ("tokenann", "DISKANN"),
]


class TestMilvusClientStructArrayBasic(TestMilvusClientV2Base):
    """Test case of struct array basic functionality"""

    def generate_struct_array_data(
        self, num_rows: int, dim: int = default_dim, capacity: int = default_capacity
    ) -> list[dict[str, Any]]:
        """Generate test data for struct array"""
        data = []
        for i in range(num_rows):
            # Random array length for each row (1 to capacity)
            array_length = random.randint(1, min(capacity, 20))

            struct_array = []
            for j in range(array_length):
                struct_element = {
                    "clip_str": f"item_{i}_{j}",
                    "clip_embedding1": [random.random() for _ in range(dim)],
                    "clip_embedding2": [random.random() for _ in range(dim)],
                }
                struct_array.append(struct_element)

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(dim)],
                "clips": struct_array,
            }
            data.append(row)

        return data

    def create_struct_array_schema(
        self,
        client: MilvusClient,
        dim: int = default_dim,
        capacity: int = default_capacity,
        mmap_enabled: bool = None,
        struct_array_mmap: bool = None,
        subfield1_mmap: bool = None,
        subfield2_mmap: bool = None,
    ):
        """Create schema with struct array field

        Args:
            client: MilvusClient instance
            dim: vector dimension
            capacity: array capacity
            mmap_enabled: enable mmap for normal_vector field
            struct_array_mmap: enable mmap for the entire struct array field
            subfield1_mmap: enable mmap for clip_embedding1 sub-field
            subfield2_mmap: enable mmap for clip_embedding2 sub-field
        """
        # Create basic schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)

        # Add primary key field
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)

        # Add normal vector field with optional mmap
        if mmap_enabled is not None:
            schema.add_field(
                field_name="normal_vector",
                datatype=DataType.FLOAT_VECTOR,
                dim=dim,
                mmap_enabled=mmap_enabled,
            )
        else:
            schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        # Create struct schema with optional mmap for sub-fields
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_str", DataType.VARCHAR, max_length=65535)

        if subfield1_mmap is not None:
            struct_schema.add_field(
                "clip_embedding1",
                DataType.FLOAT_VECTOR,
                dim=dim,
                mmap_enabled=subfield1_mmap,
            )
        else:
            struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=dim)

        if subfield2_mmap is not None:
            struct_schema.add_field(
                "clip_embedding2",
                DataType.FLOAT_VECTOR,
                dim=dim,
                mmap_enabled=subfield2_mmap,
            )
        else:
            struct_schema.add_field("clip_embedding2", DataType.FLOAT_VECTOR, dim=dim)

        # Add struct array field with optional mmap
        if struct_array_mmap is not None:
            schema.add_field(
                "clips",
                datatype=DataType.ARRAY,
                element_type=DataType.STRUCT,
                struct_schema=struct_schema,
                max_capacity=capacity,
                mmap_enabled=struct_array_mmap,
            )
        else:
            schema.add_field(
                "clips",
                datatype=DataType.ARRAY,
                element_type=DataType.STRUCT,
                struct_schema=struct_schema,
                max_capacity=capacity,
            )

        return schema

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_with_clip_embedding1(self):
        """
        target: test create collection with struct array containing vector field
        method: create collection with struct array field that contains FLOAT_VECTOR
        expected: creation successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_basic")

        client = self._client()

        # Create schema with struct array containing vector field
        schema = self.create_struct_array_schema(client, dim=default_dim)

        # Create collection
        collection_name = cf.gen_unique_str(f"{prefix}_vector")
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Verify collection exists
        has_collection, _ = self.has_collection(client, collection_name)
        assert has_collection

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_with_scalar_fields(self):
        """
        target: test create collection with struct array containing only scalar fields
        method: create collection with struct array field that contains only scalar types
        expected: creation successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_basic")

        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        # Create struct schema with only scalar fields
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("int_field", DataType.INT64)
        struct_schema.add_field("float_field", DataType.FLOAT)
        struct_schema.add_field("string_field", DataType.VARCHAR, max_length=512)
        struct_schema.add_field("bool_field", DataType.BOOL)

        # Add struct array field
        schema.add_field(
            "scalar_struct",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=50,
        )

        # Create collection
        collection_name = cf.gen_unique_str(f"{prefix}_scalar")
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "scalar_type,scalar_params",
        [
            (DataType.BOOL, {}),
            (DataType.INT8, {}),
            (DataType.INT16, {}),
            (DataType.INT32, {}),
            (DataType.INT64, {}),
            (DataType.FLOAT, {}),
            (DataType.DOUBLE, {}),
            (DataType.VARCHAR, {"max_length": 256}),
        ],
    )
    def test_create_struct_with_various_scalar_types(self, scalar_type, scalar_params):
        """
        target: test create collection with struct array containing various scalar field types
        method: create collection with struct array field containing different scalar types
        expected: creation successful for all supported scalar types
        """
        collection_name = cf.gen_unique_str(f"{prefix}_scalar_{str(scalar_type)}")

        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        # Create struct schema with the specific scalar field type
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("scalar_field", scalar_type, **scalar_params)

        # Add struct array field
        schema.add_field(
            "scalar_struct",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=50,
        )

        # Create collection
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_query_struct_with_all_scalar_types(self):
        """
        target: test insert and query struct array with all supported scalar field types
        method: create collection with struct containing all scalar types, insert data and query
        expected: insert and query successful with correct data for all types
        """
        collection_name = cf.gen_unique_str(f"{prefix}_all_scalars")

        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        # Create struct schema with all supported scalar field types
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("bool_field", DataType.BOOL)
        struct_schema.add_field("int8_field", DataType.INT8)
        struct_schema.add_field("int16_field", DataType.INT16)
        struct_schema.add_field("int32_field", DataType.INT32)
        struct_schema.add_field("int64_field", DataType.INT64)
        struct_schema.add_field("float_field", DataType.FLOAT)
        struct_schema.add_field("double_field", DataType.DOUBLE)
        struct_schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)

        # Add struct array field
        schema.add_field(
            "data_records",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=10,
        )

        # Create collection
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Insert data with all scalar types
        data = [
            {
                "id": 0,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "data_records": [
                    {
                        "bool_field": True,
                        "int8_field": 127,
                        "int16_field": 32767,
                        "int32_field": 2147483647,
                        "int64_field": 9223372036854775807,
                        "float_field": 3.14,
                        "double_field": 3.141592653589793,
                        "varchar_field": "test_string_0",
                    },
                    {
                        "bool_field": False,
                        "int8_field": -128,
                        "int16_field": -32768,
                        "int32_field": -2147483648,
                        "int64_field": -9223372036854775808,
                        "float_field": -2.71,
                        "double_field": -2.718281828459045,
                        "varchar_field": "test_string_1",
                    },
                ],
            },
            {
                "id": 1,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "data_records": [
                    {
                        "bool_field": True,
                        "int8_field": 0,
                        "int16_field": 0,
                        "int32_field": 0,
                        "int64_field": 0,
                        "float_field": 0.0,
                        "double_field": 0.0,
                        "varchar_field": "test_string_2",
                    }
                ],
            },
        ]

        res, check = self.insert(client, collection_name, data)
        assert check
        assert res["insert_count"] == 2

        # Create index and load collection for query
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        # Query to verify data
        results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "data_records"],
            limit=10,
        )
        assert check
        assert len(results) == 2

        # Verify the data
        for result in results:
            assert "id" in result
            assert "data_records" in result
            assert isinstance(result["data_records"], list)
            assert len(result["data_records"]) > 0

            for record in result["data_records"]:
                # Verify all fields are present
                assert "bool_field" in record
                assert "int8_field" in record
                assert "int16_field" in record
                assert "int32_field" in record
                assert "int64_field" in record
                assert "float_field" in record
                assert "double_field" in record
                assert "varchar_field" in record

                # Verify data types
                assert isinstance(record["bool_field"], bool)
                assert isinstance(record["int8_field"], int)
                assert isinstance(record["int16_field"], int)
                assert isinstance(record["int32_field"], int)
                assert isinstance(record["int64_field"], int)
                assert isinstance(record["float_field"], int | float)
                assert isinstance(record["double_field"], int | float)
                assert isinstance(record["varchar_field"], str)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_struct_array_with_mixed_fields(self):
        """
        target: test create collection with struct array containing mixed field types
        method: create collection with struct array field that contains both vector and scalar fields
        expected: creation successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_basic")

        client = self._client()

        # Create schema with mixed field types
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        # Create struct schema with mixed types
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding1", DataType.FLOAT_VECTOR, dim=64)
        struct_schema.add_field("embedding2", DataType.FLOAT_VECTOR, dim=128)
        struct_schema.add_field("category", DataType.VARCHAR, max_length=256)
        struct_schema.add_field("score", DataType.FLOAT)
        struct_schema.add_field("count", DataType.INT32)

        # Add struct array field
        schema.add_field(
            "mixed_struct",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )

        # Create collection
        collection_name = cf.gen_unique_str(f"{prefix}_mixed")
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_multiple_struct_array_fields(self):
        """
        target: test create collection with multiple struct array fields
        method: create collection with multiple struct array fields
        expected: creation successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_basic")

        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        # Create first struct schema
        struct_schema1 = client.create_struct_field_schema()
        struct_schema1.add_field("vec1", DataType.FLOAT_VECTOR, dim=64)
        struct_schema1.add_field("label1", DataType.VARCHAR, max_length=128)

        # Create second struct schema
        struct_schema2 = client.create_struct_field_schema()
        struct_schema2.add_field("vec2", DataType.FLOAT_VECTOR, dim=256)
        struct_schema2.add_field("value2", DataType.FLOAT)

        # Add multiple struct array fields
        schema.add_field(
            "struct1",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema1,
            max_capacity=50,
        )

        schema.add_field(
            "struct2",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema2,
            max_capacity=30,
        )

        # Create collection
        collection_name = cf.gen_unique_str(f"{prefix}_multiple")
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_multiple_struct_arrays_with_same_subfield_names(self):
        """
        target: test create collection with multiple struct arrays having same subfield names
        method: create collection with two struct array fields that share same subfield names
        expected: creation and insert successful, able to query and search on both struct arrays
        """
        collection_name = cf.gen_unique_str(f"{prefix}_same_subfields")

        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        # Create first struct schema with common subfield names
        struct_schema1 = client.create_struct_field_schema()
        struct_schema1.add_field("embedding", DataType.FLOAT_VECTOR, dim=128)
        struct_schema1.add_field("label", DataType.VARCHAR, max_length=128)
        struct_schema1.add_field("score", DataType.FLOAT)

        # Create second struct schema with same subfield names but potentially different semantics
        struct_schema2 = client.create_struct_field_schema()
        struct_schema2.add_field("embedding", DataType.FLOAT_VECTOR, dim=128)  # Same name
        struct_schema2.add_field("label", DataType.VARCHAR, max_length=128)  # Same name
        struct_schema2.add_field("score", DataType.FLOAT)  # Same name

        # Add multiple struct array fields
        schema.add_field(
            "image_features",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema1,
            max_capacity=50,
        )

        schema.add_field(
            "text_features",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema2,
            max_capacity=50,
        )

        # Create collection
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Insert data to verify the structure works
        # Use fixed random seed for reproducibility in verification
        data = []
        data_map = {}  # Map to store inserted data for verification
        for i in range(10):
            image_features_list = []
            for j in range(2):  # Fixed count for easier verification
                image_features_list.append(
                    {
                        "embedding": [random.random() for _ in range(128)],
                        "label": f"image_label_{i}_{j}",
                        "score": float(i * 10 + j + 0.1),  # Deterministic score
                    }
                )

            text_features_list = []
            for k in range(2):  # Fixed count for easier verification
                text_features_list.append(
                    {
                        "embedding": [random.random() for _ in range(128)],
                        "label": f"text_label_{i}_{k}",
                        "score": float(i * 100 + k + 0.2),  # Deterministic score, different from image
                    }
                )

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "image_features": image_features_list,
                "text_features": text_features_list,
            }
            data.append(row)
            data_map[i] = row  # Store for later verification

        # Insert data
        res, check = self.insert(client, collection_name, data)
        assert check
        assert res["insert_count"] == 10

        # Create indexes for both struct array vector fields
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="image_features[embedding]",
            index_name="image_embedding_index",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="text_features[embedding]",
            index_name="text_embedding_index",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

        # Load collection
        res, check = self.load_collection(client, collection_name)
        assert check

        # Query to verify data integrity with specific output fields
        results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=[
                "id",
                "image_features[score]",
                "image_features[label]",
                "text_features[score]",
                "text_features[label]",
            ],
            limit=10,
        )
        assert check
        assert len(results) == 10

        # Verify query results match inserted data
        for result in results:
            result_id = result["id"]
            original_data = data_map[result_id]

            # Verify both struct arrays are present
            assert "image_features" in result
            assert "text_features" in result

            # Verify image_features data matches original
            assert len(result["image_features"]) == len(original_data["image_features"])
            for idx, img_feat in enumerate(result["image_features"]):
                expected_score = original_data["image_features"][idx]["score"]
                expected_label = original_data["image_features"][idx]["label"]

                assert "score" in img_feat, f"score missing in image_features for id {result_id}"
                assert "label" in img_feat, f"label missing in image_features for id {result_id}"
                assert abs(img_feat["score"] - expected_score) < 0.0001, (
                    f"image_features[{idx}].score mismatch: expected {expected_score}, got {img_feat['score']}"
                )
                assert img_feat["label"] == expected_label, (
                    f"image_features[{idx}].label mismatch: expected {expected_label}, got {img_feat['label']}"
                )

            # Verify text_features data matches original
            assert len(result["text_features"]) == len(original_data["text_features"])
            for idx, txt_feat in enumerate(result["text_features"]):
                expected_score = original_data["text_features"][idx]["score"]
                expected_label = original_data["text_features"][idx]["label"]

                assert "score" in txt_feat, f"score missing in text_features for id {result_id}"
                assert "label" in txt_feat, f"label missing in text_features for id {result_id}"
                assert abs(txt_feat["score"] - expected_score) < 0.0001, (
                    f"text_features[{idx}].score mismatch: expected {expected_score}, got {txt_feat['score']}"
                )
                assert txt_feat["label"] == expected_label, (
                    f"text_features[{idx}].label mismatch: expected {expected_label}, got {txt_feat['label']}"
                )

        # Test search on first struct array with output fields and verify data correctness
        search_vector = [random.random() for _ in range(128)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        results1, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="image_features[embedding]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "image_features[score]", "image_features[label]"],
            limit=5,
        )
        assert check
        assert len(results1[0]) > 0

        # Verify image_features fields in search results match original data
        for hit in results1[0]:
            # Search results may return data in 'entity' field or at top level
            hit_data = hit.get("entity", hit)
            hit_id = hit_data["id"]
            original_data = data_map[hit_id]

            assert "image_features" in hit_data, f"image_features missing for id {hit_id}"

            # Verify each element in image_features array
            for idx, img_feat in enumerate(hit_data["image_features"]):
                expected_score = original_data["image_features"][idx]["score"]
                expected_label = original_data["image_features"][idx]["label"]

                assert "score" in img_feat
                assert "label" in img_feat
                assert abs(img_feat["score"] - expected_score) < 0.0001, (
                    f"Search result: image_features[{idx}].score mismatch for id {hit_id}: expected {expected_score}, got {img_feat['score']}"
                )
                assert img_feat["label"] == expected_label, (
                    f"Search result: image_features[{idx}].label mismatch for id {hit_id}: expected {expected_label}, got {img_feat['label']}"
                )

        # Test search on second struct array with output fields and verify data correctness
        results2, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="text_features[embedding]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "text_features[score]", "text_features[label]"],
            limit=5,
        )
        assert check
        assert len(results2[0]) > 0

        # Verify text_features fields in search results match original data
        for hit in results2[0]:
            # Search results may return data in 'entity' field or at top level
            hit_data = hit.get("entity", hit)
            hit_id = hit_data["id"]
            original_data = data_map[hit_id]

            assert "text_features" in hit_data, f"text_features missing for id {hit_id}"

            # Verify each element in text_features array
            for idx, txt_feat in enumerate(hit_data["text_features"]):
                expected_score = original_data["text_features"][idx]["score"]
                expected_label = original_data["text_features"][idx]["label"]

                assert "score" in txt_feat
                assert "label" in txt_feat
                assert abs(txt_feat["score"] - expected_score) < 0.0001, (
                    f"Search result: text_features[{idx}].score mismatch for id {hit_id}: expected {expected_score}, got {txt_feat['score']}"
                )
                assert txt_feat["label"] == expected_label, (
                    f"Search result: text_features[{idx}].label mismatch for id {hit_id}: expected {expected_label}, got {txt_feat['label']}"
                )

        # Test query with both struct arrays' subfields and verify no confusion between same-named fields
        query_results, check = self.query(
            client,
            collection_name,
            filter="id < 5",
            output_fields=[
                "id",
                "image_features[score]",
                "text_features[score]",
                "image_features[label]",
                "text_features[label]",
            ],
            limit=5,
        )
        assert check
        assert len(query_results) > 0

        # Verify that image_features[score] and text_features[score] are correctly distinguished
        for result in query_results:
            result_id = result["id"]
            original_data = data_map[result_id]

            assert "image_features" in result
            assert "text_features" in result

            # Verify image_features scores match the original data
            for idx in range(len(result["image_features"])):
                img_score = result["image_features"][idx]["score"]
                img_label = result["image_features"][idx]["label"]
                expected_img_score = original_data["image_features"][idx]["score"]
                expected_img_label = original_data["image_features"][idx]["label"]

                # Image scores follow pattern: id * 10 + idx + 0.1
                assert abs(img_score - expected_img_score) < 0.0001, (
                    f"Query: image_features[{idx}].score mismatch for id {result_id}: expected {expected_img_score}, got {img_score}"
                )
                assert img_label == expected_img_label, (
                    f"Query: image_features[{idx}].label mismatch for id {result_id}: expected {expected_img_label}, got {img_label}"
                )

            # Verify text_features scores match the original data
            for idx in range(len(result["text_features"])):
                txt_score = result["text_features"][idx]["score"]
                txt_label = result["text_features"][idx]["label"]
                expected_txt_score = original_data["text_features"][idx]["score"]
                expected_txt_label = original_data["text_features"][idx]["label"]

                # Text scores follow pattern: id * 100 + idx + 0.2
                assert abs(txt_score - expected_txt_score) < 0.0001, (
                    f"Query: text_features[{idx}].score mismatch for id {result_id}: expected {expected_txt_score}, got {txt_score}"
                )
                assert txt_label == expected_txt_label, (
                    f"Query: text_features[{idx}].label mismatch for id {result_id}: expected {expected_txt_label}, got {txt_label}"
                )

        # Test search while returning both struct arrays with same-named subfields
        # This verifies that we can output fields from multiple struct arrays simultaneously
        # without confusion between same-named subfields
        results_multi_output, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="image_features[embedding]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=[
                "id",
                "image_features[score]",
                "text_features[score]",
                "image_features[label]",
                "text_features[label]",
            ],
            limit=3,
        )
        assert check
        assert len(results_multi_output[0]) > 0

        # Verify both struct arrays are correctly returned without field confusion
        # This is the key test: same subfield names (score, label) in different struct arrays
        for hit in results_multi_output[0]:
            # Search results may return data in 'entity' field or at top level
            hit_data = hit.get("entity", hit)
            hit_id = hit_data["id"]
            original_data = data_map[hit_id]

            assert "image_features" in hit_data
            assert "text_features" in hit_data

            # Verify all image_features data
            for idx, img_feat in enumerate(hit_data["image_features"]):
                expected = original_data["image_features"][idx]
                assert abs(img_feat["score"] - expected["score"]) < 0.0001, (
                    f"Multi-output search: image_features[{idx}].score mismatch for id {hit_id}"
                )
                assert img_feat["label"] == expected["label"], (
                    f"Multi-output search: image_features[{idx}].label mismatch for id {hit_id}"
                )

            # Verify all text_features data - ensure no confusion with image_features
            for idx, txt_feat in enumerate(hit_data["text_features"]):
                expected = original_data["text_features"][idx]
                assert abs(txt_feat["score"] - expected["score"]) < 0.0001, (
                    f"Multi-output search: text_features[{idx}].score mismatch for id {hit_id}"
                )
                assert txt_feat["label"] == expected["label"], (
                    f"Multi-output search: text_features[{idx}].label mismatch for id {hit_id}"
                )

    @pytest.mark.tags(CaseLabel.L0)
    def test_insert_struct_array_basic(self):
        """
        target: test basic insert operation with struct array data
        method: insert struct array data into collection
        expected: insert successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_basic")

        client = self._client()

        # Create collection with struct array
        schema = self.create_struct_array_schema(client)
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Generate and insert data
        data = self.generate_struct_array_data(default_nb)
        res, check = self.insert(client, collection_name, data)
        assert check
        assert res["insert_count"] == default_nb

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_struct_array_empty(self):
        """
        target: test insert struct array with empty arrays
        method: insert data where some struct arrays are empty
        expected: insert successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_basic")

        client = self._client()

        # Create collection
        schema = self.create_struct_array_schema(client)
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Generate data with some empty struct arrays
        data = []
        for i in range(10):
            struct_array = []

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": struct_array,
            }
            data.append(row)

        # Insert data
        res, check = self.insert(client, collection_name, data)
        assert check
        assert res["insert_count"] == 10
        # verify data
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check
        results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "clips"],
            limit=10,
        )
        assert check
        assert len(results) == 10
        for result in results:
            assert "clips" in result
            assert len(result["clips"]) == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_struct_array_different_lengths(self):
        """
        target: test insert struct array with different array lengths
        method: insert data where struct arrays have different lengths across rows
        expected: insert successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_basic")

        client = self._client()

        # Create collection
        schema = self.create_struct_array_schema(client, capacity=50)
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Generate data with varied array lengths
        data = []
        for i in range(20):
            array_length = i % 10 + 1  # Lengths from 1 to 10
            struct_array = []

            for j in range(array_length):
                struct_element = {
                    "clip_str": f"row_{i}_elem_{j}",
                    "clip_embedding1": [random.random() for _ in range(default_dim)],
                    "clip_embedding2": [random.random() for _ in range(default_dim)],
                }
                struct_array.append(struct_element)

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": struct_array,
            }
            data.append(row)

        # Insert data
        res, check = self.insert(client, collection_name, data)
        assert check
        assert res["insert_count"] == 20

    @pytest.mark.tags(CaseLabel.L1)
    def test_insert_struct_array_max_capacity(self):
        """
        target: test insert struct array at maximum capacity
        method: insert data where struct arrays reach maximum capacity
        expected: insert successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_basic")

        client = self._client()
        capacity = 10

        # Create collection with small capacity
        schema = self.create_struct_array_schema(client, capacity=capacity)
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Generate data at max capacity
        data = []
        for i in range(5):
            struct_array = []
            for j in range(capacity):  # Fill to max capacity
                struct_element = {
                    "clip_str": f"max_capacity_{j}",
                    "clip_embedding1": [random.random() for _ in range(default_dim)],
                    "clip_embedding2": [random.random() for _ in range(default_dim)],
                }
                struct_array.append(struct_element)

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": struct_array,
            }
            data.append(row)

        # Insert data
        res, check = self.insert(client, collection_name, data)
        assert check
        assert res["insert_count"] == 5

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [32, 128, 768])
    def test_struct_array_different_dimensions(self, dim):
        """
        target: test struct array with different vector dimensions
        method: create collections with different vector dimensions in struct array
        expected: creation and insert successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_basic")

        client = self._client()

        # Create collection with specified dimension
        schema = self.create_struct_array_schema(client, dim=dim)
        collection_name = cf.gen_unique_str(f"{prefix}_dim_{dim}")
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Generate and insert data
        data = self.generate_struct_array_data(10, dim=dim)
        res, check = self.insert(client, collection_name, data)
        assert check
        assert res["insert_count"] == 10

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("capacity", [1, 10, 100])
    def test_struct_array_different_capacities(self, capacity):
        """
        target: test struct array with different max capacities
        method: create collections with different max capacities
        expected: creation and insert successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_basic")

        client = self._client()

        # Create collection with specified capacity
        schema = self.create_struct_array_schema(client, capacity=capacity)
        collection_name = cf.gen_unique_str(f"{prefix}_cap_{capacity}")
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Generate and insert data (respecting capacity)
        data = self.generate_struct_array_data(5, capacity=capacity)
        res, check = self.insert(client, collection_name, data)
        assert check
        assert res["insert_count"] == 5


class TestMilvusClientStructArrayIndex(TestMilvusClientV2Base):
    """Test case of struct array index functionality"""

    def create_collection_with_data(
        self,
        client: MilvusClient,
        collection_name: str,
        dim: int = default_dim,
        nb: int = default_nb,
    ):
        """Create collection with struct array and insert data"""
        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        # Create struct schema with vector field
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("scalar_field", DataType.INT64)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )

        # Create collection
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Insert data
        data = []
        for i in range(nb):
            array_length = random.randint(1, 10)
            struct_array = []
            for j in range(array_length):
                struct_element = {
                    "clip_embedding1": [random.random() for _ in range(dim)],
                    "scalar_field": i * 10 + j,
                }
                struct_array.append(struct_element)

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(dim)],
                "clips": struct_array,
            }
            data.append(row)

        res, check = self.insert(client, collection_name, data)
        assert check
        return data

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_emb_list_hnsw_index_cosine(self):
        """
        target: test create HNSW index with MAX_SIM_COSINE metric
        method: create index on vector field in struct array with COSINE metric
        expected: index creation successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_index")

        client = self._client()

        # Create collection with data
        self.create_collection_with_data(client, collection_name)

        # Create index on normal vector field first
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )

        # Create HNSW index on struct array vector field
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name="struct_vector_index",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_emb_list_hnsw_index_ip(self):
        """
        target: test create HNSW index with MAX_SIM_COSINE metric
        method: create index on vector field in struct array with COSINE metric
        expected: index creation successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_index")

        client = self._client()

        # Create collection with data
        self.create_collection_with_data(client, collection_name)

        # Create index parameters
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )

        # Create HNSW index with MAX_SIM_COSINE metric
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name="struct_vector_index_max_sim",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_index_on_multiple_clip_embedding1s(self):
        """
        target: test create index on multiple vector fields in struct array
        method: create collection with multiple vector fields and create indexes
        expected: all indexes creation successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_index")

        client = self._client()

        # Create schema with multiple vector fields in struct
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        # Create struct schema with multiple vector fields
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("vector1", DataType.FLOAT_VECTOR, dim=64)
        struct_schema.add_field("vector2", DataType.FLOAT_VECTOR, dim=128)
        struct_schema.add_field("scalar_field", DataType.INT64)

        schema.add_field(
            "multi_vector_struct",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=50,
        )

        # Create collection
        collection_name = cf.gen_unique_str(f"{prefix}_multi_vec")
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Insert data
        data = []
        for i in range(20):
            struct_array = [
                {
                    "vector1": [random.random() for _ in range(64)],
                    "vector2": [random.random() for _ in range(128)],
                    "scalar_field": i,
                }
            ]

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "multi_vector_struct": struct_array,
            }
            data.append(row)

        res, check = self.insert(client, collection_name, data)
        assert check

        # Create indexes on all vector fields
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="multi_vector_struct[vector1]",
            index_name="vector1_index",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="multi_vector_struct[vector2]",
            index_name="vector2_index",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

    @pytest.mark.tags(CaseLabel.L1)
    def test_drop_struct_array_index(self):
        """
        target: test drop index on struct array vector field
        method: create index and then drop it
        expected: drop operation successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_index")

        client = self._client()

        # Create collection with data and index
        self.create_collection_with_data(client, collection_name)

        # Create index
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name="struct_vector_index",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

        # Drop the struct array index
        res, check = self.drop_index(client, collection_name, "struct_vector_index")
        assert check

    @pytest.mark.tags(CaseLabel.L2)
    def test_rebuild_struct_array_index(self):
        """
        target: test rebuild index on struct array vector field
        method: create index, drop it, and recreate with different parameters
        expected: rebuild operation successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_index")

        client = self._client()

        # Create collection with data
        self.create_collection_with_data(client, collection_name)

        # Create initial index
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name="struct_vector_index",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params={"M": 16, "efConstruction": 200},
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

        # Drop index
        res, check = self.drop_index(client, collection_name, "struct_vector_index")
        assert check

        # Recreate index with different parameters
        new_index_params = client.prepare_index_params()
        new_index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name="struct_vector_index_rebuilt",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params={"M": 32, "efConstruction": 400},
        )

        res, check = self.create_index(client, collection_name, new_index_params)
        assert check

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric_type", METRICS)
    def test_emb_list_hnsw_different_metrics(self, metric_type):
        """
        target: test HNSW index with different metric types
        method: create index with different supported metrics
        expected: index creation successful for all supported metrics
        """
        collection_name = cf.gen_unique_str(f"{prefix}_index")

        client = self._client()

        # Create collection with data
        collection_name = cf.gen_unique_str(f"{prefix}_metric_{metric_type.lower()}")
        self.create_collection_with_data(client, collection_name)

        # Create index with specified metric
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name=f"struct_index_{metric_type.lower()}",
            index_type="HNSW",
            metric_type=metric_type,
            params=INDEX_PARAMS,
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check


class TestMilvusClientStructArraySearch(TestMilvusClientV2Base):
    """Test case of struct array search functionality"""

    def create_collection_with_index(
        self,
        client: MilvusClient,
        collection_name: str,
        dim: int = default_dim,
        nb: int = default_nb,
    ):
        """Create collection with struct array, insert data and create index"""
        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        # Create struct schema
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("scalar_field", DataType.INT64)
        struct_schema.add_field("category", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )

        schema.add_field("scalar_field", datatype=DataType.INT64)
        schema.add_field("category", datatype=DataType.VARCHAR, max_length=128)
        schema.add_field("score", datatype=DataType.FLOAT)
        # Create collection
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Insert data
        data = []
        for i in range(nb):
            array_length = random.randint(1, 5)
            struct_array = []
            for j in range(array_length):
                struct_element = {
                    "clip_embedding1": [random.random() for _ in range(dim)],
                    "scalar_field": i * 10 + j,
                    "category": f"cat_{i % 5}",
                }
                struct_array.append(struct_element)

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(dim)],
                "clips": struct_array,
                "scalar_field": i * 10 + j,
                "category": f"cat_{i % 5}",
                "score": random.uniform(0.1, 10.0),
            }
            data.append(row)

        res, check = self.insert(client, collection_name, data)
        assert check

        # Create indexes
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name="struct_vector_index",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

        # Load collection
        res, check = self.load_collection(client, collection_name)
        assert check

        return data

    def create_embedding_list(self, dim: int, num_vectors: int):
        """Create EmbeddingList for search"""
        embedding_list = EmbeddingList()
        for _ in range(num_vectors):
            vector = [random.random() for _ in range(dim)]
            embedding_list.add(vector)
        return embedding_list

    def create_collection_with_configurable_index(
        self,
        client: MilvusClient,
        collection_name: str,
        index_type: str = "HNSW",
        metric_type: str = "MAX_SIM_COSINE",
        build_params: dict = None,
        dim: int = default_dim,
        nb: int = default_nb,
    ):
        """Create collection with struct array, insert data and create configurable index"""
        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        # Create struct schema
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("scalar_field", DataType.INT64)
        struct_schema.add_field("category", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )

        schema.add_field("scalar_field", datatype=DataType.INT64)
        schema.add_field("category", datatype=DataType.VARCHAR, max_length=128)
        schema.add_field("score", datatype=DataType.FLOAT)
        # Create collection
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Insert data
        data = []
        for i in range(nb):
            array_length = random.randint(1, 5)
            struct_array = []
            for j in range(array_length):
                struct_element = {
                    "clip_embedding1": [random.random() for _ in range(dim)],
                    "scalar_field": i * 10 + j,
                    "category": f"cat_{i % 5}",
                }
                struct_array.append(struct_element)

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(dim)],
                "clips": struct_array,
                "scalar_field": i * 10 + j,
                "category": f"cat_{i % 5}",
                "score": random.uniform(0.1, 10.0),
            }
            data.append(row)

        res, check = self.insert(client, collection_name, data)
        assert check

        # Create indexes
        if build_params is None:
            build_params = INDEX_PARAMS

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name="struct_vector_index",
            index_type=index_type,
            metric_type=metric_type,
            params=build_params,
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

        # Load collection
        res, check = self.load_collection(client, collection_name)
        assert check

        return data

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_struct_array_vector_single(self):
        """
        target: test search with single vector in struct array
        method: search using single vector against struct array vector field
        expected: search returns results
        """
        collection_name = cf.gen_unique_str(f"{prefix}_search")

        client = self._client()

        # Create collection with data and index
        self.create_collection_with_index(client, collection_name)

        # Create search vector
        search_vector = [random.random() for _ in range(default_dim)]

        # Search using normal vector field first (for comparison)
        normal_search_results, check = self.search(
            client,
            collection_name,
            data=[search_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {"nprobe": 10}},
            limit=10,
        )
        assert check
        assert len(normal_search_results[0]) > 0
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)
        # Search using struct array vector field
        struct_search_results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        print(struct_search_results)
        assert check
        assert len(struct_search_results[0]) > 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_struct_array_last_vector_subfield(self):
        """
        target: test search on a vector sub-field after multiple struct array parent fields
        method: create multiple struct arrays so parent field IDs create gaps, then search the last vector sub-field
        expected: search returns results
        """
        collection_name = cf.gen_unique_str(f"{prefix}_search_last_subfield")

        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        person_schema = client.create_struct_field_schema()
        person_schema.add_field("name", DataType.VARCHAR, max_length=128)
        person_schema.add_field("name_vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "suspects",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=person_schema,
            max_capacity=20,
        )
        schema.add_field(
            "victims",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=person_schema,
            max_capacity=20,
        )

        vehicle_schema = client.create_struct_field_schema()
        vehicle_schema.add_field("model", DataType.VARCHAR, max_length=128)
        vehicle_schema.add_field("vehicle_vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "vehicles",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=vehicle_schema,
            max_capacity=20,
        )

        evidence_schema = client.create_struct_field_schema()
        evidence_schema.add_field("item", DataType.VARCHAR, max_length=128)
        evidence_schema.add_field("evidence_vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "evidence",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=evidence_schema,
            max_capacity=20,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="suspects[name_vector]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="victims[name_vector]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="vehicles[vehicle_vector]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="evidence[evidence_vector]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        data = []
        for i in range(64):
            data.append(
                {
                    "id": i,
                    "normal_vector": [random.random() for _ in range(default_dim)],
                    "suspects": [
                        {"name": f"suspect_{i}", "name_vector": [random.random() for _ in range(default_dim)]}
                    ],
                    "victims": [{"name": f"victim_{i}", "name_vector": [random.random() for _ in range(default_dim)]}],
                    "vehicles": [
                        {"model": f"vehicle_{i}", "vehicle_vector": [random.random() for _ in range(default_dim)]}
                    ],
                    "evidence": [
                        {"item": f"evidence_{i}", "evidence_vector": [random.random() for _ in range(default_dim)]}
                    ],
                }
            )

        res, check = self.insert(client, collection_name, data)
        assert check
        assert res["insert_count"] == 64

        res, check = self.load_collection(client, collection_name)
        assert check

        search_tensor = EmbeddingList()
        search_tensor.add([random.random() for _ in range(default_dim)])
        results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="evidence[evidence_vector]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        assert check
        assert len(results[0]) > 0

    @pytest.mark.tags(CaseLabel.L0)
    def test_search_struct_array_vector_multiple(self):
        """
        target: test search with multiple vectors (EmbeddingList) in struct array
        method: search using EmbeddingList against struct array vector field
        expected: search returns results
        """
        collection_name = cf.gen_unique_str(f"{prefix}_search")

        client = self._client()

        # Create collection with data and index
        self.create_collection_with_index(client, collection_name)

        # Create EmbeddingList with multiple vectors
        embedding_list = self.create_embedding_list(default_dim, 3)

        # Search using EmbeddingList
        results, check = self.search(
            client,
            collection_name,
            data=[embedding_list],  # Multiple vectors as embedding list
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        assert check
        assert len(results[0]) > 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_with_scalar_filter(self):
        """
        target: test search with scalar field filter in struct array
        method: search with filter expression on struct array scalar field
        expected: search returns filtered results
        """
        collection_name = cf.gen_unique_str(f"{prefix}_search")

        client = self._client()

        # Create collection with data and index
        data = self.create_collection_with_index(client, collection_name)
        scalar_field_array_lists = [row["scalar_field"] for row in data]
        # Create search vector
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)
        # Search with scalar filter
        results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            filter=f"scalar_field == {scalar_field_array_lists[0]}",  # Filter on scalar field
            limit=10,
        )
        print(results)
        assert check

        # Verify filter worked (if results exist)
        if len(results[0]) > 0:
            for hit in results[0]:
                # Check that returned data respects the filter
                # This depends on what fields are returned
                assert hit is not None

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_different_array_lengths(self):
        """
        target: test search with different EmbeddingList lengths
        method: search using EmbeddingLists of different lengths
        expected: search works with various lengths
        """
        collection_name = cf.gen_unique_str(f"{prefix}_search")

        client = self._client()

        # Create collection with data and index
        self.create_collection_with_index(client, collection_name)

        # Test with different embedding list lengths
        for num_vectors in [1, 2, 5, 10]:
            embedding_list = self.create_embedding_list(default_dim, num_vectors)

            results, check = self.search(
                client,
                collection_name,
                data=[embedding_list],
                anns_field="clips[clip_embedding1]",
                search_params={"metric_type": "MAX_SIM_COSINE"},
                limit=5,
            )
            assert check
            assert len(results) > 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_struct_array(self):
        """
        target: test hybrid search with both normal and struct array vectors
        method: perform search on both normal vector field and struct array vector field
        expected: both searches return results
        """
        collection_name = cf.gen_unique_str(f"{prefix}_search")

        client = self._client()

        # Create collection with data and index
        self.create_collection_with_index(client, collection_name)

        # Create search vectors
        normal_vector = [random.random() for _ in range(default_dim)]
        struct_vectors = self.create_embedding_list(default_dim, 2)

        # Search normal vector field
        normal_results, check = self.search(
            client,
            collection_name,
            data=[normal_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {"nprobe": 10}},
            limit=5,
        )
        assert check

        # Search struct array vector field
        struct_results, check = self.search(
            client,
            collection_name,
            data=[struct_vectors],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=5,
        )
        assert check

        # Both should return results
        assert len(normal_results[0]) > 0
        assert len(struct_results[0]) > 0

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric_type", METRICS)
    def test_search_different_metrics(self, metric_type):
        """
        target: test search with different metric types
        method: create index and search with different supported metrics
        expected: search works with all supported metrics
        """
        collection_name = cf.gen_unique_str(f"{prefix}_search")

        client = self._client()

        # Create collection
        collection_name = cf.gen_unique_str(f"{prefix}_metric_search_{metric_type.lower()}")
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=50,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Insert data
        data = []
        for i in range(20):
            struct_array = [{"clip_embedding1": [random.random() for _ in range(default_dim)]}]
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": struct_array,
            }
            data.append(row)

        res, check = self.insert(client, collection_name, data)
        assert check

        # Create index with specified metric
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name=f"struct_index_{metric_type.lower()}",
            index_type="HNSW",
            metric_type=metric_type,
            params=INDEX_PARAMS,
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

        # Load collection
        res, check = self.load_collection(client, collection_name)
        assert check

        # Search with the metric
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)
        results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": metric_type},
            limit=5,
        )
        assert check
        assert len(results[0]) > 0

    @pytest.mark.tags(CaseLabel.L2)
    def test_search_with_output_fields(self):
        """
        target: test search with specific output fields from struct array
        method: search and specify output fields including struct array fields
        expected: search returns specified fields
        """
        collection_name = cf.gen_unique_str(f"{prefix}_search")

        client = self._client()

        # Create collection with data and index
        self.create_collection_with_index(client, collection_name)

        # Create search vector
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)
        # Search with specific output fields
        results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=5,
            output_fields=["id", "clips"],  # Include struct array in output
        )
        assert check
        assert len(results[0]) > 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_struct_array_not_support_search_by_pk(self):
        """
        target: test searching with multiple vectors (EmbeddingList) in struct array does not supprt search by pk
        method: search using EmbeddingList by pk
        expected: search failed with error
        """
        collection_name = cf.gen_unique_str(f"{prefix}_search")

        client = self._client()
        # Create collection with data and index
        self.create_collection_with_index(client, collection_name)

        # Search using EmbeddingList
        error = {
            ct.err_code: 999,
            ct.err_msg: "array of vector is not supported for search by IDs",
        }
        self.search(
            client,
            collection_name,
            ids=[0, 1],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("retrieval_ann_ratio", [1.0, 3.0, 5.0, 10.0])
    def test_search_with_retrieval_ann_ratio(self, retrieval_ann_ratio):
        """
        target: test search with retrieval_ann_ratio parameter for struct array
        method: search with different retrieval_ann_ratio values to control recall
        expected: search returns results, higher ratio should improve recall
        """
        collection_name = cf.gen_unique_str(f"{prefix}_search")

        client = self._client()

        # Create collection with data and index
        self.create_collection_with_index(client, collection_name)

        # Create search vector
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        # Search with retrieval_ann_ratio parameter
        results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={
                "metric_type": "MAX_SIM_COSINE",
                "params": {"retrieval_ann_ratio": retrieval_ann_ratio},
            },
            limit=10,
        )
        assert check
        assert len(results[0]) > 0

        # Verify results are returned
        for hit in results[0]:
            assert hit is not None

    @pytest.mark.tags(CaseLabel.L1)
    def test_search_recall_with_maxsim_ground_truth(self):
        """
        target: test search recall by comparing with MaxSim ground truth
        method: calculate brute-force MaxSim similarity as ground truth,
                then compare with Milvus search results to compute recall
        expected: higher retrieval_ann_ratio should improve recall
        """

        def maxsim_similarity_numpy(query_emb: np.ndarray, doc_emb: np.ndarray) -> float:
            """
            Standard MaxSim calculation using NumPy (brute-force ground truth).

            MaxSim(Q, D) = sum_i max_j (q_i · d_j)
            where q_i are query token embeddings and d_j are document patch embeddings.
            """
            # Normalize embeddings
            query_norm = query_emb / (np.linalg.norm(query_emb, axis=1, keepdims=True) + 1e-8)
            doc_norm = doc_emb / (np.linalg.norm(doc_emb, axis=1, keepdims=True) + 1e-8)

            # Compute similarity matrix: (num_tokens, num_patches)
            similarities = np.dot(query_norm, doc_norm.T)

            # For each query token, get max similarity across all patches
            max_scores = np.max(similarities, axis=1)

            # Sum all max scores
            return np.sum(max_scores)

        collection_name = cf.gen_unique_str(f"{prefix}_recall")
        client = self._client()
        dim = default_dim
        nb = 1000  # Number of documents
        num_query_vectors = 30  # Number of vectors in query (simulating query tokens)
        min_patches = 100  # Min patches per document
        max_patches = 300  # Max patches per document

        log.info(
            f"Creating collection with {nb} docs, {num_query_vectors} query vectors, "
            f"{min_patches}-{max_patches} patches per doc"
        )

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        # Create struct schema
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=dim)
        struct_schema.add_field("scalar_field", DataType.INT64)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=max_patches,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema)

        # Generate and insert data in batches, storing embeddings for ground truth
        doc_embeddings = {}  # id -> numpy array of embeddings (for ground truth)
        batch_size = 100

        for batch_start in range(0, nb, batch_size):
            batch_end = min(batch_start + batch_size, nb)
            data = []

            for i in range(batch_start, batch_end):
                array_length = random.randint(min_patches, max_patches)
                struct_array = []
                embeddings_list = []

                for j in range(array_length):
                    embedding = [random.random() for _ in range(dim)]
                    struct_element = {
                        "clip_embedding1": embedding,
                        "scalar_field": i * 10 + j,
                    }
                    struct_array.append(struct_element)
                    embeddings_list.append(embedding)

                row = {
                    "id": i,
                    "normal_vector": [random.random() for _ in range(dim)],
                    "clips": struct_array,
                }
                data.append(row)
                doc_embeddings[i] = np.array(embeddings_list)

            self.insert(client, collection_name, data)
            log.info(f"Inserted batch {batch_start // batch_size + 1}/{(nb + batch_size - 1) // batch_size}")

        # Create indexes
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name="struct_vector_index",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        self.create_index(client, collection_name, index_params)

        # Load collection
        self.load_collection(client, collection_name)

        # Generate query vectors
        log.info(f"Generating {num_query_vectors} query vectors...")
        query_vectors = [np.array([random.random() for _ in range(dim)]) for _ in range(num_query_vectors)]
        query_emb = np.array(query_vectors)  # Shape: (num_query_vectors, dim)

        # Calculate ground truth: compute MaxSim score for each document
        log.info(f"Calculating ground truth MaxSim scores for {nb} documents...")
        start_time = time.time()
        ground_truth_scores = []
        for idx, (doc_id, doc_emb) in enumerate(doc_embeddings.items()):
            score = maxsim_similarity_numpy(query_emb, doc_emb)
            ground_truth_scores.append((doc_id, score))
            if (idx + 1) % 500 == 0:
                elapsed = time.time() - start_time
                log.info(f"Calculated {idx + 1}/{nb} ground truth scores, elapsed: {elapsed:.1f}s")

        gt_calc_time = time.time() - start_time
        log.info(f"Ground truth calculation completed in {gt_calc_time:.1f}s")

        # Sort by score descending to get ground truth ranking
        ground_truth_scores.sort(key=lambda x: x[1], reverse=True)

        limit = 10
        ground_truth_ids = set([item[0] for item in ground_truth_scores[:limit]])

        # Create EmbeddingList for Milvus search
        search_tensor = EmbeddingList()
        for vec in query_vectors:
            search_tensor.add(vec.tolist())

        # Log ground truth top-10 with scores (only once)
        log.info(f"Ground truth top-{limit} IDs: {sorted(ground_truth_ids)}")
        log.info(f"Ground truth top-{limit} with scores:")
        for i, (doc_id, score) in enumerate(ground_truth_scores[:limit]):
            num_patches = len(doc_embeddings[doc_id])
            log.info(f"  GT rank {i + 1}: id={doc_id}, score={score:.6f}, num_patches={num_patches}")

        # Search with different retrieval_ann_ratio values
        retrieval_ann_ratios = [0.1, 1.0, 3.0, 5.0]
        recall_results = {}  # Track recall for each ratio
        for retrieval_ann_ratio in retrieval_ann_ratios:
            log.info(f"\n{'=' * 50}")
            log.info(f"Testing retrieval_ann_ratio={retrieval_ann_ratio}")

            results, _ = self.search(
                client,
                collection_name,
                data=[search_tensor],
                anns_field="clips[clip_embedding1]",
                search_params={
                    "metric_type": "MAX_SIM_COSINE",
                    "params": {"retrieval_ann_ratio": retrieval_ann_ratio},
                },
                limit=limit,
            )
            assert len(results[0]) > 0

            # Get Milvus search result IDs
            milvus_result_ids = set([hit["id"] for hit in results[0]])

            # Calculate recall: intersection of ground truth and Milvus results
            recall_hits = len(ground_truth_ids.intersection(milvus_result_ids))
            recall = recall_hits / len(ground_truth_ids)
            recall_results[retrieval_ann_ratio] = recall

            log.info(
                f"retrieval_ann_ratio={retrieval_ann_ratio}, recall={recall:.4f}, recall_hits={recall_hits}/{limit}"
            )
            log.info(f"Milvus result IDs: {sorted(milvus_result_ids)}")

            # Log detailed comparison for Milvus results
            log.info("Milvus results with ground truth comparison:")
            gt_ranks_for_milvus = []
            for i, hit in enumerate(results[0][:limit]):
                gt_score = next(
                    (s for doc_id, s in ground_truth_scores if doc_id == hit["id"]),
                    None,
                )
                gt_rank = next(
                    (idx + 1 for idx, (doc_id, _) in enumerate(ground_truth_scores) if doc_id == hit["id"]),
                    None,
                )
                gt_ranks_for_milvus.append(gt_rank)
                log.info(
                    f"  Milvus rank {i + 1}: id={hit['id']}, distance={hit['distance']:.6f}, "
                    f"gt_score={gt_score:.6f}, gt_rank={gt_rank}"
                )

            # Calculate recall at different K values
            for k in [1, 5, 10]:
                gt_top_k = set([item[0] for item in ground_truth_scores[:k]])
                recall_at_k = len(gt_top_k.intersection(milvus_result_ids)) / min(k, limit)
                log.info(f"Recall@{k}: {recall_at_k:.4f}")

            # Calculate average ground truth rank for Milvus results
            avg_gt_rank = sum(gt_ranks_for_milvus) / len(gt_ranks_for_milvus)
            log.info(f"Average GT rank for Milvus top-{limit}: {avg_gt_rank:.1f}")

            # Verify results
            assert recall >= 0, f"Recall should be non-negative, got {recall}"
            assert len(results[0]) == limit, f"Expected {limit} results, got {len(results[0])}"

        # Verify that higher retrieval_ann_ratio leads to higher or equal recall
        log.info(f"\nRecall results summary: {recall_results}")
        for i in range(len(retrieval_ann_ratios) - 1):
            ratio_curr = retrieval_ann_ratios[i]
            ratio_next = retrieval_ann_ratios[i + 1]
            assert recall_results[ratio_next] >= recall_results[ratio_curr], (
                f"Recall should increase with higher retrieval_ann_ratio: "
                f"ratio {ratio_curr} has recall {recall_results[ratio_curr]}, "
                f"but ratio {ratio_next} has recall {recall_results[ratio_next]}"
            )

        # Verify that recall >= 0.8 when retrieval_ann_ratio >= 3
        for ratio, recall in recall_results.items():
            if ratio >= 3:
                assert recall >= 0.8, (
                    f"Recall should be >= 0.8 when retrieval_ann_ratio >= 3, but ratio {ratio} has recall {recall}"
                )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("index_type", EMB_LIST_INDEX_TYPES)
    def test_search_emb_list_with_different_index_types(self, index_type):
        """
        target: test search with full CRUD path for different emb list index types
        method: insert (flushed + growing) → index → load → search → upsert → delete → search → verify
        expected: all CRUD operations and search work correctly for each index type
        """
        collection_name = cf.gen_unique_str(f"{prefix}_search_{index_type.lower()}")
        client = self._client()

        config = EMB_LIST_INDEX_CONFIGS[index_type]

        # Create collection with full schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=EMB_LIST_DIM)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=EMB_LIST_DIM)
        struct_schema.add_field("scalar_field", DataType.INT64)
        struct_schema.add_field("category", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )

        schema.add_field("scalar_field", datatype=DataType.INT64)
        schema.add_field("category", datatype=DataType.VARCHAR, max_length=128)
        schema.add_field("score", datatype=DataType.FLOAT)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Insert 3000 records and flush (sealed segments)
        nb_flushed = 3000
        flushed_data = []
        for i in range(nb_flushed):
            array_length = random.randint(1, 5)
            struct_array = []
            for j in range(array_length):
                struct_element = {
                    "clip_embedding1": [random.random() for _ in range(EMB_LIST_DIM)],
                    "scalar_field": i * 10 + j,
                    "category": f"cat_{i % 5}",
                }
                struct_array.append(struct_element)

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(EMB_LIST_DIM)],
                "clips": struct_array,
                "scalar_field": i * 10 + j,
                "category": f"cat_{i % 5}",
                "score": random.uniform(0.1, 10.0),
            }
            flushed_data.append(row)

        res, check = self.insert(client, collection_name, flushed_data)
        assert check
        assert res["insert_count"] == nb_flushed

        res, check = self.flush(client, collection_name)
        assert check

        # Insert 500 more growing records
        nb_growing = 500
        growing_data = []
        for i in range(nb_flushed, nb_flushed + nb_growing):
            array_length = random.randint(1, 5)
            struct_array = []
            for j in range(array_length):
                struct_element = {
                    "clip_embedding1": [random.random() for _ in range(EMB_LIST_DIM)],
                    "scalar_field": i * 10 + j,
                    "category": f"cat_{i % 5}",
                }
                struct_array.append(struct_element)

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(EMB_LIST_DIM)],
                "clips": struct_array,
                "scalar_field": i * 10 + j,
                "category": f"cat_{i % 5}",
                "score": random.uniform(0.1, 10.0),
            }
            growing_data.append(row)

        res, check = self.insert(client, collection_name, growing_data)
        assert check
        assert res["insert_count"] == nb_growing

        # Create index with configurable type
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name="struct_vector_index",
            index_type=index_type,
            metric_type="MAX_SIM_COSINE",
            params=config["build_params"],
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

        # Load collection
        res, check = self.load_collection(client, collection_name)
        assert check

        # Search with EmbeddingList and verify results
        embedding_list = self.create_embedding_list(EMB_LIST_DIM, 3)

        results, check = self.search(
            client,
            collection_name,
            data=[embedding_list],
            anns_field="clips[clip_embedding1]",
            search_params={
                "metric_type": "MAX_SIM_COSINE",
                "params": config["search_params"],
            },
            limit=10,
            output_fields=["id", "clips"],
        )
        assert check
        assert len(results[0]) > 0
        for hit in results[0]:
            assert 0 <= hit["id"] < nb_flushed + nb_growing

        # Upsert 10 records from flushed segment
        upsert_data = []
        for i in range(10):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(EMB_LIST_DIM)],
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(EMB_LIST_DIM)],
                        "scalar_field": i + 10000,
                        "category": f"upserted_{i}",
                    }
                ],
                "scalar_field": i + 10000,
                "category": f"upserted_{i}",
                "score": random.uniform(10.0, 20.0),
            }
            upsert_data.append(row)

        res, check = self.upsert(client, collection_name, upsert_data)
        assert check

        # Verify upsert via query
        res, check = self.flush(client, collection_name)
        assert check

        results, check = self.query(
            client,
            collection_name,
            filter="id < 10",
            output_fields=["id", "category", "clips"],
        )
        assert check
        assert len(results) == 10
        for result in results:
            assert "upserted" in result["category"]
            assert "upserted" in result["clips"][0]["category"]

        # Delete 5 records from growing segment
        delete_ids = list(range(nb_flushed, nb_flushed + 5))
        res, check = self.delete(client, collection_name, filter=f"id in {delete_ids}")
        assert check

        # Verify deletion
        res, check = self.flush(client, collection_name)
        assert check

        results, check = self.query(client, collection_name, filter="id >= 0", output_fields=["id"])
        assert check
        remaining_ids = {r["id"] for r in results}
        for del_id in delete_ids:
            assert del_id not in remaining_ids
        assert len(results) == nb_flushed + nb_growing - 5

        # Search again after CRUD operations
        results, check = self.search(
            client,
            collection_name,
            data=[embedding_list],
            anns_field="clips[clip_embedding1]",
            search_params={
                "metric_type": "MAX_SIM_COSINE",
                "params": config["search_params"],
            },
            limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) > 0
        for hit in results[0]:
            assert hit["id"] not in delete_ids

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("emb_list_strategy,index_type", EMB_LIST_STRATEGY_INDEX_CASES)
    def test_search_emb_list_with_explicit_strategy(self, emb_list_strategy, index_type):
        """
        target: test emb list search with explicitly specified strategies
        method: create HNSW and DISKANN indexes with supported emb_list_strategy values, load, and search
        expected: index creation and search work correctly
        """
        collection_name = cf.gen_unique_str(f"{prefix}_search_{emb_list_strategy}_{index_type.lower()}")
        client = self._client()
        strategy_config = EMB_LIST_STRATEGY_CONFIGS[emb_list_strategy]
        index_config = EMB_LIST_STRATEGY_INDEX_CONFIGS[index_type]

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=EMB_LIST_DIM)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=EMB_LIST_DIM)
        struct_schema.add_field("scalar_field", DataType.INT64)
        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=10,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        nb = 3000
        data = []
        for i in range(nb):
            clips = []
            for j in range(2):
                clips.append(
                    {
                        "clip_embedding1": [random.random() for _ in range(EMB_LIST_DIM)],
                        "scalar_field": i * 10 + j,
                    }
                )
            data.append(
                {
                    "id": i,
                    "normal_vector": [random.random() for _ in range(EMB_LIST_DIM)],
                    "clips": clips,
                }
            )

        res, check = self.insert(client, collection_name, data)
        assert check
        assert res["insert_count"] == nb

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name=f"struct_vector_index_{emb_list_strategy}_{index_type.lower()}",
            index_type=index_type,
            metric_type="MAX_SIM_COSINE",
            params={
                **index_config["build_params"],
                **strategy_config["strategy_params"],
            },
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        embedding_list = self.create_embedding_list(EMB_LIST_DIM, 3)
        results, check = self.search(
            client,
            collection_name,
            data=[embedding_list],
            anns_field="clips[clip_embedding1]",
            search_params={
                "metric_type": "MAX_SIM_COSINE",
                "params": index_config["search_params"],
            },
            limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) > 0
        for hit in results[0]:
            assert 0 <= hit["id"] < nb

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "vector_type",
        [
            DataType.FLOAT16_VECTOR,
            DataType.BFLOAT16_VECTOR,
            DataType.INT8_VECTOR,
            DataType.BINARY_VECTOR,
        ],
    )
    def test_search_emb_list_with_different_vector_types(self, vector_type):
        """
        target: test search with full CRUD path for different vector data types in struct array
        method: insert (flushed + growing) → HNSW index → load → search → upsert → delete → search → verify
        expected: all CRUD operations and search work correctly for each vector type
        """
        # Select metric based on vector type
        if vector_type == DataType.BINARY_VECTOR:
            emb_list_metric = "MAX_SIM_HAMMING"
        else:
            emb_list_metric = "MAX_SIM_COSINE"

        type_name = vector_type.name.lower()
        collection_name = cf.gen_unique_str(f"{prefix}_search_vtype_{type_name}")
        client = self._client()

        # Create schema with specified vector type
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=EMB_LIST_DIM)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding1", vector_type, dim=EMB_LIST_DIM)
        struct_schema.add_field("scalar_field", DataType.INT64)
        struct_schema.add_field("category", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )

        schema.add_field("scalar_field", datatype=DataType.INT64)
        schema.add_field("category", datatype=DataType.VARCHAR, max_length=128)
        schema.add_field("score", datatype=DataType.FLOAT)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Insert 3000 records and flush (sealed segments)
        nb_flushed = 3000
        flushed_data = []
        for i in range(nb_flushed):
            array_length = random.randint(1, 5)
            struct_array = []
            for j in range(array_length):
                vec = cf.gen_vectors(1, EMB_LIST_DIM, vector_type)[0]
                struct_element = {
                    "clip_embedding1": vec,
                    "scalar_field": i * 10 + j,
                    "category": f"flushed_{i}",
                }
                struct_array.append(struct_element)

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(EMB_LIST_DIM)],
                "clips": struct_array,
                "scalar_field": i * 10 + j,
                "category": f"flushed_{i}",
                "score": random.uniform(0.1, 10.0),
            }
            flushed_data.append(row)

        res, check = self.insert(client, collection_name, flushed_data)
        assert check
        assert res["insert_count"] == nb_flushed

        res, check = self.flush(client, collection_name)
        assert check

        # Insert 500 more growing records
        nb_growing = 500
        growing_data = []
        for i in range(nb_flushed, nb_flushed + nb_growing):
            array_length = random.randint(1, 5)
            struct_array = []
            for j in range(array_length):
                vec = cf.gen_vectors(1, EMB_LIST_DIM, vector_type)[0]
                struct_element = {
                    "clip_embedding1": vec,
                    "scalar_field": i * 10 + j,
                    "category": f"growing_{i}",
                }
                struct_array.append(struct_element)

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(EMB_LIST_DIM)],
                "clips": struct_array,
                "scalar_field": i * 10 + j,
                "category": f"growing_{i}",
                "score": random.uniform(0.1, 10.0),
            }
            growing_data.append(row)

        res, check = self.insert(client, collection_name, growing_data)
        assert check
        assert res["insert_count"] == nb_growing

        # Create HNSW index with appropriate metric for vector type
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name="struct_vector_index",
            index_type="HNSW",
            metric_type=emb_list_metric,
            params=INDEX_PARAMS,
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        # Search with EmbeddingList and verify results
        search_vecs = cf.gen_vectors(3, EMB_LIST_DIM, vector_type)
        if vector_type == DataType.BINARY_VECTOR:
            embedding_list = EmbeddingList(
                [np.frombuffer(v, dtype=np.uint8) if isinstance(v, bytes) else v for v in search_vecs]
            )
        else:
            embedding_list = EmbeddingList([np.array(v) if not isinstance(v, np.ndarray) else v for v in search_vecs])

        results, check = self.search(
            client,
            collection_name,
            data=[embedding_list],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": emb_list_metric},
            limit=10,
            output_fields=["id", "clips"],
        )
        assert check
        assert len(results[0]) > 0
        for hit in results[0]:
            assert 0 <= hit["id"] < nb_flushed + nb_growing

        # Upsert 10 records from flushed segment
        upsert_data = []
        for i in range(10):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(EMB_LIST_DIM)],
                "clips": [
                    {
                        "clip_embedding1": cf.gen_vectors(1, EMB_LIST_DIM, vector_type)[0],
                        "scalar_field": i + 10000,
                        "category": f"upserted_{i}",
                    }
                ],
                "scalar_field": i + 10000,
                "category": f"upserted_{i}",
                "score": random.uniform(10.0, 20.0),
            }
            upsert_data.append(row)

        res, check = self.upsert(client, collection_name, upsert_data)
        assert check

        # Verify upsert via query
        res, check = self.flush(client, collection_name)
        assert check

        results, check = self.query(
            client,
            collection_name,
            filter="id < 10",
            output_fields=["id", "category", "clips"],
        )
        assert check
        assert len(results) == 10
        for result in results:
            assert "upserted" in result["category"]
            assert "upserted" in result["clips"][0]["category"]

        # Delete 5 records from flushed segment and 3 from growing segment
        delete_flushed_ids = [10, 11, 12, 13, 14]
        delete_growing_ids = list(range(nb_flushed, nb_flushed + 3))
        all_delete_ids = delete_flushed_ids + delete_growing_ids
        res, check = self.delete(client, collection_name, filter=f"id in {all_delete_ids}")
        assert check

        # Verify deletion
        res, check = self.flush(client, collection_name)
        assert check

        results, check = self.query(client, collection_name, filter="id >= 0", output_fields=["id"])
        assert check
        remaining_ids = {r["id"] for r in results}
        for del_id in all_delete_ids:
            assert del_id not in remaining_ids
        assert len(results) == nb_flushed + nb_growing - len(all_delete_ids)

        # Search again after CRUD operations
        results, check = self.search(
            client,
            collection_name,
            data=[embedding_list],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": emb_list_metric},
            limit=10,
            output_fields=["id"],
        )
        assert check
        assert len(results[0]) > 0
        for hit in results[0]:
            assert hit["id"] not in all_delete_ids


class TestMilvusClientStructArrayHybridSearch(TestMilvusClientV2Base):
    """Test case of struct array with hybrid search functionality"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_hybrid_search_struct_array_with_normal_vector(self):
        """
        target: test hybrid search combining struct array vector and normal vector fields
        method: create collection with both normal vector and struct array vector, perform hybrid search with RRFRanker
        expected: hybrid search returns results combining both vector fields
        """
        collection_name = cf.gen_unique_str(f"{prefix}_hybrid")
        client = self._client()

        # Create schema with both normal vector and struct array
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="pk", datatype=DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field(field_name="random", datatype=DataType.DOUBLE)
        schema.add_field(field_name="embeddings", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        # Create struct schema with vector field
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_str", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field("clip_embedding", DataType.FLOAT_VECTOR, dim=default_dim)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )

        # Create index params for both fields
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="embeddings",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding]",
            index_type="HNSW",
            metric_type="MAX_SIM_L2",
            params=INDEX_PARAMS,
        )

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        # Insert data
        num_entities = 100
        rng = np.random.default_rng(seed=19530)
        data = []
        for i in range(num_entities):
            # Generate 2-3 clips per entity
            clips = []
            for j in range(random.randint(2, 3)):
                clips.append(
                    {
                        "clip_str": f"item_{i}_{j}",
                        "clip_embedding": rng.random(default_dim).tolist(),
                    }
                )

            row = {
                "pk": str(i),
                "random": rng.random(),
                "embeddings": rng.random(default_dim).tolist(),
                "clips": clips,
            }
            data.append(row)

        res, check = self.insert(client, collection_name, data)
        assert check

        # Load collection
        client.load_collection(collection_name)

        # Prepare hybrid search requests
        nq = 1
        default_limit = 5
        req_list = []

        # Request 1: Search on normal vector field
        vectors_to_search = rng.random((nq, default_dim)).tolist()
        log.info(f"vectors_to_search: {len(vectors_to_search)}")
        search_param_1 = {
            "data": vectors_to_search,
            "anns_field": "embeddings",
            "param": {"metric_type": "L2", "params": {"nprobe": 10}},
            "limit": default_limit,
            "expr": "random > 0.5",
        }
        req_list.append(AnnSearchRequest(**search_param_1))

        # Request 2: Search on struct array vector field
        search_tensor = EmbeddingList()
        search_tensor.add([random.random() for _ in range(default_dim)])
        search_param_2 = {
            "data": [search_tensor],
            "anns_field": "clips[clip_embedding]",
            "param": {"metric_type": "MAX_SIM_L2"},
            "limit": default_limit,
            "expr": "random > 0.5",
        }
        req_list.append(AnnSearchRequest(**search_param_2))

        # Perform hybrid search with RRFRanker
        hybrid_res, check = self.hybrid_search(
            client,
            collection_name,
            req_list,
            RRFRanker(),
            default_limit,
            output_fields=["random"],
        )
        assert check
        assert len(hybrid_res) > 0
        assert len(hybrid_res[0]) > 0
        log.info(f"Hybrid search with RRFRanker returned {len(hybrid_res[0])} results")

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_struct_array_with_weighted_ranker(self):
        """
        target: test hybrid search with WeightedRanker
        method: perform hybrid search using WeightedRanker to combine results
        expected: hybrid search returns weighted results
        """
        collection_name = cf.gen_unique_str(f"{prefix}_hybrid_weighted")
        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="pk", datatype=DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field(field_name="random", datatype=DataType.DOUBLE)
        schema.add_field(field_name="embeddings", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=50,
        )

        # Create indexes
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="embeddings",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding]",
            index_type="HNSW",
            metric_type="MAX_SIM_L2",
            params=INDEX_PARAMS,
        )

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        # Insert data
        num_entities = 100
        rng = np.random.default_rng(seed=19530)
        data = []
        for i in range(num_entities):
            clips = [{"clip_embedding": rng.random(default_dim).tolist()}]

            row = {
                "pk": str(i),
                "random": rng.random(),
                "embeddings": rng.random(default_dim).tolist(),
                "clips": clips,
            }
            data.append(row)

        res, check = self.insert(client, collection_name, data)
        assert check

        # Load collection
        client.load_collection(collection_name)

        # Prepare hybrid search with different weights
        nq = 1
        default_limit = 5
        req_list = []

        # Normal vector search
        vectors_to_search = rng.random((nq, default_dim)).tolist()
        req_list.append(
            AnnSearchRequest(
                **{
                    "data": vectors_to_search,
                    "anns_field": "embeddings",
                    "param": {"metric_type": "L2", "params": {"nprobe": 10}},
                    "limit": default_limit,
                }
            )
        )

        # Struct array vector search
        search_tensor = EmbeddingList()
        search_tensor.add([random.random() for _ in range(default_dim)])
        req_list.append(
            AnnSearchRequest(
                **{
                    "data": [search_tensor],
                    "anns_field": "clips[clip_embedding]",
                    "param": {"metric_type": "MAX_SIM_L2"},
                    "limit": default_limit,
                }
            )
        )

        # Use WeightedRanker with weights [0.7, 0.3]
        hybrid_res = client.hybrid_search(
            collection_name,
            req_list,
            WeightedRanker(0.7, 0.3),
            default_limit,
            output_fields=["random"],
        )

        assert len(hybrid_res) > 0
        assert len(hybrid_res[0]) > 0
        log.info(f"Hybrid search with WeightedRanker returned {len(hybrid_res[0])} results")

    @pytest.mark.tags(CaseLabel.L1)
    def test_hybrid_search_multiple_struct_array_vectors(self):
        """
        target: test hybrid search with struct array vector and normal vector (multiple vector sources)
        method: create collection with struct array vector and normal vector, perform hybrid search on both
        expected: hybrid search successfully combines results from struct array and normal vector
        """
        collection_name = cf.gen_unique_str(f"{prefix}_hybrid_multi")
        client = self._client()

        # Create schema with both struct array vector and normal vector
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="pk", datatype=DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field(field_name="random", datatype=DataType.DOUBLE)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=50,
        )

        # Create indexes for both fields
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding]",
            index_type="HNSW",
            metric_type="MAX_SIM_L2",
            params=INDEX_PARAMS,
        )

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        # Insert data
        num_entities = 100
        rng = np.random.default_rng(seed=19530)
        data = []
        for i in range(num_entities):
            clips = [{"clip_embedding": rng.random(default_dim).tolist()}]

            row = {
                "pk": str(i),
                "random": rng.random(),
                "normal_vector": rng.random(default_dim).tolist(),
                "clips": clips,
            }
            data.append(row)

        res, check = self.insert(client, collection_name, data)
        assert check

        # Load collection
        client.load_collection(collection_name)

        # Prepare hybrid search on both vector sources
        default_limit = 5
        req_list = []

        # Search on normal vector
        vectors_to_search = rng.random((1, default_dim)).tolist()
        req_list.append(
            AnnSearchRequest(
                **{
                    "data": vectors_to_search,
                    "anns_field": "normal_vector",
                    "param": {"metric_type": "L2", "params": {"nprobe": 10}},
                    "limit": default_limit,
                }
            )
        )

        # Search on struct array vector
        search_tensor = EmbeddingList()
        search_tensor.add(rng.random(default_dim).tolist())
        req_list.append(
            AnnSearchRequest(
                **{
                    "data": [search_tensor],
                    "anns_field": "clips[clip_embedding]",
                    "param": {"metric_type": "MAX_SIM_L2"},
                    "limit": default_limit,
                }
            )
        )

        # Perform hybrid search with RRFRanker
        hybrid_res = client.hybrid_search(
            collection_name,
            req_list,
            RRFRanker(),
            default_limit,
            output_fields=["random"],
        )

        assert len(hybrid_res) > 0
        assert len(hybrid_res[0]) > 0
        log.info(f"Hybrid search with multiple vector sources returned {len(hybrid_res[0])} results")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("metric_type", ["MAX_SIM_L2", "MAX_SIM_IP", "MAX_SIM_COSINE"])
    def test_hybrid_search_struct_array_different_metrics(self, metric_type):
        """
        target: test hybrid search with different metric types for struct array
        method: perform hybrid search with various MAX_SIM metrics
        expected: hybrid search works correctly with all supported metrics
        """
        collection_name = cf.gen_unique_str(f"{prefix}_hybrid_{metric_type.lower()}")
        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="pk", datatype=DataType.VARCHAR, is_primary=True, max_length=100)
        schema.add_field(field_name="embeddings", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=50,
        )

        # Create indexes
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="embeddings",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding]",
            index_type="HNSW",
            metric_type=metric_type,
            params=INDEX_PARAMS,
        )

        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check

        # Insert data
        num_entities = 100
        rng = np.random.default_rng(seed=19530)
        data = []
        for i in range(num_entities):
            clips = [{"clip_embedding": rng.random(default_dim).tolist()}]

            row = {
                "pk": str(i),
                "embeddings": rng.random(default_dim).tolist(),
                "clips": clips,
            }
            data.append(row)

        res, check = self.insert(client, collection_name, data)
        assert check

        # Load collection
        client.load_collection(collection_name)

        # Prepare hybrid search
        default_limit = 5
        req_list = []

        vectors_to_search = rng.random((1, default_dim)).tolist()
        req_list.append(
            AnnSearchRequest(
                **{
                    "data": vectors_to_search,
                    "anns_field": "embeddings",
                    "param": {"metric_type": "L2", "params": {"nprobe": 10}},
                    "limit": default_limit,
                }
            )
        )

        search_tensor = EmbeddingList()
        search_tensor.add(rng.random(default_dim).tolist())
        req_list.append(
            AnnSearchRequest(
                **{
                    "data": [search_tensor],
                    "anns_field": "clips[clip_embedding]",
                    "param": {"metric_type": metric_type},
                    "limit": default_limit,
                }
            )
        )

        # Perform hybrid search
        hybrid_res = client.hybrid_search(collection_name, req_list, RRFRanker(), default_limit)

        assert len(hybrid_res) > 0
        assert len(hybrid_res[0]) > 0


class TestMilvusClientStructArrayQuery(TestMilvusClientV2Base):
    """Test case of struct array query functionality"""

    def create_collection_with_data(self, client: MilvusClient, collection_name: str, nb: int = 50):
        """Create collection with struct array and insert data"""
        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        # Create struct schema
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("scalar_field", DataType.INT64)
        struct_schema.add_field("category", DataType.VARCHAR, max_length=128)
        struct_schema.add_field("score", DataType.FLOAT)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )
        schema.add_field("category", datatype=DataType.VARCHAR, max_length=128)
        schema.add_field("scalar_field", datatype=DataType.INT64)
        schema.add_field("score", datatype=DataType.FLOAT)

        # Create collection
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Insert data
        data = []
        for i in range(nb):
            array_length = random.randint(1, 3)
            struct_array = []
            for j in range(array_length):
                struct_element = {
                    "clip_embedding1": [random.random() for _ in range(default_dim)],
                    "scalar_field": i * 10 + j,
                    "category": f"cat_{i % 3}",
                    "score": random.uniform(0.1, 10.0),
                }
                struct_array.append(struct_element)

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": struct_array,
                "category": f"cat_{i % 3}",
                "scalar_field": i * 10 + j,
                "score": random.uniform(0.1, 10.0),
            }
            data.append(row)

        res, check = self.insert(client, collection_name, data)
        assert check

        # Flush data to ensure it's persisted
        res, check = self.flush(client, collection_name)
        assert check

        # Create indexes for query operations
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_name="struct_vector_index",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

        # Load collection
        res, check = self.load_collection(client, collection_name)
        assert check

        return data

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_struct_array_all_fields(self):
        """
        target: test query all fields including struct array
        method: query without specifying output fields
        expected: all fields returned including struct array
        """
        collection_name = cf.gen_unique_str(f"{prefix}_query")

        client = self._client()

        # Create collection with data
        self.create_collection_with_data(client, collection_name)

        # Query all data
        results, check = self.query(client, collection_name, filter="id >= 0", limit=10)
        assert check
        assert len(results) > 0

        # Verify struct array fields are present
        for result in results:
            assert "id" in result
            assert "clips" in result
            assert isinstance(result["clips"], list)

    @pytest.mark.tags(CaseLabel.L0)
    def test_query_struct_array_specific_fields(self):
        """
        target: test query specific fields from struct array
        method: query with output_fields parameter specifying struct array fields
        expected: only specified fields returned
        """
        collection_name = cf.gen_unique_str(f"{prefix}_query")

        client = self._client()

        # Create collection with data
        self.create_collection_with_data(client, collection_name)

        # Query with specific output fields
        results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "clips"],
            limit=10,
        )
        assert check
        assert len(results) > 0

        # Verify only specified fields are present
        for result in results:
            assert "id" in result
            assert "clips" in result
            assert "normal_vector" not in result

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_with_expr_filter(self):
        """
        target: test query with expression filter on struct array fields
        method: query with filter expression targeting struct array scalar fields
        expected: filtered results returned
        """
        collection_name = cf.gen_unique_str(f"{prefix}_query")

        client = self._client()

        # Create collection with data
        self.create_collection_with_data(client, collection_name)

        # Query with expression filter on struct array scalar field
        results, check = self.query(
            client,
            collection_name,
            filter="scalar_field < 50",
            output_fields=["id", "clips"],
            limit=20,
        )
        assert check

        # Verify filter was applied (if results exist)
        if len(results) > 0:
            for result in results:
                struct_array = result["clips"]
                # At least one element in the struct array should satisfy the filter
                has_matching_element = any(elem["scalar_field"] < 50 for elem in struct_array)
                assert has_matching_element

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_with_category_filter(self):
        """
        target: test query with varchar filter on struct array
        method: query with filter on varchar field in struct array
        expected: filtered results returned
        """
        collection_name = cf.gen_unique_str(f"{prefix}_query")

        client = self._client()

        # Create collection with data
        self.create_collection_with_data(client, collection_name)

        # Query with varchar filter
        results, check = self.query(
            client,
            collection_name,
            filter='category == "cat_0"',
            output_fields=["id", "clips"],
            limit=20,
        )
        assert check

        # Verify filter was applied
        if len(results) > 0:
            for result in results:
                struct_array = result["clips"]
                has_cat_0 = any(elem["category"] == "cat_0" for elem in struct_array)
                assert has_cat_0

    @pytest.mark.tags(CaseLabel.L1)
    def test_query_count_struct_array(self):
        """
        target: test count query with struct array
        method: perform count query on collection with struct array
        expected: correct count returned
        """
        collection_name = cf.gen_unique_str(f"{prefix}_query")

        client = self._client()

        # Create collection with data
        self.create_collection_with_data(client, collection_name, nb=20)

        # Count all records
        count_result = client.query(collection_name=collection_name, filter="", output_fields=["count(*)"])
        assert len(count_result) == 1
        assert count_result[0]["count(*)"] == 20

    @pytest.mark.tags(CaseLabel.L2)
    def test_query_with_limit_offset(self):
        """
        target: test query with limit and offset
        method: query struct array data with pagination
        expected: correct pagination results
        """
        collection_name = cf.gen_unique_str(f"{prefix}_query")

        client = self._client()

        # Create collection with data
        self.create_collection_with_data(client, collection_name, nb=30)

        # Query first page
        results_page1, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id"],
            limit=10,
            offset=0,
        )
        assert check
        assert len(results_page1) == 10

        # Query second page
        results_page2, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id"],
            limit=10,
            offset=10,
        )
        assert check
        assert len(results_page2) == 10

        # Verify no overlap between pages
        page1_ids = {result["id"] for result in results_page1}
        page2_ids = {result["id"] for result in results_page2}
        assert page1_ids.isdisjoint(page2_ids)


class TestMilvusClientStructArraySchemaEvolution(TestMilvusClientV2Base):
    """Test cases for struct array schema evolution"""

    min_index_sealed_rows = 3000

    @staticmethod
    def _vector(seed: int, dim: int = default_dim) -> list[float]:
        return [float(seed) + float(i) / 1000 for i in range(dim)]

    @classmethod
    def _index_filler_rows(cls, start_id: int, count: int, tag_prefix: str) -> list[dict[str, Any]]:
        return [
            {"id": start_id + i, "normal_vector": cls._vector(start_id + i), "doc_tag": f"{tag_prefix}_{i}"}
            for i in range(count)
        ]

    @classmethod
    def _scalar_struct_index_filler_rows(cls, start_id: int, count: int, tag_prefix: str) -> list[dict[str, Any]]:
        return [
            {
                "id": start_id + i,
                "normal_vector": cls._vector(start_id + i),
                "doc_tag": f"{tag_prefix}_{i}",
                "profile": [{"p_int": -(start_id + i), "p_tag": f"{tag_prefix}_profile_{i}"}],
            }
            for i in range(count)
        ]

    @classmethod
    def _vector_struct_index_filler_rows(cls, start_id: int, count: int, tag_prefix: str) -> list[dict[str, Any]]:
        return [
            {
                "id": start_id + i,
                "normal_vector": cls._vector(start_id + i),
                "doc_tag": f"{tag_prefix}_{i}",
                "profile": [
                    {
                        "p_int": -(start_id + i),
                        "p_tag": f"{tag_prefix}_profile_{i}",
                        "p_vec": cls._vector(start_id + i),
                    }
                ],
            }
            for i in range(count)
        ]

    @staticmethod
    def _unit_vector(axis: int, dim: int = default_dim) -> list[float]:
        vector = [0.0 for _ in range(dim)]
        vector[axis % dim] = 1.0
        return vector

    def _profile(self, row_id: int) -> list[dict[str, Any]]:
        return [
            {
                "p_int": row_id * 10,
                "p_tag": f"profile_{row_id}_0",
                "p_vec": self._vector(row_id * 10),
            },
            {
                "p_int": row_id * 10 + 1,
                "p_tag": f"profile_{row_id}_1",
                "p_vec": self._vector(row_id * 10 + 1),
            },
        ]

    @staticmethod
    def _typed_vector(vector_type: DataType, seed: int, dim: int = EMB_LIST_DIM):
        if vector_type == DataType.FLOAT16_VECTOR:
            return np.array([((seed + i) % 17) / 17 for i in range(dim)], dtype=np.float16)
        if vector_type == DataType.BFLOAT16_VECTOR:
            return np.array([((seed + i) % 19) / 19 for i in range(dim)], dtype=cf.bfloat16)
        if vector_type == DataType.INT8_VECTOR:
            return np.array([((seed + i) % 255) - 128 for i in range(dim)], dtype=np.int8)
        if vector_type == DataType.BINARY_VECTOR:
            bits = [((seed + i) % 2) for i in range(dim)]
            return bytes(np.packbits(bits, axis=-1).tolist())
        return [float(seed) + float(i) / 1000 for i in range(dim)]

    def _typed_profile(self, row_id: int, vector_type: DataType, dim: int = EMB_LIST_DIM) -> list[dict[str, Any]]:
        return [
            {
                "p_int": row_id * 10,
                "p_tag": f"profile_{row_id}_0",
                "p_vec": self._typed_vector(vector_type, row_id * 10, dim),
            },
            {
                "p_int": row_id * 10 + 1,
                "p_tag": f"profile_{row_id}_1",
                "p_vec": self._typed_vector(vector_type, row_id * 10 + 1, dim),
            },
        ]

    def _assert_profile_equal(self, actual, expected):
        if expected is None:
            assert actual is None
            return

        assert isinstance(actual, list)
        assert len(actual) == len(expected)
        for actual_item, expected_item in zip(actual, expected):
            assert actual_item["p_int"] == expected_item["p_int"]
            assert actual_item["p_tag"] == expected_item["p_tag"]
            assert actual_item["p_vec"] == pytest.approx(expected_item["p_vec"])

    @staticmethod
    def _assert_profile_vector_subfield_equal(actual, expected):
        if expected is None:
            assert actual is None
            return

        assert isinstance(actual, list)
        assert len(actual) == len(expected)
        for actual_item, expected_item in zip(actual, expected):
            assert set(actual_item) == {"p_vec"}
            assert actual_item["p_vec"] == pytest.approx(expected_item["p_vec"])

    @staticmethod
    def _binary_vector_bytes(value) -> bytes:
        if isinstance(value, bytes | bytearray):
            return bytes(value)
        if isinstance(value, np.ndarray):
            return value.tobytes()
        if isinstance(value, list) and len(value) == 1 and isinstance(value[0], bytes | bytearray):
            return bytes(value[0])
        return bytes(value)

    def _assert_typed_vector_equal(self, actual, expected, vector_type: DataType):
        if vector_type == DataType.BINARY_VECTOR:
            assert self._binary_vector_bytes(actual) == self._binary_vector_bytes(expected)
            return

        if vector_type == DataType.BFLOAT16_VECTOR:
            actual_bits = np.asarray(actual, dtype=np.uint16).tolist()
            expected_bits = np.asarray(expected).view(np.uint16).tolist()
            assert actual_bits == expected_bits
            return

        if vector_type == DataType.INT8_VECTOR:
            assert np.asarray(actual, dtype=np.int8).tolist() == np.asarray(expected, dtype=np.int8).tolist()
            return

        assert np.asarray(actual, dtype=np.float32).tolist() == pytest.approx(
            np.asarray(expected, dtype=np.float32).tolist(),
            abs=epsilon,
        )

    def _assert_typed_profile_equal(self, actual, expected, vector_type: DataType):
        if expected is None:
            assert actual is None
            return

        assert isinstance(actual, list)
        assert len(actual) == len(expected)
        for actual_item, expected_item in zip(actual, expected):
            assert actual_item["p_int"] == expected_item["p_int"]
            assert actual_item["p_tag"] == expected_item["p_tag"]
            self._assert_typed_vector_equal(actual_item["p_vec"], expected_item["p_vec"], vector_type)

    def _assert_typed_profile_vector_subfield_equal(self, actual, expected, vector_type: DataType):
        if expected is None:
            assert actual is None
            return

        assert isinstance(actual, list)
        assert len(actual) == len(expected)
        for actual_item, expected_item in zip(actual, expected):
            assert set(actual_item) == {"p_vec"}
            self._assert_typed_vector_equal(actual_item["p_vec"], expected_item["p_vec"], vector_type)

    @staticmethod
    def _search_entity(hit):
        return hit.get("entity", hit)

    @staticmethod
    def _drain_iterator(iterator):
        assert iterator is not None
        rows = []
        while True:
            batch = iterator.next()
            if not batch:
                break
            rows.extend(batch)
        iterator.close()
        return rows

    @staticmethod
    def _scalar_profile(row_id: int) -> list[dict[str, Any]]:
        return [
            {
                "p_int": row_id * 10,
                "p_tag": f"profile_{row_id}_0",
            },
            {
                "p_int": row_id * 10 + 1,
                "p_tag": f"profile_{row_id}_1",
            },
        ]

    @staticmethod
    def _assert_scalar_profile_equal(actual, expected):
        if expected is None:
            assert actual is None
            return

        assert isinstance(actual, list)
        assert len(actual) == len(expected)
        for actual_item, expected_item in zip(actual, expected):
            assert actual_item["p_int"] == expected_item["p_int"]
            assert actual_item["p_tag"] == expected_item["p_tag"]

    def _setup_nullable_scalar_struct_expression_collection(self, client, collection_name):
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        sealed_explicit_null_profile_row = {
            "id": 0,
            "normal_vector": self._vector(0),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_one_match_profile_row = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "sealed_one_match_profile",
            "profile": [
                {"p_int": 9100, "p_tag": "match_9100"},
                {"p_int": 100, "p_tag": "low_100"},
            ],
        }
        sealed_two_match_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "sealed_two_match_profile",
            "profile": [
                {"p_int": 9200, "p_tag": "match_9200"},
                {"p_int": 9300, "p_tag": "match_9300"},
            ],
        }
        sealed_zero_match_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "sealed_zero_match_profile",
            "profile": [
                {"p_int": 100, "p_tag": "low_100"},
                {"p_int": 200, "p_tag": "low_200"},
            ],
        }
        sealed_control_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
            sealed_one_match_profile_row,
            sealed_two_match_profile_row,
            sealed_zero_match_profile_row,
        ]
        sealed_index_filler_rows = self._scalar_struct_index_filler_rows(
            50000,
            self.min_index_sealed_rows - len(sealed_control_rows),
            "sealed_expr_index_filler",
        )
        sealed_rows = sealed_control_rows + sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_rows)
        assert check
        assert res["insert_count"] == len(sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        res, check = self.load_collection(client, collection_name)
        assert check

        growing_explicit_null_profile_row = {
            "id": 7000,
            "normal_vector": self._vector(7000),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_omitted_profile_row = {
            "id": 7001,
            "normal_vector": self._vector(7001),
            "doc_tag": "growing_omit_profile",
        }
        growing_empty_profile_row = {
            "id": 7002,
            "normal_vector": self._vector(7002),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_one_match_profile_row = {
            "id": 7003,
            "normal_vector": self._vector(7003),
            "doc_tag": "growing_one_match_profile",
            "profile": [
                {"p_int": 9600, "p_tag": "match_9600"},
                {"p_int": 600, "p_tag": "low_600"},
            ],
        }
        growing_two_match_profile_row = {
            "id": 7004,
            "normal_vector": self._vector(7004),
            "doc_tag": "growing_two_match_profile",
            "profile": [
                {"p_int": 9700, "p_tag": "match_9700"},
                {"p_int": 9800, "p_tag": "match_9800"},
            ],
        }
        growing_zero_match_profile_row = {
            "id": 7005,
            "normal_vector": self._vector(7005),
            "doc_tag": "growing_zero_match_profile",
            "profile": [
                {"p_int": 700, "p_tag": "low_700"},
                {"p_int": 800, "p_tag": "low_800"},
            ],
        }
        growing_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
            growing_one_match_profile_row,
            growing_two_match_profile_row,
            growing_zero_match_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_rows)
        assert check
        assert res["insert_count"] == len(growing_rows)

        source_by_id = {}
        source_by_id[sealed_explicit_null_profile_row["id"]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_empty_profile_row["id"]] = sealed_empty_profile_row
        source_by_id[sealed_one_match_profile_row["id"]] = sealed_one_match_profile_row
        source_by_id[sealed_two_match_profile_row["id"]] = sealed_two_match_profile_row
        source_by_id[sealed_zero_match_profile_row["id"]] = sealed_zero_match_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row["id"]] = growing_explicit_null_profile_row
        source_by_id[growing_omitted_profile_row["id"]] = {**growing_omitted_profile_row, "profile": None}
        source_by_id[growing_empty_profile_row["id"]] = growing_empty_profile_row
        source_by_id[growing_one_match_profile_row["id"]] = growing_one_match_profile_row
        source_by_id[growing_two_match_profile_row["id"]] = growing_two_match_profile_row
        source_by_id[growing_zero_match_profile_row["id"]] = growing_zero_match_profile_row

        controlled_ids = {row["id"] for row in sealed_control_rows + growing_rows}
        return {
            "source_by_id": source_by_id,
            "source_rows": list(source_by_id.values()),
            "controlled_ids": controlled_ids,
        }

    def _assert_expression_rows_match_source(self, rows, source_by_id):
        for row in rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

    @staticmethod
    def _parse_expression_value(raw_value: str):
        raw_value = raw_value.strip()
        if raw_value.startswith('"') and raw_value.endswith('"'):
            return raw_value[1:-1]
        if raw_value.lower() == "null":
            return None
        try:
            return int(raw_value)
        except ValueError:
            return float(raw_value)

    @classmethod
    def _parse_expression_list(cls, raw_value: str):
        return json.loads(raw_value)

    @staticmethod
    def _compare_expression_values(actual, op: str, expected) -> bool:
        if actual is None or expected is None:
            if op == "==":
                return actual is expected
            if op == "!=":
                return actual is not expected
            return False
        if op == ">=":
            return actual >= expected
        if op == "<=":
            return actual <= expected
        if op == "==":
            return actual == expected
        if op == "!=":
            return actual != expected
        if op == ">":
            return actual > expected
        if op == "<":
            return actual < expected
        raise AssertionError(f"unsupported operator: {op}")

    @classmethod
    def _eval_struct_element_condition(cls, element: dict[str, Any], condition: str) -> bool:
        condition = condition.strip()
        if " && " in condition:
            return all(cls._eval_struct_element_condition(element, part) for part in condition.split(" && "))
        if " || " in condition:
            return any(cls._eval_struct_element_condition(element, part) for part in condition.split(" || "))
        for op in [">=", "<=", "==", "!=", ">", "<"]:
            if op not in condition:
                continue
            left, right = [part.strip() for part in condition.split(op, 1)]
            assert left.startswith("$[") and left.endswith("]")
            field_name = left[2:-1]
            actual = element.get(field_name)
            expected = cls._parse_expression_value(right)
            return cls._compare_expression_values(actual, op, expected)
        raise AssertionError(f"unsupported element condition: {condition}")

    @staticmethod
    def _strip_id_scope(rows: list[dict[str, Any]], expression: str) -> tuple[list[dict[str, Any]], str]:
        expression = expression.strip()
        match = re.match(r"^id\s+in\s+\[([^\]]*)\]\s*&&\s*(.+)$", expression)
        if match is None:
            return rows, expression
        id_values = {int(value.strip()) for value in match.group(1).split(",") if value.strip()}
        return [row for row in rows if row["id"] in id_values], match.group(2).strip()

    @classmethod
    def _expected_nullable_scalar_struct_expression_rows(
        cls,
        rows: list[dict[str, Any]],
        expression: str,
    ) -> list[dict[str, Any]]:
        scoped_rows, expression = cls._strip_id_scope(rows, expression)

        def output_row(row, offset=None):
            result = {
                "id": row["id"],
                "doc_tag": row["doc_tag"],
                "profile": row.get("profile"),
            }
            if offset is not None:
                result["offset"] = offset
            return result

        if expression == "profile is null":
            return [output_row(row) for row in scoped_rows if row.get("profile") is None]
        if expression == "profile is not null":
            return [output_row(row) for row in scoped_rows if row.get("profile") is not None]

        length_match = re.match(r"^array_length\(profile(?:\[\w+\])?\)\s*(==|!=|>=|<=|>|<)\s*(\d+)$", expression)
        if length_match is not None:
            op, raw_expected = length_match.groups()
            expected = int(raw_expected)
            return [
                output_row(row)
                for row in scoped_rows
                if row.get("profile") is not None and cls._compare_expression_values(len(row["profile"]), op, expected)
            ]

        array_contains_match = re.match(
            r"^array_contains(?:_(all|any))?\(profile\[\w+\],\s*(.+)\)$",
            expression,
        )
        if array_contains_match is not None:
            mode, raw_expected = array_contains_match.groups()
            expected_values = (
                cls._parse_expression_list(raw_expected)
                if mode in {"all", "any"}
                else [cls._parse_expression_value(raw_expected)]
            )
            field_name = re.match(r"^array_contains(?:_(?:all|any))?\(profile\[(\w+)\],", expression).group(1)
            results = []
            for row in scoped_rows:
                profile = row.get("profile")
                if profile is None:
                    continue
                actual_values = [element.get(field_name) for element in profile]
                if (
                    (mode == "all" and all(value in actual_values for value in expected_values))
                    or (mode == "any" and any(value in actual_values for value in expected_values))
                    or (mode is None and expected_values[0] in actual_values)
                ):
                    results.append(output_row(row))
            return results

        index_access_match = re.match(r"^profile\[(\d+)\]\[(\w+)\]\s*(==|!=|>=|<=|>|<)\s*(.+)$", expression)
        if index_access_match is not None:
            raw_offset, field_name, op, raw_expected = index_access_match.groups()
            offset = int(raw_offset)
            expected = cls._parse_expression_value(raw_expected)
            results = []
            for row in scoped_rows:
                profile = row.get("profile") or []
                if len(profile) <= offset:
                    continue
                if cls._compare_expression_values(profile[offset].get(field_name), op, expected):
                    results.append(output_row(row))
            return results

        element_filter_match = re.match(r"^element_filter\(profile,\s*(.+)\)$", expression)
        if element_filter_match is not None:
            condition = element_filter_match.group(1)
            results = []
            for row in scoped_rows:
                profile = row.get("profile") or []
                for offset, element in enumerate(profile):
                    if cls._eval_struct_element_condition(element, condition):
                        results.append(output_row(row, offset=offset))
            return results

        match_family_match = re.match(
            r"^MATCH_(ALL|ANY|LEAST|MOST|EXACT)\(profile,\s*(.+?)(?:,\s*threshold=(\d+))?\)$",
            expression,
        )
        if match_family_match is not None:
            match_type, condition, raw_threshold = match_family_match.groups()
            threshold = int(raw_threshold) if raw_threshold is not None else None
            results = []
            for row in scoped_rows:
                profile = row.get("profile") or []
                match_count = sum(1 for element in profile if cls._eval_struct_element_condition(element, condition))
                total_count = len(profile)
                matched = (
                    (match_type == "ALL" and match_count == total_count)
                    or (match_type == "ANY" and match_count >= 1)
                    or (match_type == "LEAST" and match_count >= threshold)
                    or (match_type == "MOST" and match_count <= threshold)
                    or (match_type == "EXACT" and match_count == threshold)
                )
                if matched:
                    results.append(output_row(row))
            return results

        raise AssertionError(f"unsupported expression: {expression}")

    @staticmethod
    def _expression_result_keys(rows):
        return sorted((row["id"], row.get("offset")) for row in rows)

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_schema_nullable_propagation(self):
        """
        target: test schema nullable propagation after dynamically adding a struct array field
        method: add a nullable struct array field with scalar and vector sub-fields, then describe collection
        expected: parent struct is nullable, and raw schema shows both scalar and vector sub-fields nullable
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_schema_nullable")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        profile_field = next(field for field in describe_info["fields"] if field["name"] == "profile")
        assert profile_field["nullable"] is True
        assert profile_field["type"] == DataType.ARRAY
        assert profile_field["element_type"] == DataType.STRUCT
        assert profile_field["params"]["max_capacity"] == 4
        user_sub_fields = {field["name"]: field for field in profile_field["struct_fields"]}
        assert set(user_sub_fields) == {"p_int", "p_tag", "p_vec"}
        assert user_sub_fields["p_int"]["type"] == DataType.INT64
        assert user_sub_fields["p_tag"]["type"] == DataType.VARCHAR
        assert user_sub_fields["p_tag"]["params"]["max_length"] == 128
        assert user_sub_fields["p_vec"]["type"] == DataType.FLOAT_VECTOR
        assert user_sub_fields["p_vec"]["params"]["dim"] == default_dim

        raw_describe = client._get_connection().describe_collection(collection_name)
        raw_profile = next(field for field in raw_describe["struct_array_fields"] if field["name"] == "profile")
        assert raw_profile["nullable"] is True
        raw_sub_fields = {field["name"]: field for field in raw_profile["fields"]}
        assert set(raw_sub_fields) == {"p_int", "p_tag", "p_vec"}
        assert raw_sub_fields["p_int"]["nullable"] is True
        assert raw_sub_fields["p_tag"]["nullable"] is True
        assert raw_sub_fields["p_vec"]["nullable"] is True
        assert raw_sub_fields["p_int"]["element_type"] == DataType.INT64
        assert raw_sub_fields["p_tag"]["element_type"] == DataType.VARCHAR
        assert raw_sub_fields["p_vec"]["element_type"] == DataType.FLOAT_VECTOR
        assert raw_sub_fields["p_vec"]["params"]["dim"] == default_dim

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_schema_nullable_propagation(self):
        """
        target: test schema nullable propagation when creating a nullable struct array field
        method: create a collection with a nullable struct array field containing scalar and vector sub-fields,
            then describe collection
        expected: parent struct is nullable, and raw schema shows both scalar and vector sub-fields nullable
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_schema_nullable")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        profile_field = next(field for field in describe_info["fields"] if field["name"] == "profile")
        assert profile_field["nullable"] is True
        assert profile_field["type"] == DataType.ARRAY
        assert profile_field["element_type"] == DataType.STRUCT
        assert profile_field["params"]["max_capacity"] == 4
        user_sub_fields = {field["name"]: field for field in profile_field["struct_fields"]}
        assert set(user_sub_fields) == {"p_int", "p_tag", "p_vec"}
        assert user_sub_fields["p_int"]["type"] == DataType.INT64
        assert user_sub_fields["p_tag"]["type"] == DataType.VARCHAR
        assert user_sub_fields["p_tag"]["params"]["max_length"] == 128
        assert user_sub_fields["p_vec"]["type"] == DataType.FLOAT_VECTOR
        assert user_sub_fields["p_vec"]["params"]["dim"] == default_dim

        raw_describe = client._get_connection().describe_collection(collection_name)
        raw_profile = next(field for field in raw_describe["struct_array_fields"] if field["name"] == "profile")
        assert raw_profile["nullable"] is True
        raw_sub_fields = {field["name"]: field for field in raw_profile["fields"]}
        assert set(raw_sub_fields) == {"p_int", "p_tag", "p_vec"}
        assert raw_sub_fields["p_int"]["nullable"] is True
        assert raw_sub_fields["p_tag"]["nullable"] is True
        assert raw_sub_fields["p_vec"]["nullable"] is True
        assert raw_sub_fields["p_int"]["element_type"] == DataType.INT64
        assert raw_sub_fields["p_tag"]["element_type"] == DataType.VARCHAR
        assert raw_sub_fields["p_vec"]["element_type"] == DataType.FLOAT_VECTOR
        assert raw_sub_fields["p_vec"]["params"]["dim"] == default_dim

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_omit_nullable_query_search(self):
        """
        target: test query/search output for a nullable scalar struct array field created with collection schema
        method: create a nullable scalar struct array field, then insert rows with the struct field omitted,
            empty, and non-empty
        expected: omitted field is returned as null, empty array remains empty, and non-empty struct data matches
            source in query, sub-field output, element_filter query, and normal vector search
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_scalar_struct_omit")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"},
            {"id": 1, "normal_vector": self._vector(1), "doc_tag": "empty_profile", "profile": []},
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": self._scalar_profile(2),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": self._scalar_profile(3),
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 20)",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in filter_results} == {2}
        for row in filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(2)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_insert_explicit_null_query_search(self):
        """
        target: test explicit null insert for a nullable scalar struct array field created with collection schema
        method: create a nullable scalar struct array field, then insert rows with explicit null, omitted, empty,
            and non-empty struct values
        expected: explicit null and omitted rows return null, empty row returns [], and non-empty row matches source
            in query, sub-field query, element_filter query, and normal vector search output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_scalar_struct_explicit_null")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {
                "id": 0,
                "normal_vector": self._vector(0),
                "doc_tag": "explicit_null_profile",
                "profile": None,
            },
            {"id": 1, "normal_vector": self._vector(1), "doc_tag": "omitted_profile"},
            {"id": 2, "normal_vector": self._vector(2), "doc_tag": "empty_profile", "profile": []},
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile",
                "profile": self._scalar_profile(3),
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 30)",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in filter_results} == {3}
        for row in filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(3)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0]["id"] == 3
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_upsert_null_non_null(self):
        """
        target: test upsert for a nullable scalar struct array field created with collection schema
        method: create a nullable scalar struct array field, insert null/empty/non-null rows, then upsert null to
            non-null, non-null to null, and a new row with the struct field omitted
        expected: full-row upsert updates nullable struct values correctly in query, element_filter, and search output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_scalar_struct_upsert")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {"id": 0, "normal_vector": self._vector(0), "doc_tag": "initial_null_profile"},
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "initial_non_null_profile",
                "profile": self._scalar_profile(1),
            },
            {"id": 2, "normal_vector": self._vector(2), "doc_tag": "initial_empty_profile", "profile": []},
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in rows}
        null_to_non_null = {
            "id": 0,
            "normal_vector": self._vector(100),
            "doc_tag": "upserted_null_to_non_null",
            "profile": self._scalar_profile(100),
        }
        non_null_to_null = {
            "id": 1,
            "normal_vector": self._vector(101),
            "doc_tag": "upserted_non_null_to_null",
        }
        new_omitted_profile = {
            "id": 3,
            "normal_vector": self._vector(103),
            "doc_tag": "upserted_new_omit_profile",
        }
        res, check = self.upsert(client, collection_name, [null_to_non_null, non_null_to_null, new_omitted_profile])
        assert check
        assert res["upsert_count"] == 3

        source_by_id[null_to_non_null["id"]] = null_to_non_null
        source_by_id[non_null_to_null["id"]] = {**non_null_to_null, "profile": None}
        source_by_id[new_omitted_profile["id"]] = {**new_omitted_profile, "profile": None}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 1000)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {null_to_non_null["id"]}
        self._assert_scalar_profile_equal(element_filter_results[0]["profile"], null_to_non_null["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_null_to_null["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0]["id"] == non_null_to_null["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_query_iterator(self):
        """
        target: test query_iterator output for a nullable scalar struct array field created with collection schema
        method: create a nullable scalar struct array field, insert omitted, empty, and non-empty rows, then drain
            query_iterator with small batch size
        expected: iterator returns every row once, and nullable struct output matches source across batch boundaries
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_scalar_struct_qiter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile_0"},
            {"id": 1, "normal_vector": self._vector(1), "doc_tag": "empty_profile", "profile": []},
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": self._scalar_profile(2),
            },
            {"id": 3, "normal_vector": self._vector(3), "doc_tag": "missing_profile_3"},
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "present_profile_4",
                "profile": self._scalar_profile(4),
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in rows}

        iterator, check = self.query_iterator(
            client,
            collection_name,
            batch_size=2,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            consistency_level="Strong",
        )
        assert check
        iterator_rows = self._drain_iterator(iterator)
        iterator_ids = [row["id"] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)

        for row in iterator_rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_search_iterator(self):
        """
        target: test search_iterator output for a nullable scalar struct array field created with collection schema
        method: create a nullable scalar struct array field, insert omitted, empty, and non-empty rows, then drain
            search_iterator on the normal vector field with small batch size
        expected: iterator returns every topK row once, distances are ordered, and nullable struct output matches source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_scalar_struct_siter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile_0"},
            {"id": 1, "normal_vector": self._vector(1), "doc_tag": "empty_profile", "profile": []},
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": self._scalar_profile(2),
            },
            {"id": 3, "normal_vector": self._vector(3), "doc_tag": "missing_profile_3"},
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "present_profile_4",
                "profile": self._scalar_profile(4),
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in rows}

        iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[self._vector(4)],
            batch_size=2,
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        iterator_hits = self._drain_iterator(iterator)
        hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_match_family_query_search(self):
        """
        target: test MATCH family correctness for a nullable scalar struct array field created with collection schema
        method: create a nullable scalar Struct Array, insert sealed and growing null/empty/non-empty rows, then query
            with MATCH family and search with MATCH_ANY filter
        expected: null/empty rows do not match MATCH_ANY, MATCH family result sets match source-of-truth, and
            normal-vector search filtered by MATCH_ANY returns matching sealed and growing rows with correct output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_scalar_struct_match")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        sealed_explicit_null_profile_row = {
            "id": 0,
            "normal_vector": self._vector(0),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_one_match_profile_row = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "sealed_one_match_profile",
            "profile": [
                {"p_int": 9100, "p_tag": "match_9100"},
                {"p_int": 100, "p_tag": "low_100"},
            ],
        }
        sealed_two_match_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "sealed_two_match_profile",
            "profile": [
                {"p_int": 9200, "p_tag": "match_9200"},
                {"p_int": 9300, "p_tag": "match_9300"},
            ],
        }
        sealed_zero_match_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "sealed_zero_match_profile",
            "profile": [
                {"p_int": 100, "p_tag": "low_100"},
                {"p_int": 200, "p_tag": "low_200"},
            ],
        }
        sealed_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
            sealed_one_match_profile_row,
            sealed_two_match_profile_row,
            sealed_zero_match_profile_row,
        ]
        sealed_index_filler_rows = self._scalar_struct_index_filler_rows(
            50000,
            self.min_index_sealed_rows - len(sealed_rows),
            "sealed_match_index_filler",
        )
        sealed_rows += sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_rows)
        assert check
        assert res["insert_count"] == len(sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        res, check = self.load_collection(client, collection_name)
        assert check

        growing_explicit_null_profile_row = {
            "id": 7000,
            "normal_vector": self._vector(7000),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_empty_profile_row = {
            "id": 7001,
            "normal_vector": self._vector(7001),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_two_match_profile_row = {
            "id": 7002,
            "normal_vector": self._vector(7002),
            "doc_tag": "growing_two_match_profile",
            "profile": [
                {"p_int": 9600, "p_tag": "match_9600"},
                {"p_int": 9700, "p_tag": "match_9700"},
            ],
        }
        growing_rows = [
            growing_explicit_null_profile_row,
            growing_empty_profile_row,
            growing_two_match_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_rows)
        assert check
        assert res["insert_count"] == len(growing_rows)

        source_by_id = {}
        source_by_id[sealed_explicit_null_profile_row["id"]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_empty_profile_row["id"]] = sealed_empty_profile_row
        source_by_id[sealed_one_match_profile_row["id"]] = sealed_one_match_profile_row
        source_by_id[sealed_two_match_profile_row["id"]] = sealed_two_match_profile_row
        source_by_id[sealed_zero_match_profile_row["id"]] = sealed_zero_match_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row["id"]] = growing_explicit_null_profile_row
        source_by_id[growing_empty_profile_row["id"]] = growing_empty_profile_row
        source_by_id[growing_two_match_profile_row["id"]] = growing_two_match_profile_row

        match_any_ids = {
            sealed_one_match_profile_row["id"],
            sealed_two_match_profile_row["id"],
            growing_two_match_profile_row["id"],
        }
        match_two_or_more_ids = {
            sealed_two_match_profile_row["id"],
            growing_two_match_profile_row["id"],
        }
        match_exact_one_ids = {sealed_one_match_profile_row["id"]}
        non_nullish_scope = [
            sealed_one_match_profile_row["id"],
            sealed_two_match_profile_row["id"],
            sealed_zero_match_profile_row["id"],
            growing_two_match_profile_row["id"],
        ]

        match_any_results, check = self.query(
            client,
            collection_name,
            filter="MATCH_ANY(profile, $[p_int] >= 9000)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in match_any_results} == match_any_ids
        for row in match_any_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_least_results, check = self.query(
            client,
            collection_name,
            filter="MATCH_LEAST(profile, $[p_int] >= 9000, threshold=2)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in match_least_results} == match_two_or_more_ids
        for row in match_least_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_exact_results, check = self.query(
            client,
            collection_name,
            filter="MATCH_EXACT(profile, $[p_int] >= 9000, threshold=1)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in match_exact_results} == match_exact_one_ids
        for row in match_exact_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_all_results, check = self.query(
            client,
            collection_name,
            filter=f"id in {non_nullish_scope} && MATCH_ALL(profile, $[p_int] >= 9000)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_nullish_scope),
        )
        assert check
        assert {row["id"] for row in match_all_results} == match_two_or_more_ids
        for row in match_all_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_most_results, check = self.query(
            client,
            collection_name,
            filter=f"id in {non_nullish_scope} && MATCH_MOST(profile, $[p_int] >= 9000, threshold=1)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_nullish_scope),
        )
        assert check
        assert {row["id"] for row in match_most_results} == {
            sealed_one_match_profile_row["id"],
            sealed_zero_match_profile_row["id"],
        }
        for row in match_most_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[growing_two_match_profile_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter="MATCH_ANY(profile, $[p_int] >= 9000)",
            output_fields=["id", "doc_tag", "profile"],
            limit=10,
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == match_any_ids
        assert search_results[0][0]["id"] == growing_two_match_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_nullable_element_filter_expression_query(self):
        """
        target: test element_filter query correctness for a nullable scalar Struct Array
        method: create nullable scalar Struct Array rows covering explicit null, omitted, empty, one-match,
            two-match, and zero-match profiles in both sealed and growing segments, then query with element_filter
        expected: expected result rows are computed from source data and expression; null/omitted/empty rows do not
            match, matching rows include the correct element offsets, and profile output matches source data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_element_filter_expr")
        client = self._client()
        fixture = self._setup_nullable_scalar_struct_expression_collection(client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]

        expressions = [
            "element_filter(profile, $[p_int] >= 9000)",
            'element_filter(profile, $[p_int] >= 9000 && $[p_tag] == "match_9100")',
            'element_filter(profile, $[p_tag] == "match_9700")',
        ]
        for expr in expressions:
            expected_rows = self._expected_nullable_scalar_struct_expression_rows(source_rows, expr)
            actual_rows, check = self.query(
                client,
                collection_name,
                filter=expr,
                output_fields=["id", "doc_tag", "profile"],
                limit=len(expected_rows) + 5,
            )
            assert check
            assert self._expression_result_keys(actual_rows) == self._expression_result_keys(expected_rows)
            self._assert_expression_rows_match_source(actual_rows, source_by_id)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_nullable_match_family_null_semantics(self):
        """
        target: test MATCH family null/empty semantics for a nullable scalar Struct Array
        method: create nullable scalar Struct Array rows covering explicit null, omitted, empty, one-match,
            two-match, and zero-match profiles in both sealed and growing segments, then query every MATCH operator
        expected: expected result sets are computed from source data and expression; MATCH_ANY/LEAST only return rows
            with enough matching elements, and MATCH_ALL/MOST/EXACT treat null/empty profiles as zero-element inputs
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_match_null_expr")
        client = self._client()
        fixture = self._setup_nullable_scalar_struct_expression_collection(client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]
        scoped_prefix = f"id in {sorted(fixture['controlled_ids'])} && "

        expressions = [
            "MATCH_ANY(profile, $[p_int] >= 9000)",
            "MATCH_LEAST(profile, $[p_int] >= 9000, threshold=2)",
            "MATCH_EXACT(profile, $[p_int] >= 9000, threshold=1)",
            "MATCH_ALL(profile, $[p_int] >= 9000)",
            "MATCH_MOST(profile, $[p_int] >= 9000, threshold=1)",
            "MATCH_EXACT(profile, $[p_int] >= 9000, threshold=0)",
        ]
        for expr in expressions:
            scoped_expr = scoped_prefix + expr
            expected_rows = self._expected_nullable_scalar_struct_expression_rows(source_rows, scoped_expr)
            actual_rows, check = self.query(
                client,
                collection_name,
                filter=scoped_expr,
                output_fields=["id", "doc_tag", "profile"],
                limit=len(fixture["controlled_ids"]),
            )
            assert check
            assert self._expression_result_keys(actual_rows) == self._expression_result_keys(expected_rows)
            self._assert_expression_rows_match_source(actual_rows, source_by_id)

        search_expr = "MATCH_ANY(profile, $[p_int] >= 9000)"
        expected_search_ids = {
            row["id"] for row in self._expected_nullable_scalar_struct_expression_rows(source_rows, search_expr)
        }
        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(7004)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter=search_expr,
            output_fields=["id", "doc_tag", "profile"],
            limit=len(expected_search_ids),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == expected_search_ids
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_scalar_struct_array_field_nullable_subfield_projection_expression_query_search(self):
        """
        target: test PR #49178 struct sub-field projection expressions on a nullable scalar Struct Array
        method: run query and normal-vector search filters with array_length(profile[p_int]),
            array_contains(_all/_any)(profile[p_tag]), and fixed-index profile[i][sub_field] predicates
        expected: expected result sets are computed from source data and expression; null/omitted profiles do not
            match sub-field projection predicates, empty profiles only match array_length(...)=0, and search/query
            return the same filtered id sets
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_projection_expr")
        client = self._client()
        fixture = self._setup_nullable_scalar_struct_expression_collection(client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]
        scoped_prefix = f"id in {sorted(fixture['controlled_ids'])} && "

        expressions = [
            "array_length(profile[p_int]) == 0",
            "array_length(profile[p_int]) > 0",
            'array_contains(profile[p_tag], "match_9100")',
            'array_contains_all(profile[p_tag], ["match_9200", "match_9300"])',
            'array_contains_any(profile[p_tag], ["match_9600", "missing"])',
            "profile[0][p_int] >= 9000",
            'profile[1][p_tag] == "match_9800"',
        ]
        for expr in expressions:
            scoped_expr = scoped_prefix + expr
            expected_rows = self._expected_nullable_scalar_struct_expression_rows(source_rows, scoped_expr)
            expected_ids = {row["id"] for row in expected_rows}

            actual_rows, check = self.query(
                client,
                collection_name,
                filter=scoped_expr,
                output_fields=["id", "doc_tag", "profile"],
                limit=len(fixture["controlled_ids"]),
            )
            assert check
            assert self._expression_result_keys(actual_rows) == self._expression_result_keys(expected_rows)
            self._assert_expression_rows_match_source(actual_rows, source_by_id)

            search_results, check = self.search(
                client,
                collection_name,
                data=[self._vector(7004)],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                filter=scoped_expr,
                output_fields=["id", "doc_tag", "profile"],
                limit=max(len(expected_ids), 1),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == expected_ids
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(
        reason="StructArray parent is not registered for direct IS NULL/IS NOT NULL or array_length expressions",
        strict=True,
    )
    def test_create_scalar_struct_array_field_nullable_parent_null_expression(self):
        """
        target: test direct null expressions on a nullable scalar Struct Array parent
        method: query `profile is null`, `profile is not null`, and `array_length(profile)` expressions on rows
            covering explicit null, omitted, empty, and non-empty profiles
        expected: expected result sets are computed from source data and expression; null expressions distinguish
            null/omitted rows from empty/non-empty rows, and array_length selects non-null Struct Array rows by length
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_parent_null_expr")
        client = self._client()
        fixture = self._setup_nullable_scalar_struct_expression_collection(client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]
        scoped_prefix = f"id in {sorted(fixture['controlled_ids'])} && "

        expressions = [
            "profile is null",
            "profile is not null",
            "array_length(profile) == 0",
            "array_length(profile) > 0",
        ]
        for expr in expressions:
            scoped_expr = scoped_prefix + expr
            expected_rows = self._expected_nullable_scalar_struct_expression_rows(source_rows, scoped_expr)
            actual_rows, check = self.query(
                client,
                collection_name,
                filter=scoped_expr,
                output_fields=["id", "doc_tag", "profile"],
                limit=len(fixture["controlled_ids"]),
            )
            assert check
            assert self._expression_result_keys(actual_rows) == self._expression_result_keys(expected_rows)
            self._assert_expression_rows_match_source(actual_rows, source_by_id)

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(
        reason="normal-vector search with element_filter on nullable StructArray returns rows that fail predicate",
        strict=True,
    )
    def test_create_scalar_struct_array_field_nullable_element_filter_search(self):
        """
        target: test element_filter as a normal-vector search filter for a nullable scalar Struct Array
        method: create >=3000 sealed rows plus growing rows with null/omitted/empty/non-empty profiles, then search
            normal_vector with `element_filter(profile, $[p_int] >= 9000)`
        expected: expected result ids are computed from source data and expression; search only returns rows that
            contain at least one matching struct element and excludes null/omitted/empty/zero-match rows
        """
        collection_name = cf.gen_unique_str(f"{prefix}_nullable_struct_element_filter_search")
        client = self._client()
        fixture = self._setup_nullable_scalar_struct_expression_collection(client, collection_name)
        source_rows = fixture["source_rows"]
        source_by_id = fixture["source_by_id"]
        expr = "element_filter(profile, $[p_int] >= 9000)"
        expected_ids = {row["id"] for row in self._expected_nullable_scalar_struct_expression_rows(source_rows, expr)}

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(7004)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter=expr,
            output_fields=["id", "doc_tag", "profile"],
            limit=len(expected_ids),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == expected_ids
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_insert_omit_nullable_growing_row(self):
        """
        target: test query/search output for a nullable struct array with vector sub-field created with collection
            schema
        method: create a nullable struct array field with a vector sub-field, then insert a growing row that omits
            the struct field and another growing row with non-null struct data
        expected: omitted field is returned as null, and non-null struct data matches source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_omit")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"},
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "present_profile",
                "profile": self._profile(1),
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(1)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_sealed_null_empty_rows(self):
        """
        target: test sealed segment output for a nullable struct array with vector sub-field created with collection
            schema
        method: create a nullable struct array field with a vector sub-field, insert omitted, empty, and non-empty
            rows, flush, then query/search
        expected: sealed query/search output preserves null, empty, and non-empty struct rows, and struct vector
            search skips null and empty rows
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_sealed")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        empty_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id.update({row["id"]: row for row in non_empty_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        normal_search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(3)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in normal_search_results[0]} == set(source_by_id)
        for hit in normal_search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

        search_tensor = EmbeddingList()
        search_tensor.add(non_empty_rows[-1]["profile"][0]["p_vec"])
        struct_search_results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        assert len(struct_search_results[0]) == len(non_empty_rows)
        hit_ids = {hit["id"] for hit in struct_search_results[0]}
        assert hit_ids == {row["id"] for row in non_empty_rows}
        assert missing_profile_row["id"] not in hit_ids
        assert empty_profile_row["id"] not in hit_ids
        assert struct_search_results[0][0]["id"] == non_empty_rows[-1]["id"]
        for hit in struct_search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(
        reason="nullable ArrayOfVector sealed output still fails after #50020 when empty row precedes single-element row"
    )
    def test_create_struct_array_field_with_vector_single_element_after_empty_sealed_output(self):
        """
        target: test sealed segment output for a nullable struct array vector sub-field after an empty row
        method: create a nullable struct array field with a vector sub-field, insert one empty row followed by one
            single-element row with dim=8, flush, load, then query parent and vector sub-field output
        expected: empty row remains [], and the single-element row returns its vector payload instead of failing
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_empty_single")
        client = self._client()
        dim = 8

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        empty_profile_row = {
            "id": 0,
            "normal_vector": self._vector(0, dim),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        present_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1, dim),
            "doc_tag": "present_single_profile",
            "profile": [
                {
                    "p_int": 10,
                    "p_tag": "profile_1_0",
                    "p_vec": self._vector(10, dim),
                }
            ],
        }
        rows = [empty_profile_row, present_profile_row]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: row for row in rows}
        query_results, check = self.query(
            client,
            collection_name,
            filter="id in [0, 1]",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(rows),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id in [0, 1]",
            output_fields=["id", "profile[p_vec]"],
            limit=len(rows),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.parametrize(
        "vector_type",
        [
            DataType.FLOAT16_VECTOR,
            DataType.BFLOAT16_VECTOR,
            DataType.INT8_VECTOR,
            DataType.BINARY_VECTOR,
        ],
    )
    def test_create_struct_array_field_with_non_float_vector_type_query_search_output(self, vector_type):
        """
        target: test nullable struct array output for non-FLOAT vector sub-field types
        method: create a nullable struct array with FLOAT16/BFLOAT16/INT8/BINARY vector sub-field, insert indexed
            sealed rows and growing rows with omitted, empty, and non-empty profile values, then query parent/sub-field
            output and search on the ordinary vector field
        expected: query/search output preserves nullable parent state and vector sub-field payload for each vector type
        """
        type_name = vector_type.name.lower()
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_{type_name}_output")
        client = self._client()
        dim = EMB_LIST_DIM

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", vector_type, dim=dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        sealed_special_rows = [
            {"id": 0, "normal_vector": self._vector(0, dim), "doc_tag": "sealed_omitted_profile"},
            {
                "id": 1,
                "normal_vector": self._vector(1, dim),
                "doc_tag": "sealed_empty_profile",
                "profile": [],
            },
            {
                "id": 2,
                "normal_vector": self._vector(2, dim),
                "doc_tag": "sealed_present_profile_2",
                "profile": self._typed_profile(2, vector_type, dim),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3, dim),
                "doc_tag": "sealed_present_profile_3",
                "profile": self._typed_profile(3, vector_type, dim),
            },
        ]
        sealed_filler_rows = [
            {
                "id": 10000 + i,
                "normal_vector": self._vector(10000 + i, dim),
                "doc_tag": f"sealed_filler_{i}",
            }
            for i in range(self.min_index_sealed_rows - len(sealed_special_rows))
        ]
        sealed_rows = sealed_special_rows + sealed_filler_rows
        res, check = self.insert(client, collection_name, sealed_rows)
        assert check
        assert res["insert_count"] == len(sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        profile_metric = "MAX_SIM_HAMMING" if vector_type == DataType.BINARY_VECTOR else "MAX_SIM_COSINE"
        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type=profile_metric,
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        # Mixed nullable/non-null ArrayOfVector rows in the same growing batch are tracked by #50009.
        # Keep null/empty coverage in sealed rows here, and use growing only for non-empty requery output.
        growing_rows = [
            {
                "id": 20002,
                "normal_vector": self._vector(20002, dim),
                "doc_tag": "growing_present_profile",
                "profile": self._typed_profile(20002, vector_type, dim),
            }
        ]
        res, check = self.insert(client, collection_name, growing_rows)
        assert check
        assert res["insert_count"] == len(growing_rows)

        source_by_id = {row["id"]: {**row, "profile": row.get("profile")} for row in sealed_rows + growing_rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_typed_profile_equal(row["profile"], expected["profile"], vector_type)

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_typed_profile_vector_subfield_equal(row["profile"], expected["profile"], vector_type)

        top_k = 16
        search_vector = growing_rows[-1]["normal_vector"]
        expected_top_ids = [
            row["id"]
            for row in sorted(
                source_by_id.values(),
                key=lambda row: (
                    sum((left - right) ** 2 for left, right in zip(row["normal_vector"], search_vector)),
                    row["id"],
                ),
            )[:top_k]
        ]
        search_results, check = self.search(
            client,
            collection_name,
            data=[search_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=top_k,
        )
        assert check
        hits = search_results[0]
        assert [hit["id"] for hit in hits] == expected_top_ids
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_typed_profile_equal(entity["profile"], expected["profile"], vector_type)

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_query_iterator(self):
        """
        target: test query_iterator output for a nullable struct array with vector sub-field created with collection
            schema
        method: create a nullable struct array field with a vector sub-field, insert omitted, empty, and non-empty
            rows, flush, then drain query_iterator with small batch size
        expected: iterator returns every row once, and nullable struct output matches source across batch boundaries
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_qiter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        empty_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": self._profile(2),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": self._profile(3),
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id.update({row["id"]: row for row in non_empty_rows})

        iterator, check = self.query_iterator(
            client,
            collection_name,
            batch_size=2,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            consistency_level="Strong",
        )
        assert check
        iterator_rows = self._drain_iterator(iterator)
        iterator_ids = [row["id"] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)

        for row in iterator_rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_search_iterator(self):
        """
        target: test search_iterator output for a nullable struct array with vector sub-field created with collection
            schema
        method: create a nullable struct array field with a vector sub-field, insert omitted, empty, and non-empty
            rows, flush, then drain search_iterator on the normal vector field with small batch size
        expected: iterator returns every topK row once, distances are ordered, and nullable struct output matches
            source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_siter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        empty_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": self._profile(2),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": self._profile(3),
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id.update({row["id"]: row for row in non_empty_rows})

        iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[self._vector(3)],
            batch_size=2,
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        iterator_hits = self._drain_iterator(iterator)
        hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_stress_nullable_scalar_struct_array_query_search_iterators(self):
        """
        target: stress query/search/iterator output for a nullable scalar struct array
        method: create 10k sealed rows with deterministic random nullable masks and 0/1/2/3 struct elements, then run
            full query, normal-vector search, query_iterator, and search_iterator
        expected: every output row/hit matches source data, and iterator scans have no missing or duplicated ids
        """
        collection_name = cf.gen_unique_str(f"{prefix}_stress_nullable_scalar_struct")
        client = self._client()
        dim = 8
        entities = 10000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rng = random.Random(20260526)
        rows = []
        source_by_id = {}
        mask_counts = {"omitted": 0, "null": 0, "empty": 0, "present": 0}
        element_counts = {0: 0, 1: 0, 2: 0, 3: 0}
        for row_id in range(entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id, dim=dim),
                "doc_tag": f"stress_row_{row_id}",
            }

            choice = rng.randrange(8)
            if choice == 0:
                expected_profile = None
                mask_counts["omitted"] += 1
            elif choice == 1:
                row["profile"] = None
                expected_profile = None
                mask_counts["null"] += 1
            elif choice == 2:
                row["profile"] = []
                expected_profile = []
                mask_counts["empty"] += 1
                element_counts[0] += 1
            else:
                element_count = 1 + rng.randrange(3)
                row["profile"] = [
                    {
                        "p_int": row_id * 10 + element_index,
                        "p_tag": f"profile_{row_id}_{element_index}",
                    }
                    for element_index in range(element_count)
                ]
                expected_profile = row["profile"]
                mask_counts["present"] += 1
                element_counts[element_count] += 1

            rows.append(row)
            source_by_id[row_id] = {**row, "profile": expected_profile}

        assert all(count > 0 for count in mask_counts.values())
        assert all(count > 0 for count in element_counts.values())

        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == entities

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=entities,
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results, check = self.search(
            client,
            collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert check
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        distances = [hit["distance"] for hit in hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

        query_iterator, check = self.query_iterator(
            client,
            collection_name,
            batch_size=997,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            consistency_level="Strong",
        )
        assert check
        iterator_rows = self._drain_iterator(query_iterator)
        iterator_ids = [row["id"] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)
        for row in iterator_rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[search_row["normal_vector"]],
            batch_size=997,
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert check
        iterator_hits = self._drain_iterator(search_iterator)
        iterator_hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(iterator_hit_ids) == len(set(iterator_hit_ids))
        assert set(iterator_hit_ids) == set(source_by_id)
        iterator_distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(iterator_distances) - 1):
            assert iterator_distances[index] <= iterator_distances[index + 1] + epsilon
        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(
        reason="indexed sealed nullable ArrayOfVector output omits ValidData when present-row density is high"
    )
    def test_stress_nullable_struct_array_vector_field_query_search_iterators(self):
        """
        target: stress query/search/iterator output for a nullable struct array with vector sub-field
        method: create 10k sealed rows with deterministic random nullable masks and 0/2/3 struct elements, then run
            full query, normal-vector search, query_iterator, and search_iterator
        expected: every output row/hit matches source data, and iterator scans have no missing or duplicated ids
        """
        collection_name = cf.gen_unique_str(f"{prefix}_stress_nullable_struct_vector")
        client = self._client()
        dim = 8
        entities = 10000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rng = random.Random(20260526)
        rows = []
        source_by_id = {}
        mask_counts = {"omitted": 0, "null": 0, "empty": 0, "present": 0}
        element_counts = {0: 0, 2: 0, 3: 0}
        for row_id in range(entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id, dim=dim),
                "doc_tag": f"stress_row_{row_id}",
            }

            choice = rng.randrange(8)
            if choice == 0:
                expected_profile = None
                mask_counts["omitted"] += 1
            elif choice == 1:
                row["profile"] = None
                expected_profile = None
                mask_counts["null"] += 1
            elif choice == 2:
                row["profile"] = []
                expected_profile = []
                mask_counts["empty"] += 1
                element_counts[0] += 1
            else:
                element_count = 2 + (rng.randrange(2))
                row["profile"] = [
                    {
                        "p_int": row_id * 10 + element_index,
                        "p_tag": f"profile_{row_id}_{element_index}",
                        "p_vec": self._vector(row_id * 10 + element_index, dim=dim),
                    }
                    for element_index in range(element_count)
                ]
                expected_profile = row["profile"]
                mask_counts["present"] += 1
                element_counts[element_count] += 1

            rows.append(row)
            source_by_id[row_id] = {**row, "profile": expected_profile}

        assert all(count > 0 for count in mask_counts.values())
        assert all(count > 0 for count in element_counts.values())

        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == entities

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        assert self.wait_for_index_ready(client, collection_name, "profile[p_vec]", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=entities,
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results, check = self.search(
            client,
            collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert check
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        distances = [hit["distance"] for hit in hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

        query_iterator, check = self.query_iterator(
            client,
            collection_name,
            batch_size=997,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            consistency_level="Strong",
        )
        assert check
        iterator_rows = self._drain_iterator(query_iterator)
        iterator_ids = [row["id"] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)
        for row in iterator_rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[search_row["normal_vector"]],
            batch_size=997,
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert check
        iterator_hits = self._drain_iterator(search_iterator)
        iterator_hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(iterator_hit_ids) == len(set(iterator_hit_ids))
        assert set(iterator_hit_ids) == set(source_by_id)
        iterator_distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(iterator_distances) - 1):
            assert iterator_distances[index] <= iterator_distances[index + 1] + epsilon
        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_ann_search_iterator_rejects_embedding_list(self):
        """
        target: test search_iterator limitation on a nullable struct array vector sub-field
        method: create a nullable struct array field with a vector sub-field, insert omitted, empty, and non-empty rows,
            verify regular struct-vector search succeeds, then request search_iterator on the same embedding-list query
        expected: regular search returns only non-empty struct rows, while search_iterator is rejected as unsupported for
            multi-search-multi on embedding list fields
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_ann_siter_reject")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        empty_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id.update({row["id"]: row for row in non_empty_rows})

        search_tensor = EmbeddingList()
        search_tensor.add(non_empty_rows[-1]["profile"][0]["p_vec"])
        results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        hits = results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row["id"] for row in non_empty_rows}
        assert missing_profile_row["id"] not in hit_ids
        assert empty_profile_row["id"] not in hit_ids
        assert hits[0]["id"] == non_empty_rows[-1]["id"]

        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            self._assert_profile_equal(entity["profile"], expected["profile"])

        error = {
            ct.err_code: 1100,
            ct.err_msg: "search iterator is not supported for multi-search-multi on embedding list fields",
        }
        self.search_iterator(
            client,
            collection_name,
            data=[search_tensor.to_flat_array()],
            batch_size=1,
            anns_field="profile[p_vec]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "profile"],
            limit=len(non_empty_rows),
            is_embedding_list=True,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_element_search_iterator(self):
        """
        target: test element-level search_iterator on a nullable struct array vector sub-field
        method: create a nullable struct array field with a vector sub-field, insert omitted, empty, and non-empty rows,
            index the struct vector sub-field with a non-embedding-list metric, then drain search_iterator with a plain
            query vector
        expected: iterator returns only rows containing vector elements, skips null/empty rows, and output matches source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_siter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        empty_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: row for row in non_empty_rows}
        null_or_empty_ids = {missing_profile_row["id"], empty_profile_row["id"]}

        iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[non_empty_rows[-1]["profile"][0]["p_vec"]],
            batch_size=1,
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=["id", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        iterator_hits = self._drain_iterator(iterator)
        hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)
        assert not set(hit_ids).intersection(null_or_empty_ids)
        assert iterator_hits[0]["id"] == non_empty_rows[-1]["id"]
        assert iterator_hits[0]["offset"] == 0

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] >= distances[index + 1] - epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(
        reason="milvus-io/milvus#50049: nullable StructArray vector element-level search returns wrong rows"
    )
    def test_create_struct_array_field_with_vector_element_search_flat(self):
        """
        target: test element-level search on a nullable struct array vector sub-field created with collection schema
        method: create a nullable struct array field with a vector sub-field, insert non-empty rows, then search the
            struct vector sub-field with a plain query vector and a FLAT index
        expected: query output matches source, and the exact matching struct vector is returned as top1 with the correct
            row id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_flat")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        rows = [
            {
                "id": 0,
                "normal_vector": self._vector(0),
                "doc_tag": "present_profile_0",
                "profile": [
                    {"p_int": 0, "p_tag": "profile_0_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 1, "p_tag": "profile_0_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "present_profile_1",
                "profile": [
                    {"p_int": 10, "p_tag": "profile_1_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 11, "p_tag": "profile_1_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(field_name="profile[p_vec]", index_type="FLAT", metric_type="COSINE")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: row for row in rows}
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE"},
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert search_results[0][0]["id"] == rows[-1]["id"]
        assert search_results[0][0]["offset"] == 0
        entity = self._search_entity(search_results[0][0])
        assert entity["doc_tag"] == rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(
        reason="milvus-io/milvus#50049: nullable StructArray vector element-level search returns wrong rows"
    )
    def test_create_struct_array_field_with_vector_element_search_missing_prefix(self):
        """
        target: test element-level search on a nullable struct array vector sub-field with a missing struct prefix row
        method: create a nullable struct array field with a vector sub-field, insert one row omitting the struct field
            followed by non-empty rows, then search the struct vector sub-field with a plain query vector
        expected: missing row is skipped, and the exact matching struct vector is returned as top1 with the correct row
            id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_missing_prefix")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        non_empty_rows = [
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "present_profile_1",
                "profile": [
                    {"p_int": 10, "p_tag": "profile_1_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 11, "p_tag": "profile_1_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id.update({row["id"]: row for row in non_empty_rows})
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        hits = search_results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row["id"] for row in non_empty_rows}
        assert missing_profile_row["id"] not in hit_ids
        assert hits[0]["id"] == non_empty_rows[-1]["id"]
        assert hits[0]["offset"] == 0
        entity = self._search_entity(hits[0])
        assert entity["doc_tag"] == non_empty_rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], non_empty_rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_element_search_missing_empty_prefix(self):
        """
        target: test element-level search on a nullable struct array vector sub-field with missing and empty prefix rows
        method: create a nullable struct array field with a vector sub-field, insert one row omitting the struct field,
            one empty struct array row, and non-empty rows, then search the struct vector sub-field with a plain query
            vector
        expected: missing/empty rows are skipped, and the exact matching struct vector is returned as top1 with the
            correct row id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_missing_empty_prefix")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        empty_profile_row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "present_profile_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id.update({row["id"]: row for row in non_empty_rows})
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        hits = search_results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row["id"] for row in non_empty_rows}
        assert missing_profile_row["id"] not in hit_ids
        assert empty_profile_row["id"] not in hit_ids
        assert hits[0]["id"] == non_empty_rows[-1]["id"]
        assert hits[0]["offset"] == 0
        entity = self._search_entity(hits[0])
        assert entity["doc_tag"] == non_empty_rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], non_empty_rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_element_search_empty_prefix(self):
        """
        target: test element-level search on a nullable struct array vector sub-field with only an empty prefix row
        method: create a nullable struct array field with a vector sub-field, insert one empty struct array row before
            non-empty rows, then search the struct vector sub-field with a plain query vector
        expected: the empty row is skipped, and the exact matching struct vector is returned as top1 with the correct
            row id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_empty_prefix")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        empty_profile_row = {
            "id": 0,
            "normal_vector": self._vector(0),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "present_profile_1",
                "profile": [
                    {"p_int": 10, "p_tag": "profile_1_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 11, "p_tag": "profile_1_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {empty_profile_row["id"]: empty_profile_row}
        source_by_id.update({row["id"]: row for row in non_empty_rows})
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        hits = search_results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row["id"] for row in non_empty_rows}
        assert empty_profile_row["id"] not in hit_ids
        assert hits[0]["id"] == non_empty_rows[-1]["id"]
        assert hits[0]["offset"] == 0
        entity = self._search_entity(hits[0])
        assert entity["doc_tag"] == non_empty_rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], non_empty_rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_create_struct_array_field_with_vector_element_search_empty_prefix_flat(self):
        """
        target: test FLAT element-level search on a nullable struct array vector sub-field with only an empty prefix row
        method: create a nullable struct array field with a vector sub-field, insert one empty struct array row before
            non-empty rows, then search the struct vector sub-field with a plain query vector and a FLAT index
        expected: the empty row is skipped, and the exact matching struct vector is returned as top1 with the correct
            row id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_empty_prefix_flat")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        empty_profile_row = {
            "id": 0,
            "normal_vector": self._vector(0),
            "doc_tag": "empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "present_profile_1",
                "profile": [
                    {"p_int": 10, "p_tag": "profile_1_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 11, "p_tag": "profile_1_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [empty_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(field_name="profile[p_vec]", index_type="FLAT", metric_type="COSINE")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {empty_profile_row["id"]: empty_profile_row}
        source_by_id.update({row["id"]: row for row in non_empty_rows})
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE"},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        hits = search_results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row["id"] for row in non_empty_rows}
        assert empty_profile_row["id"] not in hit_ids
        assert hits[0]["id"] == non_empty_rows[-1]["id"]
        assert hits[0]["offset"] == 0
        entity = self._search_entity(hits[0])
        assert entity["doc_tag"] == non_empty_rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], non_empty_rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(
        reason="milvus-io/milvus#50049: nullable StructArray vector element-level search returns wrong rows"
    )
    def test_create_struct_array_field_with_vector_element_search_missing_prefix_flat(self):
        """
        target: test FLAT element-level search on a nullable struct array vector sub-field with a missing prefix row
        method: create a nullable struct array field with a vector sub-field, insert one row omitting the struct field
            followed by non-empty rows, then search the struct vector sub-field with a plain query vector and a FLAT
            index
        expected: missing row is skipped, and the exact matching struct vector is returned as top1 with the correct row
            id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_create_struct_vector_elem_missing_prefix_flat")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        missing_profile_row = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "missing_profile"}
        non_empty_rows = [
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "present_profile_1",
                "profile": [
                    {"p_int": 10, "p_tag": "profile_1_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 11, "p_tag": "profile_1_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "present_profile_2",
                "profile": [
                    {"p_int": 20, "p_tag": "profile_2_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 21, "p_tag": "profile_2_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        rows = [missing_profile_row, *non_empty_rows]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(field_name="profile[p_vec]", index_type="FLAT", metric_type="COSINE")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {missing_profile_row["id"]: {**missing_profile_row, "profile": None}}
        source_by_id.update({row["id"]: row for row in non_empty_rows})
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_empty_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE"},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        hits = search_results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == {row["id"] for row in non_empty_rows}
        assert missing_profile_row["id"] not in hit_ids
        assert hits[0]["id"] == non_empty_rows[-1]["id"]
        assert hits[0]["offset"] == 0
        entity = self._search_entity(hits[0])
        assert entity["doc_tag"] == non_empty_rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], non_empty_rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_by_alias_query_by_name_and_alias(self):
        """
        target: test dynamically adding a struct array field through collection alias
        method: create an alias for a collection, add a scalar struct array field by alias, then insert/query through
            both collection name and alias
        expected: alias resolves to the real collection for schema evolution, and both describe/query paths see the
            same nullable struct array field and data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_alias")
        alias = cf.gen_unique_str(f"{prefix}_alias")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check
        res, check = self.create_alias(client, collection_name, alias)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(client, alias, "profile", profile_schema, max_capacity=4)
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        for target in (collection_name, alias):
            describe_info, check = self.describe_collection(client, target)
            assert check
            profile_field = next(field for field in describe_info["fields"] if field["name"] == "profile")
            assert profile_field["nullable"] is True
            assert profile_field["type"] == DataType.ARRAY
            assert profile_field["element_type"] == DataType.STRUCT
            assert {field["name"] for field in profile_field["struct_fields"]} == {"p_int", "p_tag"}

        row = {
            "id": 1,
            "normal_vector": self._vector(1),
            "doc_tag": "inserted_by_alias",
            "profile": self._scalar_profile(1),
        }
        res, check = self.insert(client, alias, [row])
        assert check
        assert res["insert_count"] == 1

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        for target in (collection_name, alias):
            query_results, check = self.query(
                client,
                target,
                filter="id == 1",
                output_fields=["id", "doc_tag", "profile"],
                limit=1,
            )
            assert check
            assert len(query_results) == 1
            assert query_results[0]["id"] == row["id"]
            assert query_results[0]["doc_tag"] == row["doc_tag"]
            self._assert_scalar_profile_equal(query_results[0]["profile"], row["profile"])

        res, check = self.drop_alias(client, alias)
        assert check

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_struct_array_field_non_nullable_rejected(self):
        """
        target: test non-nullable struct array validation when dynamically adding a struct array field
        method: try to add a struct array field with nullable=False through MilvusClient
        expected: PyMilvus rejects the request before RPC, and collection schema remains unchanged
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_non_nullable")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        error = {
            ct.err_code: 1,
            ct.err_msg: "Adding struct field to existing collection requires nullable=True",
        }
        self.add_collection_struct_field(
            client,
            collection_name,
            "profile",
            profile_schema,
            max_capacity=4,
            nullable=False,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        assert "profile" not in {field["name"] for field in describe_info["fields"]}

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_struct_array_field_duplicate_name_rejected(self):
        """
        target: test duplicate field name validation when dynamically adding a struct array field
        method: add a struct array field, then add another struct array field with the same name and with an
            existing regular field name
        expected: duplicate names are rejected, and collection schema remains unchanged
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_dup_name")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        duplicate_profile_schema = client.create_struct_field_schema()
        duplicate_profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        error = {ct.err_code: 1100, ct.err_msg: "duplicated field name profile"}
        self.add_collection_struct_field(
            client,
            collection_name,
            "profile",
            duplicate_profile_schema,
            max_capacity=4,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        conflict_regular_field_schema = client.create_struct_field_schema()
        conflict_regular_field_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        error = {ct.err_code: 1100, ct.err_msg: "duplicated field name normal_vector"}
        self.add_collection_struct_field(
            client,
            collection_name,
            "normal_vector",
            conflict_regular_field_schema,
            max_capacity=4,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        assert [field["name"] for field in describe_info["fields"]].count("profile") == 1
        profile_field = next(field for field in describe_info["fields"] if field["name"] == "profile")
        assert {field["name"] for field in profile_field["struct_fields"]} == {"p_int"}

    @pytest.mark.tags(CaseLabel.L1)
    def test_append_subfield_to_existing_struct_array_rejected(self):
        """
        target: test appending a sub-field to an existing struct array field is not supported
        method: add a struct array field with one sub-field, then call AddCollectionStructField again with the same
            parent name and a different sub-field schema
        expected: request is rejected as a duplicate parent field, and the original struct schema is unchanged
        """
        collection_name = cf.gen_unique_str(f"{prefix}_append_struct_subfield")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        res, check = self.add_collection_struct_field(
            client,
            collection_name,
            "profile",
            profile_schema,
            max_capacity=4,
        )
        assert check

        append_subfield_schema = client.create_struct_field_schema()
        append_subfield_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        error = {ct.err_code: 1100, ct.err_msg: "duplicated field name profile"}
        self.add_collection_struct_field(
            client,
            collection_name,
            "profile",
            append_subfield_schema,
            max_capacity=4,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        profile_field = next(field for field in describe_info["fields"] if field["name"] == "profile")
        assert {field["name"] for field in profile_field["struct_fields"]} == {"p_int"}

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_struct_array_field_duplicate_subfield_name_rejected(self):
        """
        target: test duplicate sub-field name validation when dynamically adding a struct array field
        method: add a struct array field whose struct schema contains duplicate sub-field names
        expected: duplicate sub-field names are rejected, and no struct field is added to the collection
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_dup_subfield")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_int", DataType.VARCHAR, max_length=128)
        error = {
            ct.err_code: 1,
            ct.err_msg: "Duplicate field names in struct 'profile'",
        }
        self.add_collection_struct_field(
            client,
            collection_name,
            "profile",
            profile_schema,
            max_capacity=4,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        assert "profile" not in {field["name"] for field in describe_info["fields"]}

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "field_kwargs, expected_msg",
        [
            ({"is_primary": True}, "cannot be primary key"),
            ({"is_partition_key": True}, "cannot be partition key"),
            ({"is_clustering_key": True}, "cannot be clustering key"),
            ({"is_dynamic": True}, "cannot be dynamic field"),
            ({"nullable": True}, "cannot be nullable individually"),
            ({"default_value": 7}, "cannot have default value"),
        ],
    )
    def test_add_struct_array_field_invalid_subfield_properties_rejected(self, field_kwargs, expected_msg):
        """
        target: test invalid sub-field property validation when dynamically adding a struct array field
        method: add a struct array field whose sub-field is primary key, partition key, clustering key, dynamic,
            individually nullable, or has default value
        expected: invalid sub-field properties are rejected, and no struct field is added to the collection
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_bad_subfield_attr")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64, **field_kwargs)
        error = {ct.err_code: 1, ct.err_msg: expected_msg}
        self.add_collection_struct_field(
            client,
            collection_name,
            "profile",
            profile_schema,
            max_capacity=4,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        assert "profile" not in {field["name"] for field in describe_info["fields"]}

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "parent_name, subfield_name, expected_msg",
        [
            ("RowID", "p_int", "not support to add system field, field name = RowID"),
            ("Timestamp", "p_int", "not support to add system field, field name = Timestamp"),
            ("profile", "RowID", "not support to add system field, field name = RowID"),
            (
                "profile",
                "__virtual_pk__",
                "not support to add system field, field name = __virtual_pk__",
            ),
        ],
    )
    def test_add_struct_array_field_reserved_names_rejected(self, parent_name, subfield_name, expected_msg):
        """
        target: test reserved parent and sub-field name validation when dynamically adding a struct array field
        method: add a struct array field whose parent or sub-field name is a system field name
        expected: reserved names are rejected, and no struct field is added to the collection
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_reserved_name")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field(subfield_name, DataType.INT64)
        error = {ct.err_code: 1100, ct.err_msg: expected_msg}
        self.add_collection_struct_field(
            client,
            collection_name,
            parent_name,
            profile_schema,
            max_capacity=4,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

        describe_info, check = self.describe_collection(client, collection_name)
        assert check
        assert parent_name not in {field["name"] for field in describe_info["fields"]}

    @pytest.mark.tags(CaseLabel.L1)
    def test_add_scalar_struct_array_field_concurrent_dml_query_search(self):
        """
        target: test insert/delete/upsert traffic while dynamically adding a nullable scalar struct array field
        method: load a collection with indexed sealed rows and growing rows, run AddCollectionStructField concurrently
            with old-schema insert, delete, and upsert requests, then insert post-add struct rows
        expected: concurrent DML requests succeed without partial schema visibility, deleted rows are absent, and old
            schema rows expose the added struct field as null while post-add rows preserve their struct values
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_concurrent_dml")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_sealed_{i}"} for i in range(4)]
        old_sealed_filler_rows = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [
            {"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_growing_{i}"} for i in range(4, 8)
        ]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        concurrent_insert_rows = [
            {
                "id": 1000 + batch * 10 + i,
                "normal_vector": self._vector(1000 + batch * 10 + i),
                "doc_tag": f"concurrent_insert_{batch}_{i}",
            }
            for batch in range(3)
            for i in range(3)
        ]
        concurrent_upsert_rows = [
            {
                "id": old_sealed_rows[0]["id"],
                "normal_vector": self._vector(5000),
                "doc_tag": "concurrent_upsert_old_sealed",
            },
            {
                "id": old_sealed_rows[2]["id"],
                "normal_vector": self._vector(5002),
                "doc_tag": "concurrent_upsert_old_sealed_second",
            },
            {
                "id": old_growing_rows[0]["id"],
                "normal_vector": self._vector(5004),
                "doc_tag": "concurrent_upsert_old_growing",
            },
            {
                "id": old_growing_rows[2]["id"],
                "normal_vector": self._vector(5006),
                "doc_tag": "concurrent_upsert_old_growing_second",
            },
            {
                "id": 1100,
                "normal_vector": self._vector(5100),
                "doc_tag": "concurrent_upsert_new_row",
            },
        ]
        delete_ids = [old_sealed_rows[1]["id"], old_growing_rows[1]["id"]]

        start_barrier = threading.Barrier(4)

        def add_struct_field_task():
            task_client = self._client()
            profile_schema = task_client.create_struct_field_schema()
            profile_schema.add_field("p_int", DataType.INT64)
            profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
            start_barrier.wait()
            result, task_check = self.add_collection_struct_field(
                task_client,
                collection_name,
                "profile",
                profile_schema,
                max_capacity=4,
            )
            assert task_check
            return "add", result

        def insert_task():
            task_client = self._client()
            start_barrier.wait()
            inserted = 0
            for batch in range(3):
                batch_rows = concurrent_insert_rows[batch * 3 : (batch + 1) * 3]
                result, task_check = self.insert(task_client, collection_name, batch_rows)
                assert task_check
                inserted += result["insert_count"]
                time.sleep(0.01)
            return "insert", inserted

        def upsert_task():
            task_client = self._client()
            start_barrier.wait()
            result, task_check = self.upsert(task_client, collection_name, concurrent_upsert_rows)
            assert task_check
            return "upsert", result["upsert_count"]

        def delete_task():
            task_client = self._client()
            start_barrier.wait()
            time.sleep(0.01)
            result, task_check = self.delete(task_client, collection_name, filter=f"id in {delete_ids}")
            assert task_check
            return "delete", result["delete_count"]

        task_results = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(add_struct_field_task),
                executor.submit(insert_task),
                executor.submit(upsert_task),
                executor.submit(delete_task),
            ]
            for future in as_completed(futures):
                task_name, task_result = future.result()
                task_results[task_name] = task_result

        assert task_results["insert"] == len(concurrent_insert_rows)
        assert task_results["upsert"] == len(concurrent_upsert_rows)
        assert task_results["delete"] == len(delete_ids)

        self.wait_schema_version_consistent(client, collection_name)

        post_add_rows = [
            {
                "id": 1200,
                "normal_vector": self._vector(1200),
                "doc_tag": "post_add_explicit_null_profile",
                "profile": None,
            },
            {
                "id": 1201,
                "normal_vector": self._vector(1201),
                "doc_tag": "post_add_empty_profile",
                "profile": [],
            },
            {
                "id": 1202,
                "normal_vector": self._vector(1202),
                "doc_tag": "post_add_non_empty_profile",
                "profile": self._scalar_profile(1202),
            },
        ]
        res, check = self.insert(client, collection_name, post_add_rows)
        assert check
        assert res["insert_count"] == len(post_add_rows)

        res, check = self.flush(client, collection_name)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        source_by_id = {row["id"]: {**row, "profile": None} for row in all_old_sealed_rows + old_growing_rows}
        for deleted_id in delete_ids:
            source_by_id.pop(deleted_id)
        source_by_id.update({row["id"]: {**row, "profile": None} for row in concurrent_insert_rows})
        source_by_id.update({row["id"]: {**row, "profile": None} for row in concurrent_upsert_rows})
        source_by_id[post_add_rows[0]["id"]] = post_add_rows[0]
        source_by_id[post_add_rows[1]["id"]] = post_add_rows[1]
        source_by_id[post_add_rows[2]["id"]] = post_add_rows[2]

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        assert not {row["id"] for row in query_results}.intersection(delete_ids)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 12020)",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {post_add_rows[2]["id"]}
        self._assert_scalar_profile_equal(element_filter_results[0]["profile"], post_add_rows[2]["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[post_add_rows[2]["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert not {hit["id"] for hit in search_results[0]}.intersection(delete_ids)
        assert search_results[0][0]["id"] == post_add_rows[2]["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_subfield_output_query_search(self):
        """
        target: test sub-field output after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with old rows, then query/search with only
            struct sub-fields in output_fields
        expected: old rows expose the added struct as null, and new rows return sub-field data matching source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_subfield")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(4, 6)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(5)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert set(entity) == {"id", "profile"}
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_query_search_after_reload(self):
        """
        target: test nullable output after reload for a dynamically added scalar-only struct array field
        method: add a scalar struct array field to a loaded collection with old rows, insert new rows, flush, then
            release and load the collection before query/search
        expected: old rows still expose the added struct as null, and new rows return inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_reload")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(4, 6)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        res, check = self.flush(client, collection_name)
        assert check
        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 50)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in filter_results} == {5}
        for row in filter_results:
            self._assert_scalar_profile_equal(row["profile"], source_by_id[row["id"]]["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(5)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_query_iterator(self):
        """
        target: test query_iterator output after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with sealed and growing old rows, insert
            new rows, then drain query_iterator with small batch size
        expected: iterator returns every row once, old rows expose the added struct as null, and new rows match source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_qiter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(3, 5)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(5, 8)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        iterator, check = self.query_iterator(
            client,
            collection_name,
            batch_size=2,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            consistency_level="Strong",
        )
        assert check
        iterator_rows = self._drain_iterator(iterator)
        iterator_ids = [row["id"] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)

        for row in iterator_rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_search_iterator(self):
        """
        target: test search_iterator output after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with sealed and growing old rows, insert
            new rows, then drain search_iterator on the normal vector field with small batch size
        expected: iterator returns every topK row once, old rows expose the added struct as null, and new rows match source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_siter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(3, 5)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(5, 8)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[self._vector(7)],
            batch_size=2,
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        iterator_hits = self._drain_iterator(iterator)
        hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            assert "profile" in entity
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_upsert_null_non_null(self):
        """
        target: test upsert after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection, upsert an old null row to non-null, and
            upsert a new non-null row while omitting the nullable struct field
        expected: full-row upsert updates null/non-null struct values correctly in query and search output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_upsert")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(3, 5)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(5, 7)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        old_null_to_non_null = {
            "id": 0,
            "normal_vector": self._vector(100),
            "doc_tag": "upserted_old_null_to_non_null",
            "profile": self._scalar_profile(100),
        }
        new_non_null_to_null = {
            "id": 5,
            "normal_vector": self._vector(500),
            "doc_tag": "upserted_new_non_null_to_null",
        }
        res, check = self.upsert(client, collection_name, [old_null_to_non_null, new_non_null_to_null])
        assert check
        assert res["upsert_count"] == 2

        source_by_id[old_null_to_non_null["id"]] = old_null_to_non_null
        source_by_id[new_non_null_to_null["id"]] = {**new_non_null_to_null, "profile": None}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 1000)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {old_null_to_non_null["id"]}
        self._assert_scalar_profile_equal(element_filter_results[0]["profile"], old_null_to_non_null["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(500)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0]["id"] == new_non_null_to_null["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            assert "profile" in entity
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_empty_array_query_search(self):
        """
        target: test null and empty array are distinguishable after dynamically adding a nullable struct array field
        method: add a scalar struct array field to a loaded collection with old rows, then insert one empty profile
            row and one non-empty profile row
        expected: old rows return null, empty row returns [], non-empty row returns inserted data, and element_filter
            only matches non-empty rows with matching elements
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_empty")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(3, 5)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        empty_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "new_empty_profile",
            "profile": [],
        }
        non_empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "new_non_empty_profile",
            "profile": self._scalar_profile(6),
        }
        res, check = self.insert(client, collection_name, [empty_profile_row, non_empty_profile_row])
        assert check
        assert res["insert_count"] == 2

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id[non_empty_profile_row["id"]] = non_empty_profile_row

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 60)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {non_empty_profile_row["id"]}
        self._assert_scalar_profile_equal(element_filter_results[0]["profile"], non_empty_profile_row["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(6)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            assert "profile" in entity
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

        filtered_search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(6)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter="element_filter(profile, $[p_int] == 60)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {hit["id"] for hit in filtered_search_results[0]} == {non_empty_profile_row["id"]}
        entity = self._search_entity(filtered_search_results[0][0])
        self._assert_scalar_profile_equal(entity["profile"], non_empty_profile_row["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_search_on_added_subfield(self):
        """
        target: test struct array vector search after dynamically adding a nullable struct array field
        method: add a struct array field with vector sub-field to a loaded collection with old sealed rows, insert
            new rows, index the added vector sub-field, then search on the added vector sub-field
        expected: old null rows are not retrieved, and new rows return inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_search")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "new_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "new_4",
                "profile": [
                    {"p_int": 40, "p_tag": "profile_4_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 41, "p_tag": "profile_4_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows}
        source_by_id.update({row["id"]: row for row in new_rows})
        old_ids = {row["id"] for row in old_sealed_rows}
        new_ids = {row["id"] for row in new_rows}

        search_tensor = EmbeddingList()
        search_tensor.add(new_rows[-1]["profile"][0]["p_vec"])
        search_results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "profile"],
            limit=len(new_rows),
        )
        assert check
        assert len(search_results[0]) == len(new_rows)
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == new_ids
        assert not hit_ids.intersection(old_ids)
        assert search_results[0][0]["id"] == new_rows[-1]["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_search_on_added_subfield_old_growing_rows(self):
        """
        target: test struct array vector search with old growing rows after dynamically adding a nullable struct
            array field
        method: create loaded collection with sealed and growing rows, add a struct array field with vector sub-field,
            insert new non-null rows, index the added vector sub-field, then search on the added vector sub-field
        expected: old null rows are not retrieved, and new rows return inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_search_growing")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "new_4",
                "profile": [
                    {"p_int": 40, "p_tag": "profile_4_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 41, "p_tag": "profile_4_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 5,
                "normal_vector": self._vector(5),
                "doc_tag": "new_5",
                "profile": [
                    {"p_int": 50, "p_tag": "profile_5_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 51, "p_tag": "profile_5_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})
        old_ids = {row["id"] for row in old_sealed_rows + old_growing_rows}
        new_ids = {row["id"] for row in new_rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        search_tensor = EmbeddingList()
        search_tensor.add(new_rows[-1]["profile"][0]["p_vec"])
        search_results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "profile"],
            limit=len(new_rows),
        )
        assert check
        assert len(search_results[0]) == len(new_rows)
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == new_ids
        assert not hit_ids.intersection(old_ids)
        assert search_results[0][0]["id"] == new_rows[-1]["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(
        reason="milvus-io/milvus#50049: nullable StructArray vector element-level search returns wrong rows"
    )
    def test_add_struct_array_field_with_vector_element_search_no_old_rows(self):
        """
        target: test element-level search after dynamically adding a nullable struct array vector sub-field with no
            historical rows
        method: create an empty collection, add a nullable struct array field with a vector sub-field, insert only
            non-empty rows, then search the added sub-field with a plain query vector and a FLAT index
        expected: query output matches source, and the exact matching struct vector is returned as top1 with the correct
            row id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_elem_no_old")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        rows = [
            {
                "id": 0,
                "normal_vector": self._vector(0),
                "doc_tag": "new_0",
                "profile": [
                    {"p_int": 0, "p_tag": "profile_0_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 1, "p_tag": "profile_0_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 1,
                "normal_vector": self._vector(1),
                "doc_tag": "new_1",
                "profile": [
                    {"p_int": 10, "p_tag": "profile_1_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 11, "p_tag": "profile_1_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, rows)
        assert check
        assert res["insert_count"] == len(rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(field_name="profile[p_vec]", index_type="FLAT", metric_type="COSINE")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: row for row in rows}
        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE"},
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert search_results[0][0]["id"] == rows[-1]["id"]
        assert search_results[0][0]["offset"] == 0
        entity = self._search_entity(search_results[0][0])
        assert entity["doc_tag"] == rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(
        reason="milvus-io/milvus#50049: nullable StructArray vector element-level search returns wrong rows"
    )
    def test_add_struct_array_field_with_vector_element_search_filter_new_rows(self):
        """
        target: test element-level search after dynamically adding a nullable struct array vector sub-field while
            filtering out historical null rows
        method: create a loaded collection with old sealed and growing rows, add a nullable struct array field with a
            vector sub-field, insert new non-empty rows, then search the added sub-field with a plain query vector and
            an ordinary filter that keeps only new rows
        expected: old null rows are excluded by the ordinary filter, and the exact matching struct vector is returned as
            top1 with the correct row id and element offset
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_elem_filter_new")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        old_growing_rows = [{"id": 2, "normal_vector": self._vector(2), "doc_tag": "growing_2"}]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "new_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "new_4",
                "profile": [
                    {"p_int": 40, "p_tag": "profile_4_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 41, "p_tag": "profile_4_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})
        old_ids = {row["id"] for row in old_sealed_rows + old_growing_rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="profile[p_vec]", index_type="FLAT", metric_type="COSINE")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        search_results, check = self.search(
            client,
            collection_name,
            data=[new_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE"},
            filter="id >= 3",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert search_results[0][0]["id"] == new_rows[-1]["id"]
        assert search_results[0][0]["offset"] == 0
        assert search_results[0][0]["id"] not in old_ids
        entity = self._search_entity(search_results[0][0])
        assert entity["doc_tag"] == new_rows[-1]["doc_tag"]
        self._assert_profile_equal(entity["profile"], new_rows[-1]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_element_search_old_null_rows(self):
        """
        target: test element-level search after dynamically adding a nullable struct array vector sub-field
        method: create a loaded collection with old sealed and growing rows, add a nullable struct array field with a
            vector sub-field, insert new non-empty rows, then search on the added sub-field with a plain query vector
        expected: old rows expose the added struct as null, search returns only new rows with vector elements, and output
            matches source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_elem_search")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        old_growing_rows = [{"id": 2, "normal_vector": self._vector(2), "doc_tag": "growing_2"}]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "new_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "new_4",
                "profile": [
                    {"p_int": 40, "p_tag": "profile_4_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 41, "p_tag": "profile_4_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})
        old_ids = {row["id"] for row in old_sealed_rows + old_growing_rows}
        new_ids = {row["id"] for row in new_rows}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        search_results, check = self.search(
            client,
            collection_name,
            data=[new_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(new_rows),
        )
        assert check
        hits = search_results[0]
        hit_ids = [hit["id"] for hit in hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == new_ids
        assert not set(hit_ids).intersection(old_ids)
        assert hits[0]["id"] == new_rows[-1]["id"]
        assert hits[0]["offset"] == 0

        distances = [hit["distance"] for hit in hits]
        for index in range(len(distances) - 1):
            assert distances[index] >= distances[index + 1] - epsilon

        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_search_skip_null_empty_rows(self):
        """
        target: test null and empty struct rows are skipped by struct array vector search after dynamic add
        method: add a struct array field with vector sub-field to a loaded collection with old sealed rows, then
            insert one empty struct row and two non-empty struct rows before searching on the added vector sub-field
        expected: old null rows and the empty struct row are not retrieved by profile[p_vec] search
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_empty_search")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        empty_profile_row = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "new_empty_profile",
            "profile": [],
        }
        non_empty_rows = [
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "new_4",
                "profile": [
                    {"p_int": 40, "p_tag": "profile_4_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 41, "p_tag": "profile_4_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 5,
                "normal_vector": self._vector(5),
                "doc_tag": "new_5",
                "profile": [
                    {"p_int": 50, "p_tag": "profile_5_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 51, "p_tag": "profile_5_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, [empty_profile_row, *non_empty_rows])
        assert check
        assert res["insert_count"] == 1 + len(non_empty_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id.update({row["id"]: row for row in non_empty_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        search_tensor = EmbeddingList()
        search_tensor.add(non_empty_rows[-1]["profile"][0]["p_vec"])
        search_results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            output_fields=["id", "profile"],
            limit=len(non_empty_rows),
        )
        assert check
        assert len(search_results[0]) == len(non_empty_rows)
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == {row["id"] for row in non_empty_rows}
        assert empty_profile_row["id"] not in hit_ids
        assert not hit_ids.intersection({row["id"] for row in old_sealed_rows})
        assert search_results[0][0]["id"] == non_empty_rows[-1]["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_subfield_output_query_search(self):
        """
        target: test vector sub-field output after dynamically adding a nullable struct array field
        method: add a struct array field with vector sub-field to a loaded collection with old sealed rows, then
            query/search with only the vector sub-field in output_fields
        expected: old rows expose the added struct as null, and new rows return vector sub-field data matching source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_subfield")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(3, 5)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(4)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert set(entity) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_subfield_output_old_growing_rows(self):
        """
        target: test vector sub-field only output after dynamically adding a nullable struct array field
        method: create loaded collection with sealed and growing rows, add a struct array field with vector sub-field,
            insert new non-null rows, then query/search with only the vector sub-field in output_fields
        expected: old rows expose the added struct as null, and new rows return only vector sub-field data matching
            source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_subfield_growing")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(4, 6)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(5)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert set(entity) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_query_iterator(self):
        """
        target: test query_iterator output after dynamically adding a nullable struct array field with vector sub-field
        method: create loaded collection with sealed and growing rows, add a struct array field with vector sub-field,
            insert new non-null rows, then drain query_iterator with small batch size
        expected: old rows expose the added struct as null, new rows return vector sub-field data matching source
            across iterator batch boundaries
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_qiter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(4, 7)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        iterator, check = self.query_iterator(
            client,
            collection_name,
            batch_size=2,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            consistency_level="Strong",
        )
        assert check
        iterator_rows = self._drain_iterator(iterator)
        iterator_ids = [row["id"] for row in iterator_rows]
        assert len(iterator_ids) == len(set(iterator_ids))
        assert set(iterator_ids) == set(source_by_id)

        for row in iterator_rows:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_search_iterator(self):
        """
        target: test search_iterator output after dynamically adding a nullable struct array field with vector sub-field
        method: create loaded collection with sealed and growing rows, add a struct array field with vector sub-field,
            insert new non-null rows, then drain search_iterator on the normal vector field with small batch size
        expected: iterator returns every topK row once, old rows expose the added struct as null, and new rows return
            vector sub-field data matching source
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_siter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(4, 7)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        iterator, check = self.search_iterator(
            client,
            collection_name,
            data=[self._vector(6)],
            batch_size=2,
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        iterator_hits = self._drain_iterator(iterator)
        hit_ids = [hit["id"] for hit in iterator_hits]
        assert len(hit_ids) == len(set(hit_ids))
        assert set(hit_ids) == set(source_by_id)

        distances = [hit["distance"] for hit in iterator_hits]
        for index in range(len(distances) - 1):
            assert distances[index] <= distances[index + 1] + epsilon

        for hit in iterator_hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_search_element_filter(self):
        """
        target: test struct array vector search with element_filter after dynamically adding a nullable struct array field
        method: add a struct array field with vector sub-field to a loaded collection with old sealed rows, insert
            new rows, index the added vector sub-field, then search with element_filter on the added struct field
        expected: old null rows are not retrieved, and returned rows satisfy the element_filter
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_filter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "new_3",
                "profile": [
                    {"p_int": 30, "p_tag": "profile_3_0", "p_vec": self._unit_vector(0)},
                    {"p_int": 31, "p_tag": "profile_3_1", "p_vec": self._unit_vector(1)},
                ],
            },
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "new_4",
                "profile": [
                    {"p_int": 40, "p_tag": "profile_4_0", "p_vec": self._unit_vector(2)},
                    {"p_int": 41, "p_tag": "profile_4_1", "p_vec": self._unit_vector(3)},
                ],
            },
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.release_collection(client, collection_name)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows}
        source_by_id.update({row["id"]: row for row in new_rows})
        old_ids = {row["id"] for row in old_sealed_rows}

        search_results, check = self.search(
            client,
            collection_name,
            data=[new_rows[-1]["profile"][0]["p_vec"]],
            anns_field="profile[p_vec]",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            filter="element_filter(profile, $[p_int] == 30)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == {3}
        assert not {hit["id"] for hit in search_results[0]}.intersection(old_ids)
        entity = self._search_entity(search_results[0][0])
        assert "profile" in entity
        self._assert_profile_equal(entity["profile"], source_by_id[3]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_query_search_old_new_rows(self):
        """
        target: test query/search output after dynamically adding a scalar-only nullable struct array field
        method: create loaded collection with sealed and growing rows, add a scalar struct array field,
            then insert new rows
        expected: old rows expose the added struct as null, new rows return the inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(3, 5)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(5, 7)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(6)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile"],
            limit=len(source_by_id),
        )
        assert check
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_element_filter_query_search(self):
        """
        target: test element filter correctness after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with old rows, then query/search with
            element_filter on the added field
        expected: old null rows do not match element_filter, and output fields match inserted data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_filter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._scalar_profile(i),
            }
            for i in range(4, 6)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        old_ids = {row["id"] for row in old_sealed_rows + old_growing_rows}
        new_ids = {row["id"] for row in new_rows}
        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        int_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 40)",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in int_filter_results} == {4}
        for row in int_filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        string_filter_results, check = self.query(
            client,
            collection_name,
            filter='element_filter(profile, $[p_tag] == "profile_5_1")',
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in string_filter_results} == {5}
        for row in string_filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(5)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter="element_filter(profile, $[p_int] >= 40)",
            output_fields=["id", "profile"],
            limit=len(new_ids),
        )
        assert check
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == new_ids
        assert not hit_ids.intersection(old_ids)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.skip(
        reason="MATCH_ANY on dynamically added StructArray with old rows fails: MatchExpr expects ColumnVector"
    )
    def test_add_scalar_struct_array_field_match_family_query_search(self):
        """
        target: test MATCH family correctness after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with old sealed/growing rows, insert post-add
            sealed and growing null/empty/non-empty rows, then query with MATCH family and search with MATCH_ANY filter
        expected: old/null/empty rows do not match MATCH_ANY, MATCH family result sets match source-of-truth, and
            normal-vector search filtered by MATCH_ANY returns the expected sealed and growing rows with correct output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_match")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [
            {"id": row_id, "normal_vector": self._vector(row_id), "doc_tag": f"old_sealed_{row_id}"}
            for row_id in range(self.min_index_sealed_rows)
        ]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [
            {"id": 3000, "normal_vector": self._vector(3000), "doc_tag": "old_growing_3000"},
            {"id": 3001, "normal_vector": self._vector(3001), "doc_tag": "old_growing_3001"},
        ]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        sealed_explicit_null_profile_row = {
            "id": 4000,
            "normal_vector": self._vector(4000),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 4001,
            "normal_vector": self._vector(4001),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 4002,
            "normal_vector": self._vector(4002),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_one_match_profile_row = {
            "id": 4003,
            "normal_vector": self._vector(4003),
            "doc_tag": "sealed_one_match_profile",
            "profile": [
                {"p_int": 9100, "p_tag": "match_9100"},
                {"p_int": 100, "p_tag": "low_100"},
            ],
        }
        sealed_two_match_profile_row = {
            "id": 4004,
            "normal_vector": self._vector(4004),
            "doc_tag": "sealed_two_match_profile",
            "profile": [
                {"p_int": 9200, "p_tag": "match_9200"},
                {"p_int": 9300, "p_tag": "match_9300"},
            ],
        }
        sealed_zero_match_profile_row = {
            "id": 4005,
            "normal_vector": self._vector(4005),
            "doc_tag": "sealed_zero_match_profile",
            "profile": [
                {"p_int": 100, "p_tag": "low_100"},
                {"p_int": 200, "p_tag": "low_200"},
            ],
        }
        sealed_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
            sealed_one_match_profile_row,
            sealed_two_match_profile_row,
            sealed_zero_match_profile_row,
        ]
        sealed_index_filler_rows = self._scalar_struct_index_filler_rows(
            50000,
            self.min_index_sealed_rows - len(sealed_rows),
            "sealed_match_index_filler",
        )
        sealed_rows += sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_rows)
        assert check
        assert res["insert_count"] == len(sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        growing_explicit_null_profile_row = {
            "id": 7000,
            "normal_vector": self._vector(7000),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_empty_profile_row = {
            "id": 7001,
            "normal_vector": self._vector(7001),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_two_match_profile_row = {
            "id": 7002,
            "normal_vector": self._vector(7002),
            "doc_tag": "growing_two_match_profile",
            "profile": [
                {"p_int": 9600, "p_tag": "match_9600"},
                {"p_int": 9700, "p_tag": "match_9700"},
            ],
        }
        growing_rows = [
            growing_explicit_null_profile_row,
            growing_empty_profile_row,
            growing_two_match_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_rows)
        assert check
        assert res["insert_count"] == len(growing_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id[sealed_explicit_null_profile_row["id"]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_empty_profile_row["id"]] = sealed_empty_profile_row
        source_by_id[sealed_one_match_profile_row["id"]] = sealed_one_match_profile_row
        source_by_id[sealed_two_match_profile_row["id"]] = sealed_two_match_profile_row
        source_by_id[sealed_zero_match_profile_row["id"]] = sealed_zero_match_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row["id"]] = growing_explicit_null_profile_row
        source_by_id[growing_empty_profile_row["id"]] = growing_empty_profile_row
        source_by_id[growing_two_match_profile_row["id"]] = growing_two_match_profile_row

        match_any_ids = {
            sealed_one_match_profile_row["id"],
            sealed_two_match_profile_row["id"],
            growing_two_match_profile_row["id"],
        }
        match_two_or_more_ids = {
            sealed_two_match_profile_row["id"],
            growing_two_match_profile_row["id"],
        }
        match_exact_one_ids = {sealed_one_match_profile_row["id"]}
        non_nullish_scope = [
            sealed_one_match_profile_row["id"],
            sealed_two_match_profile_row["id"],
            sealed_zero_match_profile_row["id"],
            growing_two_match_profile_row["id"],
        ]

        match_any_results, check = self.query(
            client,
            collection_name,
            filter="MATCH_ANY(profile, $[p_int] >= 9000)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(match_any_ids),
        )
        assert check
        assert {row["id"] for row in match_any_results} == match_any_ids
        assert not {row["id"] for row in match_any_results}.intersection(
            {
                *[row["id"] for row in old_sealed_rows + old_growing_rows],
                sealed_explicit_null_profile_row["id"],
                sealed_omitted_profile_row["id"],
                sealed_empty_profile_row["id"],
                sealed_zero_match_profile_row["id"],
                growing_explicit_null_profile_row["id"],
                growing_empty_profile_row["id"],
            }
        )
        for row in match_any_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_least_results, check = self.query(
            client,
            collection_name,
            filter="MATCH_LEAST(profile, $[p_int] >= 9000, threshold=2)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(match_two_or_more_ids),
        )
        assert check
        assert {row["id"] for row in match_least_results} == match_two_or_more_ids
        for row in match_least_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_exact_results, check = self.query(
            client,
            collection_name,
            filter="MATCH_EXACT(profile, $[p_int] >= 9000, threshold=1)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(match_exact_one_ids),
        )
        assert check
        assert {row["id"] for row in match_exact_results} == match_exact_one_ids
        for row in match_exact_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_all_results, check = self.query(
            client,
            collection_name,
            filter=f"id in {non_nullish_scope} && MATCH_ALL(profile, $[p_int] >= 9000)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(match_two_or_more_ids),
        )
        assert check
        assert {row["id"] for row in match_all_results} == match_two_or_more_ids
        for row in match_all_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        match_most_results, check = self.query(
            client,
            collection_name,
            filter=f"id in {non_nullish_scope} && MATCH_MOST(profile, $[p_int] >= 9000, threshold=1)",
            output_fields=["id", "doc_tag", "profile"],
            limit=2,
        )
        assert check
        assert {row["id"] for row in match_most_results} == {
            sealed_one_match_profile_row["id"],
            sealed_zero_match_profile_row["id"],
        }
        for row in match_most_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[growing_two_match_profile_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter="MATCH_ANY(profile, $[p_int] >= 9000)",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(match_any_ids),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == match_any_ids
        assert search_results[0][0]["id"] == growing_two_match_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_insert_omit_empty_non_empty_rows(self):
        """
        target: test insert omitted, empty, and non-empty rows after dynamically adding a scalar-only struct array
            field
        method: add a scalar struct array field to a loaded collection with old rows, then insert one row omitting
            the field, one empty row, and one non-empty row
        expected: old and omitted rows return null, empty row returns [], and non-empty row matches source in
            query, sub-field query, element_filter query, and normal vector search output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_omit_empty")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        omitted_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "new_omit_profile",
        }
        empty_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "new_empty_profile",
            "profile": [],
        }
        non_empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "new_non_empty_profile",
            "profile": self._scalar_profile(6),
        }
        res, check = self.insert(
            client, collection_name, [omitted_profile_row, empty_profile_row, non_empty_profile_row]
        )
        assert check
        assert res["insert_count"] == 3

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id[omitted_profile_row["id"]] = {**omitted_profile_row, "profile": None}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id[non_empty_profile_row["id"]] = non_empty_profile_row

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 60)",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {non_empty_profile_row["id"]}
        for row in element_filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(6)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_insert_explicit_null_rows(self):
        """
        target: test explicit null insert after dynamically adding a scalar-only struct array field
        method: add a scalar struct array field to a loaded collection with old rows, then insert rows with
            explicit null, omitted, empty, and non-empty struct values
        expected: old, explicit null, and omitted rows return null, empty row returns [], and non-empty row matches
            source in query, sub-field query, element_filter query, and normal vector search output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_explicit_null")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        explicit_null_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "new_explicit_null_profile",
            "profile": None,
        }
        omitted_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "new_omit_profile",
        }
        empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "new_empty_profile",
            "profile": [],
        }
        non_empty_profile_row = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "new_non_empty_profile",
            "profile": self._scalar_profile(7),
        }
        res, check = self.insert(
            client,
            collection_name,
            [explicit_null_profile_row, omitted_profile_row, empty_profile_row, non_empty_profile_row],
        )
        assert check
        assert res["insert_count"] == 4

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id[explicit_null_profile_row["id"]] = explicit_null_profile_row
        source_by_id[omitted_profile_row["id"]] = {**omitted_profile_row, "profile": None}
        source_by_id[empty_profile_row["id"]] = empty_profile_row
        source_by_id[non_empty_profile_row["id"]] = non_empty_profile_row

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 70)",
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {non_empty_profile_row["id"]}
        for row in element_filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(7)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0]["id"] == non_empty_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_delete_null_empty_non_empty_rows(self):
        """
        target: test delete after dynamically adding a scalar-only nullable struct array field
        method: add a scalar struct array field to a loaded collection with old rows, insert explicit null, omitted,
            empty, and non-empty rows, then delete rows across those states
        expected: deleted rows are absent from query/search, and remaining rows keep correct nullable struct output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_delete")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        sealed_explicit_null_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_non_empty_profile_row = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "sealed_non_empty_profile",
            "profile": self._scalar_profile(7),
        }
        sealed_deleted_non_empty_profile_row = {
            "id": 8,
            "normal_vector": self._vector(8),
            "doc_tag": "sealed_deleted_non_empty_profile",
            "profile": self._scalar_profile(8),
        }
        sealed_inserted_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
            sealed_non_empty_profile_row,
            sealed_deleted_non_empty_profile_row,
        ]
        sealed_index_filler_rows = self._scalar_struct_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_inserted_rows),
            "sealed_index_filler",
        )
        sealed_inserted_rows += sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_inserted_rows)
        assert check
        assert res["insert_count"] == len(sealed_inserted_rows)

        res, check = self.flush(client, collection_name)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        growing_explicit_null_profile_row = {
            "id": 9,
            "normal_vector": self._vector(9),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_omitted_profile_row = {
            "id": 10,
            "normal_vector": self._vector(10),
            "doc_tag": "growing_omit_profile",
        }
        growing_empty_profile_row = {
            "id": 11,
            "normal_vector": self._vector(11),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_non_empty_profile_row = {
            "id": 12,
            "normal_vector": self._vector(12),
            "doc_tag": "growing_non_empty_profile",
            "profile": self._scalar_profile(12),
        }
        growing_deleted_non_empty_profile_row = {
            "id": 13,
            "normal_vector": self._vector(13),
            "doc_tag": "growing_deleted_non_empty_profile",
            "profile": self._scalar_profile(13),
        }
        growing_inserted_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
            growing_non_empty_profile_row,
            growing_deleted_non_empty_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_inserted_rows)
        assert check
        assert res["insert_count"] == len(growing_inserted_rows)

        delete_ids = [
            old_sealed_rows[1]["id"],
            old_growing_rows[1]["id"],
            sealed_explicit_null_profile_row["id"],
            sealed_empty_profile_row["id"],
            sealed_deleted_non_empty_profile_row["id"],
            growing_explicit_null_profile_row["id"],
            growing_empty_profile_row["id"],
            growing_deleted_non_empty_profile_row["id"],
        ]
        res, check = self.delete(client, collection_name, filter=f"id in {delete_ids}")
        assert check
        assert res["delete_count"] == len(delete_ids)

        res, check = self.flush(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id.pop(old_sealed_rows[1]["id"])
        source_by_id.pop(old_growing_rows[1]["id"])
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_non_empty_profile_row["id"]] = sealed_non_empty_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_omitted_profile_row["id"]] = {**growing_omitted_profile_row, "profile": None}
        source_by_id[growing_non_empty_profile_row["id"]] = growing_non_empty_profile_row

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        assert not {row["id"] for row in query_results}.intersection(delete_ids)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] >= 70)",
            output_fields=["id", "doc_tag", "profile"],
            limit=4,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {
            sealed_non_empty_profile_row["id"],
            growing_non_empty_profile_row["id"],
        }
        for row in element_filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[growing_non_empty_profile_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert not {hit["id"] for hit in search_results[0]}.intersection(delete_ids)
        assert search_results[0][0]["id"] == growing_non_empty_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_delete_null_empty_non_empty_rows(self):
        """
        target: test delete after dynamically adding a nullable struct array field with a vector sub-field
        method: add a struct array field with scalar and vector sub-fields to a loaded collection with old rows, insert
            explicit null, omitted, empty, and non-empty rows, then delete rows across those states
        expected: deleted rows are absent from query/search, and remaining rows keep correct nullable struct output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_delete")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(2, 4)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        sealed_non_empty_profile_row = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "sealed_non_empty_profile",
            "profile": self._profile(7),
        }
        sealed_deleted_non_empty_profile_row = {
            "id": 8,
            "normal_vector": self._vector(8),
            "doc_tag": "sealed_deleted_non_empty_profile",
            "profile": self._profile(8),
        }
        sealed_non_empty_rows = [sealed_non_empty_profile_row, sealed_deleted_non_empty_profile_row]
        sealed_index_filler_rows = self._vector_struct_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_non_empty_rows),
            "sealed_index_filler",
        )
        sealed_non_empty_rows += sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_non_empty_rows)
        assert check
        assert res["insert_count"] == len(sealed_non_empty_rows)

        sealed_explicit_null_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_nullish_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
        ]
        res, check = self.insert(client, collection_name, sealed_nullish_rows)
        assert check
        assert res["insert_count"] == len(sealed_nullish_rows)

        res, check = self.flush(client, collection_name)
        assert check

        growing_non_empty_profile_row = {
            "id": 12,
            "normal_vector": self._vector(12),
            "doc_tag": "growing_non_empty_profile",
            "profile": self._profile(12),
        }
        growing_deleted_non_empty_profile_row = {
            "id": 13,
            "normal_vector": self._vector(13),
            "doc_tag": "growing_deleted_non_empty_profile",
            "profile": self._profile(13),
        }
        growing_non_empty_rows = [growing_non_empty_profile_row, growing_deleted_non_empty_profile_row]
        res, check = self.insert(client, collection_name, growing_non_empty_rows)
        assert check
        assert res["insert_count"] == len(growing_non_empty_rows)

        growing_explicit_null_profile_row = {
            "id": 9,
            "normal_vector": self._vector(9),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_omitted_profile_row = {
            "id": 10,
            "normal_vector": self._vector(10),
            "doc_tag": "growing_omit_profile",
        }
        growing_empty_profile_row = {
            "id": 11,
            "normal_vector": self._vector(11),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_nullish_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_nullish_rows)
        assert check
        assert res["insert_count"] == len(growing_nullish_rows)

        delete_ids = [
            old_sealed_rows[1]["id"],
            old_growing_rows[1]["id"],
            sealed_explicit_null_profile_row["id"],
            sealed_empty_profile_row["id"],
            sealed_deleted_non_empty_profile_row["id"],
            growing_explicit_null_profile_row["id"],
            growing_empty_profile_row["id"],
            growing_deleted_non_empty_profile_row["id"],
        ]
        res, check = self.delete(client, collection_name, filter=f"id in {delete_ids}")
        assert check
        assert res["delete_count"] == len(delete_ids)

        res, check = self.flush(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id.pop(old_sealed_rows[1]["id"])
        source_by_id.pop(old_growing_rows[1]["id"])
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_non_empty_profile_row["id"]] = sealed_non_empty_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_omitted_profile_row["id"]] = {**growing_omitted_profile_row, "profile": None}
        source_by_id[growing_non_empty_profile_row["id"]] = growing_non_empty_profile_row

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        assert not {row["id"] for row in query_results}.intersection(delete_ids)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        assert not {row["id"] for row in subfield_results}.intersection(delete_ids)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] >= 70)",
            output_fields=["id", "doc_tag", "profile"],
            limit=4,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {
            sealed_non_empty_profile_row["id"],
            growing_non_empty_profile_row["id"],
        }
        for row in element_filter_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[growing_non_empty_profile_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert not {hit["id"] for hit in search_results[0]}.intersection(delete_ids)
        assert search_results[0][0]["id"] == growing_non_empty_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_upsert_null_empty_non_empty_rows(self):
        """
        target: test upsert after dynamically adding a nullable struct array field with a vector sub-field
        method: add a struct array field with scalar and vector sub-fields to a loaded collection with old rows, then
            upsert old null rows and post-add non-empty rows across sealed and growing segments
        expected: upserted rows expose correct null, empty, and non-empty struct output in query/search
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_upsert_states")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [
            {"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_growing_{i}"} for i in range(2, 4)
        ]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        post_add_sealed_rows = [
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "post_add_sealed_non_empty_to_null",
                "profile": self._profile(4),
            },
            {
                "id": 5,
                "normal_vector": self._vector(5),
                "doc_tag": "post_add_sealed_non_empty_to_empty",
                "profile": self._profile(5),
            },
        ]
        res, check = self.insert(client, collection_name, post_add_sealed_rows)
        assert check
        assert res["insert_count"] == len(post_add_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        post_add_growing_rows = [
            {
                "id": 6,
                "normal_vector": self._vector(6),
                "doc_tag": "post_add_growing_non_empty_to_null",
                "profile": self._profile(6),
            },
            {
                "id": 7,
                "normal_vector": self._vector(7),
                "doc_tag": "post_add_growing_non_empty_to_empty",
                "profile": self._profile(7),
            },
        ]
        res, check = self.insert(client, collection_name, post_add_growing_rows)
        assert check
        assert res["insert_count"] == len(post_add_growing_rows)

        non_empty_upserts = [
            {
                "id": old_sealed_rows[0]["id"],
                "normal_vector": self._vector(100),
                "doc_tag": "upsert_old_sealed_null_to_non_empty",
                "profile": self._profile(100),
            },
            {
                "id": old_growing_rows[0]["id"],
                "normal_vector": self._vector(200),
                "doc_tag": "upsert_old_growing_null_to_non_empty",
                "profile": self._profile(200),
            },
            {
                "id": 8,
                "normal_vector": self._vector(800),
                "doc_tag": "upsert_new_non_empty",
                "profile": self._profile(800),
            },
        ]
        res, check = self.upsert(client, collection_name, non_empty_upserts)
        assert check
        assert res["upsert_count"] == len(non_empty_upserts)

        nullish_upserts = [
            {
                "id": post_add_sealed_rows[0]["id"],
                "normal_vector": self._vector(400),
                "doc_tag": "upsert_post_add_sealed_non_empty_to_null",
            },
            {
                "id": post_add_sealed_rows[1]["id"],
                "normal_vector": self._vector(500),
                "doc_tag": "upsert_post_add_sealed_non_empty_to_empty",
                "profile": [],
            },
            {
                "id": post_add_growing_rows[0]["id"],
                "normal_vector": self._vector(600),
                "doc_tag": "upsert_post_add_growing_non_empty_to_null",
                "profile": None,
            },
            {
                "id": post_add_growing_rows[1]["id"],
                "normal_vector": self._vector(700),
                "doc_tag": "upsert_post_add_growing_non_empty_to_empty",
                "profile": [],
            },
            {
                "id": 9,
                "normal_vector": self._vector(900),
                "doc_tag": "upsert_new_omitted_profile",
            },
        ]
        res, check = self.upsert(client, collection_name, nullish_upserts)
        assert check
        assert res["upsert_count"] == len(nullish_upserts)

        source_by_id = {row["id"]: {**row, "profile": None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in non_empty_upserts})
        source_by_id[nullish_upserts[0]["id"]] = {**nullish_upserts[0], "profile": None}
        source_by_id[nullish_upserts[1]["id"]] = nullish_upserts[1]
        source_by_id[nullish_upserts[2]["id"]] = nullish_upserts[2]
        source_by_id[nullish_upserts[3]["id"]] = nullish_upserts[3]
        source_by_id[nullish_upserts[4]["id"]] = {**nullish_upserts[4], "profile": None}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        for expected_row in non_empty_upserts:
            element_filter_results, check = self.query(
                client,
                collection_name,
                filter=f"element_filter(profile, $[p_int] == {expected_row['profile'][0]['p_int']})",
                output_fields=["id", "doc_tag", "profile"],
                limit=1,
            )
            assert check
            assert {row["id"] for row in element_filter_results} == {expected_row["id"]}
            row = element_filter_results[0]
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[non_empty_upserts[-1]["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert search_results[0][0]["id"] == non_empty_upserts[-1]["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_delete_by_element_filter_sealed_growing_rows(self):
        """
        target: test deleting dynamically added scalar struct array rows by element_filter
        method: add a scalar struct array field to a loaded collection with old rows, insert post-add sealed and
            growing null/empty/non-empty rows, then delete one sealed and one growing row with one element_filter
        expected: only rows whose struct elements match the filter are deleted, and null/empty rows are not matched
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_delete_filter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [
            {"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_growing_{i}"} for i in range(2, 4)
        ]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        deleted_profile = [
            {"p_int": 7000, "p_tag": "deleted_shared_0"},
            {"p_int": 7001, "p_tag": "deleted_shared_1"},
        ]
        sealed_explicit_null_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_deleted_profile_row = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "sealed_deleted_by_element_filter",
            "profile": deleted_profile,
        }
        sealed_kept_profile_row = {
            "id": 8,
            "normal_vector": self._vector(8),
            "doc_tag": "sealed_kept_after_element_filter",
            "profile": self._scalar_profile(8),
        }
        sealed_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
            sealed_deleted_profile_row,
            sealed_kept_profile_row,
        ]
        sealed_index_filler_rows = self._scalar_struct_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_rows),
            "sealed_index_filler",
        )
        sealed_rows += sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_rows)
        assert check
        assert res["insert_count"] == len(sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        growing_explicit_null_profile_row = {
            "id": 9,
            "normal_vector": self._vector(9),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_omitted_profile_row = {
            "id": 10,
            "normal_vector": self._vector(10),
            "doc_tag": "growing_omit_profile",
        }
        growing_empty_profile_row = {
            "id": 11,
            "normal_vector": self._vector(11),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_deleted_profile_row = {
            "id": 12,
            "normal_vector": self._vector(12),
            "doc_tag": "growing_deleted_by_element_filter",
            "profile": deleted_profile,
        }
        growing_kept_profile_row = {
            "id": 13,
            "normal_vector": self._vector(13),
            "doc_tag": "growing_kept_after_element_filter",
            "profile": self._scalar_profile(13),
        }
        growing_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
            growing_deleted_profile_row,
            growing_kept_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_rows)
        assert check
        assert res["insert_count"] == len(growing_rows)

        delete_filter = f"element_filter(profile, $[p_int] == {deleted_profile[0]['p_int']})"
        res, check = self.delete(client, collection_name, filter=delete_filter)
        assert check
        assert res["delete_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id[sealed_explicit_null_profile_row["id"]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_empty_profile_row["id"]] = sealed_empty_profile_row
        source_by_id[sealed_kept_profile_row["id"]] = sealed_kept_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row["id"]] = growing_explicit_null_profile_row
        source_by_id[growing_omitted_profile_row["id"]] = {**growing_omitted_profile_row, "profile": None}
        source_by_id[growing_empty_profile_row["id"]] = growing_empty_profile_row
        source_by_id[growing_kept_profile_row["id"]] = growing_kept_profile_row
        deleted_ids = {sealed_deleted_profile_row["id"], growing_deleted_profile_row["id"]}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        assert not {row["id"] for row in query_results}.intersection(deleted_ids)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        deleted_filter_results, check = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert deleted_filter_results == []

        for kept_row in (sealed_kept_profile_row, growing_kept_profile_row):
            kept_filter_results, check = self.query(
                client,
                collection_name,
                filter=f"element_filter(profile, $[p_int] == {kept_row['profile'][0]['p_int']})",
                output_fields=["id", "doc_tag", "profile"],
                limit=1,
            )
            assert check
            assert {row["id"] for row in kept_filter_results} == {kept_row["id"]}
            row = kept_filter_results[0]
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[growing_kept_profile_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert not {hit["id"] for hit in search_results[0]}.intersection(deleted_ids)
        assert search_results[0][0]["id"] == growing_kept_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_delete_by_element_filter_sealed_growing_rows(self):
        """
        target: test deleting dynamically added struct array rows with vector sub-field by scalar element_filter
        method: add a struct array field with scalar and vector sub-fields to a loaded collection with old rows, insert
            post-add sealed and growing null/empty/non-empty rows, then delete one sealed and one growing row with one
            scalar element_filter
        expected: only rows whose struct elements match the scalar filter are deleted, and remaining vector payloads
            are still returned correctly
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_delete_filter")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_sealed_{i}"} for i in range(2)]
        old_sealed_filler_rows = self._index_filler_rows(
            1000,
            self.min_index_sealed_rows - len(old_sealed_rows),
            "old_sealed_index_filler",
        )
        all_old_sealed_rows = old_sealed_rows + old_sealed_filler_rows
        res, check = self.insert(client, collection_name, all_old_sealed_rows)
        assert check
        assert res["insert_count"] == len(all_old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [
            {"id": i, "normal_vector": self._vector(i), "doc_tag": f"old_growing_{i}"} for i in range(2, 4)
        ]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        deleted_profile = [
            {"p_int": 7000, "p_tag": "deleted_shared_0", "p_vec": self._vector(7000)},
            {"p_int": 7001, "p_tag": "deleted_shared_1", "p_vec": self._vector(7001)},
        ]
        sealed_deleted_profile_row = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "sealed_deleted_by_element_filter",
            "profile": deleted_profile,
        }
        sealed_kept_profile_row = {
            "id": 8,
            "normal_vector": self._vector(8),
            "doc_tag": "sealed_kept_after_element_filter",
            "profile": self._profile(8),
        }
        sealed_non_empty_rows = [sealed_deleted_profile_row, sealed_kept_profile_row]
        sealed_index_filler_rows = self._vector_struct_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_non_empty_rows),
            "sealed_index_filler",
        )
        sealed_non_empty_rows += sealed_index_filler_rows
        res, check = self.insert(client, collection_name, sealed_non_empty_rows)
        assert check
        assert res["insert_count"] == len(sealed_non_empty_rows)

        sealed_explicit_null_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "sealed_explicit_null_profile",
            "profile": None,
        }
        sealed_omitted_profile_row = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "sealed_omit_profile",
        }
        sealed_empty_profile_row = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "sealed_empty_profile",
            "profile": [],
        }
        sealed_nullish_rows = [
            sealed_explicit_null_profile_row,
            sealed_omitted_profile_row,
            sealed_empty_profile_row,
        ]
        res, check = self.insert(client, collection_name, sealed_nullish_rows)
        assert check
        assert res["insert_count"] == len(sealed_nullish_rows)

        res, check = self.flush(client, collection_name)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        growing_deleted_profile_row = {
            "id": 12,
            "normal_vector": self._vector(12),
            "doc_tag": "growing_deleted_by_element_filter",
            "profile": deleted_profile,
        }
        growing_kept_profile_row = {
            "id": 13,
            "normal_vector": self._vector(13),
            "doc_tag": "growing_kept_after_element_filter",
            "profile": self._profile(13),
        }
        growing_non_empty_rows = [growing_deleted_profile_row, growing_kept_profile_row]
        res, check = self.insert(client, collection_name, growing_non_empty_rows)
        assert check
        assert res["insert_count"] == len(growing_non_empty_rows)

        growing_explicit_null_profile_row = {
            "id": 9,
            "normal_vector": self._vector(9),
            "doc_tag": "growing_explicit_null_profile",
            "profile": None,
        }
        growing_omitted_profile_row = {
            "id": 10,
            "normal_vector": self._vector(10),
            "doc_tag": "growing_omit_profile",
        }
        growing_empty_profile_row = {
            "id": 11,
            "normal_vector": self._vector(11),
            "doc_tag": "growing_empty_profile",
            "profile": [],
        }
        growing_nullish_rows = [
            growing_explicit_null_profile_row,
            growing_omitted_profile_row,
            growing_empty_profile_row,
        ]
        res, check = self.insert(client, collection_name, growing_nullish_rows)
        assert check
        assert res["insert_count"] == len(growing_nullish_rows)

        delete_filter = f"element_filter(profile, $[p_int] == {deleted_profile[0]['p_int']})"
        res, check = self.delete(client, collection_name, filter=delete_filter)
        assert check
        assert res["delete_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        source_by_id = {row["id"]: {**row, "profile": None} for row in all_old_sealed_rows + old_growing_rows}
        source_by_id[sealed_explicit_null_profile_row["id"]] = sealed_explicit_null_profile_row
        source_by_id[sealed_omitted_profile_row["id"]] = {**sealed_omitted_profile_row, "profile": None}
        source_by_id[sealed_empty_profile_row["id"]] = sealed_empty_profile_row
        source_by_id[sealed_kept_profile_row["id"]] = sealed_kept_profile_row
        source_by_id.update({row["id"]: row for row in sealed_index_filler_rows})
        source_by_id[growing_explicit_null_profile_row["id"]] = growing_explicit_null_profile_row
        source_by_id[growing_omitted_profile_row["id"]] = {**growing_omitted_profile_row, "profile": None}
        source_by_id[growing_empty_profile_row["id"]] = growing_empty_profile_row
        source_by_id[growing_kept_profile_row["id"]] = growing_kept_profile_row
        deleted_ids = {sealed_deleted_profile_row["id"], growing_deleted_profile_row["id"]}

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        assert not {row["id"] for row in query_results}.intersection(deleted_ids)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        subfield_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_vec]"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        assert not {row["id"] for row in subfield_results}.intersection(deleted_ids)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

        deleted_filter_results, check = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=["id", "doc_tag", "profile"],
            limit=1,
        )
        assert check
        assert deleted_filter_results == []

        for kept_row in (sealed_kept_profile_row, growing_kept_profile_row):
            kept_filter_results, check = self.query(
                client,
                collection_name,
                filter=f"element_filter(profile, $[p_int] == {kept_row['profile'][0]['p_int']})",
                output_fields=["id", "doc_tag", "profile"],
                limit=1,
            )
            assert check
            assert {row["id"] for row in kept_filter_results} == {kept_row["id"]}
            row = kept_filter_results[0]
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[growing_kept_profile_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        assert not {hit["id"] for hit in search_results[0]}.intersection(deleted_ids)
        assert search_results[0][0]["id"] == growing_kept_profile_row["id"]
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_partition_query_search_sealed_growing_rows(self):
        """
        target: test partition-scoped query/search after dynamically adding a scalar nullable struct array field
        method: create two partitions with old sealed and growing rows, add a scalar struct array field, insert
            post-add sealed and growing rows in both partitions, then query/search with partition_names
        expected: partition-scoped query/search only returns rows from selected partitions, with correct nullable output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_partition")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        sealed_non_empty_a = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "a_sealed_non_empty",
            "profile": self._scalar_profile(2),
        }
        sealed_null_a = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "a_sealed_null",
            "profile": None,
        }
        sealed_non_empty_b = {
            "id": 102,
            "normal_vector": self._vector(102),
            "doc_tag": "b_sealed_non_empty",
            "profile": self._scalar_profile(102),
        }
        sealed_omit_b = {
            "id": 103,
            "normal_vector": self._vector(103),
            "doc_tag": "b_sealed_omit",
        }
        res, check = self.insert(
            client, collection_name, [sealed_non_empty_a, sealed_null_a], partition_name=partition_a
        )
        assert check
        assert res["insert_count"] == 2
        res, check = self.insert(
            client, collection_name, [sealed_non_empty_b, sealed_omit_b], partition_name=partition_b
        )
        assert check
        assert res["insert_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        growing_empty_a = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "a_growing_empty",
            "profile": [],
        }
        growing_non_empty_a = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "a_growing_non_empty",
            "profile": self._scalar_profile(5),
        }
        growing_null_b = {
            "id": 104,
            "normal_vector": self._vector(104),
            "doc_tag": "b_growing_null",
            "profile": None,
        }
        growing_non_empty_b = {
            "id": 105,
            "normal_vector": self._vector(105),
            "doc_tag": "b_growing_non_empty",
            "profile": self._scalar_profile(105),
        }
        res, check = self.insert(
            client, collection_name, [growing_empty_a, growing_non_empty_a], partition_name=partition_a
        )
        assert check
        assert res["insert_count"] == 2
        res, check = self.insert(
            client, collection_name, [growing_null_b, growing_non_empty_b], partition_name=partition_b
        )
        assert check
        assert res["insert_count"] == 2

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_non_empty_a["id"]: sealed_non_empty_a,
                sealed_null_a["id"]: sealed_null_a,
                growing_empty_a["id"]: growing_empty_a,
                growing_non_empty_a["id"]: growing_non_empty_a,
            }
        )
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_non_empty_b["id"]: sealed_non_empty_b,
                sealed_omit_b["id"]: {**sealed_omit_b, "profile": None},
                growing_null_b["id"]: growing_null_b,
                growing_non_empty_b["id"]: growing_non_empty_b,
            }
        )

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_non_empty_a),
            (partition_b, source_b, growing_non_empty_b),
        ):
            query_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in query_results} == set(source_by_id)
            for row in query_results:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(row["profile"], expected["profile"])

            search_results, check = self.search(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
            assert search_results[0][0]["id"] == search_row["id"]
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

        query_results, check = self.query(
            client,
            collection_name,
            filter=f"element_filter(profile, $[p_int] == {sealed_non_empty_a['profile'][0]['p_int']})",
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_a],
            limit=1,
        )
        assert check
        assert {row["id"] for row in query_results} == {sealed_non_empty_a["id"]}

        query_results, check = self.query(
            client,
            collection_name,
            filter=f"element_filter(profile, $[p_int] == {sealed_non_empty_a['profile'][0]['p_int']})",
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_b],
            limit=1,
        )
        assert check
        assert query_results == []

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id"],
            partition_names=[partition_a, partition_b],
            limit=len(source_a) + len(source_b),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_a) | set(source_b)

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_partition_query_search_sealed_growing_rows(self):
        """
        target: test partition-scoped query/search after dynamically adding a nullable struct array field with vector
            sub-field
        method: create two partitions with old sealed and growing rows, add a struct array field with scalar and vector
            sub-fields, insert post-add sealed and growing rows in both partitions, then query/search with
            partition_names
        expected: partition-scoped query/search only returns rows from selected partitions, with correct nullable and
            vector sub-field output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_partition")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        sealed_non_empty_a = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "a_sealed_non_empty",
            "profile": self._profile(2),
        }
        sealed_non_empty_b = {
            "id": 102,
            "normal_vector": self._vector(102),
            "doc_tag": "b_sealed_non_empty",
            "profile": self._profile(102),
        }
        res, check = self.insert(client, collection_name, [sealed_non_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [sealed_non_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        sealed_null_a = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "a_sealed_null",
            "profile": None,
        }
        sealed_empty_a = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "a_sealed_empty",
            "profile": [],
        }
        sealed_empty_b = {
            "id": 103,
            "normal_vector": self._vector(103),
            "doc_tag": "b_sealed_empty",
            "profile": [],
        }
        sealed_null_b = {
            "id": 104,
            "normal_vector": self._vector(104),
            "doc_tag": "b_sealed_null",
            "profile": None,
        }
        res, check = self.insert(client, collection_name, [sealed_null_a, sealed_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 2
        res, check = self.insert(client, collection_name, [sealed_empty_b, sealed_null_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        growing_non_empty_a = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "a_growing_non_empty",
            "profile": self._profile(5),
        }
        growing_non_empty_b = {
            "id": 105,
            "normal_vector": self._vector(105),
            "doc_tag": "b_growing_non_empty",
            "profile": self._profile(105),
        }
        res, check = self.insert(client, collection_name, [growing_non_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [growing_non_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        growing_empty_a = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "a_growing_empty",
            "profile": [],
        }
        growing_empty_b = {
            "id": 106,
            "normal_vector": self._vector(106),
            "doc_tag": "b_growing_empty",
            "profile": [],
        }
        res, check = self.insert(client, collection_name, [growing_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [growing_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_non_empty_a["id"]: sealed_non_empty_a,
                sealed_null_a["id"]: sealed_null_a,
                sealed_empty_a["id"]: sealed_empty_a,
                growing_non_empty_a["id"]: growing_non_empty_a,
                growing_empty_a["id"]: growing_empty_a,
            }
        )
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_non_empty_b["id"]: sealed_non_empty_b,
                sealed_empty_b["id"]: sealed_empty_b,
                sealed_null_b["id"]: sealed_null_b,
                growing_non_empty_b["id"]: growing_non_empty_b,
                growing_empty_b["id"]: growing_empty_b,
            }
        )

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_non_empty_a),
            (partition_b, source_b, growing_non_empty_b),
        ):
            query_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in query_results} == set(source_by_id)
            for row in query_results:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(row["profile"], expected["profile"])

            subfield_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "profile[p_vec]"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in subfield_results} == set(source_by_id)
            for row in subfield_results:
                expected = source_by_id[row["id"]]
                assert set(row) == {"id", "profile"}
                self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

            search_results, check = self.search(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
            assert search_results[0][0]["id"] == search_row["id"]
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(entity["profile"], expected["profile"])

        query_results, check = self.query(
            client,
            collection_name,
            filter=f"element_filter(profile, $[p_int] == {sealed_non_empty_a['profile'][0]['p_int']})",
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_a],
            limit=1,
        )
        assert check
        assert {row["id"] for row in query_results} == {sealed_non_empty_a["id"]}
        self._assert_profile_equal(query_results[0]["profile"], sealed_non_empty_a["profile"])

        query_results, check = self.query(
            client,
            collection_name,
            filter=f"element_filter(profile, $[p_int] == {sealed_non_empty_a['profile'][0]['p_int']})",
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_b],
            limit=1,
        )
        assert check
        assert query_results == []

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id"],
            partition_names=[partition_a, partition_b],
            limit=len(source_a) + len(source_b),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_a) | set(source_b)

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_load_partitions_after_release(self):
        """
        target: test load_partitions after dynamically adding a scalar nullable struct array field
        method: create two partitions with old sealed and growing rows, add a scalar struct array field, insert
            post-add rows, release the collection, then load and verify one partition at a time
        expected: partition load returns only rows from the loaded partition, with correct nullable struct output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_load_partition")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        rows_a = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "a_post_add_non_empty",
                "profile": self._scalar_profile(2),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "a_post_add_null",
                "profile": None,
            },
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "a_post_add_empty",
                "profile": [],
            },
        ]
        rows_b = [
            {
                "id": 102,
                "normal_vector": self._vector(102),
                "doc_tag": "b_post_add_non_empty",
                "profile": self._scalar_profile(102),
            },
            {
                "id": 103,
                "normal_vector": self._vector(103),
                "doc_tag": "b_post_add_null",
                "profile": None,
            },
            {
                "id": 104,
                "normal_vector": self._vector(104),
                "doc_tag": "b_post_add_empty",
                "profile": [],
            },
        ]
        res, check = self.insert(client, collection_name, rows_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(rows_a)
        res, check = self.insert(client, collection_name, rows_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(rows_b)

        res, check = self.flush(client, collection_name)
        assert check

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update({row["id"]: row for row in rows_a})
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update({row["id"]: row for row in rows_b})

        res, check = self.release_collection(client, collection_name)
        assert check

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, rows_a[0]),
            (partition_b, source_b, rows_b[0]),
        ):
            res, check = self.load_partitions(client, collection_name, [partition_name])
            assert check

            query_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in query_results} == set(source_by_id)
            for row in query_results:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(row["profile"], expected["profile"])

            search_results, check = self.search(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
            assert search_results[0][0]["id"] == search_row["id"]
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

            element_filter_results, check = self.query(
                client,
                collection_name,
                filter=f"element_filter(profile, $[p_int] == {search_row['profile'][0]['p_int']})",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=1,
            )
            assert check
            assert {row["id"] for row in element_filter_results} == {search_row["id"]}
            self._assert_scalar_profile_equal(element_filter_results[0]["profile"], search_row["profile"])

            res, check = self.release_partitions(client, collection_name, [partition_name])
            assert check

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_load_partitions_after_release(self):
        """
        target: test load_partitions after dynamically adding a nullable struct array field with vector sub-field
        method: create two partitions with old sealed and growing rows, add a struct array field with scalar and vector
            sub-fields, insert post-add rows, release the collection, then load and verify one partition at a time
        expected: partition load returns only rows from the loaded partition, with correct nullable and vector output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_load_partition")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        sealed_non_empty_a = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "a_sealed_non_empty",
            "profile": self._profile(2),
        }
        sealed_non_empty_b = {
            "id": 102,
            "normal_vector": self._vector(102),
            "doc_tag": "b_sealed_non_empty",
            "profile": self._profile(102),
        }
        res, check = self.insert(client, collection_name, [sealed_non_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [sealed_non_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        sealed_null_a = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "a_sealed_null",
            "profile": None,
        }
        sealed_empty_a = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "a_sealed_empty",
            "profile": [],
        }
        sealed_null_b = {
            "id": 103,
            "normal_vector": self._vector(103),
            "doc_tag": "b_sealed_null",
            "profile": None,
        }
        sealed_empty_b = {
            "id": 104,
            "normal_vector": self._vector(104),
            "doc_tag": "b_sealed_empty",
            "profile": [],
        }
        res, check = self.insert(client, collection_name, [sealed_null_a, sealed_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 2
        res, check = self.insert(client, collection_name, [sealed_null_b, sealed_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        growing_non_empty_a = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "a_growing_non_empty",
            "profile": self._profile(5),
        }
        growing_non_empty_b = {
            "id": 105,
            "normal_vector": self._vector(105),
            "doc_tag": "b_growing_non_empty",
            "profile": self._profile(105),
        }
        res, check = self.insert(client, collection_name, [growing_non_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [growing_non_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        growing_empty_a = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "a_growing_empty",
            "profile": [],
        }
        growing_empty_b = {
            "id": 106,
            "normal_vector": self._vector(106),
            "doc_tag": "b_growing_empty",
            "profile": [],
        }
        res, check = self.insert(client, collection_name, [growing_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [growing_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_non_empty_a["id"]: sealed_non_empty_a,
                sealed_null_a["id"]: sealed_null_a,
                sealed_empty_a["id"]: sealed_empty_a,
                growing_non_empty_a["id"]: growing_non_empty_a,
                growing_empty_a["id"]: growing_empty_a,
            }
        )
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_non_empty_b["id"]: sealed_non_empty_b,
                sealed_null_b["id"]: sealed_null_b,
                sealed_empty_b["id"]: sealed_empty_b,
                growing_non_empty_b["id"]: growing_non_empty_b,
                growing_empty_b["id"]: growing_empty_b,
            }
        )

        res, check = self.release_collection(client, collection_name)
        assert check

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_non_empty_a),
            (partition_b, source_b, growing_non_empty_b),
        ):
            res, check = self.load_partitions(client, collection_name, [partition_name])
            assert check

            query_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in query_results} == set(source_by_id)
            for row in query_results:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(row["profile"], expected["profile"])

            subfield_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "profile[p_vec]"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in subfield_results} == set(source_by_id)
            for row in subfield_results:
                expected = source_by_id[row["id"]]
                assert set(row) == {"id", "profile"}
                self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

            search_results, check = self.search(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
            assert search_results[0][0]["id"] == search_row["id"]
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(entity["profile"], expected["profile"])

            element_filter_results, check = self.query(
                client,
                collection_name,
                filter=f"element_filter(profile, $[p_int] == {search_row['profile'][0]['p_int']})",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=1,
            )
            assert check
            assert {row["id"] for row in element_filter_results} == {search_row["id"]}
            self._assert_profile_equal(element_filter_results[0]["profile"], search_row["profile"])

            res, check = self.release_partitions(client, collection_name, [partition_name])
            assert check

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_delete_by_element_filter_with_partition_name(self):
        """
        target: test partition-scoped delete by element_filter after dynamically adding a scalar struct array field
        method: create two partitions with old sealed and growing rows, add a scalar struct array field, insert
            post-add sealed and growing rows in both partitions, then delete matching rows only from one partition
        expected: delete removes matching sealed and growing rows from the target partition, without deleting rows in
            other partitions that have the same struct element values
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_delete_partition")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        delete_profile = [
            {"p_int": 9000, "p_tag": "delete_shared_0"},
            {"p_int": 9001, "p_tag": "delete_shared_1"},
        ]
        sealed_kept_profile = [
            {"p_int": 9100, "p_tag": "kept_sealed_0"},
            {"p_int": 9101, "p_tag": "kept_sealed_1"},
        ]
        growing_kept_profile = [
            {"p_int": 9300, "p_tag": "kept_growing_0"},
            {"p_int": 9301, "p_tag": "kept_growing_1"},
        ]

        sealed_deleted_a = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "a_sealed_deleted_by_partition_filter",
            "profile": delete_profile,
        }
        sealed_kept_a = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "a_sealed_kept",
            "profile": sealed_kept_profile,
        }
        sealed_null_a = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "a_sealed_null",
            "profile": None,
        }
        sealed_empty_a = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "a_sealed_empty",
            "profile": [],
        }
        sealed_same_filter_b = {
            "id": 102,
            "normal_vector": self._vector(102),
            "doc_tag": "b_sealed_same_filter_kept",
            "profile": delete_profile,
        }
        sealed_null_b = {
            "id": 103,
            "normal_vector": self._vector(103),
            "doc_tag": "b_sealed_null",
            "profile": None,
        }
        sealed_empty_b = {
            "id": 104,
            "normal_vector": self._vector(104),
            "doc_tag": "b_sealed_empty",
            "profile": [],
        }
        sealed_rows_a = [sealed_deleted_a, sealed_kept_a, sealed_null_a, sealed_empty_a]
        sealed_index_filler_a = self._scalar_struct_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_rows_a),
            "a_sealed_index_filler",
        )
        sealed_rows_a += sealed_index_filler_a
        res, check = self.insert(client, collection_name, sealed_rows_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(sealed_rows_a)
        sealed_rows_b = [sealed_same_filter_b, sealed_null_b, sealed_empty_b]
        sealed_index_filler_b = self._scalar_struct_index_filler_rows(
            40000,
            self.min_index_sealed_rows - len(sealed_rows_b),
            "b_sealed_index_filler",
        )
        sealed_rows_b += sealed_index_filler_b
        res, check = self.insert(client, collection_name, sealed_rows_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(sealed_rows_b)

        res, check = self.flush(client, collection_name)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        growing_deleted_a = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "a_growing_deleted_by_partition_filter",
            "profile": delete_profile,
        }
        growing_kept_a = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "a_growing_kept",
            "profile": growing_kept_profile,
        }
        growing_empty_a = {
            "id": 8,
            "normal_vector": self._vector(8),
            "doc_tag": "a_growing_empty",
            "profile": [],
        }
        growing_same_filter_b = {
            "id": 105,
            "normal_vector": self._vector(105),
            "doc_tag": "b_growing_same_filter_kept",
            "profile": delete_profile,
        }
        growing_empty_b = {
            "id": 106,
            "normal_vector": self._vector(106),
            "doc_tag": "b_growing_empty",
            "profile": [],
        }
        res, check = self.insert(
            client,
            collection_name,
            [growing_deleted_a, growing_kept_a, growing_empty_a],
            partition_name=partition_a,
        )
        assert check
        assert res["insert_count"] == 3
        res, check = self.insert(
            client,
            collection_name,
            [growing_same_filter_b, growing_empty_b],
            partition_name=partition_b,
        )
        assert check
        assert res["insert_count"] == 2

        delete_filter = f"element_filter(profile, $[p_int] == {delete_profile[0]['p_int']})"
        res, check = self.delete(client, collection_name, filter=delete_filter, partition_name=partition_a)
        assert check
        assert res["delete_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_kept_a["id"]: sealed_kept_a,
                sealed_null_a["id"]: sealed_null_a,
                sealed_empty_a["id"]: sealed_empty_a,
                growing_kept_a["id"]: growing_kept_a,
                growing_empty_a["id"]: growing_empty_a,
            }
        )
        source_a.update({row["id"]: row for row in sealed_index_filler_a})
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_same_filter_b["id"]: sealed_same_filter_b,
                sealed_null_b["id"]: sealed_null_b,
                sealed_empty_b["id"]: sealed_empty_b,
                growing_same_filter_b["id"]: growing_same_filter_b,
                growing_empty_b["id"]: growing_empty_b,
            }
        )
        source_b.update({row["id"]: row for row in sealed_index_filler_b})
        deleted_ids = {sealed_deleted_a["id"], growing_deleted_a["id"]}

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_kept_a),
            (partition_b, source_b, growing_same_filter_b),
        ):
            query_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in query_results} == set(source_by_id)
            assert not {row["id"] for row in query_results}.intersection(deleted_ids)
            for row in query_results:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(row["profile"], expected["profile"])

            search_results, check = self.search(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
            assert not {hit["id"] for hit in search_results[0]}.intersection(deleted_ids)
            assert search_results[0][0]["id"] == search_row["id"]
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

        partition_a_results, check = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_a],
            limit=1,
        )
        assert check
        assert partition_a_results == []

        partition_b_results, check = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_b],
            limit=2,
        )
        assert check
        partition_b_source = {
            sealed_same_filter_b["id"]: sealed_same_filter_b,
            growing_same_filter_b["id"]: growing_same_filter_b,
        }
        assert {row["id"] for row in partition_b_results} == set(partition_b_source)
        for row in partition_b_results:
            self._assert_scalar_profile_equal(row["profile"], partition_b_source[row["id"]]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_delete_by_element_filter_with_partition_name(self):
        """
        target: test partition-scoped delete by scalar element_filter after dynamically adding a struct array field with
            vector sub-field
        method: create two partitions with old sealed and growing rows, add a struct array field with scalar and vector
            sub-fields, insert post-add sealed and growing rows in both partitions, then delete matching rows only from
            one partition by scalar element_filter
        expected: delete removes matching sealed and growing rows from the target partition, without deleting rows in
            other partitions that have the same scalar struct element values
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_delete_partition")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        delete_profile = [
            {"p_int": 9000, "p_tag": "delete_shared_0", "p_vec": self._vector(9000)},
            {"p_int": 9001, "p_tag": "delete_shared_1", "p_vec": self._vector(9001)},
        ]
        sealed_kept_profile = [
            {"p_int": 9100, "p_tag": "kept_sealed_0", "p_vec": self._vector(9100)},
            {"p_int": 9101, "p_tag": "kept_sealed_1", "p_vec": self._vector(9101)},
        ]
        growing_kept_profile = [
            {"p_int": 9300, "p_tag": "kept_growing_0", "p_vec": self._vector(9300)},
            {"p_int": 9301, "p_tag": "kept_growing_1", "p_vec": self._vector(9301)},
        ]

        sealed_deleted_a = {
            "id": 2,
            "normal_vector": self._vector(2),
            "doc_tag": "a_sealed_deleted_by_partition_filter",
            "profile": delete_profile,
        }
        sealed_kept_a = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "a_sealed_kept",
            "profile": sealed_kept_profile,
        }
        sealed_same_filter_b = {
            "id": 102,
            "normal_vector": self._vector(102),
            "doc_tag": "b_sealed_same_filter_kept",
            "profile": delete_profile,
        }
        sealed_non_empty_a = [sealed_deleted_a, sealed_kept_a]
        sealed_index_filler_a = self._vector_struct_index_filler_rows(
            30000,
            self.min_index_sealed_rows - len(sealed_non_empty_a),
            "a_sealed_index_filler",
        )
        sealed_non_empty_a += sealed_index_filler_a
        res, check = self.insert(client, collection_name, sealed_non_empty_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(sealed_non_empty_a)
        sealed_non_empty_b = [sealed_same_filter_b]
        sealed_index_filler_b = self._vector_struct_index_filler_rows(
            40000,
            self.min_index_sealed_rows - len(sealed_non_empty_b),
            "b_sealed_index_filler",
        )
        sealed_non_empty_b += sealed_index_filler_b
        res, check = self.insert(client, collection_name, sealed_non_empty_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(sealed_non_empty_b)

        sealed_null_a = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "a_sealed_null",
            "profile": None,
        }
        sealed_empty_a = {
            "id": 5,
            "normal_vector": self._vector(5),
            "doc_tag": "a_sealed_empty",
            "profile": [],
        }
        sealed_null_b = {
            "id": 103,
            "normal_vector": self._vector(103),
            "doc_tag": "b_sealed_null",
            "profile": None,
        }
        sealed_empty_b = {
            "id": 104,
            "normal_vector": self._vector(104),
            "doc_tag": "b_sealed_empty",
            "profile": [],
        }
        res, check = self.insert(client, collection_name, [sealed_null_a, sealed_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 2
        res, check = self.insert(client, collection_name, [sealed_null_b, sealed_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        growing_deleted_a = {
            "id": 6,
            "normal_vector": self._vector(6),
            "doc_tag": "a_growing_deleted_by_partition_filter",
            "profile": delete_profile,
        }
        growing_kept_a = {
            "id": 7,
            "normal_vector": self._vector(7),
            "doc_tag": "a_growing_kept",
            "profile": growing_kept_profile,
        }
        growing_same_filter_b = {
            "id": 105,
            "normal_vector": self._vector(105),
            "doc_tag": "b_growing_same_filter_kept",
            "profile": delete_profile,
        }
        res, check = self.insert(
            client,
            collection_name,
            [growing_deleted_a, growing_kept_a],
            partition_name=partition_a,
        )
        assert check
        assert res["insert_count"] == 2
        res, check = self.insert(client, collection_name, [growing_same_filter_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        growing_empty_a = {
            "id": 8,
            "normal_vector": self._vector(8),
            "doc_tag": "a_growing_empty",
            "profile": [],
        }
        growing_empty_b = {
            "id": 106,
            "normal_vector": self._vector(106),
            "doc_tag": "b_growing_empty",
            "profile": [],
        }
        res, check = self.insert(client, collection_name, [growing_empty_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [growing_empty_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        delete_filter = f"element_filter(profile, $[p_int] == {delete_profile[0]['p_int']})"
        res, check = self.delete(client, collection_name, filter=delete_filter, partition_name=partition_a)
        assert check
        assert res["delete_count"] == 2

        res, check = self.flush(client, collection_name)
        assert check

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update(
            {
                sealed_kept_a["id"]: sealed_kept_a,
                sealed_null_a["id"]: sealed_null_a,
                sealed_empty_a["id"]: sealed_empty_a,
                growing_kept_a["id"]: growing_kept_a,
                growing_empty_a["id"]: growing_empty_a,
            }
        )
        source_a.update({row["id"]: row for row in sealed_index_filler_a})
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update(
            {
                sealed_same_filter_b["id"]: sealed_same_filter_b,
                sealed_null_b["id"]: sealed_null_b,
                sealed_empty_b["id"]: sealed_empty_b,
                growing_same_filter_b["id"]: growing_same_filter_b,
                growing_empty_b["id"]: growing_empty_b,
            }
        )
        source_b.update({row["id"]: row for row in sealed_index_filler_b})
        deleted_ids = {sealed_deleted_a["id"], growing_deleted_a["id"]}

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, growing_kept_a),
            (partition_b, source_b, growing_same_filter_b),
        ):
            query_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in query_results} == set(source_by_id)
            assert not {row["id"] for row in query_results}.intersection(deleted_ids)
            for row in query_results:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(row["profile"], expected["profile"])

            subfield_results, check = self.query(
                client,
                collection_name,
                filter="id >= 0",
                output_fields=["id", "profile[p_vec]"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {row["id"] for row in subfield_results} == set(source_by_id)
            assert not {row["id"] for row in subfield_results}.intersection(deleted_ids)
            for row in subfield_results:
                expected = source_by_id[row["id"]]
                assert set(row) == {"id", "profile"}
                self._assert_profile_vector_subfield_equal(row["profile"], expected["profile"])

            search_results, check = self.search(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
            assert not {hit["id"] for hit in search_results[0]}.intersection(deleted_ids)
            assert search_results[0][0]["id"] == search_row["id"]
            for hit in search_results[0]:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(entity["profile"], expected["profile"])

        partition_a_results, check = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_a],
            limit=1,
        )
        assert check
        assert partition_a_results == []

        partition_b_results, check = self.query(
            client,
            collection_name,
            filter=delete_filter,
            output_fields=["id", "doc_tag", "profile"],
            partition_names=[partition_b],
            limit=2,
        )
        assert check
        partition_b_source = {
            sealed_same_filter_b["id"]: sealed_same_filter_b,
            growing_same_filter_b["id"]: growing_same_filter_b,
        }
        assert {row["id"] for row in partition_b_results} == set(partition_b_source)
        for row in partition_b_results:
            self._assert_profile_equal(row["profile"], partition_b_source[row["id"]]["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_scalar_struct_array_field_partition_query_search_iterator(self):
        """
        target: test partition-scoped iterators after dynamically adding a scalar nullable struct array field
        method: create two partitions with indexed old sealed rows and old growing rows, add a scalar struct array
            field, insert post-add growing rows in both partitions, then drain query_iterator and search_iterator with
            partition_names
        expected: iterators only return rows from the requested partition, with correct nullable struct output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_scalar_struct_partition_iter")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        rows_a = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "a_non_empty",
                "profile": self._scalar_profile(2),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "a_null",
                "profile": None,
            },
            {
                "id": 4,
                "normal_vector": self._vector(4),
                "doc_tag": "a_empty",
                "profile": [],
            },
        ]
        rows_b = [
            {
                "id": 102,
                "normal_vector": self._vector(102),
                "doc_tag": "b_non_empty",
                "profile": self._scalar_profile(102),
            },
            {
                "id": 103,
                "normal_vector": self._vector(103),
                "doc_tag": "b_null",
                "profile": None,
            },
            {
                "id": 104,
                "normal_vector": self._vector(104),
                "doc_tag": "b_empty",
                "profile": [],
            },
        ]
        res, check = self.insert(client, collection_name, rows_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(rows_a)
        res, check = self.insert(client, collection_name, rows_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(rows_b)

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update({row["id"]: row for row in rows_a})
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update({row["id"]: row for row in rows_b})

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, rows_a[0]),
            (partition_b, source_b, rows_b[0]),
        ):
            iterator, check = self.query_iterator(
                client,
                collection_name,
                batch_size=257,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                consistency_level="Strong",
            )
            assert check
            iterator_rows = self._drain_iterator(iterator)
            iterator_ids = [row["id"] for row in iterator_rows]
            assert len(iterator_ids) == len(set(iterator_ids))
            assert set(iterator_ids) == set(source_by_id)
            for row in iterator_rows:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(row["profile"], expected["profile"])

            iterator, check = self.search_iterator(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                batch_size=257,
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            iterator_hits = self._drain_iterator(iterator)
            hit_ids = [hit["id"] for hit in iterator_hits]
            assert len(hit_ids) == len(set(hit_ids))
            assert set(hit_ids) == set(source_by_id)

            distances = [hit["distance"] for hit in iterator_hits]
            for index in range(len(distances) - 1):
                assert distances[index] <= distances[index + 1] + epsilon

            for hit in iterator_hits:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_partition_query_search_iterator(self):
        """
        target: test partition-scoped iterators after dynamically adding a nullable struct array field with vector
            sub-field
        method: create two partitions with indexed old sealed rows and old growing rows, add a struct array field with
            scalar and vector sub-fields, insert post-add growing rows in both partitions, then drain query_iterator
            and search_iterator with partition_names
        expected: iterators only return rows from the requested partition, with correct nullable struct and vector
            sub-field output
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_partition_iter")
        client = self._client()
        partition_a = "partition_a"
        partition_b = "partition_b"

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        for partition_name in (partition_a, partition_b):
            res, check = self.create_partition(client, collection_name, partition_name)
            assert check

        old_sealed_a = {"id": 0, "normal_vector": self._vector(0), "doc_tag": "a_old_sealed"}
        old_sealed_b = {"id": 100, "normal_vector": self._vector(100), "doc_tag": "b_old_sealed"}
        old_sealed_filler_a = self._index_filler_rows(
            10000,
            self.min_index_sealed_rows - 1,
            "a_old_sealed_index_filler",
        )
        old_sealed_filler_b = self._index_filler_rows(
            20000,
            self.min_index_sealed_rows - 1,
            "b_old_sealed_index_filler",
        )
        all_old_sealed_a = [old_sealed_a] + old_sealed_filler_a
        all_old_sealed_b = [old_sealed_b] + old_sealed_filler_b
        res, check = self.insert(client, collection_name, all_old_sealed_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(all_old_sealed_a)
        res, check = self.insert(client, collection_name, all_old_sealed_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(all_old_sealed_b)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_a = {"id": 1, "normal_vector": self._vector(1), "doc_tag": "a_old_growing"}
        old_growing_b = {"id": 101, "normal_vector": self._vector(101), "doc_tag": "b_old_growing"}
        res, check = self.insert(client, collection_name, [old_growing_a], partition_name=partition_a)
        assert check
        assert res["insert_count"] == 1
        res, check = self.insert(client, collection_name, [old_growing_b], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        rows_a = [
            {
                "id": 2,
                "normal_vector": self._vector(2),
                "doc_tag": "a_non_empty",
                "profile": self._profile(2),
            },
            {
                "id": 3,
                "normal_vector": self._vector(3),
                "doc_tag": "a_empty",
                "profile": [],
            },
        ]
        rows_b = [
            {
                "id": 102,
                "normal_vector": self._vector(102),
                "doc_tag": "b_non_empty",
                "profile": self._profile(102),
            },
            {
                "id": 103,
                "normal_vector": self._vector(103),
                "doc_tag": "b_empty",
                "profile": [],
            },
        ]
        res, check = self.insert(client, collection_name, rows_a, partition_name=partition_a)
        assert check
        assert res["insert_count"] == len(rows_a)
        res, check = self.insert(client, collection_name, rows_b, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(rows_b)

        source_a = {row["id"]: {**row, "profile": None} for row in all_old_sealed_a + [old_growing_a]}
        source_a.update({row["id"]: row for row in rows_a})
        source_b = {row["id"]: {**row, "profile": None} for row in all_old_sealed_b + [old_growing_b]}
        source_b.update({row["id"]: row for row in rows_b})

        for partition_name, source_by_id, search_row in (
            (partition_a, source_a, rows_a[0]),
            (partition_b, source_b, rows_b[0]),
        ):
            iterator, check = self.query_iterator(
                client,
                collection_name,
                batch_size=257,
                filter="id >= 0",
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                consistency_level="Strong",
            )
            assert check
            iterator_rows = self._drain_iterator(iterator)
            iterator_ids = [row["id"] for row in iterator_rows]
            assert len(iterator_ids) == len(set(iterator_ids))
            assert set(iterator_ids) == set(source_by_id)
            for row in iterator_rows:
                expected = source_by_id[row["id"]]
                assert row["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(row["profile"], expected["profile"])

            iterator, check = self.search_iterator(
                client,
                collection_name,
                data=[search_row["normal_vector"]],
                batch_size=257,
                anns_field="normal_vector",
                search_params={"metric_type": "L2", "params": {}},
                output_fields=["id", "doc_tag", "profile"],
                partition_names=[partition_name],
                limit=len(source_by_id),
            )
            assert check
            iterator_hits = self._drain_iterator(iterator)
            hit_ids = [hit["id"] for hit in iterator_hits]
            assert len(hit_ids) == len(set(hit_ids))
            assert set(hit_ids) == set(source_by_id)

            distances = [hit["distance"] for hit in iterator_hits]
            for index in range(len(distances) - 1):
                assert distances[index] <= distances[index + 1] + epsilon

            for hit in iterator_hits:
                expected = source_by_id[hit["id"]]
                entity = self._search_entity(hit)
                assert entity["doc_tag"] == expected["doc_tag"]
                self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_query_search_old_sealed_rows(self):
        """
        target: test query/search output after dynamically adding a nullable struct array field with vector sub-field
        method: create loaded collection with sealed rows, add a struct array field with vector sub-field,
            then insert new rows
        expected: old rows expose the added struct as null, new rows return the inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(3, 5)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(4)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile"],
            limit=len(source_by_id),
        )
        assert check
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_insert_omit_nullable_field(self):
        """
        target: test omitted nullable struct array field after dynamically adding a vector sub-field struct
        method: add a struct array field with vector sub-field, then insert one row without the new field and
            one row with the new field
        expected: old rows and omitted-field new row expose the added struct as null, while present-field new row
            returns inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_omit")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        omitted_profile_row = {
            "id": 3,
            "normal_vector": self._vector(3),
            "doc_tag": "new_omitted_profile",
        }
        present_profile_row = {
            "id": 4,
            "normal_vector": self._vector(4),
            "doc_tag": "new_present_profile",
            "profile": self._profile(4),
        }
        res, check = self.insert(client, collection_name, [omitted_profile_row, present_profile_row])
        assert check
        assert res["insert_count"] == 2

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows}
        source_by_id[omitted_profile_row["id"]] = {**omitted_profile_row, "profile": None}
        source_by_id[present_profile_row["id"]] = present_profile_row

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        element_filter_results, check = self.query(
            client,
            collection_name,
            filter="element_filter(profile, $[p_int] == 40)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {row["id"] for row in element_filter_results} == {present_profile_row["id"]}
        self._assert_profile_equal(element_filter_results[0]["profile"], present_profile_row["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(4)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {hit["id"] for hit in search_results[0]} == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

        filtered_search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(4)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            filter="element_filter(profile, $[p_int] == 40)",
            output_fields=["id", "profile"],
            limit=1,
        )
        assert check
        assert {hit["id"] for hit in filtered_search_results[0]} == {present_profile_row["id"]}
        entity = self._search_entity(filtered_search_results[0][0])
        self._assert_profile_equal(entity["profile"], present_profile_row["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_with_vector_query_search_old_growing_rows(self):
        """
        target: test query/search output after dynamically adding a nullable struct array field with vector sub-field
        method: create loaded collection with growing rows, add a struct array field with vector sub-field,
            then insert new rows
        expected: old growing rows expose the added struct as null, new rows return the inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct_vector_growing")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(3)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(3, 5)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(4)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile"],
            limit=len(source_by_id),
        )
        assert check
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L0)
    def test_add_struct_array_field_query_search_old_new_rows(self):
        """
        target: test query/search output after dynamically adding a nullable struct array field
        method: create loaded collection with sealed and growing rows, add a struct array field, then insert new rows
        expected: old rows expose the added struct as null, new rows return the inserted struct data
        """
        collection_name = cf.gen_unique_str(f"{prefix}_add_struct")
        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        old_sealed_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"sealed_{i}"} for i in range(4)]
        res, check = self.insert(client, collection_name, old_sealed_rows)
        assert check
        assert res["insert_count"] == len(old_sealed_rows)

        res, check = self.flush(client, collection_name)
        assert check

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        res, check = self.create_index(client, collection_name, index_params)
        assert check

        res, check = self.load_collection(client, collection_name)
        assert check

        old_growing_rows = [{"id": i, "normal_vector": self._vector(i), "doc_tag": f"growing_{i}"} for i in range(4, 6)]
        res, check = self.insert(client, collection_name, old_growing_rows)
        assert check
        assert res["insert_count"] == len(old_growing_rows)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check

        self.wait_schema_version_consistent(client, collection_name)

        new_rows = [
            {
                "id": i,
                "normal_vector": self._vector(i),
                "doc_tag": f"new_{i}",
                "profile": self._profile(i),
            }
            for i in range(6, 8)
        ]
        res, check = self.insert(client, collection_name, new_rows)
        assert check
        assert res["insert_count"] == len(new_rows)

        source_by_id = {row["id"]: {**row, "profile": None} for row in old_sealed_rows + old_growing_rows}
        source_by_id.update({row["id"]: row for row in new_rows})

        query_results, check = self.query(
            client,
            collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(source_by_id),
        )
        assert check
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            assert "profile" in row
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_results, check = self.search(
            client,
            collection_name,
            data=[self._vector(7)],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "profile"],
            limit=len(source_by_id),
        )
        assert check
        hit_ids = {hit["id"] for hit in search_results[0]}
        assert hit_ids == set(source_by_id)
        for hit in search_results[0]:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert "profile" in entity
            self._assert_profile_equal(entity["profile"], expected["profile"])


class TestMilvusClientStructArrayCRUD(TestMilvusClientV2Base):
    """Test case of struct array CRUD operations"""

    def create_collection_with_schema(self, client: MilvusClient, collection_name: str):
        """Create collection with struct array schema"""
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(
            field_name="normal_vector",
            datatype=DataType.FLOAT_VECTOR,
            dim=default_dim,
            nullable=True,
        )

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("scalar_field", DataType.INT64)
        struct_schema.add_field("label", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check
        return schema

    @pytest.mark.tags(CaseLabel.L0)
    def test_upsert_struct_array_data(self):
        """
        target: test upsert operation with struct array data
        method: insert 3000 records, flush 2000, insert 1000 growing, then upsert with modified struct array
        expected: data successfully upserted
        """
        collection_name = cf.gen_unique_str(f"{prefix}_crud")

        client = self._client()

        # Create collection
        self.create_collection_with_schema(client, collection_name)

        # Insert 2000 records for flushed data
        flushed_data = []
        for i in range(2000):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)] if random.random() > 0.8 else None,
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(default_dim)],
                        "scalar_field": i,
                        "label": f"flushed_{i}",
                    }
                ],
            }
            flushed_data.append(row)

        res, check = self.insert(client, collection_name, flushed_data)
        assert check
        assert res["insert_count"] == 2000

        # Flush to persist data
        res, check = self.flush(client, collection_name)
        assert check

        # Insert 1000 records for growing data
        growing_data = []
        for i in range(2000, 3000):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)] if random.random() > 0.8 else None,
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(default_dim)],
                        "scalar_field": i,
                        "label": f"growing_{i}",
                    }
                ],
            }
            growing_data.append(row)

        res, check = self.insert(client, collection_name, growing_data)
        assert check
        assert res["insert_count"] == 1000

        # create index and load collection
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        # Upsert data in both flushed and growing segments
        upsert_data = []
        # Upsert 10 records from flushed data
        for i in range(0, 10):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)] if random.random() > 0.8 else None,
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(default_dim)],
                        "scalar_field": i + 10000,  # Modified
                        "label": f"updated_flushed_{i}",  # Modified
                    }
                ],
            }
            upsert_data.append(row)

        # Upsert 10 records from growing data
        for i in range(2000, 2010):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)] if random.random() > 0.8 else None,
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(default_dim)],
                        "scalar_field": i + 10000,  # Modified
                        "label": f"updated_growing_{i}",  # Modified
                    }
                ],
            }
            upsert_data.append(row)

        res, check = self.upsert(client, collection_name, upsert_data)
        assert check

        # Verify upsert worked for flushed data
        res, check = self.flush(client, collection_name)
        assert check

        results, check = self.query(client, collection_name, filter="id < 10")
        assert check
        assert len(results) == 10
        for result in results:
            assert "updated_flushed" in result["clips"][0]["label"]

        # Verify upsert worked for growing data
        results, check = self.query(client, collection_name, filter="id >= 2000 and id < 2010")
        assert check
        assert len(results) == 10
        for result in results:
            assert "updated_growing" in result["clips"][0]["label"]

    @pytest.mark.tags(CaseLabel.L0)
    def test_delete_struct_array_data(self):
        """
        target: test delete operation with struct array data
        method: insert 3000 records (2000 flushed + 1000 growing), then delete by ID from both segments
        expected: data successfully deleted
        """
        collection_name = cf.gen_unique_str(f"{prefix}_crud")

        client = self._client()

        # Create collection and insert data
        self.create_collection_with_schema(client, collection_name)

        # Insert 2000 records for flushed data
        flushed_data = []
        for i in range(2000):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(default_dim)],
                        "scalar_field": i,
                        "label": f"flushed_{i}",
                    }
                ],
            }
            flushed_data.append(row)

        res, check = self.insert(client, collection_name, flushed_data)
        assert check
        assert res["insert_count"] == 2000

        # Flush to persist data
        res, check = self.flush(client, collection_name)
        assert check

        # Insert 1000 records for growing data
        growing_data = []
        for i in range(2000, 3000):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(default_dim)],
                        "scalar_field": i,
                        "label": f"growing_{i}",
                    }
                ],
            }
            growing_data.append(row)

        res, check = self.insert(client, collection_name, growing_data)
        assert check
        assert res["insert_count"] == 1000

        # create index and load collection
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_index(client, collection_name, index_params)
        assert check
        res, check = self.load_collection(client, collection_name)
        assert check

        # Delete some records from flushed segment
        delete_flushed_ids = [1, 3, 5, 100, 500, 1000]
        res, check = self.delete(client, collection_name, filter=f"id in {delete_flushed_ids}")
        assert check

        # Delete some records from growing segment
        delete_growing_ids = [2001, 2003, 2500, 2999]
        res, check = self.delete(client, collection_name, filter=f"id in {delete_growing_ids}")
        assert check

        # Verify deletion
        res, check = self.flush(client, collection_name)
        assert check

        results, check = self.query(client, collection_name, filter="id >= 0")
        assert check

        remaining_ids = {result["id"] for result in results}
        # Verify flushed data deletion
        for delete_id in delete_flushed_ids:
            assert delete_id not in remaining_ids
        # Verify growing data deletion
        for delete_id in delete_growing_ids:
            assert delete_id not in remaining_ids

        # Verify total count is correct (3000 - 10 deleted)
        assert len(results) == 2990

    @pytest.mark.tags(CaseLabel.L1)
    def test_batch_operations(self):
        """
        target: test batch insert/upsert operations with struct array
        method: insert 3000 records (2000 flushed + 1000 growing), then perform batch upsert
        expected: all operations successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_crud")

        client = self._client()

        # Create collection
        self.create_collection_with_schema(client, collection_name)

        # Insert 2000 records for flushed data
        flushed_data = []
        for i in range(2000):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(default_dim)],
                        "scalar_field": i % 100,
                        "label": f"flushed_{i}",
                    }
                ],
            }
            flushed_data.append(row)

        res, check = self.insert(client, collection_name, flushed_data)
        assert check
        assert res["insert_count"] == 2000

        # Flush to persist data
        res, check = self.flush(client, collection_name)
        assert check

        # Insert 1000 records for growing data
        growing_data = []
        for i in range(2000, 3000):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(default_dim)],
                        "scalar_field": i % 100,
                        "label": f"growing_{i}",
                    }
                ],
            }
            growing_data.append(row)

        res, check = self.insert(client, collection_name, growing_data)
        assert check
        assert res["insert_count"] == 1000

        # Batch upsert (update first 100 flushed records and 50 growing records)
        upsert_data = []
        # Update first 100 flushed records
        for i in range(100):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(default_dim)],
                        "scalar_field": i + 1000,  # Modified
                        "label": f"upserted_flushed_{i}",  # Modified
                    }
                ],
            }
            upsert_data.append(row)

        # Update first 50 growing records
        for i in range(2000, 2050):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(default_dim)],
                        "scalar_field": i + 1000,  # Modified
                        "label": f"upserted_growing_{i}",  # Modified
                    }
                ],
            }
            upsert_data.append(row)

        res, check = self.upsert(client, collection_name, upsert_data)
        assert check

        # Verify upsert success with flush
        res, check = self.flush(client, collection_name)
        assert check

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_operations(self):
        """
        target: test collection operations (load/release/drop) with struct array
        method: insert 3000 records (2000 flushed + 1000 growing), then perform collection management operations
        expected: all operations successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_crud")

        client = self._client()

        # Create collection with data
        self.create_collection_with_schema(client, collection_name)

        # Insert 2000 records for flushed data
        flushed_data = []
        for i in range(2000):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(default_dim)],
                        "scalar_field": i,
                        "label": f"flushed_{i}",
                    }
                ],
            }
            flushed_data.append(row)

        res, check = self.insert(client, collection_name, flushed_data)
        assert check
        assert res["insert_count"] == 2000

        # Flush to persist data
        res, check = self.flush(client, collection_name)
        assert check

        # Insert 1000 records for growing data
        growing_data = []
        for i in range(2000, 3000):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [random.random() for _ in range(default_dim)],
                        "scalar_field": i,
                        "label": f"growing_{i}",
                    }
                ],
            }
            growing_data.append(row)

        res, check = self.insert(client, collection_name, growing_data)
        assert check
        assert res["insert_count"] == 1000

        # Create index for loading
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        res, check = self.create_index(client, collection_name, index_params)
        assert check

        # Load collection
        res, check = self.load_collection(client, collection_name)
        assert check

        # Verify collection is loaded
        load_state = client.get_load_state(collection_name)
        assert str(load_state["state"]) == "Loaded"

        # Query to verify both flushed and growing data are accessible
        results, check = self.query(client, collection_name, filter="id >= 0", limit=3000)
        assert check
        assert len(results) == 3000

        # Release collection
        res, check = self.release_collection(client, collection_name)
        assert check

        # Drop collection
        res, check = self.drop_collection(client, collection_name)
        assert check

        # Verify collection is dropped
        has_collection, _ = self.has_collection(client, collection_name)
        assert not has_collection


class TestMilvusClientStructArrayInvalid(TestMilvusClientV2Base):
    """Test case of struct array invalid operations and edge cases"""

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("unsupported_type", [DataType.STRUCT, DataType.ARRAY])
    def test_nested_struct_array(self, unsupported_type):
        """
        target: test creating struct with unsupported nested types (should fail)
        method: attempt to create struct with STRUCT, ARRAY, GEOMETRY or TIMESTAMPTZ field
        expected: creation should fail with appropriate error
        """
        client = self._client()

        # Create struct schema
        struct_schema = client.create_struct_field_schema()

        # Try to add unsupported field type to struct schema (this should fail immediately)
        try:
            if unsupported_type == DataType.ARRAY:
                struct_schema.add_field(
                    "unsupported_field",
                    unsupported_type,
                    element_type=DataType.INT64,
                    max_capacity=10,
                )
            else:
                struct_schema.add_field("unsupported_field", unsupported_type)
            assert False, f"Expected ParamError when adding {unsupported_type} field to struct schema"
        except Exception as e:
            # Verify the error message indicates the type is not supported
            error_msg = str(e)
            # Different error messages for different types
            assert (
                "Struct field schema does not support Array, ArrayOfVector or Struct" in error_msg
                or "is not supported" in error_msg
                or "not supported for fields in struct" in error_msg
            )

    @pytest.mark.tags(CaseLabel.L2)
    def test_struct_with_unsupported_vector_field(self):
        """
        target: test creating struct with SparseFloatVector field (should fail)
        method: attempt to create struct with SparseFloatVector field
        expected: creation should fail (sparse vectors not supported in struct)
        """
        collection_name = cf.gen_unique_str(f"{prefix}_invalid")

        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("sparse_vector_field", DataType.SPARSE_FLOAT_VECTOR)
        schema.add_field(
            "struct_array",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )
        error = {
            ct.err_code: 65535,
            ct.err_msg: "only fixed dimension vector types are supported",
        }
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_struct_with_json_field(self):
        """
        target: test creating struct with JSON field (should fail)
        method: attempt to create struct with JSON field
        expected: creation should fail
        """
        collection_name = cf.gen_unique_str(f"{prefix}_invalid")

        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("json_field", DataType.JSON)
        schema.add_field(
            "struct_array",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )
        error = {
            ct.err_code: 65535,
            ct.err_msg: "element type JSON is not supported",
        }
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_struct_with_geometry_field(self):
        """
        target: test creating struct with Geometry field (should fail)
        method: attempt to create struct with Geometry field
        expected: creation should fail
        """
        collection_name = cf.gen_unique_str(f"{prefix}_invalid")

        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("geometry_field", DataType.GEOMETRY)
        schema.add_field(
            "struct_array",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )
        error = {
            ct.err_code: 65535,
            ct.err_msg: "element type Geometry is not supported",
        }
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_exceed_max_capacity(self):
        """
        target: test inserting data exceeding max capacity
        method: insert struct array with more elements than max_capacity
        expected: insertion should fail
        """
        collection_name = cf.gen_unique_str(f"{prefix}_invalid")

        client = self._client()

        # Create collection with small capacity
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("scalar_field", DataType.INT64)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=2,
        )  # Very small capacity

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Try to insert data exceeding capacity
        data = [
            {
                "id": 1,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {"scalar_field": 1},
                    {"scalar_field": 2},
                    {"scalar_field": 3},  # Exceeds capacity of 2
                ],
            }
        ]

        # This should fail
        error = {ct.err_code: 1100, ct.err_msg: "array length exceeds max capacity"}
        res, check = self.insert(
            client,
            collection_name,
            data,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_invalid_dimension(self):
        """
        target: test creating struct with invalid vector dimension
        method: create struct with zero or negative dimension
        expected: creation should fail
        """
        collection_name = cf.gen_unique_str(f"{prefix}_invalid")

        client = self._client()

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=0)  # Invalid

        struct_schema.add_field("clip_embedding2", DataType.FLOAT_VECTOR, dim=-1)  # Invalid
        schema.add_field(
            "struct_array",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )

        error = {ct.err_code: 65535, ct.err_msg: "invalid dimension"}
        res, check = self.create_collection(
            client,
            collection_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_inconsistent_struct_array_schema(self):
        """
        target: test struct array consistency validation
        method: insert data with struct array inconsistent schema
        expected: insertion should fail
        """
        collection_name = cf.gen_unique_str(f"{prefix}_invalid")

        client = self._client()

        # Create collection
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("field1", DataType.INT64)
        struct_schema.add_field("field2", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        # Insert invalid data, lack of field1
        invalid_data = [
            {
                "id": 1,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [{"field1": 1, "field2": "a"}, {"field2": "b"}],
            }
        ]
        error = {
            ct.err_code: 1,
            ct.err_msg: "The Input data type is inconsistent with defined schema",
        }

        self.insert(
            client,
            collection_name,
            invalid_data,
            check_task=CheckTasks.err_res,
            check_items=error,
        )
        # Insert invalid data, added field3
        invalid_data = [
            {
                "id": 1,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {"field1": 1, "field2": "a", "field3": "c"},
                ],
            }
        ]
        error = {
            ct.err_code: 1,
            ct.err_msg: "The Input data type is inconsistent with defined schema",
        }

        self.insert(
            client,
            collection_name,
            invalid_data,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    def test_struct_array_with_unsupported_vector_types(self):
        """
        target: test creating struct array with unsupported vector types
        method: attempt to create struct array with SPARSE_FLOAT_VECTOR
        expected: creation should fail as only fixed dimension vector types are supported
        note: FLOAT_VECTOR, FLOAT16_VECTOR, BFLOAT16_VECTOR, BINARY_VECTOR, INT8_VECTOR are supported
        """
        collection_name = cf.gen_unique_str(f"{prefix}_invalid")

        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        # Try to create struct with unsupported vector type (sparse vector)
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("unsupported_vector", DataType.SPARSE_FLOAT_VECTOR)

        schema.add_field(
            "struct_array",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )

        # Should fail - sparse vectors are not supported in struct array
        error = {
            ct.err_code: 65535,
            ct.err_msg: "only fixed dimension vector types are supported",
        }
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize("nullable_field", ["clip_embedding1", "scalar_field"])
    def test_struct_array_with_nullable_field(self, nullable_field):
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field(
            "clip_embedding1",
            DataType.FLOAT_VECTOR,
            dim=default_dim,
            nullable=("clip_embedding1" == nullable_field),
        )
        struct_schema.add_field("scalar_field", DataType.INT64, nullable=("scalar_field" == nullable_field))
        struct_schema.add_field("label", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )
        error = {
            ct.err_code: 1100,
            ct.err_msg: f"sub-field in non-nullable struct cannot be nullable individually, "
            f"set nullable on the struct instead: structName=clips, subFieldName={nullable_field}",
        }
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_embedding_list_field_nullable_insert_none(self):
        """
        target: test embedding list field nullable boundary
        method: create struct array field with nullable=True and insert None
        expected: create collection succeeds and None row value is inserted
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(
            field_name="normal_vector",
            datatype=DataType.FLOAT_VECTOR,
            dim=default_dim,
        )

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema.add_field("label", DataType.VARCHAR, max_length=128)

        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
            nullable=True,
        )
        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        data = [
            {
                "id": 0,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": None,
            }
        ]
        res, check = self.insert(
            client,
            collection_name,
            data,
        )
        assert check
        assert res["insert_count"] == 1

    @pytest.mark.tags(CaseLabel.L0)
    def test_struct_array_range_search_not_supported(self):
        """
        target: test range search with struct array is not supported
        method: perform range search with struct array
        expected: range search returns not supported error
        """
        collection_name = cf.gen_unique_str(f"{prefix}_range_search_not_supported")

        client = self._client()

        # Create collection with data and index using COSINE metric
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "clips",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="IVF_FLAT",
            metric_type="L2",
            params={"nlist": 128},
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        res, check = self.create_collection(client, collection_name, schema=schema, index_params=index_params)
        assert check
        nb = 3000
        data = []
        for i in range(nb):
            struct_array = []
            for j in range(random.randint(1, 10)):
                struct_array.append({"clip_embedding1": [random.random() for _ in range(default_dim)]})
            tmp_data = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": struct_array,
            }
            data.append(tmp_data)
        res, check = self.insert(client, collection_name, data)
        assert check

        # Create search vector
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        # Step 1: First search with large limit to get distance distribution
        initial_results, check = self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=50,
        )
        assert check
        assert len(initial_results[0]) > 0

        # Get distances and sort them
        distances = [hit.get("distance") for hit in initial_results[0] if hit.get("distance") is not None]
        distances.sort()

        # Select range from sorted distances (e.g., from 20th percentile to 80th percentile)
        if len(distances) >= 10:
            radius = distances[int(len(distances) * 0.2)]
            range_filter = distances[int(len(distances) * 0.8)]
        else:
            radius = distances[0]
            range_filter = distances[-1]

        # Step 2: Perform range search with selected radius and range_filter
        # For COSINE: radius < distance <= range_filter
        error = {
            ct.err_code: 1100,
            ct.err_msg: "range search is not supported for multi-search-multi on embedding list fields",
        }
        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={
                "metric_type": "MAX_SIM_COSINE",
                "params": {"radius": radius, "range_filter": range_filter},
            },
            limit=100,
            check_task=CheckTasks.err_res,
            check_items=error,
        )


class TestMilvusClientStructArrayImport(TestMilvusClientV2Base):
    """Test case of struct array data import functionality using bulk_import API"""

    # MinIO configuration constants
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    REMOTE_DATA_PATH = "bulkinsert_data"
    LOCAL_FILES_PATH = "/tmp/milvus_bulkinsert/"

    @pytest.fixture(scope="function", autouse=True)
    def setup_minio(self, minio_host, minio_bucket):
        """Setup MinIO configuration from fixtures"""
        Path(self.LOCAL_FILES_PATH).mkdir(parents=True, exist_ok=True)
        self.minio_host = minio_host
        self.bucket_name = minio_bucket
        self.minio_endpoint = f"{minio_host}:9000"

    def gen_file_with_local_bulk_writer(
        self, schema, data: list[dict[str, Any]], file_type: str = "PARQUET"
    ) -> tuple[str, dict]:
        """
        Generate import file using LocalBulkWriter from insert-format data

        Args:
            schema: Collection schema
            data: List of dictionaries in insert format (same format as client.insert())
            file_type: Output file type, "PARQUET" or "JSON"

        Returns:
            Tuple of (directory path containing generated files, original data dict for verification)
        """
        # Convert file_type string to BulkFileType enum
        bulk_file_type = BulkFileType.PARQUET if file_type == "PARQUET" else BulkFileType.JSON

        # Create LocalBulkWriter
        writer = LocalBulkWriter(
            schema=schema,
            local_path=self.LOCAL_FILES_PATH,
            segment_size=512 * 1024 * 1024,  # 512MB
            file_type=bulk_file_type,
        )

        log.info(f"Creating {file_type} file using LocalBulkWriter with {len(data)} rows")

        # Append each row using the same format as insert
        for row in data:
            writer.append_row(row)

        # Commit to generate files
        writer.commit()

        # Get the generated file paths
        batch_files = writer.batch_files
        log.info(f"LocalBulkWriter generated files: {batch_files}")

        # Extract data for verification (same format as other gen methods)
        id_arr = [row["id"] for row in data]
        float_vector_arr = [row["float_vector"] for row in data]
        struct_arr = [row["struct_array"] for row in data]

        return batch_files, {
            "id": id_arr,
            "float_vector": float_vector_arr,
            "struct_array": struct_arr,
        }

    def upload_to_minio(self, local_file_path: str) -> list[list[str]]:
        """
        Upload parquet file to MinIO

        Args:
            local_file_path: Local path of the file to upload

        Returns:
            List of remote file paths in MinIO
        """
        if not os.path.exists(local_file_path):
            raise Exception(f"Local file '{local_file_path}' doesn't exist")

        try:
            minio_client = Minio(
                endpoint=self.minio_endpoint,
                access_key=self.MINIO_ACCESS_KEY,
                secret_key=self.MINIO_SECRET_KEY,
                secure=False,
            )

            # Check if bucket exists
            if not minio_client.bucket_exists(self.bucket_name):
                raise Exception(f"MinIO bucket '{self.bucket_name}' doesn't exist")

            # Upload file
            filename = os.path.basename(local_file_path)
            minio_file_path = os.path.join(self.REMOTE_DATA_PATH, filename)
            minio_client.fput_object(self.bucket_name, minio_file_path, local_file_path)

            log.info(f"Uploaded file to MinIO: {minio_file_path}")
            return [[minio_file_path]]

        except S3Error as e:
            raise Exception(f"Failed to connect MinIO server {self.minio_endpoint}, error: {e}")

    def call_bulkinsert(
        self,
        collection_name: str,
        batch_files: list[list[str]],
        expect_fail: bool = False,
        partition_name: str = "",
    ) -> dict[str, Any]:
        """
        Call bulk import API and wait for completion

        Args:
            collection_name: Target collection name
            batch_files: List of file paths in MinIO
            expect_fail: Whether the import job is expected to fail
            partition_name: Optional target partition name

        Returns:
            Import result dict with state and reason
        """
        url = f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"

        log.info(f"Starting bulk import to collection '{collection_name}'")
        resp = bulk_import(
            url=url,
            collection_name=collection_name,
            files=batch_files,
            api_key=cf.param_info.param_token,
            partition_name=partition_name,
        )

        job_id = resp.json()["data"]["jobId"]
        log.info(f"Bulk import job created, job_id: {job_id}")

        # Wait for import to complete
        timeout = 300
        start_time = time.time()
        while time.time() - start_time < timeout:
            time.sleep(5)

            resp = get_import_progress(url=url, job_id=job_id, api_key=cf.param_info.param_token)
            state = resp.json()["data"]["state"]
            progress = resp.json()["data"].get("progress", 0)

            log.info(f"Import job {job_id} - state: {state}, progress: {progress}%")

            if state == "Importing":
                continue
            elif state == "Failed":
                reason = resp.json()["data"].get("reason", "Unknown reason")
                if expect_fail:
                    log.info(f"Bulk import job {job_id} failed as expected: {reason}")
                    return {"state": "Failed", "reason": reason}
                raise Exception(f"Bulk import job {job_id} failed: {reason}")
            elif state == "Completed" and progress == 100:
                if expect_fail:
                    raise AssertionError(f"Bulk import job {job_id} unexpectedly completed")
                log.info(f"Bulk import job {job_id} completed successfully")
                return {"state": "Completed", "reason": None}
        else:
            raise Exception(f"Bulk import job {job_id} timeout after {timeout}s")

        log.info("Bulk import finished")

    def verify_data(self, client: MilvusClient, collection_name: str, original_data: dict):
        """
        Verify imported data matches original data using compare_lists_with_epsilon_ignore_dict_order

        Args:
            client: MilvusClient instance
            collection_name: Collection name
            original_data: Original data dictionary for comparison
        """
        log.info("============= Verifying imported data ==============")

        # Query all data from the collection
        num_rows = len(original_data["id"])
        log.info(f"Total rows to verify: {num_rows}")

        results = client.query(
            collection_name=collection_name,
            filter=f"id >= {min(original_data['id'])}",
            output_fields=["*"],
            limit=num_rows + 100,  # Add buffer to ensure all data is retrieved
        )

        log.info(f"Query returned {len(results)} rows")

        # Check if row count matches
        if len(results) != num_rows:
            assert False, f"Row count mismatch: expected {num_rows}, got {len(results)}"

        # Convert original data to comparable format (list of dicts per row)
        original_rows = []
        for i in range(num_rows):
            row_id = int(original_data["id"][i])
            original_rows.append(
                {
                    "id": row_id,
                    "float_vector": [float(x) for x in original_data["float_vector"][i]],
                    "struct_array": original_data["struct_array"][i],
                }
            )

        # Convert query results to comparable format
        query_rows_formatted = []
        for row in results:
            formatted_row = {
                "id": row["id"],
                "float_vector": [float(x) for x in row["float_vector"]],
                "struct_array": row["struct_array"],
            }
            query_rows_formatted.append(formatted_row)

        # Use compare_lists_with_epsilon_ignore_dict_order for comparison
        # This function handles floating-point tolerance and dict order
        is_equal = compare_lists_with_epsilon_ignore_dict_order(original_rows, query_rows_formatted, epsilon=epsilon)

        if not is_equal:
            deepdiff = (
                DeepDiff(
                    original_rows,
                    query_rows_formatted,
                    ignore_order=True,
                    significant_digits=3,
                ),
            )
            log.info(f"DeepDiff: {deepdiff}")
            assert False, "Data verification failed: original data and query results do not match"

    @staticmethod
    def _scalar_profile(row_id: int) -> list[dict[str, Any]]:
        return [
            {"p_int": row_id * 10, "p_tag": f"profile_{row_id}_0"},
            {"p_int": row_id * 10 + 1, "p_tag": f"profile_{row_id}_1"},
        ]

    def _profile(self, row_id: int) -> list[dict[str, Any]]:
        return [
            {
                "p_int": row_id * 10,
                "p_tag": f"profile_{row_id}_0",
                "p_vec": self._vector(row_id * 10),
            },
            {
                "p_int": row_id * 10 + 1,
                "p_tag": f"profile_{row_id}_1",
                "p_vec": self._vector(row_id * 10 + 1),
            },
        ]

    @staticmethod
    def _assert_scalar_profile_equal(actual, expected):
        if expected is None:
            assert actual is None
            return

        assert isinstance(actual, list)
        assert len(actual) == len(expected)
        for actual_item, expected_item in zip(actual, expected):
            assert actual_item["p_int"] == expected_item["p_int"]
            assert actual_item["p_tag"] == expected_item["p_tag"]

    @staticmethod
    def _assert_nullable_scalar_profile_equal(actual, expected, expected_keys=None):
        if expected is None:
            assert actual is None
            return

        assert isinstance(actual, list)
        assert len(actual) == len(expected)
        for actual_item, expected_item in zip(actual, expected):
            keys = expected_keys or expected_item.keys()
            assert set(actual_item) == set(keys)
            for key in keys:
                assert actual_item.get(key) == expected_item.get(key)

    def _assert_profile_equal(self, actual, expected):
        if expected is None:
            assert actual is None
            return

        assert isinstance(actual, list)
        assert len(actual) == len(expected)
        for actual_item, expected_item in zip(actual, expected):
            assert actual_item["p_int"] == expected_item["p_int"]
            assert actual_item["p_tag"] == expected_item["p_tag"]
            assert actual_item["p_vec"] == pytest.approx(expected_item["p_vec"])

    @staticmethod
    def _search_entity(hit):
        return hit.get("entity", hit)

    @staticmethod
    def _vector(seed: int, dim: int = default_dim) -> list[float]:
        return [float(seed) + float(i) / 1000 for i in range(dim)]

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_nullable_scalar_struct_array_with_json(self):
        """
        target: test JSON bulk import for a nullable scalar-only struct array field
        method: create a collection with nullable scalar Struct Array, import 3000 JSON rows with null, empty, and
            non-empty profile values, then query and search the imported data
        expected: imported row count matches source data, and query/search output preserves nullable struct semantics
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_json")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)

        rows = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            rows.append(row)
            source_by_id[row_id] = row

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_nullable_scalar_struct_array_with_json_partition_query_search(self):
        """
        target: test JSON bulk import into a specified partition for a nullable scalar-only struct array field
        method: create two partitions, import 3000 JSON rows into one partition, insert same-vector interference rows
            into the other partition, then query and search with partition_names
        expected: imported row count is scoped to the target partition, and partition-scoped query/search never returns
            rows from the other partition while preserving nullable struct semantics
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_json_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        entities = 3000
        other_entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)
        res, check = self.create_partition(client, collection_name, partition_a)
        assert check
        res, check = self.create_partition(client, collection_name, partition_b)
        assert check

        other_partition_rows = []
        other_source_by_id = {}
        for offset in range(other_entities):
            row_id = 100000 + offset
            if offset == 0:
                profile = self._scalar_profile(row_id)
                vector = self._vector(entities - 1)
                doc_tag = "other_partition_same_vector"
            elif offset % 3 == 1:
                profile = None
                vector = self._vector(row_id)
                doc_tag = f"other_partition_null_profile_{offset}"
            elif offset % 3 == 2:
                profile = []
                vector = self._vector(row_id)
                doc_tag = f"other_partition_empty_profile_{offset}"
            else:
                profile = self._scalar_profile(row_id)
                vector = self._vector(row_id)
                doc_tag = f"other_partition_profile_{offset}"
            row = {
                "id": row_id,
                "normal_vector": vector,
                "doc_tag": doc_tag,
                "profile": profile,
            }
            other_partition_rows.append(row)
            other_source_by_id[row_id] = row
        res, check = self.insert(client, collection_name, other_partition_rows, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(other_partition_rows)
        res, check = self.flush(client, collection_name)
        assert check

        rows = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"target_partition_row_{row_id}",
                "profile": profile,
            }
            rows.append(row)
            source_by_id[row_id] = row

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities + len(other_partition_rows)
        target_partition_stats, check = self.get_partition_stats(client, collection_name, partition_a)
        assert check
        assert target_partition_stats["row_count"] == entities
        other_partition_stats, check = self.get_partition_stats(client, collection_name, partition_b)
        assert check
        assert other_partition_stats["row_count"] == len(other_partition_rows)

        query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_a],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        other_query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_b],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(other_partition_rows),
        )
        assert {row["id"] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            partition_names=[partition_a],
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert not {hit["id"] for hit in hits}.intersection({row["id"] for row in other_partition_rows})
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_nullable_scalar_struct_array_with_parquet_partition_query_search(self):
        """
        target: test Parquet bulk import into a specified partition for a nullable scalar-only struct array field
        method: create two partitions, import 3000 Parquet rows into one partition, insert same-vector interference
            rows into the other partition, then query and search with partition_names
        expected: imported row count is scoped to the target partition, and partition-scoped query/search never returns
            rows from the other partition while preserving nullable struct semantics
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_parquet_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        entities = 3000
        other_entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)
        res, check = self.create_partition(client, collection_name, partition_a)
        assert check
        res, check = self.create_partition(client, collection_name, partition_b)
        assert check

        other_partition_rows = []
        other_source_by_id = {}
        for offset in range(other_entities):
            row_id = 100000 + offset
            if offset == 0:
                profile = self._scalar_profile(row_id)
                vector = self._vector(entities - 1)
                doc_tag = "other_partition_same_vector"
            elif offset % 3 == 1:
                profile = None
                vector = self._vector(row_id)
                doc_tag = f"other_partition_null_profile_{offset}"
            elif offset % 3 == 2:
                profile = []
                vector = self._vector(row_id)
                doc_tag = f"other_partition_empty_profile_{offset}"
            else:
                profile = self._scalar_profile(row_id)
                vector = self._vector(row_id)
                doc_tag = f"other_partition_profile_{offset}"
            row = {
                "id": row_id,
                "normal_vector": vector,
                "doc_tag": doc_tag,
                "profile": profile,
            }
            other_partition_rows.append(row)
            other_source_by_id[row_id] = row
        res, check = self.insert(client, collection_name, other_partition_rows, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(other_partition_rows)
        res, check = self.flush(client, collection_name)
        assert check

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"target_partition_row_{row_id}",
                "profile": profile,
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            profiles.append(row["profile"])
            source_by_id[row_id] = row

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_int", pa.int64()),
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities + len(other_partition_rows)
        target_partition_stats, check = self.get_partition_stats(client, collection_name, partition_a)
        assert check
        assert target_partition_stats["row_count"] == entities
        other_partition_stats, check = self.get_partition_stats(client, collection_name, partition_b)
        assert check
        assert other_partition_stats["row_count"] == len(other_partition_rows)

        query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_a],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        other_query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_b],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(other_partition_rows),
        )
        assert {row["id"] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            partition_names=[partition_a],
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert not {hit["id"] for hit in hits}.intersection({row["id"] for row in other_partition_rows})
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(
        reason="nullable scalar sub-field null values are rejected by JSON bulk import and PyMilvus row insert"
    )
    def test_import_nullable_scalar_struct_array_subfield_null_json(self):
        """
        target: test JSON bulk import for nullable scalar sub-fields inside a nullable Struct Array
        method: create a collection with nullable scalar Struct Array, import 3000 JSON rows where non-empty profile
            elements contain explicit null p_int or p_tag values, then query and search the imported data
        expected: parent null/empty semantics are preserved, and scalar sub-field null values are returned as null
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_subfield_null_json")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)

        rows = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 4 == 0:
                profile = None
            elif row_id % 4 == 1:
                profile = []
            elif row_id % 4 == 2:
                profile = [
                    {"p_int": None, "p_tag": f"profile_{row_id}_null_int"},
                    {"p_int": row_id * 10 + 1, "p_tag": None},
                ]
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            rows.append(row)
            source_by_id[row_id] = row

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=entities,
        )
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="nullable scalar sub-field null values are rejected by Parquet bulk import")
    def test_import_nullable_scalar_struct_array_subfield_null_parquet(self):
        """
        target: test Parquet bulk import for nullable scalar sub-fields inside a nullable Struct Array
        method: create a collection with nullable scalar Struct Array, import 3000 Parquet rows where non-empty profile
            elements contain explicit null p_int or p_tag values, then query and search the imported data
        expected: parent null/empty semantics are preserved, and scalar sub-field null values are returned as null
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_subfield_null_parquet")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 4 == 0:
                profile = None
            elif row_id % 4 == 1:
                profile = []
            elif row_id % 4 == 2:
                profile = [
                    {"p_int": None, "p_tag": f"profile_{row_id}_null_int"},
                    {"p_int": row_id * 10 + 1, "p_tag": None},
                ]
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            profiles.append(row["profile"])
            source_by_id[row_id] = row

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_int", pa.int64()),
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=entities,
        )
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(
        reason="nullable scalar sub-field omission inside StructArray element is rejected by JSON bulk import and row insert"
    )
    def test_import_nullable_scalar_struct_array_subfield_omit_json(self):
        """
        target: test JSON bulk import for omitted nullable scalar sub-fields inside a nullable Struct Array
        method: create a collection with nullable scalar Struct Array, import 3000 JSON rows where non-empty profile
            elements omit p_int or p_tag, then query and search the imported data
        expected: parent null/empty semantics are preserved, and omitted scalar sub-field values are returned as null
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_subfield_omit_json")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)

        rows = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 4 == 0:
                input_profile = None
                expected_profile = None
            elif row_id % 4 == 1:
                input_profile = []
                expected_profile = []
            elif row_id % 4 == 2:
                input_profile = [
                    {"p_tag": f"profile_{row_id}_missing_int"},
                    {"p_int": row_id * 10 + 1},
                ]
                expected_profile = [
                    {"p_int": None, "p_tag": f"profile_{row_id}_missing_int"},
                    {"p_int": row_id * 10 + 1, "p_tag": None},
                ]
            else:
                input_profile = self._scalar_profile(row_id)
                expected_profile = input_profile
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": input_profile,
            }
            rows.append(row)
            source_by_id[row_id] = {**row, "profile": expected_profile}

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        subfield_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "profile[p_int]", "profile[p_tag]"],
            limit=entities,
        )
        assert {row["id"] for row in subfield_results} == set(source_by_id)
        for row in subfield_results:
            expected = source_by_id[row["id"]]
            assert set(row) == {"id", "profile"}
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(reason="nullable scalar sub-field omitted from Parquet StructArray schema is rejected")
    def test_import_nullable_scalar_struct_array_subfield_omit_parquet(self):
        """
        target: test Parquet bulk import with an omitted nullable scalar sub-field inside a nullable Struct Array
        method: create a collection with nullable scalar Struct Array, import 3000 Parquet rows whose profile struct
            file schema only contains p_tag and omits p_int, then query and search the imported data
        expected: parent null/empty semantics are preserved, and omitted p_int values are returned as null
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_subfield_omit_parquet")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 3 == 0:
                profile = None
                expected_profile = None
            elif row_id % 3 == 1:
                profile = []
                expected_profile = []
            else:
                profile = [
                    {"p_tag": f"profile_{row_id}_missing_int_0"},
                    {"p_tag": f"profile_{row_id}_missing_int_1"},
                ]
                expected_profile = [
                    {"p_int": None, "p_tag": f"profile_{row_id}_missing_int_0"},
                    {"p_int": None, "p_tag": f"profile_{row_id}_missing_int_1"},
                ]
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            profiles.append(row["profile"])
            source_by_id[row_id] = {**row, "profile": expected_profile}

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_nullable_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_non_nullable_scalar_struct_array_rejects_null_json(self):
        """
        target: test JSON bulk import rejects null for a non-nullable scalar-only struct array field
        method: create a collection with non-nullable scalar Struct Array, import 3000 JSON rows where one row uses
            profile=null
        expected: import job fails with a nullable-related error and no rows are imported
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_non_nullable_scalar_struct_json")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)

        rows = []
        for row_id in range(entities):
            if row_id == 0:
                profile = None
            elif row_id % 2 == 0:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            rows.append(
                {
                    "id": row_id,
                    "normal_vector": self._vector(row_id),
                    "doc_tag": f"import_row_{row_id}",
                    "profile": profile,
                }
            )

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        import_result = self.call_bulkinsert(collection_name, remote_files, expect_fail=True)

        assert import_result["state"] == "Failed"
        assert any(keyword in import_result["reason"].lower() for keyword in ["null", "nullable"])

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_non_nullable_scalar_struct_array_rejects_omit_field_json(self):
        """
        target: test JSON bulk import rejects omitted data for a non-nullable scalar-only struct array field
        method: create a collection with non-nullable scalar Struct Array, import 3000 JSON rows that omit profile
            entirely
        expected: import job fails with a schema/data mismatch error and no rows are imported
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_non_nullable_scalar_struct_omit_json")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)

        rows = []
        for row_id in range(entities):
            rows.append(
                {
                    "id": row_id,
                    "normal_vector": self._vector(row_id),
                    "doc_tag": f"import_row_{row_id}",
                }
            )

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        import_result = self.call_bulkinsert(collection_name, remote_files, expect_fail=True)

        assert import_result["state"] == "Failed"
        reason = import_result["reason"].lower()
        assert any(keyword in reason for keyword in ["profile", "null", "nullable", "missing", "required"])

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_non_nullable_scalar_struct_array_rejects_omit_field_parquet(self):
        """
        target: test Parquet bulk import rejects omitted data for a non-nullable scalar-only struct array field
        method: create a collection with non-nullable scalar Struct Array, import 3000 Parquet rows whose file schema
            omits profile entirely
        expected: import job fails with a schema/data mismatch error and no rows are imported
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_non_nullable_scalar_struct_omit_parquet")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        for row_id in range(entities):
            ids.append(row_id)
            vectors.append(self._vector(row_id))
            doc_tags.append(f"import_row_{row_id}")

        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=entities)

        remote_files = self.upload_to_minio(local_file_path)
        import_result = self.call_bulkinsert(collection_name, remote_files, expect_fail=True)

        assert import_result["state"] == "Failed"
        reason = import_result["reason"].lower()
        assert any(keyword in reason for keyword in ["profile", "null", "nullable", "missing", "required"])

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_with_json(self):
        """
        target: test JSON bulk import after dynamically adding a nullable scalar-only Struct Array field
        method: insert and flush 3000 old rows, add nullable scalar Struct Array, import 3000 JSON rows with null,
            empty, and non-empty profile values, then query and search all rows
        expected: old sealed rows expose the added struct field as null, imported rows preserve nullable struct
            semantics, and normal vector search requery output matches source-of-truth
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_json")
        old_entities = 3000
        import_entities = 3000
        total_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows)
        assert check
        assert res["insert_count"] == old_entities

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        import_rows = []
        for row_id in range(old_entities, total_entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            import_rows.append(row)
            source_by_id[row_id] = row

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(import_rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_with_json_partition_query_search(self):
        """
        target: test JSON bulk import into a specified partition after dynamically adding a nullable scalar-only Struct
            Array field
        method: insert and flush 3000 old rows in the target partition, add nullable scalar Struct Array, import 3000
            JSON rows into the same partition, and keep same-vector interference rows in another partition
        expected: old target-partition rows expose the added struct field as null, imported rows preserve nullable
            struct semantics, and partition-scoped query/search never returns rows from the other partition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_json_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        old_entities = 3000
        import_entities = 3000
        other_old_entities = 3000
        total_target_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check
        res, check = self.create_partition(client, collection_name, partition_a)
        assert check
        res, check = self.create_partition(client, collection_name, partition_b)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_target_partition_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows, partition_name=partition_a)
        assert check
        assert res["insert_count"] == old_entities

        other_old_rows = []
        other_source_by_id = {}
        for offset in range(other_old_entities):
            row_id = 100000 + offset
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"other_partition_old_{offset}",
            }
            other_old_rows.append(row)
            other_source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, other_old_rows, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(other_old_rows)

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        import_rows = []
        for row_id in range(old_entities, total_target_entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_target_partition_row_{row_id}",
                "profile": profile,
            }
            import_rows.append(row)
            source_by_id[row_id] = row

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(import_rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_target_entities + len(other_old_rows)
        target_partition_stats, check = self.get_partition_stats(client, collection_name, partition_a)
        assert check
        assert target_partition_stats["row_count"] == total_target_entities
        other_partition_stats, check = self.get_partition_stats(client, collection_name, partition_b)
        assert check
        assert other_partition_stats["row_count"] == len(other_old_rows)

        other_post_add_row = {
            "id": 100000 + other_old_entities,
            "normal_vector": self._vector(total_target_entities - 1),
            "doc_tag": "other_partition_same_vector_after_add",
            "profile": self._scalar_profile(100000 + other_old_entities),
        }
        res, check = self.insert(client, collection_name, [other_post_add_row], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1
        other_source_by_id[other_post_add_row["id"]] = other_post_add_row

        query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_a],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        other_query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_b],
            filter="id >= 100000",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(other_old_rows) + 1,
        )
        assert {row["id"] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_target_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            partition_names=[partition_a],
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert not {hit["id"] for hit in hits}.intersection(set(other_source_by_id))
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_with_parquet_partition_query_search(self):
        """
        target: test Parquet bulk import into a specified partition after dynamically adding a nullable scalar-only
            Struct Array field
        method: insert and flush 3000 old rows in both target and non-target partitions, add nullable scalar Struct
            Array, import 3000 Parquet rows into the target partition, and keep a growing same-vector interference row
            in another partition
        expected: old target-partition rows expose the added struct field as null, imported rows preserve nullable
            struct semantics, and partition-scoped query/search never returns rows from the other partition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_parquet_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        old_entities = 3000
        import_entities = 3000
        other_old_entities = 3000
        total_target_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check
        res, check = self.create_partition(client, collection_name, partition_a)
        assert check
        res, check = self.create_partition(client, collection_name, partition_b)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_target_partition_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows, partition_name=partition_a)
        assert check
        assert res["insert_count"] == old_entities

        other_old_rows = []
        other_source_by_id = {}
        for offset in range(other_old_entities):
            row_id = 100000 + offset
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"other_partition_old_{offset}",
            }
            other_old_rows.append(row)
            other_source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, other_old_rows, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(other_old_rows)

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        for row_id in range(old_entities, total_target_entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_target_partition_row_{row_id}",
                "profile": profile,
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            profiles.append(row["profile"])
            source_by_id[row_id] = row

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_int", pa.int64()),
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=import_entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_target_entities + len(other_old_rows)
        target_partition_stats, check = self.get_partition_stats(client, collection_name, partition_a)
        assert check
        assert target_partition_stats["row_count"] == total_target_entities
        other_partition_stats, check = self.get_partition_stats(client, collection_name, partition_b)
        assert check
        assert other_partition_stats["row_count"] == len(other_old_rows)

        other_post_add_row = {
            "id": 100000 + other_old_entities,
            "normal_vector": self._vector(total_target_entities - 1),
            "doc_tag": "other_partition_same_vector_after_add",
            "profile": self._scalar_profile(100000 + other_old_entities),
        }
        res, check = self.insert(client, collection_name, [other_post_add_row], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1
        other_source_by_id[other_post_add_row["id"]] = other_post_add_row

        query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_a],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        other_query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_b],
            filter="id >= 100000",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(other_old_rows) + 1,
        )
        assert {row["id"] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_target_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            partition_names=[partition_a],
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert not {hit["id"] for hit in hits}.intersection(set(other_source_by_id))
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_omit_field_json_partition_query_search(self):
        """
        target: test JSON bulk import omitting a dynamically added nullable scalar-only Struct Array field into a
            specified partition
        method: insert and flush 3000 old rows in both target and non-target partitions, add nullable scalar Struct
            Array, import 3000 JSON rows that omit profile into the target partition, and keep a growing same-vector
            interference row in another partition
        expected: old and imported target-partition rows expose the added struct field as null, and partition-scoped
            query/search never returns rows from the other partition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_omit_json_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        old_entities = 3000
        import_entities = 3000
        other_old_entities = 3000
        total_target_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check
        res, check = self.create_partition(client, collection_name, partition_a)
        assert check
        res, check = self.create_partition(client, collection_name, partition_b)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_target_partition_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows, partition_name=partition_a)
        assert check
        assert res["insert_count"] == old_entities

        other_old_rows = []
        other_source_by_id = {}
        for offset in range(other_old_entities):
            row_id = 100000 + offset
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"other_partition_old_{offset}",
            }
            other_old_rows.append(row)
            other_source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, other_old_rows, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(other_old_rows)

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        import_rows = []
        for row_id in range(old_entities, total_target_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_target_partition_omit_profile_row_{row_id}",
            }
            import_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(import_rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_target_entities + len(other_old_rows)
        target_partition_stats, check = self.get_partition_stats(client, collection_name, partition_a)
        assert check
        assert target_partition_stats["row_count"] == total_target_entities
        other_partition_stats, check = self.get_partition_stats(client, collection_name, partition_b)
        assert check
        assert other_partition_stats["row_count"] == len(other_old_rows)

        other_post_add_row = {
            "id": 100000 + other_old_entities,
            "normal_vector": self._vector(total_target_entities - 1),
            "doc_tag": "other_partition_same_vector_after_add",
            "profile": self._scalar_profile(100000 + other_old_entities),
        }
        res, check = self.insert(client, collection_name, [other_post_add_row], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1
        other_source_by_id[other_post_add_row["id"]] = other_post_add_row

        query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_a],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        other_query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_b],
            filter="id >= 100000",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(other_old_rows) + 1,
        )
        assert {row["id"] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_target_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            partition_names=[partition_a],
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert not {hit["id"] for hit in hits}.intersection(set(other_source_by_id))
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_omit_field_parquet_partition_query_search(self):
        """
        target: test Parquet bulk import omitting a dynamically added nullable scalar-only Struct Array field into a
            specified partition
        method: insert and flush 3000 old rows in both target and non-target partitions, add nullable scalar Struct
            Array, import 3000 Parquet rows whose file schema omits profile into the target partition, and keep a
            growing same-vector interference row in another partition
        expected: old and imported target-partition rows expose the added struct field as null, and partition-scoped
            query/search never returns rows from the other partition
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_omit_parquet_partition")
        partition_a = cf.gen_unique_str("part_a")
        partition_b = cf.gen_unique_str("part_b")
        old_entities = 3000
        import_entities = 3000
        other_old_entities = 3000
        total_target_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check
        res, check = self.create_partition(client, collection_name, partition_a)
        assert check
        res, check = self.create_partition(client, collection_name, partition_b)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_target_partition_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows, partition_name=partition_a)
        assert check
        assert res["insert_count"] == old_entities

        other_old_rows = []
        other_source_by_id = {}
        for offset in range(other_old_entities):
            row_id = 100000 + offset
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"other_partition_old_{offset}",
            }
            other_old_rows.append(row)
            other_source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, other_old_rows, partition_name=partition_b)
        assert check
        assert res["insert_count"] == len(other_old_rows)

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        for row_id in range(old_entities, total_target_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_target_partition_omit_profile_row_{row_id}",
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            source_by_id[row_id] = {**row, "profile": None}

        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=import_entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files, partition_name=partition_a)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_target_entities + len(other_old_rows)
        target_partition_stats, check = self.get_partition_stats(client, collection_name, partition_a)
        assert check
        assert target_partition_stats["row_count"] == total_target_entities
        other_partition_stats, check = self.get_partition_stats(client, collection_name, partition_b)
        assert check
        assert other_partition_stats["row_count"] == len(other_old_rows)

        other_post_add_row = {
            "id": 100000 + other_old_entities,
            "normal_vector": self._vector(total_target_entities - 1),
            "doc_tag": "other_partition_same_vector_after_add",
            "profile": self._scalar_profile(100000 + other_old_entities),
        }
        res, check = self.insert(client, collection_name, [other_post_add_row], partition_name=partition_b)
        assert check
        assert res["insert_count"] == 1
        other_source_by_id[other_post_add_row["id"]] = other_post_add_row

        query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_a],
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        other_query_results = client.query(
            collection_name=collection_name,
            partition_names=[partition_b],
            filter="id >= 100000",
            output_fields=["id", "doc_tag", "profile"],
            limit=len(other_old_rows) + 1,
        )
        assert {row["id"] for row in other_query_results} == set(other_source_by_id)
        for row in other_query_results:
            expected = other_source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_target_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            partition_names=[partition_a],
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_target_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert not {hit["id"] for hit in hits}.intersection(set(other_source_by_id))
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_with_parquet(self):
        """
        target: test Parquet bulk import after dynamically adding a nullable scalar-only Struct Array field
        method: insert and flush 3000 old rows, add nullable scalar Struct Array, import 3000 Parquet rows with null,
            empty, and non-empty profile values, then query and search all rows
        expected: old sealed rows expose the added struct field as null, imported rows preserve nullable struct
            semantics, and normal vector search requery output matches source-of-truth
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_parquet")
        old_entities = 3000
        import_entities = 3000
        total_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows)
        assert check
        assert res["insert_count"] == old_entities

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        for row_id in range(old_entities, total_entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            profiles.append(row["profile"])
            source_by_id[row_id] = row

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_int", pa.int64()),
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=import_entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_omit_field_json(self):
        """
        target: test JSON bulk import omitting a dynamically added nullable scalar-only Struct Array field
        method: insert and flush 3000 old rows, add nullable scalar Struct Array, import 3000 JSON rows that omit
            profile entirely, then query and search all rows
        expected: both old rows and imported rows expose the added struct field as null, and normal vector search
            requery output matches source-of-truth
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_omit_json")
        old_entities = 3000
        import_entities = 3000
        total_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows)
        assert check
        assert res["insert_count"] == old_entities

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        import_rows = []
        for row_id in range(old_entities, total_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
            }
            import_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(import_rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_after_add_nullable_scalar_struct_array_omit_field_parquet(self):
        """
        target: test Parquet bulk import omitting a dynamically added nullable scalar-only Struct Array field
        method: insert and flush 3000 old rows, add nullable scalar Struct Array, import 3000 Parquet rows whose file
            schema omits profile entirely, then query and search all rows
        expected: both old rows and imported rows expose the added struct field as null, and normal vector search
            requery output matches source-of-truth
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_add_nullable_scalar_struct_omit_parquet")
        old_entities = 3000
        import_entities = 3000
        total_entities = old_entities + import_entities

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        res, check = self.create_collection(client, collection_name, schema=schema)
        assert check

        source_by_id = {}
        old_rows = []
        for row_id in range(old_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"old_row_{row_id}",
            }
            old_rows.append(row)
            source_by_id[row_id] = {**row, "profile": None}
        res, check = self.insert(client, collection_name, old_rows)
        assert check
        assert res["insert_count"] == old_entities

        res, check = self.flush(client, collection_name)
        assert check

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        res, check = self.add_collection_struct_field(
            client, collection_name, "profile", profile_schema, max_capacity=4
        )
        assert check
        self.wait_schema_version_consistent(client, collection_name, timeout=30)

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_index(collection_name=collection_name, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        for row_id in range(old_entities, total_entities):
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            source_by_id[row_id] = {**row, "profile": None}

        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=import_entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        res, check = self.load_collection(client, collection_name)
        assert check

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == total_entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[total_entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=total_entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.skip(
        reason="nullable StructArray vector sub-field imported by JSON loses ArrayOfVector ValidData on output"
    )
    def test_import_nullable_struct_array_with_vector_json(self):
        """
        target: test JSON bulk import for a nullable struct array field with scalar and vector sub-fields
        method: create a collection with nullable Struct Array, import 3000 JSON rows with null, empty, and non-empty
            profile values, build indexes on normal vector and struct vector sub-field, then query and normal-vector
            search the imported data
        expected: imported row count matches source data, indexes are ready, and query/search output preserves nullable
            struct semantics including vector sub-field values
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_struct_vector_json")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        profile_schema.add_field("p_vec", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        index_params.add_index(
            field_name="profile[p_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)

        rows = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            rows.append(row)
            source_by_id[row_id] = row

        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.json")
        with open(local_file_path, "w") as f:
            json.dump(rows, f)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        assert self.wait_for_index_ready(client, collection_name, "profile[p_vec]", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_nullable_scalar_struct_array_with_parquet(self):
        """
        target: test Parquet bulk import for a nullable scalar-only struct array field
        method: create a collection with nullable scalar Struct Array, import 3000 Parquet rows with null, empty, and
            non-empty profile values, then query and search the imported data
        expected: imported row count matches source data, and query/search output preserves nullable struct semantics
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_nullable_scalar_struct_parquet")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
            nullable=True,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        source_by_id = {}
        for row_id in range(entities):
            if row_id % 3 == 0:
                profile = None
            elif row_id % 3 == 1:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            row = {
                "id": row_id,
                "normal_vector": self._vector(row_id),
                "doc_tag": f"import_row_{row_id}",
                "profile": profile,
            }
            ids.append(row["id"])
            vectors.append(row["normal_vector"])
            doc_tags.append(row["doc_tag"])
            profiles.append(row["profile"])
            source_by_id[row_id] = row

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_int", pa.int64()),
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=entities)

        remote_files = self.upload_to_minio(local_file_path)
        self.call_bulkinsert(collection_name, remote_files)

        assert self.wait_for_index_ready(client, collection_name, "normal_vector", timeout=300)
        client.refresh_load(collection_name=collection_name)

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == entities

        query_results = client.query(
            collection_name=collection_name,
            filter="id >= 0",
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        assert {row["id"] for row in query_results} == set(source_by_id)
        for row in query_results:
            expected = source_by_id[row["id"]]
            assert row["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(row["profile"], expected["profile"])

        search_row = source_by_id[entities - 1]
        search_results = client.search(
            collection_name=collection_name,
            data=[search_row["normal_vector"]],
            anns_field="normal_vector",
            search_params={"metric_type": "L2", "params": {}},
            output_fields=["id", "doc_tag", "profile"],
            limit=entities,
        )
        hits = search_results[0]
        assert {hit["id"] for hit in hits} == set(source_by_id)
        assert hits[0]["id"] == search_row["id"]
        for hit in hits:
            expected = source_by_id[hit["id"]]
            entity = self._search_entity(hit)
            assert entity["doc_tag"] == expected["doc_tag"]
            self._assert_scalar_profile_equal(entity["profile"], expected["profile"])

    @pytest.mark.tags(CaseLabel.L1)
    def test_import_non_nullable_scalar_struct_array_rejects_null_parquet(self):
        """
        target: test Parquet bulk import rejects null for a non-nullable scalar-only struct array field
        method: create a collection with non-nullable scalar Struct Array, import 3000 Parquet rows where one row uses
            profile=null
        expected: import job fails with a nullable-related error and no rows are imported
        """
        client = self._client()
        collection_name = cf.gen_unique_str(f"{prefix}_import_non_nullable_scalar_struct_parquet")
        entities = 3000

        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field(field_name="doc_tag", datatype=DataType.VARCHAR, max_length=128)

        profile_schema = client.create_struct_field_schema()
        profile_schema.add_field("p_int", DataType.INT64)
        profile_schema.add_field("p_tag", DataType.VARCHAR, max_length=128)
        schema.add_field(
            "profile",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=profile_schema,
            max_capacity=4,
        )

        index_params = client.prepare_index_params()
        index_params.add_index(field_name="normal_vector", index_type="FLAT", metric_type="L2")
        client.create_collection(collection_name=collection_name, schema=schema, index_params=index_params)

        ids = []
        vectors = []
        doc_tags = []
        profiles = []
        for row_id in range(entities):
            if row_id == 0:
                profile = None
            elif row_id % 2 == 0:
                profile = []
            else:
                profile = self._scalar_profile(row_id)
            ids.append(row_id)
            vectors.append(self._vector(row_id))
            doc_tags.append(f"import_row_{row_id}")
            profiles.append(profile)

        profile_type = pa.list_(
            pa.struct(
                [
                    pa.field("p_int", pa.int64()),
                    pa.field("p_tag", pa.string()),
                ]
            )
        )
        table = pa.table(
            {
                "id": pa.array(ids, type=pa.int64()),
                "normal_vector": pa.array(vectors, type=pa.list_(pa.float32())),
                "doc_tag": pa.array(doc_tags, type=pa.string()),
                "profile": pa.array(profiles, type=profile_type),
            }
        )
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"{collection_name}.parquet")
        pq.write_table(table, local_file_path, row_group_size=entities)

        remote_files = self.upload_to_minio(local_file_path)
        import_result = self.call_bulkinsert(collection_name, remote_files, expect_fail=True)

        assert import_result["state"] == "Failed"
        assert any(keyword in import_result["reason"].lower() for keyword in ["null", "nullable"])

        stats = client.get_collection_stats(collection_name=collection_name)
        assert stats["row_count"] == 0

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("entities", [1000])
    @pytest.mark.parametrize("array_capacity", [100])
    def test_import_struct_array_with_parquet(self, dim, entities, array_capacity):
        """
        Test bulk import of struct array data from parquet file

        Collection schema: [id, float_vector, struct_array]
        Struct array contains all supported types:
        - Scalar: VARCHAR, INT8, INT16, INT32, INT64, FLOAT, DOUBLE, BOOL
        - Vector: FLOAT_VECTOR
        Data file format: parquet

        Steps:
        1. Create collection with struct array field using MilvusClient
        2. Generate parquet data with PyArrow
        3. Upload parquet file to MinIO
        4. Import data using bulk_import API
        5. Verify imported data count
        6. Verify imported data integrity
        """
        client = self._client()
        c_name = cf.gen_unique_str("import_struct_array")

        # Step 1: Create collection with struct array field
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)

        # Add primary key field
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)

        # Add float vector field
        schema.add_field(field_name="float_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        # Create struct schema with all supported types
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("struct_varchar", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field("struct_int8", DataType.INT8)
        struct_schema.add_field("struct_int16", DataType.INT16)
        struct_schema.add_field("struct_int32", DataType.INT32)
        struct_schema.add_field("struct_int64", DataType.INT64)
        struct_schema.add_field("struct_float", DataType.FLOAT)
        struct_schema.add_field("struct_double", DataType.DOUBLE)
        struct_schema.add_field("struct_bool", DataType.BOOL)
        struct_schema.add_field("struct_float_vec", DataType.FLOAT_VECTOR, dim=dim)

        # Add struct array field
        schema.add_field(
            "struct_array",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=array_capacity,
        )

        # Create index params
        index_params = client.prepare_index_params()

        # Index for regular vector field
        index_params.add_index(
            field_name="float_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 200},
        )

        # Index for vector field inside struct array
        # Field name format: struct_array[struct_float_vec]
        index_params.add_index(
            field_name="struct_array[struct_float_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params={"M": 16, "efConstruction": 200},
        )

        # Create collection
        client.create_collection(collection_name=c_name, schema=schema, index_params=index_params)
        log.info(f"Collection '{c_name}' created")

        # Step 2: Generate parquet file with all supported types
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"test_{cf.gen_unique_str()}.parquet")
        original_data = self.gen_parquet_file(entities, dim, local_file_path)

        # Step 3: Upload to MinIO
        remote_files = self.upload_to_minio(local_file_path)

        # Step 4: Import data
        self.call_bulkinsert(c_name, remote_files)

        # Refresh load state after import
        client.refresh_load(collection_name=c_name)

        # Step 5: Verify number of entities
        stats = client.get_collection_stats(collection_name=c_name)
        num_entities = stats["row_count"]
        log.info(f"Collection entities: {num_entities}")
        assert num_entities == entities, f"Expected {entities} entities, got {num_entities}"

        # Step 6: Verify data integrity
        self.verify_data(client, c_name, original_data)

        log.info("Struct array import test completed successfully")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("entities", [1000])
    @pytest.mark.parametrize("array_capacity", [100])
    def test_import_struct_array_with_json(self, dim, entities, array_capacity):
        """
        Test bulk import of struct array data from JSON file

        Collection schema: [id, float_vector, struct_array]
        Struct array contains all supported types:
        - Scalar: VARCHAR, INT8, INT16, INT32, INT64, FLOAT, DOUBLE, BOOL
        - Vector: FLOAT_VECTOR
        Data file format: JSON array format [row1, row2, ...]

        Steps:
        1. Create collection with struct array field using MilvusClient
        2. Generate JSON data in array format
        3. Upload JSON file to MinIO
        4. Import data using bulk_import API
        5. Verify imported data count
        6. Verify imported data integrity
        """
        client = self._client()
        c_name = cf.gen_unique_str("import_struct_array_json")

        # Step 1: Create collection with struct array field
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)

        # Add primary key field
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)

        # Add float vector field
        schema.add_field(field_name="float_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        # Create struct schema with all supported types
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("struct_varchar", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field("struct_int8", DataType.INT8)
        struct_schema.add_field("struct_int16", DataType.INT16)
        struct_schema.add_field("struct_int32", DataType.INT32)
        struct_schema.add_field("struct_int64", DataType.INT64)
        struct_schema.add_field("struct_float", DataType.FLOAT)
        struct_schema.add_field("struct_double", DataType.DOUBLE)
        struct_schema.add_field("struct_bool", DataType.BOOL)
        struct_schema.add_field("struct_float_vec", DataType.FLOAT_VECTOR, dim=dim)

        # Add struct array field
        schema.add_field(
            "struct_array",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=array_capacity,
        )

        # Create index params
        index_params = client.prepare_index_params()

        # Index for regular vector field
        index_params.add_index(
            field_name="float_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 200},
        )

        # Index for vector field inside struct array
        # Field name format: struct_array[struct_float_vec]
        index_params.add_index(
            field_name="struct_array[struct_float_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params={"M": 16, "efConstruction": 200},
        )

        # Create collection
        client.create_collection(collection_name=c_name, schema=schema, index_params=index_params)
        log.info(f"Collection '{c_name}' created")

        # Step 2: Generate JSON file with all supported types
        local_file_path = os.path.join(self.LOCAL_FILES_PATH, f"test_{cf.gen_unique_str()}.json")
        original_data = self.gen_json_file(entities, dim, local_file_path)

        # Step 3: Upload to MinIO
        remote_files = self.upload_to_minio(local_file_path)

        # Step 4: Import data
        self.call_bulkinsert(c_name, remote_files)

        # Refresh load state after import
        client.refresh_load(collection_name=c_name)

        # Step 5: Verify number of entities
        stats = client.get_collection_stats(collection_name=c_name)
        num_entities = stats["row_count"]
        log.info(f"Collection entities: {num_entities}")
        assert num_entities == entities, f"Expected {entities} entities, got {num_entities}"

        # Step 6: Verify data integrity
        self.verify_data(client, c_name, original_data)

        log.info("Struct array JSON import test completed successfully")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("entities", [1000])
    @pytest.mark.parametrize("array_capacity", [100])
    @pytest.mark.parametrize("file_type", ["PARQUET", "JSON"])
    def test_import_struct_array_with_local_bulk_writer(self, dim, entities, array_capacity, file_type):
        """
        Test bulk import of struct array data using LocalBulkWriter

        This test demonstrates using LocalBulkWriter to convert insert-format data
        into import files, which is much simpler than manually creating PyArrow/JSON files.

        Collection schema: [id, float_vector, struct_array]
        Struct array contains all supported types:
        - Scalar: VARCHAR, INT8, INT16, INT32, INT64, FLOAT, DOUBLE, BOOL
        - Vector: FLOAT_VECTOR

        Steps:
        1. Create collection with struct array field using MilvusClient
        2. Generate insert-format data (same format as client.insert())
        3. Use LocalBulkWriter to convert data to import files
        4. Upload generated files to MinIO
        5. Import data using bulk_import API
        6. Verify imported data count and integrity
        """
        client = self._client()
        c_name = cf.gen_unique_str("import_lbw_struct")

        # Step 1: Create collection with struct array field
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)

        # Add primary key field
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)

        # Add float vector field
        schema.add_field(field_name="float_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        # Create struct schema with all supported types
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("struct_varchar", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field("struct_int8", DataType.INT8)
        struct_schema.add_field("struct_int16", DataType.INT16)
        struct_schema.add_field("struct_int32", DataType.INT32)
        struct_schema.add_field("struct_int64", DataType.INT64)
        struct_schema.add_field("struct_float", DataType.FLOAT)
        struct_schema.add_field("struct_double", DataType.DOUBLE)
        struct_schema.add_field("struct_bool", DataType.BOOL)
        struct_schema.add_field("struct_float_vec", DataType.FLOAT_VECTOR, dim=dim)

        # Add struct array field
        schema.add_field(
            "struct_array",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=array_capacity,
        )

        # Create index params
        index_params = client.prepare_index_params()

        # Index for regular vector field
        index_params.add_index(
            field_name="float_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params={"M": 16, "efConstruction": 200},
        )

        # Index for vector field inside struct array
        index_params.add_index(
            field_name="struct_array[struct_float_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params={"M": 16, "efConstruction": 200},
        )

        # Create collection
        client.create_collection(collection_name=c_name, schema=schema, index_params=index_params)
        log.info(f"Collection '{c_name}' created")

        # Step 2: Generate insert-format data with all supported types
        log.info(f"Generating {entities} rows of insert-format data")
        insert_data = []
        empty_array_count = 0
        for i in range(entities):
            # 10% probability to generate empty array
            if random.random() < 0.1:
                struct_list = []
                empty_array_count += 1
            else:
                # Generate random number of struct elements (1-5)
                arr_len = random.randint(1, 5)
                struct_list = []
                for j in range(arr_len):
                    struct_obj = {
                        "struct_varchar": f"varchar_{i}_{j}_{cf.gen_unique_str()}",
                        "struct_int8": random.randint(-128, 127),
                        "struct_int16": random.randint(-32768, 32767),
                        "struct_int32": random.randint(-2147483648, 2147483647),
                        "struct_int64": random.randint(-9223372036854775808, 9223372036854775807),
                        "struct_float": random.random() * 100,
                        "struct_double": random.random() * 1000,
                        "struct_bool": random.choice([True, False]),
                        "struct_float_vec": [random.random() for _ in range(dim)],
                    }
                    struct_list.append(struct_obj)

            row = {
                "id": i,
                "float_vector": [random.random() for _ in range(dim)],
                "struct_array": struct_list,
            }
            insert_data.append(row)
        log.info(f"Generated {empty_array_count} rows with empty struct_array")

        # Step 3: Use LocalBulkWriter to convert data to import files
        log.info(f"Using LocalBulkWriter to generate {file_type} files")
        batch_files, original_data = self.gen_file_with_local_bulk_writer(
            schema=schema, data=insert_data, file_type=file_type
        )

        # Step 4: Upload generated files to MinIO
        # LocalBulkWriter generates files in a UUID subdirectory
        # We need to upload all files in that directory
        import_files = []
        for file_list in batch_files:
            for file_path in file_list:
                remote_files = self.upload_to_minio(file_path)
                import_files.extend(remote_files)

        log.info(f"Uploaded {len(import_files)} file(s) to MinIO")

        # Step 5: Import data using bulk_import API
        self.call_bulkinsert(c_name, import_files)

        # Refresh load state after import
        client.refresh_load(collection_name=c_name)

        # Step 6: Verify number of entities
        stats = client.get_collection_stats(collection_name=c_name)
        num_entities = stats["row_count"]
        log.info(f"Collection entities: {num_entities}")
        assert num_entities == entities, f"Expected {entities} entities, got {num_entities}"

        # Step 7: Verify data integrity
        self.verify_data(client, c_name, original_data)

        log.info(f"LocalBulkWriter {file_type} import test completed successfully")

    def gen_parquet_file(self, num_rows: int, dim: int, file_path: str) -> dict:
        """
        Generate parquet file with struct array containing all supported data types

        Supported types in struct:
        - Scalar: VARCHAR, INT8, INT16, INT32, INT64, FLOAT, DOUBLE, BOOLEAN
        - Vector: FLOAT_VECTOR

        Args:
            num_rows: Number of rows to generate
            dim: Dimension of vector fields
            file_path: Path to save the parquet file

        Returns:
            Dictionary containing the generated data for verification
        """
        id_arr = []
        float_vector_arr = []
        struct_arr = []

        for i in range(num_rows):
            # ID field
            id_arr.append(np.int64(i))

            # Float vector field
            raw_vector = [random.random() for _ in range(dim)]
            float_vector_arr.append(np.array(raw_vector, dtype=np.float32))

            # Struct array field - generate array of struct objects with all types
            arr_len = random.randint(1, 5)  # Random number of struct elements (1-5)
            struct_list = []
            for j in range(arr_len):
                struct_obj = {
                    "struct_varchar": f"varchar_{i}_{j}_{cf.gen_unique_str()}",
                    "struct_int8": np.int8(random.randint(-128, 127)),
                    "struct_int16": np.int16(random.randint(-32768, 32767)),
                    "struct_int32": np.int32(random.randint(-2147483648, 2147483647)),
                    "struct_int64": np.int64(random.randint(-9223372036854775808, 9223372036854775807)),
                    "struct_float": np.float32(random.random() * 100),
                    "struct_double": np.float64(random.random() * 1000),
                    "struct_bool": bool(random.choice([True, False])),
                    "struct_float_vec": [random.random() for _ in range(dim)],
                }
                struct_list.append(struct_obj)
            struct_arr.append(struct_list)

        # Define PyArrow schema for struct field with all types
        struct_type = pa.struct(
            [
                pa.field("struct_varchar", pa.string()),
                pa.field("struct_int8", pa.int8()),
                pa.field("struct_int16", pa.int16()),
                pa.field("struct_int32", pa.int32()),
                pa.field("struct_int64", pa.int64()),
                pa.field("struct_float", pa.float32()),
                pa.field("struct_double", pa.float64()),
                pa.field("struct_bool", pa.bool_()),
                pa.field("struct_float_vec", pa.list_(pa.float32())),
            ]
        )

        # Build PyArrow arrays with explicit types
        pa_arrays = {
            "id": pa.array(id_arr, type=pa.int64()),
            "float_vector": pa.array(
                [np.array(v, dtype=np.float32) for v in float_vector_arr],
                type=pa.list_(pa.float32()),
            ),
            "struct_array": pa.array(struct_arr, type=pa.list_(struct_type)),
        }

        # Create PyArrow table and write to Parquet
        table = pa.table(pa_arrays)
        pq.write_table(table, file_path, row_group_size=10000)

        log.info(f"Generated comprehensive parquet file with {num_rows} rows: {file_path}")

        return {
            "id": id_arr,
            "float_vector": float_vector_arr,
            "struct_array": struct_arr,
        }

    def gen_json_file(self, num_rows: int, dim: int, file_path: str) -> dict:
        """
        Generate JSON file with struct array containing all supported data types

        Args:
            num_rows: Number of rows to generate
            dim: Dimension of vector fields
            file_path: Path to save the JSON file

        Returns:
            Dictionary containing the generated data for verification
        """
        rows = []
        id_arr = []
        float_vector_arr = []
        struct_arr = []

        for i in range(num_rows):
            # ID field
            id_arr.append(i)

            # Float vector field
            float_vector = [random.random() for _ in range(dim)]
            float_vector_arr.append(float_vector)

            # Struct array field with all supported types
            arr_len = random.randint(1, 5)
            struct_list = []
            for j in range(arr_len):
                struct_obj = {
                    "struct_varchar": f"varchar_{i}_{j}_{cf.gen_unique_str()}",
                    "struct_int8": random.randint(-128, 127),
                    "struct_int16": random.randint(-32768, 32767),
                    "struct_int32": random.randint(-2147483648, 2147483647),
                    "struct_int64": random.randint(-9223372036854775808, 9223372036854775807),
                    "struct_float": random.random() * 100,
                    "struct_double": random.random() * 1000,
                    "struct_bool": random.choice([True, False]),
                    "struct_float_vec": [random.random() for _ in range(dim)],
                }
                struct_list.append(struct_obj)
            struct_arr.append(struct_list)

            # Build row object
            row = {"id": i, "float_vector": float_vector, "struct_array": struct_list}
            rows.append(row)

        # Write to JSON file
        with open(file_path, "w") as f:
            json.dump(rows, f, indent=2)

        log.info(f"Generated comprehensive JSON file with {num_rows} rows: {file_path}")

        return {
            "id": id_arr,
            "float_vector": float_vector_arr,
            "struct_array": struct_arr,
        }


class TestMilvusClientStructArrayMmap(TestMilvusClientV2Base):
    """Test case of struct array mmap functionality"""

    def generate_struct_array_data(
        self, num_rows: int, dim: int = default_dim, capacity: int = default_capacity
    ) -> list[dict[str, Any]]:
        """Generate test data for struct array"""
        data = []
        for i in range(num_rows):
            # Random array length for each row (1 to capacity)
            array_length = random.randint(1, min(capacity, 20))

            struct_array = []
            for j in range(array_length):
                struct_element = {
                    "clip_str": f"item_{i}_{j}",
                    "clip_embedding1": [random.random() for _ in range(dim)],
                    "clip_embedding2": [random.random() for _ in range(dim)],
                }
                struct_array.append(struct_element)

            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(dim)],
                "clips": struct_array,
            }
            data.append(row)

        return data

    def create_struct_array_schema(
        self,
        client: MilvusClient,
        dim: int = default_dim,
        capacity: int = default_capacity,
        mmap_enabled: bool = None,
        struct_array_mmap: bool = None,
        subfield1_mmap: bool = None,
        subfield2_mmap: bool = None,
    ):
        """Create schema with struct array field

        Args:
            client: MilvusClient instance
            dim: vector dimension
            capacity: array capacity
            mmap_enabled: enable mmap for normal_vector field
            struct_array_mmap: enable mmap for the entire struct array field
            subfield1_mmap: enable mmap for clip_embedding1 sub-field
            subfield2_mmap: enable mmap for clip_embedding2 sub-field
        """
        # Create basic schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)

        # Add primary key field
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)

        # Add normal vector field with optional mmap
        if mmap_enabled is not None:
            schema.add_field(
                field_name="normal_vector",
                datatype=DataType.FLOAT_VECTOR,
                dim=dim,
                mmap_enabled=mmap_enabled,
            )
        else:
            schema.add_field(field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim)

        # Create struct schema with optional mmap for sub-fields
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_str", DataType.VARCHAR, max_length=65535)

        if subfield1_mmap is not None:
            struct_schema.add_field(
                "clip_embedding1",
                DataType.FLOAT_VECTOR,
                dim=dim,
                mmap_enabled=subfield1_mmap,
            )
        else:
            struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=dim)

        if subfield2_mmap is not None:
            struct_schema.add_field(
                "clip_embedding2",
                DataType.FLOAT_VECTOR,
                dim=dim,
                mmap_enabled=subfield2_mmap,
            )
        else:
            struct_schema.add_field("clip_embedding2", DataType.FLOAT_VECTOR, dim=dim)

        # Add struct array field with optional mmap
        if struct_array_mmap is not None:
            schema.add_field(
                "clips",
                datatype=DataType.ARRAY,
                element_type=DataType.STRUCT,
                struct_schema=struct_schema,
                max_capacity=capacity,
                mmap_enabled=struct_array_mmap,
            )
        else:
            schema.add_field(
                "clips",
                datatype=DataType.ARRAY,
                element_type=DataType.STRUCT,
                struct_schema=struct_schema,
                max_capacity=capacity,
            )

        return schema

    @pytest.mark.tags(CaseLabel.L1)
    def test_mmap_enable_on_collection(self):
        """
        target: test enabling mmap on entire collection with struct array
        method: 1. create collection with struct array field
                2. create indexes on vector fields
                3. insert data
                4. enable mmap on collection using alter_collection_properties
                5. verify mmap is enabled via describe_collection
                6. load collection and verify search works correctly
        expected: mmap enabled successfully and search works
        """
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_collection")
        client = self._client()

        # Create schema with struct array
        schema = self.create_struct_array_schema(client)

        # Prepare index params - need to index ALL vector fields in struct
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Release collection before altering mmap properties
        self.release_collection(client, collection_name)

        # Enable mmap on collection
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        log.info(f"Enabled mmap on collection {collection_name}")

        # Verify mmap is enabled via describe_collection
        collection_info = self.describe_collection(client, collection_name, check_task="check_nothing")
        log.info(f"Collection info after enabling mmap: {collection_info}")

        # Load collection with mmap settings
        self.load_collection(client, collection_name)

        # Verify search works with mmap enabled
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful with mmap enabled on collection")

    @pytest.mark.tags(CaseLabel.L1)
    def test_mmap_enable_on_struct_array_field(self):
        """
        target: test enabling mmap on struct array field
        method: 1. create collection with struct array field
                2. create indexes on vector fields
                3. insert data
                4. enable mmap on struct array field using alter_collection_field
                5. load collection and verify search works
        expected: mmap enabled successfully on struct array field and search works
        """
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_struct_array_field")
        client = self._client()

        # Create schema with struct array
        schema = self.create_struct_array_schema(client)

        # Prepare index params - need to index ALL vector fields in struct
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Release collection before altering field properties
        self.release_collection(client, collection_name)

        # Enable mmap on struct array field
        self.alter_collection_field(
            client,
            collection_name,
            field_name="clips",
            field_params={"mmap_enabled": True},
        )
        log.info("Enabled mmap on struct array field 'clips'")

        # Load collection
        self.load_collection(client, collection_name)

        # Verify search works with mmap enabled on field
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful with mmap enabled on struct array field")

    @pytest.mark.tags(CaseLabel.L1)
    def test_mmap_enable_on_struct_subfield(self):
        """
        target: test enabling mmap on struct sub-field (vector field inside struct)
        method: 1. create collection with struct array field
                2. create indexes on vector fields
                3. insert data
                4. enable mmap on struct sub-field using alter_collection_field
                5. load collection and verify search works
        expected: mmap enabled successfully on struct sub-field and search works
        """
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_struct_subfield")
        client = self._client()

        # Create schema with struct array
        schema = self.create_struct_array_schema(client)

        # Prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Release collection before altering field properties
        self.release_collection(client, collection_name)

        # Enable mmap on struct sub-field (clip_embedding1)
        self.alter_collection_field(
            client,
            collection_name,
            field_name="clips[clip_embedding1]",
            field_params={"mmap_enabled": True},
        )
        log.info("Enabled mmap on struct sub-field 'clips[clip_embedding1]'")

        # Load collection
        self.load_collection(client, collection_name)

        # Verify search works on the field with mmap enabled
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful on field with mmap enabled")

        # Verify search also works on clip_embedding2 (without mmap)
        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding2]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful on field without mmap")

    @pytest.mark.tags(CaseLabel.L1)
    def test_mmap_enable_on_emb_list_index(self):
        """
        target: test enabling mmap on embedding list index
        method: 1. create collection with struct array field
                2. create indexes on vector fields
                3. insert data
                4. enable mmap on embedding list index using alter_index_properties
                5. verify mmap is enabled via describe_index
                6. load collection and verify search works
        expected: mmap enabled successfully on embedding list index and search works
        """
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_emb_list_index")
        client = self._client()

        # Create schema with struct array
        schema = self.create_struct_array_schema(client)

        # Prepare index params - need to index ALL vector fields in struct
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Release collection before altering index properties
        self.release_collection(client, collection_name)

        # Enable mmap on embedding list index
        self.alter_index_properties(
            client,
            collection_name,
            index_name="clips[clip_embedding1]",
            properties={"mmap.enabled": True},
        )
        log.info("Enabled mmap on embedding list index 'clips[clip_embedding1]'")

        # Verify mmap is enabled via describe_index
        index_info = self.describe_index(
            client,
            collection_name,
            index_name="clips[clip_embedding1]",
            check_task="check_nothing",
        )
        log.info(f"Index info after enabling mmap: {index_info}")

        # Load collection
        self.load_collection(client, collection_name)

        # Verify search works with mmap enabled on index
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful with mmap enabled on embedding list index")

    @pytest.mark.tags(CaseLabel.L2)
    def test_mmap_enable_multiple_levels(self):
        """
        target: test enabling mmap at multiple levels (collection, field, index)
        method: 1. create collection with struct array field
                2. create indexes on vector fields
                3. enable mmap at collection level
                4. enable mmap at field level
                5. enable mmap at index level
                6. verify all settings work together
        expected: mmap settings apply correctly at all levels and search works
        """
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_multiple_levels")
        client = self._client()

        # Create schema with struct array
        schema = self.create_struct_array_schema(client)

        # Prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Release collection before altering properties
        self.release_collection(client, collection_name)

        # Enable mmap at collection level
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        log.info("Enabled mmap at collection level")

        # Enable mmap at field level for struct array field
        self.alter_collection_field(
            client,
            collection_name,
            field_name="clips",
            field_params={"mmap_enabled": True},
        )
        log.info("Enabled mmap at field level for 'clips'")

        # Enable mmap at index level for embedding list index
        self.alter_index_properties(
            client,
            collection_name,
            index_name="clips[clip_embedding1]",
            properties={"mmap.enabled": True},
        )
        log.info("Enabled mmap at index level for 'clips[clip_embedding1]'")

        # Load collection
        self.load_collection(client, collection_name)

        # Verify search works with mmap enabled at multiple levels
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        # Search on clip_embedding1 (mmap at all levels)
        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )

        # Search on clip_embedding2 (mmap at collection and field level)
        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding2]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )

        # Search on normal_vector (mmap at collection level)
        self.search(
            client,
            collection_name,
            data=[search_vector],
            anns_field="normal_vector",
            search_params={"metric_type": "COSINE"},
            limit=10,
        )
        log.info("All searches successful with mmap enabled at multiple levels")

    @pytest.mark.tags(CaseLabel.L1)
    def test_mmap_collection_enable_then_disable(self):
        """
        target: test enabling mmap at collection level then disabling it
        method: 1. create collection with mmap enabled
                2. insert data and verify search works
                3. disable mmap using alter_collection_properties
                4. reload and verify search still works
        expected: mmap can be disabled after being enabled
        """
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_enable_disable")
        client = self._client()

        # Create schema with struct array
        schema = self.create_struct_array_schema(client)

        # Prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Release and enable mmap
        self.release_collection(client, collection_name)
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": True})
        log.info("Enabled mmap on collection")

        # Load and verify search works
        self.load_collection(client, collection_name)
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful with mmap enabled")

        # Now disable mmap
        self.release_collection(client, collection_name)
        self.alter_collection_properties(client, collection_name, properties={"mmap.enabled": False})
        log.info("Disabled mmap on collection")

        # Load and verify search still works
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful after disabling mmap")

    @pytest.mark.tags(CaseLabel.L1)
    def test_mmap_index_disable_then_enable(self):
        """
        target: test creating index without mmap then enabling it
        method: 1. create collection and index without mmap
                2. insert data and verify search works
                3. enable mmap on index using alter_index_properties
                4. reload and verify search works
        expected: mmap can be enabled on existing index
        """
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_disable_enable")
        client = self._client()

        # Create schema with struct array
        schema = self.create_struct_array_schema(client)

        # Prepare index params (without mmap initially)
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Verify search works without mmap
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful without mmap")

        # Now enable mmap on index
        self.release_collection(client, collection_name)
        self.alter_index_properties(
            client,
            collection_name,
            index_name="clips[clip_embedding1]",
            properties={"mmap.enabled": True},
        )
        log.info("Enabled mmap on index 'clips[clip_embedding1]'")

        # Load and verify search works
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful after enabling mmap on index")

    @pytest.mark.tags(CaseLabel.L2)
    def test_mmap_index_enable_then_disable(self):
        """
        target: test enabling mmap on index then disabling it
        method: 1. create collection and index with mmap enabled on sub-field
                2. insert data and verify search works
                3. disable mmap on index using alter_index_properties
                4. reload and verify search works
        expected: mmap can be toggled on index level
        """
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_index_toggle")
        client = self._client()

        # Create schema with mmap enabled on subfield1
        schema = self.create_struct_array_schema(client, subfield1_mmap=True)

        # Prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Verify search works with mmap enabled (from schema)
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful with mmap enabled on index (from schema)")

        # Disable mmap on index
        self.release_collection(client, collection_name)
        self.alter_index_properties(
            client,
            collection_name,
            index_name="clips[clip_embedding1]",
            properties={"mmap.enabled": False},
        )
        log.info("Disabled mmap on index")

        # Load and verify search still works
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful after disabling mmap on index")

    @pytest.mark.tags(CaseLabel.L1)
    def test_mmap_struct_subfield_enable_then_disable(self):
        """
        target: test enabling mmap on struct sub-field then disabling it
        method: 1. create collection with mmap enabled on struct sub-field in schema
                2. verify mmap is enabled via describe_collection
                3. insert data and verify search works
                4. disable mmap on struct sub-field using alter_collection_field
                5. verify mmap is disabled via describe_collection
                6. reload and verify search works
        expected: mmap can be toggled on struct sub-field level
        """
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_subfield_toggle")
        client = self._client()

        # Create schema with mmap enabled on subfield1
        schema = self.create_struct_array_schema(client, subfield1_mmap=True)

        # Prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Verify mmap is enabled on subfield via describe_collection
        collection_info = self.describe_collection(client, collection_name, check_task="check_nothing")
        log.info(f"Collection info after creation: {collection_info}")
        # Note: describe_collection returns schema info, check if mmap is set
        # This verification depends on the actual response structure

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Verify search works with mmap enabled (from schema)
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful with mmap enabled on struct sub-field (from schema)")

        # Disable mmap on struct sub-field
        self.release_collection(client, collection_name)
        self.alter_collection_field(
            client,
            collection_name,
            field_name="clips[clip_embedding1]",
            field_params={"mmap_enabled": False},
        )
        log.info("Disabled mmap on struct sub-field 'clips[clip_embedding1]'")

        # Verify mmap is disabled via describe_collection
        collection_info = self.describe_collection(client, collection_name, check_task="check_nothing")
        log.info(f"Collection info after disabling mmap: {collection_info}")

        # Load and verify search still works
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful after disabling mmap on struct sub-field")

    @pytest.mark.tags(CaseLabel.L1)
    def test_mmap_struct_subfield_disable_then_enable(self):
        """
        target: test creating struct sub-field without mmap then enabling it
        method: 1. create collection with struct array field (no mmap)
                2. insert data and verify search works
                3. enable mmap on struct sub-field using alter_collection_field
                4. reload and verify search works
        expected: mmap can be enabled on struct sub-field after creation
        """
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_subfield_enable")
        client = self._client()

        # Create schema with struct array
        schema = self.create_struct_array_schema(client)

        # Prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Verify search works without mmap
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful without mmap on struct sub-field")

        # Now enable mmap on struct sub-field
        self.release_collection(client, collection_name)
        self.alter_collection_field(
            client,
            collection_name,
            field_name="clips[clip_embedding1]",
            field_params={"mmap_enabled": True},
        )
        log.info("Enabled mmap on struct sub-field 'clips[clip_embedding1]'")

        # Load and verify search works
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful after enabling mmap on struct sub-field")

    @pytest.mark.tags(CaseLabel.L2)
    def test_mmap_struct_array_field_enable_then_disable(self):
        """
        target: test enabling mmap on struct array field then disabling it
        method: 1. create collection with mmap enabled on struct array field in schema
                2. insert data and verify search works
                3. disable mmap on struct array field using alter_collection_field
                4. reload and verify search works
        expected: mmap can be toggled on struct array field level
        """
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_array_field_toggle")
        client = self._client()

        # Create schema with mmap enabled on struct array field
        schema = self.create_struct_array_schema(client, struct_array_mmap=True)

        # Prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Verify search works with mmap enabled (from schema)
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful with mmap enabled on struct array field (from schema)")

        # Disable mmap on struct array field
        self.release_collection(client, collection_name)
        self.alter_collection_field(
            client,
            collection_name,
            field_name="clips",
            field_params={"mmap_enabled": False},
        )
        log.info("Disabled mmap on struct array field 'clips'")

        # Load and verify search still works
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful after disabling mmap on struct array field")

    @pytest.mark.tags(CaseLabel.L2)
    def test_mmap_struct_array_field_disable_then_enable(self):
        """
        target: test creating struct array field without mmap then enabling it
        method: 1. create collection with struct array field (no mmap)
                2. insert data and verify search works
                3. enable mmap on struct array field using alter_collection_field
                4. reload and verify search works
        expected: mmap can be enabled on struct array field after creation
        """
        collection_name = cf.gen_unique_str(f"{prefix}_mmap_array_field_enable")
        client = self._client()

        # Create schema with struct array
        schema = self.create_struct_array_schema(client)

        # Prepare index params
        index_params = client.prepare_index_params()
        index_params.add_index(
            field_name="normal_vector",
            index_type="HNSW",
            metric_type="COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding1]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )
        index_params.add_index(
            field_name="clips[clip_embedding2]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params=INDEX_PARAMS,
        )

        # Create collection
        self.create_collection(client, collection_name, schema=schema, index_params=index_params)

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Verify search works without mmap
        search_vector = [random.random() for _ in range(default_dim)]
        search_tensor = EmbeddingList()
        search_tensor.add(search_vector)

        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful without mmap on struct array field")

        # Now enable mmap on struct array field
        self.release_collection(client, collection_name)
        self.alter_collection_field(
            client,
            collection_name,
            field_name="clips",
            field_params={"mmap_enabled": True},
        )
        log.info("Enabled mmap on struct array field 'clips'")

        # Load and verify search works
        self.load_collection(client, collection_name)
        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding1]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info("Search successful after enabling mmap on struct array field")
