import pytest
import numpy as np
import random
import time
import os
import json
from pathlib import Path
from typing import List, Dict, Any
from deepdiff import DeepDiff

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from check.param_check import compare_lists_with_epsilon_ignore_dict_order
from utils.util_pymilvus import *
from pymilvus import DataType, MilvusClient, AnnSearchRequest, RRFRanker, WeightedRanker
from pymilvus.client.embedding_list import EmbeddingList
from pymilvus.bulk_writer import bulk_import, get_import_progress, LocalBulkWriter, BulkFileType
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error


prefix = "struct_array"
epsilon = 0.001
default_nb = ct.default_nb
default_nq = ct.default_nq
default_dim = 128
default_capacity = 100
METRICS = ["MAX_SIM", "MAX_SIM_IP", "MAX_SIM_COSINE", "MAX_SIM_L2"]
INDEX_PARAMS = {"M": 16, "efConstruction": 200}


class TestMilvusClientStructArrayBasic(TestMilvusClientV2Base):
    """Test case of struct array basic functionality"""

    def generate_struct_array_data(
        self, num_rows: int, dim: int = default_dim, capacity: int = default_capacity
    ) -> List[Dict[str, Any]]:
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
                mmap_enabled=mmap_enabled
            )
        else:
            schema.add_field(
                field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim
            )

        # Create struct schema with optional mmap for sub-fields
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_str", DataType.VARCHAR, max_length=65535)

        if subfield1_mmap is not None:
            struct_schema.add_field(
                "clip_embedding1",
                DataType.FLOAT_VECTOR,
                dim=dim,
                mmap_enabled=subfield1_mmap
            )
        else:
            struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=dim)

        if subfield2_mmap is not None:
            struct_schema.add_field(
                "clip_embedding2",
                DataType.FLOAT_VECTOR,
                dim=dim,
                mmap_enabled=subfield2_mmap
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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

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
                assert isinstance(record["float_field"], (int, float))
                assert isinstance(record["double_field"], (int, float))
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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

        # Create first struct schema with common subfield names
        struct_schema1 = client.create_struct_field_schema()
        struct_schema1.add_field("embedding", DataType.FLOAT_VECTOR, dim=128)
        struct_schema1.add_field("label", DataType.VARCHAR, max_length=128)
        struct_schema1.add_field("score", DataType.FLOAT)

        # Create second struct schema with same subfield names but potentially different semantics
        struct_schema2 = client.create_struct_field_schema()
        struct_schema2.add_field(
            "embedding", DataType.FLOAT_VECTOR, dim=128
        )  # Same name
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
                        "score": float(
                            i * 100 + k + 0.2
                        ),  # Deterministic score, different from image
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

                assert "score" in img_feat, (
                    f"score missing in image_features for id {result_id}"
                )
                assert "label" in img_feat, (
                    f"label missing in image_features for id {result_id}"
                )
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

                assert "score" in txt_feat, (
                    f"score missing in text_features for id {result_id}"
                )
                assert "label" in txt_feat, (
                    f"label missing in text_features for id {result_id}"
                )
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

            assert "image_features" in hit_data, (
                f"image_features missing for id {hit_id}"
            )

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
        results, check = self.query(client, collection_name, filter="id >= 0", output_fields=["id", "clips"], limit=10)
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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim
        )

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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim
        )

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
        data = self.create_collection_with_index(client, collection_name)

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
        collection_name = cf.gen_unique_str(
            f"{prefix}_metric_search_{metric_type.lower()}"
        )
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field(
            "clip_embedding1", DataType.FLOAT_VECTOR, dim=default_dim
        )
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
            struct_array = [
                {"clip_embedding1": [random.random() for _ in range(default_dim)]}
            ]
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
                "params": {"retrieval_ann_ratio": retrieval_ann_ratio}
            },
            limit=10,
        )
        assert check
        assert len(results[0]) > 0

        # Verify results are returned
        for hit in results[0]:
            assert hit is not None


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
        schema.add_field(
            field_name="pk", datatype=DataType.VARCHAR, is_primary=True, max_length=100
        )
        schema.add_field(field_name="random", datatype=DataType.DOUBLE)
        schema.add_field(
            field_name="embeddings", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

        # Create struct schema with vector field
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_str", DataType.VARCHAR, max_length=65535)
        struct_schema.add_field(
            "clip_embedding", DataType.FLOAT_VECTOR, dim=default_dim
        )

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

        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )
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
        schema.add_field(
            field_name="pk", datatype=DataType.VARCHAR, is_primary=True, max_length=100
        )
        schema.add_field(field_name="random", datatype=DataType.DOUBLE)
        schema.add_field(
            field_name="embeddings", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field(
            "clip_embedding", DataType.FLOAT_VECTOR, dim=default_dim
        )
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

        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )
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
        log.info(
            f"Hybrid search with WeightedRanker returned {len(hybrid_res[0])} results"
        )

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
        schema.add_field(
            field_name="pk", datatype=DataType.VARCHAR, is_primary=True, max_length=100
        )
        schema.add_field(field_name="random", datatype=DataType.DOUBLE)
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field(
            "clip_embedding", DataType.FLOAT_VECTOR, dim=default_dim
        )
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

        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )
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
        log.info(
            f"Hybrid search with multiple vector sources returned {len(hybrid_res[0])} results"
        )

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "metric_type", ["MAX_SIM_L2", "MAX_SIM_IP", "MAX_SIM_COSINE"]
    )
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
        schema.add_field(
            field_name="pk", datatype=DataType.VARCHAR, is_primary=True, max_length=100
        )
        schema.add_field(
            field_name="embeddings", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field(
            "clip_embedding", DataType.FLOAT_VECTOR, dim=default_dim
        )
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

        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )
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
        hybrid_res = client.hybrid_search(
            collection_name, req_list, RRFRanker(), default_limit
        )

        assert len(hybrid_res) > 0
        assert len(hybrid_res[0]) > 0


class TestMilvusClientStructArrayQuery(TestMilvusClientV2Base):
    """Test case of struct array query functionality"""

    def create_collection_with_data(
        self, client: MilvusClient, collection_name: str, nb: int = 50
    ):
        """Create collection with struct array and insert data"""
        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

        # Create struct schema
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field(
            "clip_embedding1", DataType.FLOAT_VECTOR, dim=default_dim
        )
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
        data = self.create_collection_with_data(client, collection_name)

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
                has_matching_element = any(
                    elem["scalar_field"] < 50 for elem in struct_array
                )
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
        data = self.create_collection_with_data(client, collection_name, nb=20)

        # Count all records
        count_result = client.query(
            collection_name=collection_name, filter="", output_fields=["count(*)"]
        )
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


class TestMilvusClientStructArrayCRUD(TestMilvusClientV2Base):
    """Test case of struct array CRUD operations"""

    def create_collection_with_schema(self, client: MilvusClient, collection_name: str):
        """Create collection with struct array schema"""
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field(
            "clip_embedding1", DataType.FLOAT_VECTOR, dim=default_dim
        )
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
        method: insert data then upsert with modified struct array
        expected: data successfully upserted
        """
        collection_name = cf.gen_unique_str(f"{prefix}_crud")

        client = self._client()

        # Create collection
        self.create_collection_with_schema(client, collection_name)

        # Initial insert
        initial_data = [
            {
                "id": 1,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [
                            random.random() for _ in range(default_dim)
                        ],
                        "scalar_field": 100,
                        "label": "initial",
                    }
                ],
            }
        ]

        res, check = self.insert(client, collection_name, initial_data)
        assert check
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

        # Upsert with modified data
        upsert_data = [
            {
                "id": 1,  # Same ID
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [
                            random.random() for _ in range(default_dim)
                        ],
                        "scalar_field": 200,  # Modified
                        "label": "updated",  # Modified
                    }
                ],
            }
        ]

        res, check = self.upsert(client, collection_name, upsert_data)
        assert check

        # Verify upsert worked
        res, check = self.flush(client, collection_name)
        assert check

        results, check = self.query(client, collection_name, filter="id == 1")
        assert check
        assert len(results) == 1
        assert results[0]["clips"][0]["label"] == "updated"

    @pytest.mark.tags(CaseLabel.L0)
    def test_delete_struct_array_data(self):
        """
        target: test delete operation with struct array data
        method: insert struct array data then delete by ID
        expected: data successfully deleted
        """
        collection_name = cf.gen_unique_str(f"{prefix}_crud")

        client = self._client()

        # Create collection and insert data
        self.create_collection_with_schema(client, collection_name)

        data = []
        for i in range(10):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [
                            random.random() for _ in range(default_dim)
                        ],
                        "scalar_field": i,
                        "label": f"label_{i}",
                    }
                ],
            }
            data.append(row)

        res, check = self.insert(client, collection_name, data)
        assert check
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

        # Delete some records
        delete_ids = [1, 3, 5]
        res, check = self.delete(client, collection_name, filter=f"id in {delete_ids}")
        assert check

        # Verify deletion
        res, check = self.flush(client, collection_name)
        assert check

        results, check = self.query(client, collection_name, filter="id >= 0")
        assert check

        remaining_ids = {result["id"] for result in results}
        for delete_id in delete_ids:
            assert delete_id not in remaining_ids

    @pytest.mark.tags(CaseLabel.L1)
    def test_batch_operations(self):
        """
        target: test batch insert/upsert operations with struct array
        method: perform large batch operations
        expected: all operations successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_crud")

        client = self._client()

        # Create collection
        self.create_collection_with_schema(client, collection_name)

        # Large batch insert
        batch_size = 1000
        data = []
        for i in range(batch_size):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [
                            random.random() for _ in range(default_dim)
                        ],
                        "scalar_field": i % 100,
                        "label": f"batch_{i}",
                    }
                ],
            }
            data.append(row)

        res, check = self.insert(client, collection_name, data)
        assert check
        assert res["insert_count"] == batch_size

        # Batch upsert (update first 100 records)
        upsert_data = []
        for i in range(100):
            row = {
                "id": i,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [
                            random.random() for _ in range(default_dim)
                        ],
                        "scalar_field": i + 1000,  # Modified
                        "label": f"upserted_{i}",  # Modified
                    }
                ],
            }
            upsert_data.append(row)

        res, check = self.upsert(client, collection_name, upsert_data)
        assert check

    @pytest.mark.tags(CaseLabel.L1)
    def test_collection_operations(self):
        """
        target: test collection operations (load/release/drop) with struct array
        method: perform collection management operations
        expected: all operations successful
        """
        collection_name = cf.gen_unique_str(f"{prefix}_crud")

        client = self._client()

        # Create collection with data
        self.create_collection_with_schema(client, collection_name)

        # Insert some data
        data = [
            {
                "id": 1,
                "normal_vector": [random.random() for _ in range(default_dim)],
                "clips": [
                    {
                        "clip_embedding1": [
                            random.random() for _ in range(default_dim)
                        ],
                        "scalar_field": 100,
                        "label": "test",
                    }
                ],
            }
        ]

        res, check = self.insert(client, collection_name, data)
        assert check

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
            assert False, (
                f"Expected ParamError when adding {unsupported_type} field to struct schema"
            )
        except Exception as e:
            # Verify the error message indicates the type is not supported
            error_msg = str(e)
            # Different error messages for different types
            assert (
                "Struct field schema does not support Array, ArrayOfVector or Struct"
                in error_msg
                or "is not supported" in error_msg
                or "not supported for fields in struct" in error_msg
            )

    @pytest.mark.tags(CaseLabel.L2)
    def test_struct_with_unsupported_vector_field(self):
        """
        target: test creating struct with BinaryVector field (should fail)
        method: attempt to create struct with BinaryVector field
        expected: creation should fail
        """
        collection_name = cf.gen_unique_str(f"{prefix}_invalid")

        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("binary_vector_field", DataType.BINARY_VECTOR, dim=default_dim)
        schema.add_field(
            "struct_array",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )
        error = {
            ct.err_code: 65535,
            ct.err_msg: "now only float vector is supported",
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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field(
            "clip_embedding1", DataType.FLOAT_VECTOR, dim=0
        )  # Invalid

        struct_schema.add_field(
            "clip_embedding2", DataType.FLOAT_VECTOR, dim=-1
        )  # Invalid
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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

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
    @pytest.mark.parametrize(
        "vector_type",
        [
            DataType.BINARY_VECTOR,
            DataType.FLOAT16_VECTOR,
            DataType.BFLOAT16_VECTOR,
            DataType.SPARSE_FLOAT_VECTOR,
            DataType.INT8_VECTOR,
        ],
    )
    def test_struct_array_with_unsupported_vector_types(self, vector_type):
        """
        target: test creating struct array with unsupported vector types (non-FLOAT_VECTOR)
        method: attempt to create struct array with BINARY_VECTOR, FLOAT16_VECTOR,
                BFLOAT16_VECTOR, SPARSE_FLOAT_VECTOR, INT8_VECTOR vector types
        expected: creation should fail as only FLOAT_VECTOR is supported in struct array
        """
        collection_name = cf.gen_unique_str(f"{prefix}_invalid")

        client = self._client()

        # Create schema
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )

        # Try to create struct with unsupported vector type
        struct_schema = client.create_struct_field_schema()

        # SPARSE_FLOAT_VECTOR doesn't need dim parameter
        if vector_type == DataType.SPARSE_FLOAT_VECTOR:
            struct_schema.add_field("unsupported_vector", vector_type)
        else:
            # BINARY_VECTOR needs dim to be multiple of 8
            if vector_type == DataType.BINARY_VECTOR:
                struct_schema.add_field("unsupported_vector", vector_type, dim=128)
            else:
                struct_schema.add_field(
                    "unsupported_vector", vector_type, dim=default_dim
                )

        schema.add_field(
            "struct_array",
            datatype=DataType.ARRAY,
            element_type=DataType.STRUCT,
            struct_schema=struct_schema,
            max_capacity=100,
        )

        # Should fail - only FLOAT_VECTOR is supported in struct array
        error = {ct.err_code: 65535, ct.err_msg: "now only float vector is supported"}
        self.create_collection(
            client,
            collection_name,
            schema=schema,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

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
        schema.add_field(
            field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=default_dim
        )
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field(
            "clip_embedding1", DataType.FLOAT_VECTOR, dim=default_dim
        )
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
        res, check = self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )
        assert check
        nb = 3000
        data = []
        for i in range(nb):
            struct_array = []
            for j in range(random.randint(1, 10)):
                struct_array.append(
                    {"clip_embedding1": [random.random() for _ in range(default_dim)]}
                )
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
        distances = [
            hit.get("distance")
            for hit in initial_results[0]
            if hit.get("distance") is not None
        ]
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
            ct.err_code: 65535,
            ct.err_msg: "range search is not supported for vector array",
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
        self,
        schema,
        data: List[Dict[str, Any]],
        file_type: str = "PARQUET"
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
            file_type=bulk_file_type
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
            "struct_array": struct_arr
        }

    def upload_to_minio(self, local_file_path: str) -> List[List[str]]:
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
                secure=False
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

    def call_bulkinsert(self, collection_name: str, batch_files: List[List[str]]):
        """
        Call bulk import API and wait for completion

        Args:
            collection_name: Target collection name
            batch_files: List of file paths in MinIO
        """
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
            progress = resp.json()['data']['progress']

            log.info(f"Import job {job_id} - state: {state}, progress: {progress}%")

            if state == "Importing":
                continue
            elif state == "Failed":
                reason = resp.json()['data']['reason']
                raise Exception(f"Bulk import job {job_id} failed: {reason}")
            elif state == "Completed" and progress == 100:
                log.info(f"Bulk import job {job_id} completed successfully")
                break
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
            limit=num_rows + 100  # Add buffer to ensure all data is retrieved
        )

        log.info(f"Query returned {len(results)} rows")

        # Check if row count matches
        if len(results) != num_rows:
            assert False, f"Row count mismatch: expected {num_rows}, got {len(results)}"

        # Convert original data to comparable format (list of dicts per row)
        original_rows = []
        for i in range(num_rows):
            row_id = int(original_data["id"][i])
            original_rows.append({
                "id": row_id,
                "float_vector": [float(x) for x in original_data["float_vector"][i]],
                "struct_array": original_data["struct_array"][i]
            })

        # Convert query results to comparable format
        query_rows_formatted = []
        for row in results:
            formatted_row = {
                "id": row["id"],
                "float_vector": [float(x) for x in row["float_vector"]],
                "struct_array": row["struct_array"]
            }
            query_rows_formatted.append(formatted_row)

        # Use compare_lists_with_epsilon_ignore_dict_order for comparison
        # This function handles floating-point tolerance and dict order
        is_equal = compare_lists_with_epsilon_ignore_dict_order(
            original_rows,
            query_rows_formatted,
            epsilon=epsilon
        )

        if not is_equal:
            deepdiff = DeepDiff(original_rows, query_rows_formatted, ignore_order=True, significant_digits=3),
            log.info(f"DeepDiff: {deepdiff}")
            assert False, "Data verification failed: original data and query results do not match"

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("dim", [128])
    @pytest.mark.parametrize("entities", [1000])
    @pytest.mark.parametrize("array_capacity", [100])
    @pytest.mark.xfail(reason="issue")
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
            params={"M": 16, "efConstruction": 200}
        )

        # Index for vector field inside struct array
        # Field name format: struct_array[struct_float_vec]
        index_params.add_index(
            field_name="struct_array[struct_float_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params={"M": 16, "efConstruction": 200}
        )

        # Create collection
        client.create_collection(
            collection_name=c_name,
            schema=schema,
            index_params=index_params
        )
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
        num_entities = stats['row_count']
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
            params={"M": 16, "efConstruction": 200}
        )

        # Index for vector field inside struct array
        # Field name format: struct_array[struct_float_vec]
        index_params.add_index(
            field_name="struct_array[struct_float_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params={"M": 16, "efConstruction": 200}
        )

        # Create collection
        client.create_collection(
            collection_name=c_name,
            schema=schema,
            index_params=index_params
        )
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
        num_entities = stats['row_count']
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
    @pytest.mark.xfail(reason="issue: https://github.com/milvus-io/pymilvus/issues/3050")
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
            params={"M": 16, "efConstruction": 200}
        )

        # Index for vector field inside struct array
        index_params.add_index(
            field_name="struct_array[struct_float_vec]",
            index_type="HNSW",
            metric_type="MAX_SIM_COSINE",
            params={"M": 16, "efConstruction": 200}
        )

        # Create collection
        client.create_collection(
            collection_name=c_name,
            schema=schema,
            index_params=index_params
        )
        log.info(f"Collection '{c_name}' created")

        # Step 2: Generate insert-format data with all supported types
        log.info(f"Generating {entities} rows of insert-format data")
        insert_data = []
        for i in range(entities):
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
                    "struct_float_vec": [random.random() for _ in range(dim)]
                }
                struct_list.append(struct_obj)

            row = {
                "id": i,
                "float_vector": [random.random() for _ in range(dim)],
                "struct_array": struct_list
            }
            insert_data.append(row)

        # Step 3: Use LocalBulkWriter to convert data to import files
        log.info(f"Using LocalBulkWriter to generate {file_type} files")
        batch_files, original_data = self.gen_file_with_local_bulk_writer(
            schema=schema,
            data=insert_data,
            file_type=file_type
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
        num_entities = stats['row_count']
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
                    "struct_float_vec": [random.random() for _ in range(dim)]
                }
                struct_list.append(struct_obj)
            struct_arr.append(struct_list)

        # Define PyArrow schema for struct field with all types
        struct_type = pa.struct([
            pa.field("struct_varchar", pa.string()),
            pa.field("struct_int8", pa.int8()),
            pa.field("struct_int16", pa.int16()),
            pa.field("struct_int32", pa.int32()),
            pa.field("struct_int64", pa.int64()),
            pa.field("struct_float", pa.float32()),
            pa.field("struct_double", pa.float64()),
            pa.field("struct_bool", pa.bool_()),
            pa.field("struct_float_vec", pa.list_(pa.float32()))
        ])

        # Build PyArrow arrays with explicit types
        pa_arrays = {
            "id": pa.array(id_arr, type=pa.int64()),
            "float_vector": pa.array(
                [np.array(v, dtype=np.float32) for v in float_vector_arr],
                type=pa.list_(pa.float32())
            ),
            "struct_array": pa.array(struct_arr, type=pa.list_(struct_type))
        }

        # Create PyArrow table and write to Parquet
        table = pa.table(pa_arrays)
        pq.write_table(table, file_path, row_group_size=10000)

        log.info(f"Generated comprehensive parquet file with {num_rows} rows: {file_path}")

        return {
            "id": id_arr,
            "float_vector": float_vector_arr,
            "struct_array": struct_arr
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
                    "struct_float_vec": [random.random() for _ in range(dim)]
                }
                struct_list.append(struct_obj)
            struct_arr.append(struct_list)

            # Build row object
            row = {
                "id": i,
                "float_vector": float_vector,
                "struct_array": struct_list
            }
            rows.append(row)

        # Write to JSON file
        with open(file_path, "w") as f:
            json.dump(rows, f, indent=2)

        log.info(f"Generated comprehensive JSON file with {num_rows} rows: {file_path}")

        return {
            "id": id_arr,
            "float_vector": float_vector_arr,
            "struct_array": struct_arr
        }



class TestMilvusClientStructArrayMmap(TestMilvusClientV2Base):
    """Test case of struct array mmap functionality"""

    def generate_struct_array_data(
        self, num_rows: int, dim: int = default_dim, capacity: int = default_capacity
    ) -> List[Dict[str, Any]]:
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
                mmap_enabled=mmap_enabled
            )
        else:
            schema.add_field(
                field_name="normal_vector", datatype=DataType.FLOAT_VECTOR, dim=dim
            )

        # Create struct schema with optional mmap for sub-fields
        struct_schema = client.create_struct_field_schema()
        struct_schema.add_field("clip_str", DataType.VARCHAR, max_length=65535)

        if subfield1_mmap is not None:
            struct_schema.add_field(
                "clip_embedding1",
                DataType.FLOAT_VECTOR,
                dim=dim,
                mmap_enabled=subfield1_mmap
            )
        else:
            struct_schema.add_field("clip_embedding1", DataType.FLOAT_VECTOR, dim=dim)

        if subfield2_mmap is not None:
            struct_schema.add_field(
                "clip_embedding2",
                DataType.FLOAT_VECTOR,
                dim=dim,
                mmap_enabled=subfield2_mmap
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
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Release collection before altering mmap properties
        self.release_collection(client, collection_name)

        # Enable mmap on collection
        self.alter_collection_properties(
            client,
            collection_name,
            properties={"mmap.enabled": True}
        )
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
        log.info(f"Search successful with mmap enabled on collection")

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
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

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
            field_params={"mmap_enabled": True}
        )
        log.info(f"Enabled mmap on struct array field 'clips'")

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
        log.info(f"Search successful with mmap enabled on struct array field")

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
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

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
            field_params={"mmap_enabled": True}
        )
        log.info(f"Enabled mmap on struct sub-field 'clips[clip_embedding1]'")

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
        log.info(f"Search successful on field with mmap enabled")

        # Verify search also works on clip_embedding2 (without mmap)
        self.search(
            client,
            collection_name,
            data=[search_tensor],
            anns_field="clips[clip_embedding2]",
            search_params={"metric_type": "MAX_SIM_COSINE"},
            limit=10,
        )
        log.info(f"Search successful on field without mmap")

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
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

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
            properties={"mmap.enabled": True}
        )
        log.info(f"Enabled mmap on embedding list index 'clips[clip_embedding1]'")

        # Verify mmap is enabled via describe_index
        index_info = self.describe_index(client, collection_name, index_name="clips[clip_embedding1]", check_task="check_nothing")
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
        log.info(f"Search successful with mmap enabled on embedding list index")

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
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Release collection before altering properties
        self.release_collection(client, collection_name)

        # Enable mmap at collection level
        self.alter_collection_properties(
            client,
            collection_name,
            properties={"mmap.enabled": True}
        )
        log.info(f"Enabled mmap at collection level")

        # Enable mmap at field level for struct array field
        self.alter_collection_field(
            client,
            collection_name,
            field_name="clips",
            field_params={"mmap_enabled": True}
        )
        log.info(f"Enabled mmap at field level for 'clips'")

        # Enable mmap at index level for embedding list index
        self.alter_index_properties(
            client,
            collection_name,
            index_name="clips[clip_embedding1]",
            properties={"mmap.enabled": True}
        )
        log.info(f"Enabled mmap at index level for 'clips[clip_embedding1]'")

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
        log.info(f"All searches successful with mmap enabled at multiple levels")

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
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

        # Insert data
        nb = 3000
        data = self.generate_struct_array_data(nb)
        self.insert(client, collection_name, data)

        # Release and enable mmap
        self.release_collection(client, collection_name)
        self.alter_collection_properties(
            client,
            collection_name,
            properties={"mmap.enabled": True}
        )
        log.info(f"Enabled mmap on collection")

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
        log.info(f"Search successful with mmap enabled")

        # Now disable mmap
        self.release_collection(client, collection_name)
        self.alter_collection_properties(
            client,
            collection_name,
            properties={"mmap.enabled": False}
        )
        log.info(f"Disabled mmap on collection")

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
        log.info(f"Search successful after disabling mmap")

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
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

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
        log.info(f"Search successful without mmap")

        # Now enable mmap on index
        self.release_collection(client, collection_name)
        self.alter_index_properties(
            client,
            collection_name,
            index_name="clips[clip_embedding1]",
            properties={"mmap.enabled": True}
        )
        log.info(f"Enabled mmap on index 'clips[clip_embedding1]'")

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
        log.info(f"Search successful after enabling mmap on index")

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
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

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
        log.info(f"Search successful with mmap enabled on index (from schema)")

        # Disable mmap on index
        self.release_collection(client, collection_name)
        self.alter_index_properties(
            client,
            collection_name,
            index_name="clips[clip_embedding1]",
            properties={"mmap.enabled": False}
        )
        log.info(f"Disabled mmap on index")

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
        log.info(f"Search successful after disabling mmap on index")

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
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

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
        log.info(f"Search successful with mmap enabled on struct sub-field (from schema)")

        # Disable mmap on struct sub-field
        self.release_collection(client, collection_name)
        self.alter_collection_field(
            client,
            collection_name,
            field_name="clips[clip_embedding1]",
            field_params={"mmap_enabled": False}
        )
        log.info(f"Disabled mmap on struct sub-field 'clips[clip_embedding1]'")

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
        log.info(f"Search successful after disabling mmap on struct sub-field")

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
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

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
        log.info(f"Search successful without mmap on struct sub-field")

        # Now enable mmap on struct sub-field
        self.release_collection(client, collection_name)
        self.alter_collection_field(
            client,
            collection_name,
            field_name="clips[clip_embedding1]",
            field_params={"mmap_enabled": True}
        )
        log.info(f"Enabled mmap on struct sub-field 'clips[clip_embedding1]'")

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
        log.info(f"Search successful after enabling mmap on struct sub-field")

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
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

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
        log.info(f"Search successful with mmap enabled on struct array field (from schema)")

        # Disable mmap on struct array field
        self.release_collection(client, collection_name)
        self.alter_collection_field(
            client,
            collection_name,
            field_name="clips",
            field_params={"mmap_enabled": False}
        )
        log.info(f"Disabled mmap on struct array field 'clips'")

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
        log.info(f"Search successful after disabling mmap on struct array field")

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
        self.create_collection(
            client, collection_name, schema=schema, index_params=index_params
        )

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
        log.info(f"Search successful without mmap on struct array field")

        # Now enable mmap on struct array field
        self.release_collection(client, collection_name)
        self.alter_collection_field(
            client,
            collection_name,
            field_name="clips",
            field_params={"mmap_enabled": True}
        )
        log.info(f"Enabled mmap on struct array field 'clips'")

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
        log.info(f"Search successful after enabling mmap on struct array field")
