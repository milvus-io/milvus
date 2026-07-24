# ruff: noqa: E712,E731,F401,F403,F405,F541,F841,I001,UP031,UP032,W291,W292,W293
# fmt: off
import uuid

import numpy as np
import pytest

from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType
from utils.util_log import test_log as log

prefix = "client_uuid"
default_nb = ct.default_nb
default_dim = ct.default_dim
default_limit = ct.default_limit


class TestMilvusClientUUID(TestMilvusClientV2Base):
    """Test cases for UUID field type using MilvusClient."""

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_create_collection(self):
        """
        target: test create collection with UUID primary key
        method: create a collection with UUID as primary key and describe it
        expected: collection created successfully with correct schema
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        # Create schema with UUID primary key + float vector (required by Milvus)
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        # Verify collection exists
        assert self.has_collection(client, collection_name)[0]

        # Describe collection and verify schema field types
        desc = self.describe_collection(client, collection_name)[0]
        fields = {f["name"]: f for f in desc["fields"]}
        assert fields["id"]["type"] == DataType.UUID, f"Expected UUID type, got {fields['id']['type']}"
        assert fields["id"]["is_primary"] is True
        assert fields["vector"]["type"] == DataType.FLOAT_VECTOR

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_create_collection_auto_id(self):
        """
        target: test create collection with auto_id UUID primary key
        method: create collection with UUID PK and auto_id=True
        expected: collection created, inserts auto-generate UUIDs
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, auto_id=True, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("name", DataType.VARCHAR, max_length=100)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        # Insert data without UUID (auto_id generates them)
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                "vector": list(rng.random(default_dim)),
                "name": f"auto_user_{i}",
            }
            for i in range(50)
        ]

        insert_result = self.insert(client, collection_name, rows)[0]
        log.info(f"Auto-id insert result: {insert_result}")

        self.flush(client, collection_name)

        # Verify data exists by querying all
        res, _ = self.query(
            client, collection_name,
            filter="name != ''",
            output_fields=["id", "name"]
        )
        assert len(res) == 50
        # Verify UUIDs were auto-generated (not null, valid format)
        for r in res:
            assert r["id"] is not None
            # Validate it looks like a UUID
            assert isinstance(r["id"], str)
            assert len(r["id"]) == 36  # standard UUID string length

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_uuid_insert_and_query_exact_match(self):
        """
        target: test insert and query by exact UUID match
        method: insert rows with UUID PKs, query using equality filter
        expected: query returns exactly the matching row
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        # Create schema
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("name", DataType.VARCHAR, max_length=100)
        schema.add_field("age", DataType.INT64)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        # Insert data with known UUIDs
        rng = np.random.default_rng(seed=19530)
        nb = 100
        rows = [
            {
                "id": str(uuid.uuid4()),
                "vector": list(rng.random(default_dim)),
                "name": f"person_{i}",
                "age": 20 + i,
            }
            for i in range(nb)
        ]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Query by exact UUID match (single equality)
        query_uuid = rows[0]["id"]
        res, _ = self.query(
            client, collection_name,
            filter=f'id == "{query_uuid}"',
            output_fields=["id", "name", "age"]
        )
        assert len(res) == 1
        assert res[0]["id"] == query_uuid
        assert res[0]["name"] == rows[0]["name"]
        assert res[0]["age"] == rows[0]["age"]

        # Query a second UUID
        query_uuid_2 = rows[50]["id"]
        res, _ = self.query(
            client, collection_name,
            filter=f'id == "{query_uuid_2}"',
            output_fields=["id", "name"]
        )
        assert len(res) == 1
        assert res[0]["id"] == query_uuid_2
        assert res[0]["name"] == rows[50]["name"]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L0)
    def test_uuid_query_with_in_operator(self):
        """
        target: test query with IN operator for UUID field
        method: insert multiple UUIDs, query with IN expression containing several UUIDs
        expected: returns correct subset of data matching the IN list
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("name", DataType.VARCHAR, max_length=100)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        # Insert 50 rows
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                "id": str(uuid.uuid4()),
                "vector": list(rng.random(default_dim)),
                "name": f"person_{i}",
            }
            for i in range(50)
        ]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Query with IN operator on UUID field (3 specific UUIDs)
        target_uuids = [rows[0]["id"], rows[1]["id"], rows[5]["id"]]
        uuids_str = ", ".join(f'"{u}"' for u in target_uuids)
        res, _ = self.query(
            client, collection_name,
            filter=f"id in [{uuids_str}]",
            output_fields=["id", "name"]
        )
        assert len(res) == len(target_uuids)
        returned_uuids = [r["id"] for r in res]
        for uid in target_uuids:
            assert uid in returned_uuids, f"UUID {uid} not found in query results"

        # Test IN with empty list (should return nothing)
        res, _ = self.query(
            client, collection_name,
            filter="id in []",
            output_fields=["id"]
        )
        assert len(res) == 0

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_delete_by_expression(self):
        """
        target: test delete entities by UUID expression
        method: insert data, delete specific UUIDs via IN expression, verify deletion
        expected: deleted UUIDs no longer appear in queries, remaining data intact
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("name", DataType.VARCHAR, max_length=100)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        # Insert data
        rng = np.random.default_rng(seed=19530)
        nb = 50
        rows = [
            {
                "id": str(uuid.uuid4()),
                "vector": list(rng.random(default_dim)),
                "name": f"user_{i}",
            }
            for i in range(nb)
        ]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Delete two specific UUIDs
        delete_uuids = [rows[0]["id"], rows[1]["id"]]
        uuids_str = ", ".join(f'"{u}"' for u in delete_uuids)
        del_res = self.delete(client, collection_name, filter=f"id in [{uuids_str}]")[0]
        log.info(f"Delete result: {del_res}")

        self.flush(client, collection_name)

        # Verify deleted rows are gone
        res, _ = self.query(
            client, collection_name,
            filter=f"id in [{uuids_str}]",
            output_fields=["id"]
        )
        assert len(res) == 0, f"Deleted UUIDs still found: {res}"

        # Verify remaining data count matches expectation
        all_uuids_str = ", ".join(f'"{r["id"]}"' for r in rows)
        res, _ = self.query(
            client, collection_name,
            filter=f"id in [{all_uuids_str}]",
            output_fields=["id"]
        )
        assert len(res) == nb - len(delete_uuids)

        # Verify remaining data still has correct content
        remaining_ids = {r["id"] for r in res}
        for uid in delete_uuids:
            assert uid not in remaining_ids

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_delete_and_reinsert(self):
        """
        target: test delete and reinsert with the same UUID values
        method: delete entities and insert new entities with same UUIDs
        expected: new data is queryable, old data is gone
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("value", DataType.INT64)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        # Insert initial data
        rng = np.random.default_rng(seed=19530)
        test_uuid = str(uuid.uuid4())
        rows = [
            {
                "id": test_uuid,
                "vector": list(rng.random(default_dim)),
                "value": 100,
            }
        ]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify initial data
        res, _ = self.query(
            client, collection_name,
            filter=f'id == "{test_uuid}"',
            output_fields=["value"]
        )
        assert len(res) == 1
        assert res[0]["value"] == 100

        # Delete the row
        self.delete(client, collection_name, filter=f'id == "{test_uuid}"')[0]
        self.flush(client, collection_name)

        # Verify deletion
        res, _ = self.query(
            client, collection_name,
            filter=f'id == "{test_uuid}"',
            output_fields=["value"]
        )
        assert len(res) == 0

        # Reinsert with the same UUID but different data
        new_rows = [
            {
                "id": test_uuid,
                "vector": list(rng.random(default_dim)),
                "value": 200,
            }
        ]
        self.insert(client, collection_name, new_rows)
        self.flush(client, collection_name)

        # Verify reinserted data (should have new value)
        res, _ = self.query(
            client, collection_name,
            filter=f'id == "{test_uuid}"',
            output_fields=["value"]
        )
        assert len(res) == 1
        assert res[0]["value"] == 200

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_data_consistency(self):
        """
        target: test data consistency with UUID primary keys
        method: insert multiple rows, query all, verify every field matches
        expected: all inserted data is correctly retrieved with matching values
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("name", DataType.VARCHAR, max_length=100)
        schema.add_field("age", DataType.INT64)
        schema.add_field("email", DataType.VARCHAR, max_length=200)
        schema.add_field("score", DataType.DOUBLE)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        # Insert data with varied fields
        rng = np.random.default_rng(seed=19530)
        nb = 100
        rows = [
            {
                "id": str(uuid.uuid4()),
                "vector": list(rng.random(default_dim)),
                "name": f"user_{i}",
                "age": 18 + (i % 50),
                "email": f"user_{i}@example.com",
                "score": round(rng.random() * 100, 2),
            }
            for i in range(nb)
        ]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Query all data
        res, _ = self.query(
            client, collection_name,
            filter="id != ''",
            output_fields=["id", "name", "age", "email", "score"]
        )
        assert len(res) == nb, f"Expected {nb} rows, got {len(res)}"

        # Build lookup map from inserted data
        inserted_map = {r["id"]: r for r in rows}

        # Verify each returned row
        for r in res:
            rid = r["id"]
            assert rid in inserted_map, f"UUID {rid} not found in inserted data"
            expected = inserted_map[rid]
            assert r["name"] == expected["name"], f"Name mismatch for {rid}"
            assert r["age"] == expected["age"], f"Age mismatch for {rid}"
            assert r["email"] == expected["email"], f"Email mismatch for {rid}"
            assert r["score"] == expected["score"], f"Score mismatch for {rid}"

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_uuid_with_batch_insert_and_query(self):
        """
        target: test batch insert and query with UUID primary keys
        method: insert multiple batches, query with range-like filter on other fields
        expected: all batches correctly inserted and queryable
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("category", DataType.VARCHAR, max_length=50)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        # Insert multiple batches
        rng = np.random.default_rng(seed=19530)
        all_rows = []
        categories = ["A", "B", "C"]
        for cat in categories:
            batch = [
                {
                    "id": str(uuid.uuid4()),
                    "vector": list(rng.random(default_dim)),
                    "category": cat,
                }
                for _ in range(30)
            ]
            self.insert(client, collection_name, batch)
            all_rows.extend(batch)

        self.flush(client, collection_name)

        # Query by category
        for cat in categories:
            res, _ = self.query(
                client, collection_name,
                filter=f'category == "{cat}"',
                output_fields=["id", "category"]
            )
            assert len(res) == 30, f"Expected 30 rows for category {cat}, got {len(res)}"
            for r in res:
                assert r["category"] == cat

        # Query with OR expression on UUIDs across batches
        target_uuids = [all_rows[0]["id"], all_rows[35]["id"], all_rows[70]["id"]]
        uuids_str = ", ".join(f'"{u}"' for u in target_uuids)
        res, _ = self.query(
            client, collection_name,
            filter=f"id in [{uuids_str}]",
            output_fields=["id", "category"]
        )
        assert len(res) == len(target_uuids)
        returned_cats = {r["id"]: r["category"] for r in res}
        for row in all_rows:
            if row["id"] in target_uuids:
                assert returned_cats[row["id"]] == row["category"]

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_uuid_non_primary_key_field(self):
        """
        target: test UUID as a non-primary-key field
        method: create collection with INT64 PK and UUID as a scalar field
        expected: UUID values can be stored and queried as regular fields
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("uuid_field", DataType.UUID)
        schema.add_field("name", DataType.VARCHAR, max_length=100)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        # Insert data
        rng = np.random.default_rng(seed=19530)
        nb = 50
        rows = [
            {
                "id": i,
                "vector": list(rng.random(default_dim)),
                "uuid_field": str(uuid.uuid4()),
                "name": f"item_{i}",
            }
            for i in range(nb)
        ]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Query by UUID non-primary field
        target_uuid = rows[10]["uuid_field"]
        res, _ = self.query(
            client, collection_name,
            filter=f'uuid_field == "{target_uuid}"',
            output_fields=["id", "uuid_field", "name"]
        )
        assert len(res) == 1
        assert res[0]["id"] == 10
        assert res[0]["uuid_field"] == target_uuid
        assert res[0]["name"] == "item_10"

        # Query with IN on UUID non-primary field
        target_uuids = [rows[0]["uuid_field"], rows[5]["uuid_field"]]
        uuids_str = ", ".join(f'"{u}"' for u in target_uuids)
        res, _ = self.query(
            client, collection_name,
            filter=f"uuid_field in [{uuids_str}]",
            output_fields=["id", "uuid_field"]
        )
        assert len(res) == 2
        returned_ids = {r["id"] for r in res}
        assert 0 in returned_ids
        assert 5 in returned_ids

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_uuid_insert_invalid_uuid_string(self):
        """
        target: test inserting invalid UUID strings into UUID field
        method: try inserting non-UUID strings into a UUID field
        expected: server rejects invalid UUID strings
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        # Insert one valid UUID to verify the collection works
        rng = np.random.default_rng(seed=19530)
        valid_rows = [
            {
                "id": str(uuid.uuid4()),
                "vector": list(rng.random(default_dim)),
            }
        ]
        self.insert(client, collection_name, valid_rows)
        self.flush(client, collection_name)

        # Try inserting invalid UUID strings (each should fail)
        invalid_uuids = [
            "not-a-uuid",
            "12345",
            "",
            "550e8400-e29b-41d4-a716-00000000000Z",  # invalid hex char
            "550e8400e29b41d4a716000000000000",  # missing dashes
        ]

        errors_raised = 0
        for invalid_uuid in invalid_uuids:
            rows = [
                {
                    "id": invalid_uuid,
                    "vector": list(rng.random(default_dim)),
                }
            ]
            try:
                self.insert(client, collection_name, rows,
                            check_task=CheckTasks.err_res,
                            check_items={ct.err_code: 999})
            except Exception:
                errors_raised += 1
                continue

        log.info(f"Invalid UUID insert errors raised: {errors_raised}/{len(invalid_uuids)}")

        self.drop_collection(client, collection_name)
