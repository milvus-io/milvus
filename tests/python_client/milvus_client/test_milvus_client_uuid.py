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

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        assert self.has_collection(client, collection_name)[0]

        desc = self.describe_collection(client, collection_name)[0]
        fields = {f["name"]: f for f in desc["fields"]}
        assert fields["id"]["type"] == DataType.UUID, f"Expected UUID type, got {fields['id']['type']}"
        assert fields["id"]["is_primary"] is True
        assert fields["vector"]["type"] == DataType.FLOAT_VECTOR

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_insert_and_query_exact_match(self):
        """
        target: test insert and query by exact UUID match
        method: insert rows with UUID PK, then query by exact uuid
        expected: exact match returns the inserted row
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rng = np.random.default_rng(seed=19530)
        ids = [str(uuid.uuid4()) for _ in range(100)]
        rows = [{"id": uid, "vector": list(rng.random(default_dim))} for uid in ids]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        target = ids[0]
        res, _ = self.query(client, collection_name, filter=f'id == "{target}"', output_fields=["id"])
        assert len(res) == 1
        assert res[0]["id"] == target

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_query_with_in_operator(self):
        """
        target: test query with IN operator on UUID field
        method: insert rows with UUID PK, then query with id in [...]
        expected: IN query returns matching rows
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rng = np.random.default_rng(seed=19531)
        ids = [str(uuid.uuid4()) for _ in range(50)]
        rows = [{"id": uid, "vector": list(rng.random(default_dim))} for uid in ids]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        subset = ids[0:10]
        in_expr = "id in " + str(subset).replace("'", '"')
        res, _ = self.query(client, collection_name, filter=in_expr, output_fields=["id"])
        assert len(res) == len(subset)
        assert {r["id"] for r in res} == set(subset)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_delete_by_expression(self):
        """
        target: test delete by UUID expression
        method: insert rows with UUID PK, delete by id == uuid
        expected: deleted row no longer queryable
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rng = np.random.default_rng(seed=19532)
        ids = [str(uuid.uuid4()) for _ in range(20)]
        rows = [{"id": uid, "vector": list(rng.random(default_dim))} for uid in ids]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        target = ids[0]
        self.delete(client, collection_name, filter=f'id == "{target}"')
        self.flush(client, collection_name)

        res, _ = self.query(client, collection_name, filter=f'id == "{target}"', output_fields=["id"])
        assert len(res) == 0

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_delete_and_reinsert(self):
        """
        target: test delete and reinsert with same UUIDs
        method: insert rows, delete by id in [...], reinsert same UUIDs
        expected: reinserted rows queryable again
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rng = np.random.default_rng(seed=19533)
        ids = [str(uuid.uuid4()) for _ in range(20)]
        rows = [{"id": uid, "vector": list(rng.random(default_dim))} for uid in ids]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        subset = ids[0:5]
        in_expr = "id in " + str(subset).replace("'", '"')
        self.delete(client, collection_name, filter=in_expr)
        self.flush(client, collection_name)

        res, _ = self.query(client, collection_name, filter=in_expr, output_fields=["id"])
        assert len(res) == 0

        reinsert = [{"id": uid, "vector": list(rng.random(default_dim))} for uid in subset]
        self.insert(client, collection_name, reinsert)
        self.flush(client, collection_name)

        res, _ = self.query(client, collection_name, filter=in_expr, output_fields=["id"])
        assert len(res) == len(subset)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_data_consistency(self):
        """
        target: test data consistency across multiple field types with UUID PK
        method: insert rows with UUID PK plus varchar/int fields, query each
        expected: all fields round-trip correctly
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("name", DataType.VARCHAR, max_length=100)
        schema.add_field("age", DataType.INT64)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rng = np.random.default_rng(seed=19534)
        rows = [
            {"id": str(uuid.uuid4()), "name": f"user_{i}", "age": i, "vector": list(rng.random(default_dim))}
            for i in range(50)
        ]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        res, _ = self.query(client, collection_name, filter="age >= 0", output_fields=["id", "name", "age"])
        assert len(res) == 50
        for r in res:
            assert r["name"].startswith("user_")
            assert isinstance(r["age"], int)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_batch_insert_and_query(self):
        """
        target: test batch insert and cross-batch UUID query
        method: insert in two batches, query across all
        expected: all rows from both batches queryable
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rng = np.random.default_rng(seed=19535)
        batch1 = [{"id": str(uuid.uuid4()), "vector": list(rng.random(default_dim))} for _ in range(30)]
        batch2 = [{"id": str(uuid.uuid4()), "vector": list(rng.random(default_dim))} for _ in range(30)]

        self.insert(client, collection_name, batch1)
        self.insert(client, collection_name, batch2)
        self.flush(client, collection_name)

        all_ids = [r["id"] for r in batch1] + [r["id"] for r in batch2]
        res, _ = self.query(client, collection_name, filter="id != ''", output_fields=["id"])
        assert len(res) == len(all_ids)
        assert {r["id"] for r in res} == set(all_ids)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_as_non_primary_scalar_field(self):
        """
        target: test UUID as non-PK scalar field
        method: create collection with int PK and UUID scalar field, insert, query
        expected: UUID scalar field round-trips correctly
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("uid", DataType.UUID)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rng = np.random.default_rng(seed=19536)
        uids = [str(uuid.uuid4()) for _ in range(50)]
        rows = [{"id": i, "uid": uids[i], "vector": list(rng.random(default_dim))} for i in range(50)]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        res, _ = self.query(client, collection_name, filter="id >= 0", output_fields=["id", "uid"])
        assert len(res) == 50
        assert {r["uid"] for r in res} == set(uids)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_invalid_input_rejected(self):
        """
        target: test invalid UUID string rejection at insert
        method: insert a row with a malformed UUID value
        expected: insert fails with an error
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        bad_row = [{"id": "not-a-valid-uuid", "vector": [0.1] * default_dim}]
        self.insert(client, collection_name, bad_row, check_task=CheckTasks.err_res)

        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_uuid_normalization_to_lowercase(self):
        """
        target: test UUID normalization to lowercase
        method: insert a UUID in uppercase, query with lowercase
        expected: stored value is normalized to lowercase
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)

        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field("id", DataType.UUID, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        self.create_collection(client, collection_name, schema=schema, consistency_level="Strong")

        rng = np.random.default_rng(seed=19537)
        upper = str(uuid.uuid4()).upper()
        rows = [{"id": upper, "vector": list(rng.random(default_dim))}]

        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        res, _ = self.query(client, collection_name, filter=f'id == "{upper.lower()}"', output_fields=["id"])
        assert len(res) == 1
        assert res[0]["id"] == upper.lower()

        self.drop_collection(client, collection_name)
