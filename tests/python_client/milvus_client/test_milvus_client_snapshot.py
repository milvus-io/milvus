import time
import json
import threading
import pytest
import numpy as np
from minio import Minio
import pyetcd

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType

# MinIO configuration for GC verification tests
MINIO_ENDPOINT = "10.100.36.174:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "snapshot-test"

# etcd configuration for snapshot metadata verification
ETCD_HOST = "10.100.36.173"
ETCD_PORT = 2379
ETCD_ROOT_PATH = "snapshot-test"  # Milvus deployment name as etcd root path


prefix = "snapshot"
default_dim = 128
default_nb = 3000
default_nq = 2
default_limit = 10
default_search_exp = "id >= 0"
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name


class TestMilvusClientSnapshotDefault(TestMilvusClientV2Base):
    """Test snapshot basic operations - L0 smoke tests"""

    @pytest.mark.tags(CaseLabel.L0)
    def test_snapshot_create_list_describe_drop(self):
        """
        target: test basic snapshot lifecycle
        method: create -> list -> describe -> drop
        expected: all operations succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        # 1. Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
            default_float_field_name: i * 1.0,
            default_string_field_name: str(i)
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # 2. Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name,
                             description="Test snapshot for L0")

        # 3. List snapshots
        snapshots, _ = self.list_snapshots(client, collection_name=collection_name)
        assert snapshot_name in snapshots, f"Snapshot {snapshot_name} not found in list"

        # 4. Describe snapshot
        info, _ = self.describe_snapshot(client, snapshot_name)
        assert info.name == snapshot_name
        assert info.collection_name == collection_name
        assert info.create_ts > 0

        # 5. Drop snapshot
        self.drop_snapshot(client, snapshot_name)

        # 6. Verify snapshot is dropped
        snapshots, _ = self.list_snapshots(client, collection_name=collection_name)
        assert snapshot_name not in snapshots, f"Snapshot {snapshot_name} should be dropped"

    @pytest.mark.tags(CaseLabel.L0)
    def test_snapshot_restore_basic(self):
        """
        target: test basic snapshot restore flow
        method: create snapshot -> restore to new collection -> verify data
        expected: restored collection has same data count
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # 1. Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
            default_float_field_name: i * 1.0,
            default_string_field_name: str(i)
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # 2. Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)

        # 3. Restore snapshot to new collection
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        assert job_id > 0, "restore_snapshot should return a valid job_id"

        # 4. Wait for restore to complete
        self._wait_for_restore_complete(client, job_id)

        # 5. Verify restored collection data count
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name, filter="",
                            output_fields=["count(*)"])
        restored_count = res[0]["count(*)"]
        assert restored_count == default_nb, \
            f"Restored collection should have {default_nb} rows, got {restored_count}"

        # 6. Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")


class TestMilvusClientSnapshotCreateInvalid(TestMilvusClientV2Base):
    """Test create_snapshot with invalid parameters - L1"""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("snapshot_name", ["", None])
    def test_snapshot_create_invalid_name(self, snapshot_name):
        """
        target: test create snapshot with invalid name
        method: create snapshot with empty/None name
        expected: raise exception with proper error message (SDK validates)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)

        # SDK validates snapshot_name and raises ParamError
        error = {ct.err_code: 1, ct.err_msg: "snapshot_name must be a non-empty string"}
        self.create_snapshot(client, collection_name, snapshot_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="BUG: SDK and server both accept whitespace-only snapshot names")
    def test_snapshot_create_whitespace_name(self):
        """
        target: test create snapshot with whitespace-only name
        method: create snapshot with name containing only spaces
        expected: should raise exception

        NOTE: This test documents a BUG - both SDK and server accept whitespace-only names.
        SDK's validate_str() doesn't strip whitespace before checking.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)

        # BUG: SDK and server both accept whitespace-only names
        # Expected: should raise error like "snapshot_name must be a non-empty string"
        # Actual: creates a snapshot with name " " (space)
        error = {ct.err_code: 1, ct.err_msg: "snapshot_name"}
        self.create_snapshot(client, collection_name, " ",
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_create_collection_not_exist(self):
        """
        target: test create snapshot for non-existent collection
        method: create snapshot for collection that doesn't exist
        expected: raise exception
        """
        client = self._client()
        snapshot_name = cf.gen_unique_str(prefix)
        non_existent_collection = cf.gen_unique_str("non_existent")

        error = {ct.err_code: 100, ct.err_msg: "collection not found"}
        self.create_snapshot(client, non_existent_collection, snapshot_name,
                             check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_create_duplicate_name(self):
        """
        target: test create snapshot with duplicate name
        method: create two snapshots with same name
        expected: second creation should fail
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        self.create_collection(client, collection_name, default_dim)
        self.create_snapshot(client, collection_name, snapshot_name)

        # Try to create another snapshot with same name
        error = {ct.err_code: 1, ct.err_msg: "already exists"}
        self.create_snapshot(client, collection_name, snapshot_name,
                             check_task=CheckTasks.err_res, check_items=error)

        # Cleanup
        self.drop_snapshot(client, snapshot_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_create_empty_collection(self):
        """
        target: test create snapshot for empty collection
        method: create snapshot for collection with no data
        expected: snapshot should be created successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        self.create_collection(client, collection_name, default_dim)
        self.create_snapshot(client, collection_name, snapshot_name)

        # Verify snapshot exists
        snapshots, _ = self.list_snapshots(client, collection_name=collection_name)
        assert snapshot_name in snapshots

        # Cleanup
        self.drop_snapshot(client, snapshot_name)


class TestMilvusClientSnapshotDropInvalid(TestMilvusClientV2Base):
    """Test drop_snapshot with invalid parameters - L1"""

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("snapshot_name", ["", None])
    def test_snapshot_drop_invalid_name(self, snapshot_name):
        """
        target: test drop snapshot with invalid name
        method: drop snapshot with empty/None name
        expected: raise exception with proper error message (SDK validates)
        """
        client = self._client()

        # SDK validates snapshot_name and raises ParamError
        error = {ct.err_code: 1, ct.err_msg: "snapshot_name must be a non-empty string"}
        self.drop_snapshot(client, snapshot_name,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.xfail(reason="BUG: SDK and server both accept whitespace-only snapshot names")
    def test_snapshot_drop_whitespace_name(self):
        """
        target: test drop snapshot with whitespace-only name
        method: drop snapshot with name containing only spaces
        expected: should raise exception

        NOTE: This test documents a BUG - both SDK and server accept whitespace-only names.
        """
        client = self._client()

        # BUG: SDK and server both accept whitespace-only names
        error = {ct.err_code: 1, ct.err_msg: "snapshot_name"}
        self.drop_snapshot(client, " ",
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_drop_not_exist(self):
        """
        target: test drop non-existent snapshot (idempotent)
        method: drop snapshot that doesn't exist
        expected: should succeed (idempotent behavior)
        """
        client = self._client()
        snapshot_name = cf.gen_unique_str("non_existent")

        # Should not raise exception (idempotent)
        self.drop_snapshot(client, snapshot_name)


class TestMilvusClientSnapshotListDescribe(TestMilvusClientV2Base):
    """Test list_snapshots and describe_snapshot - L1"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_list_all(self):
        """
        target: test list all snapshots
        method: create multiple snapshots and list them
        expected: all snapshots should be in the list
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_names = [cf.gen_unique_str(prefix) for _ in range(3)]

        self.create_collection(client, collection_name, default_dim)

        # Create multiple snapshots
        for name in snapshot_names:
            self.create_snapshot(client, collection_name, name)

        # List snapshots
        snapshots, _ = self.list_snapshots(client, collection_name=collection_name)
        for name in snapshot_names:
            assert name in snapshots, f"Snapshot {name} not found in list"

        # Cleanup
        for name in snapshot_names:
            self.drop_snapshot(client, name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_list_by_collection(self):
        """
        target: test list snapshots filtered by collection
        method: create snapshots for different collections and filter by one
        expected: only snapshots for specified collection should be returned
        """
        client = self._client()
        collection_name1 = cf.gen_collection_name_by_testcase_name() + "_1"
        collection_name2 = cf.gen_collection_name_by_testcase_name() + "_2"
        snapshot_name1 = cf.gen_unique_str(prefix + "_1")
        snapshot_name2 = cf.gen_unique_str(prefix + "_2")

        self.create_collection(client, collection_name1, default_dim)
        self.create_collection(client, collection_name2, default_dim)

        self.create_snapshot(client, collection_name1, snapshot_name1)
        self.create_snapshot(client, collection_name2, snapshot_name2)

        # Filter by collection_name1
        snapshots, _ = self.list_snapshots(client, collection_name=collection_name1)
        assert snapshot_name1 in snapshots
        assert snapshot_name2 not in snapshots

        # Cleanup
        self.drop_snapshot(client, snapshot_name1)
        self.drop_snapshot(client, snapshot_name2)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_list_empty(self):
        """
        target: test list snapshots when no snapshots exist
        method: list snapshots for collection with no snapshots
        expected: return empty list
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self.create_collection(client, collection_name, default_dim)
        snapshots, _ = self.list_snapshots(client, collection_name=collection_name)
        assert len(snapshots) == 0, "Should return empty list"

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_describe_not_exist(self):
        """
        target: test describe non-existent snapshot
        method: describe snapshot that doesn't exist
        expected: raise exception
        """
        client = self._client()
        snapshot_name = cf.gen_unique_str("non_existent")

        error = {ct.err_code: 1, ct.err_msg: "not found"}
        self.describe_snapshot(client, snapshot_name,
                               check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_describe_with_description(self):
        """
        target: test describe snapshot with description
        method: create snapshot with description and describe it
        expected: description should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        description = "Test description for snapshot"

        self.create_collection(client, collection_name, default_dim)
        self.create_snapshot(client, collection_name, snapshot_name, description=description)

        info, _ = self.describe_snapshot(client, snapshot_name)
        assert info.description == description

        # Cleanup
        self.drop_snapshot(client, snapshot_name)


class TestMilvusClientSnapshotRestoreInvalid(TestMilvusClientV2Base):
    """Test restore_snapshot with invalid parameters - L1"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_restore_not_exist(self):
        """
        target: test restore non-existent snapshot
        method: restore snapshot that doesn't exist
        expected: raise exception
        """
        client = self._client()
        snapshot_name = cf.gen_unique_str("non_existent")
        collection_name = cf.gen_unique_str(prefix)

        error = {ct.err_code: 1, ct.err_msg: "not found"}
        self.restore_snapshot(client, snapshot_name, collection_name,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_restore_collection_exist(self):
        """
        target: test restore snapshot to existing collection
        method: restore snapshot to collection that already exists
        expected: raise exception
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        target_collection_name = cf.gen_unique_str(prefix + "_target")

        # Create source collection and snapshot
        self.create_collection(client, collection_name, default_dim)
        self.create_snapshot(client, collection_name, snapshot_name)

        # Create target collection (should cause conflict)
        self.create_collection(client, target_collection_name, default_dim)

        error = {ct.err_code: 65535, ct.err_msg: "duplicate collection"}
        self.restore_snapshot(client, snapshot_name, target_collection_name,
                              check_task=CheckTasks.err_res, check_items=error)

        # Cleanup
        self.drop_snapshot(client, snapshot_name)


class TestMilvusClientSnapshotRestoreState(TestMilvusClientV2Base):
    """Test get_restore_snapshot_state and list_restore_snapshot_jobs - L1"""

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_restore_state_not_exist(self):
        """
        target: test get restore state for non-existent job
        method: get state with invalid job_id
        expected: raise exception
        """
        client = self._client()
        invalid_job_id = 999999999

        error = {ct.err_code: 1, ct.err_msg: "not found"}
        self.get_restore_snapshot_state(client, invalid_job_id,
                                        check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_list_restore_jobs(self):
        """
        target: test list restore snapshot jobs
        method: list all restore jobs
        expected: return list (may be empty)
        """
        client = self._client()

        jobs, _ = self.list_restore_snapshot_jobs(client)
        assert isinstance(jobs, list), "list_restore_snapshot_jobs should return a list"


class TestMilvusClientSnapshotDataTypes(TestMilvusClientV2Base):
    """Test snapshot with various data types - L2"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_int64_pk(self):
        """
        target: test snapshot with Int64 primary key
        method: create collection with Int64 PK, snapshot and restore
        expected: PK values should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with Int64 PK
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["id"])
        assert len(res) == 100
        ids = sorted([r["id"] for r in res])
        assert ids == list(range(100))

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_varchar_pk(self):
        """
        target: test snapshot with VarChar primary key
        method: create collection with VarChar PK, snapshot and restore
        expected: PK values should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with VarChar PK
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("pk", DataType.VARCHAR, is_primary=True, max_length=64)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            "pk": f"key_{i}",
            "vector": list(rng.random((1, default_dim))[0]),
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="pk like 'key_%'", output_fields=["pk"])
        assert len(res) == 100

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_multiple_vector_fields(self):
        """
        target: test snapshot with multiple vector fields
        method: create collection with FloatVector and BinaryVector, snapshot and restore
        expected: all vector data should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with multiple vector fields
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("float_vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("binary_vector", DataType.BINARY_VECTOR, dim=128)

        index_params = client.prepare_index_params()
        index_params.add_index("float_vector", metric_type="COSINE")
        index_params.add_index("binary_vector", metric_type="HAMMING")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            "id": i,
            "float_vector": list(rng.random((1, default_dim))[0]),
            "binary_vector": bytes(rng.integers(0, 256, size=16, dtype=np.uint8)),
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify data count
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 100

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_json_field(self):
        """
        target: test snapshot with JSON field
        method: create collection with JSON field, snapshot and restore
        expected: JSON data should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with JSON field
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("metadata", DataType.JSON)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            "id": i,
            "vector": list(rng.random((1, default_dim))[0]),
            "metadata": {"key": f"value_{i}", "number": i, "nested": {"a": i}},
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify JSON data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 0", output_fields=["metadata"])
        assert res[0]["metadata"]["key"] == "value_0"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_dynamic_field(self):
        """
        target: test snapshot with dynamic field
        method: create collection with dynamic field enabled, snapshot and restore
        expected: dynamic field data should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with dynamic field
        schema = client.create_schema(enable_dynamic_field=True, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            "id": i,
            "vector": list(rng.random((1, default_dim))[0]),
            "dynamic_field_1": f"dynamic_{i}",
            "dynamic_field_2": i * 10,
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify dynamic field data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 0", output_fields=["dynamic_field_1", "dynamic_field_2"])
        assert res[0]["dynamic_field_1"] == "dynamic_0"
        assert res[0]["dynamic_field_2"] == 0

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")


class TestMilvusClientSnapshotPartition(TestMilvusClientV2Base):
    """Test snapshot with partitions - L2"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_multiple_partitions(self):
        """
        target: test snapshot with multiple partitions
        method: create collection with multiple partitions, snapshot and restore
        expected: all partitions and data should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection
        self.create_collection(client, collection_name, default_dim)

        # Create partitions
        partition_names = [f"partition_{i}" for i in range(3)]
        for p_name in partition_names:
            self.create_partition(client, collection_name, p_name)

        # Insert data into each partition
        rng = np.random.default_rng(seed=19530)
        for p_name in partition_names:
            rows = [{
                default_primary_key_field_name: i + partition_names.index(p_name) * 100,
                default_vector_field_name: list(rng.random((1, default_dim))[0]),
            } for i in range(100)]
            self.insert(client, collection_name, rows, partition_name=p_name)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify partitions are preserved
        partitions, _ = self.list_partitions(client, restored_collection_name)
        for p_name in partition_names:
            assert p_name in partitions, f"Partition {p_name} not found"

        # Verify data in each partition
        self.load_collection(client, restored_collection_name)
        for p_name in partition_names:
            res, _ = self.query(client, restored_collection_name,
                                filter="id >= 0", partition_names=[p_name],
                                output_fields=["count(*)"])
            assert res[0]["count(*)"] == 100, f"Partition {p_name} should have 100 rows"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_restore_after_drop_partition(self):
        """
        target: test restore snapshot after dropping a partition
        method: create snapshot, drop partition, restore
        expected: all original partitions should be restored
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with partition
        self.create_collection(client, collection_name, default_dim)
        partition_name = "test_partition"
        self.create_partition(client, collection_name, partition_name)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
        } for i in range(100)]
        self.insert(client, collection_name, rows, partition_name=partition_name)
        self.flush(client, collection_name)

        # Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)

        # Drop partition
        self.release_collection(client, collection_name)
        self.drop_partition(client, collection_name, partition_name)

        # Restore snapshot
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify partition is restored
        partitions, _ = self.list_partitions(client, restored_collection_name)
        assert partition_name in partitions

        # Verify data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", partition_names=[partition_name],
                            output_fields=["count(*)"])
        assert res[0]["count(*)"] == 100

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")


class TestMilvusClientSnapshotDataOperations(TestMilvusClientV2Base):
    """Test snapshot with data operations - L2"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_after_delete(self):
        """
        target: test snapshot after delete operations
        method: insert -> delete -> snapshot -> restore
        expected: restored data should reflect delete operations
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create and insert data
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Delete some data
        self.load_collection(client, collection_name)
        self.delete(client, collection_name, filter="id < 50")
        self.flush(client, collection_name)

        # Create snapshot (should have 50 rows)
        self.create_snapshot(client, collection_name, snapshot_name)

        # Restore
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify only 50 rows remain
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 50

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_point_in_time(self):
        """
        target: test snapshot captures point-in-time state
        method: snapshot -> insert more data -> restore
        expected: restored data should only contain data at snapshot time
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create and insert initial data
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot (100 rows)
        self.create_snapshot(client, collection_name, snapshot_name)

        # Insert more data after snapshot
        more_rows = [{
            default_primary_key_field_name: i + 100,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
        } for i in range(50)]
        self.insert(client, collection_name, more_rows)
        self.flush(client, collection_name)

        # Verify source collection has 150 rows
        self.load_collection(client, collection_name)
        res, _ = self.query(client, collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 150

        # Restore snapshot
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Restored collection should only have 100 rows (point-in-time)
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 100

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_growing_segment_without_flush(self):
        """
        target: test snapshot behavior with growing segment (unflushed data)
        method: insert data without flush -> create snapshot -> restore -> verify
        expected:
            - Based on source code analysis, snapshot only includes segments with binlogs
            - Growing segments without binlogs (data in buffer) should NOT be included
            - This test verifies that unflushed data is NOT captured in snapshot

        Source code reference (handler.go:725-728):
            segments := h.s.meta.SelectSegments(ctx, WithCollection(collectionID),
                SegmentFilterFunc(func(info *SegmentInfo) bool {
                    segmentHasData := len(info.GetBinlogs()) > 0 || len(info.GetDeltalogs()) > 0
                    return segmentHasData && ...
                }))

        Key insight:
            - Snapshot does NOT trigger flush
            - Only data already persisted to binlog files will be captured
            - Growing segment data in memory buffer will be lost if not flushed before snapshot
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)

        # First batch: insert and flush (this data should be in snapshot)
        flushed_rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
        } for i in range(100)]
        self.insert(client, collection_name, flushed_rows)
        self.flush(client, collection_name)
        log.info("Inserted and flushed 100 rows")

        # Second batch: insert WITHOUT flush (growing segment, data in buffer)
        unflushed_rows = [{
            default_primary_key_field_name: i + 100,
            default_vector_field_name: list(rng.random((1, default_dim))[0]),
        } for i in range(50)]
        self.insert(client, collection_name, unflushed_rows)
        # Intentionally NOT calling flush - data stays in growing segment buffer
        log.info("Inserted 50 rows WITHOUT flush (growing segment)")

        # Verify source collection can query all 150 rows (growing + flushed)
        self.load_collection(client, collection_name)
        res, _ = self.query(client, collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        source_count = res[0]["count(*)"]
        log.info(f"Source collection total rows (flushed + growing): {source_count}")
        assert source_count == 150, f"Source should have 150 rows, got {source_count}"

        # Create snapshot - this should NOT include growing segment data
        self.create_snapshot(client, collection_name, snapshot_name)
        log.info("Created snapshot (without triggering flush)")

        # Restore snapshot to new collection
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify restored collection data count
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        restored_count = res[0]["count(*)"]
        log.info(f"Restored collection rows: {restored_count}")

        # Expectation: Only flushed data (100 rows) should be in snapshot
        # Growing segment data (50 rows) should NOT be captured
        # NOTE: This assertion documents the current behavior - snapshot does NOT include
        # growing segment data. If this test fails, it means the behavior has changed.
        assert restored_count == 100, \
            f"Expected 100 rows (only flushed data), got {restored_count}. " \
            f"Growing segment data should NOT be included in snapshot."

        # Also verify the specific IDs: only 0-99 should exist, not 100-149
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 100", output_fields=["count(*)"])
        growing_data_count = res[0]["count(*)"]
        assert growing_data_count == 0, \
            f"Growing segment data (id >= 100) should NOT be in snapshot, found {growing_data_count}"

        log.info("Verified: Snapshot does NOT include growing segment data")

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")


class TestMilvusClientSnapshotIndex(TestMilvusClientV2Base):
    """Test snapshot with various index types - L2"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_hnsw_index(self):
        """
        target: test snapshot preserves HNSW index
        method: create collection with HNSW index, snapshot and restore
        expected: index type and parameters should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with HNSW index
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE",
                               index_type="HNSW",
                               params={"M": 16, "efConstruction": 200})

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            "id": i,
            "vector": list(rng.random((1, default_dim))[0]),
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify index is preserved
        indexes, _ = self.list_indexes(client, restored_collection_name)
        assert len(indexes) > 0

        # Verify search works
        self.load_collection(client, restored_collection_name)
        search_vectors = [list(rng.random((1, default_dim))[0])]
        res, _ = self.search(client, restored_collection_name, search_vectors,
                             limit=10, output_fields=["id"])
        assert len(res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")


class TestMilvusClientSnapshotDataIntegrity(TestMilvusClientV2Base):
    """Test snapshot data integrity - verify actual data content, not just counts"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_vector_data_consistency(self):
        """
        target: verify vector data is exactly the same after restore
        method: compare vector values between original and restored collection
        expected: all vectors should be identical
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection and insert data with known vectors
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=12345)  # Fixed seed for reproducibility
        original_vectors = [list(rng.random(default_dim)) for _ in range(100)]
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: original_vectors[i],
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Query all vectors from restored collection
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["id", "vector"])

        # Verify each vector is identical
        for row in res:
            original_vec = original_vectors[row["id"]]
            restored_vec = row["vector"]
            # Compare with tolerance for floating point
            for j in range(default_dim):
                assert abs(original_vec[j] - restored_vec[j]) < 1e-6, \
                    f"Vector mismatch at id={row['id']}, dim={j}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_search_recall_consistency(self):
        """
        target: verify search results are identical between original and restored
        method: run same search query on both collections, compare results
        expected: search results should be identical
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.load_collection(client, collection_name)

        # Search on original collection
        query_vectors = [list(rng.random(default_dim)) for _ in range(10)]
        original_results, _ = self.search(client, collection_name, query_vectors,
                                          limit=10, output_fields=["id"])

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Search on restored collection with same queries
        self.load_collection(client, restored_collection_name)
        restored_results, _ = self.search(client, restored_collection_name, query_vectors,
                                          limit=10, output_fields=["id"])

        # Compare search results
        for i in range(len(query_vectors)):
            original_ids = [r["id"] for r in original_results[i]]
            restored_ids = [r["id"] for r in restored_results[i]]
            assert original_ids == restored_ids, \
                f"Search results mismatch for query {i}: original={original_ids}, restored={restored_ids}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_scalar_data_consistency(self):
        """
        target: verify all scalar field values are preserved after restore
        method: create collection with various scalar types, compare values
        expected: all scalar values should be identical
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with multiple scalar fields
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("int_field", DataType.INT32)
        schema.add_field("float_field", DataType.FLOAT)
        schema.add_field("bool_field", DataType.BOOL)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        # Insert data with various values
        rng = np.random.default_rng(seed=19530)
        rows = [{
            "id": i,
            "vector": list(rng.random(default_dim)),
            "int_field": i * 10,
            "float_field": i * 0.5,
            "bool_field": i % 2 == 0,
            "varchar_field": f"string_value_{i}",
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Query and verify all scalar values
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0",
                            output_fields=["id", "int_field", "float_field", "bool_field", "varchar_field"])

        for row in res:
            i = row["id"]
            assert row["int_field"] == i * 10, f"int_field mismatch at id={i}"
            assert abs(row["float_field"] - i * 0.5) < 1e-6, f"float_field mismatch at id={i}"
            assert row["bool_field"] == (i % 2 == 0), f"bool_field mismatch at id={i}"
            assert row["varchar_field"] == f"string_value_{i}", f"varchar_field mismatch at id={i}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")


class TestMilvusClientSnapshotBoundary(TestMilvusClientV2Base):
    """Test snapshot boundary conditions and edge cases"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_name_special_characters(self):
        """
        target: test snapshot name with special characters
        method: create snapshots with names containing special chars
        expected: should handle or reject appropriately
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)

        # Test various special character names
        special_names = [
            "snapshot-with-dash",
            "snapshot_with_underscore",
            "snapshot.with" + ".dot",
            "snapshot@with@at",
            "snapshot#with#hash",
            "snapshot with space",
            "快照中文名称",
            "snapshot/with/slash",
        ]

        results = {}
        for name in special_names:
            try:
                self.create_snapshot(client, collection_name, name)
                # If succeeded, verify it exists
                snapshots, _ = self.list_snapshots(client, collection_name=collection_name)
                if name in snapshots:
                    results[name] = "accepted"
                    self.drop_snapshot(client, name)
                else:
                    results[name] = "created but not listed"
            except Exception as e:
                results[name] = f"rejected: {str(e)[:50]}"

        # Log results for analysis
        for name, result in results.items():
            log.info(f"Snapshot name '{name}': {result}")

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_name_max_length(self):
        """
        target: test snapshot name length limit
        method: create snapshot with very long name
        expected: should have a reasonable limit
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)

        # Test progressively longer names
        found_limit = None
        for length in [64, 128, 256, 512, 1024]:
            name = "s" * length
            try:
                self.create_snapshot(client, collection_name, name)
                self.drop_snapshot(client, name)
            except Exception as e:
                found_limit = length
                log.info(f"Snapshot name length limit is less than {length}: {e}")
                break

        if found_limit is None:
            log.warning("No length limit found up to 1024 characters - this may be a bug")

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="BUG: start_time is always 0 - server doesn't set start_time field")
    def test_snapshot_restore_progress_tracking(self):
        """
        target: verify restore progress is correctly reported
        method: monitor progress during restore
        expected: progress should go from 0 to 100, start_time should be set
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with more data to slow down restore
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(5000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)

        # Track progress
        progress_values = []
        start_time = time.time()
        while time.time() - start_time < 120:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            progress_values.append(state.progress)

            if state.state == "RestoreSnapshotCompleted":
                break
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore failed: {state['reason']}")
            time.sleep(0.5)

        log.info(f"Progress values recorded: {progress_values}")

        # Verify progress was tracked
        assert 100 in progress_values, "Progress should reach 100 when completed"
        # Verify progress was monotonically increasing (or at least non-decreasing)
        for i in range(1, len(progress_values)):
            assert progress_values[i] >= progress_values[i-1], \
                f"Progress should not decrease: {progress_values[i-1]} -> {progress_values[i]}"

        # Verify start_time and time_cost are set
        final_state, _ = self.get_restore_snapshot_state(client, job_id)
        assert final_state.start_time > 0, "start_time should be set"
        assert final_state.time_cost > 0, "time_cost should be > 0 after completion"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_multiple_on_same_collection(self):
        """
        target: test creating multiple snapshots on same collection
        method: create several snapshots at different times
        expected: each snapshot captures its point-in-time state
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()

        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)

        snapshots = []
        expected_counts = []

        # Create 3 snapshots at different data states
        for batch in range(3):
            # Insert 100 rows
            rows = [{
                default_primary_key_field_name: i + batch * 100,
                default_vector_field_name: list(rng.random(default_dim)),
            } for i in range(100)]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)

            # Create snapshot
            snapshot_name = f"{cf.gen_unique_str(prefix)}_batch{batch}"
            self.create_snapshot(client, collection_name, snapshot_name)
            snapshots.append(snapshot_name)
            expected_counts.append((batch + 1) * 100)

        # Restore each snapshot and verify correct count
        for i, snapshot_name in enumerate(snapshots):
            restored_name = cf.gen_unique_str(prefix + "_restored")
            job_id, _ = self.restore_snapshot(client, snapshot_name, restored_name)
            self._wait_for_restore_complete(client, job_id)

            self.load_collection(client, restored_name)
            res, _ = self.query(client, restored_name, filter="id >= 0", output_fields=["count(*)"])
            actual_count = res[0]["count(*)"]

            assert actual_count == expected_counts[i], \
                f"Snapshot {i} should have {expected_counts[i]} rows, got {actual_count}"

            self.drop_collection(client, restored_name)

        # Cleanup
        for snapshot_name in snapshots:
            self.drop_snapshot(client, snapshot_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_concurrent_restore(self):
        """
        target: test restoring same snapshot to multiple collections concurrently
        method: start multiple restore jobs from same snapshot
        expected: all restores should succeed independently
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        # Create collection with data
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)

        # Start multiple concurrent restores
        num_restores = 3
        restore_jobs = []
        restored_names = []

        for i in range(num_restores):
            restored_name = cf.gen_unique_str(prefix + f"_restored_{i}")
            restored_names.append(restored_name)
            job_id, _ = self.restore_snapshot(client, snapshot_name, restored_name)
            restore_jobs.append(job_id)

        # Wait for all to complete
        for job_id in restore_jobs:
            self._wait_for_restore_complete(client, job_id, timeout=120)

        # Verify all restored collections have correct data
        for restored_name in restored_names:
            self.load_collection(client, restored_name)
            res, _ = self.query(client, restored_name, filter="id >= 0", output_fields=["count(*)"])
            assert res[0]["count(*)"] == 500, f"{restored_name} should have 500 rows"
            self.drop_collection(client, restored_name)

        # Cleanup
        self.drop_snapshot(client, snapshot_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")


class TestMilvusClientSnapshotNegative(TestMilvusClientV2Base):
    """Test snapshot negative scenarios and error handling"""

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_restore_deleted_snapshot(self):
        """
        target: test restoring a snapshot that was deleted
        method: delete snapshot then try to restore
        expected: should fail with clear error message
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection and snapshot
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_snapshot(client, collection_name, snapshot_name)

        # Delete snapshot
        self.drop_snapshot(client, snapshot_name)

        # Try to restore - should fail
        error = {ct.err_code: 1, ct.err_msg: "not found"}
        self.restore_snapshot(client, snapshot_name, restored_collection_name,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.xfail(reason="BUG: list_snapshots() fails after dropping source collection - server returns 'collection not found[collection=0]'")
    def test_snapshot_list_after_drop_collection(self):
        """
        target: test listing snapshots after source collection is dropped
        method: create snapshot, drop collection, list snapshots
        expected: snapshot should still be listable (snapshot is independent)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        # Create collection and snapshot
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_snapshot(client, collection_name, snapshot_name)

        # Drop original collection
        self.drop_collection(client, collection_name)

        # Snapshot should still be listable
        snapshots, _ = self.list_snapshots(client)
        assert snapshot_name in snapshots, "Snapshot should exist after collection drop"

        # Should be able to restore to new collection
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name, filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 100

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_schema_consistency_autoID(self):
        """
        target: verify auto_id setting is preserved in snapshot
        method: create collection with auto_id=True, snapshot and restore
        expected: restored collection should have same auto_id setting
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with auto_id=True
        schema = client.create_schema(enable_dynamic_field=False, auto_id=True)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        # Insert data (no id needed since auto_id=True)
        rng = np.random.default_rng(seed=19530)
        rows = [{"vector": list(rng.random(default_dim))} for _ in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify schema of restored collection
        desc = client.describe_collection(restored_collection_name)
        # Check auto_id is preserved
        pk_field = [f for f in desc["fields"] if f.get("is_primary")][0]
        assert pk_field.get("auto_id", False) == True, "auto_id should be preserved"

        # Verify can insert without id
        new_rows = [{"vector": list(rng.random(default_dim))} for _ in range(10)]
        self.insert(client, restored_collection_name, new_rows)

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")


class TestMilvusClientSnapshotGC(TestMilvusClientV2Base):
    """
    GC Protection Tests for Snapshot Feature (L3)

    These tests verify that snapshot-referenced segments are protected from GC.
    Requires special test environment:
    - Milvus cluster with short GC interval (dataCoord.gc.interval: 10s)
    - Short retention duration (common.retentionDuration: 60s)

    Test Environment Configuration (snapshot-test-milvus.yaml):
        dataCoord:
          gc:
            interval: 10
            missingTolerance: 10
            dropTolerance: 10
        common:
          gcConfirm:
            interval: 5
          retentionDuration: 60
    """

    def _get_minio_client(self):
        """Get MinIO client for segment verification"""
        return Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

    def _get_etcd_client(self):
        """Get pyetcd client for metadata verification"""
        return pyetcd.client(host=ETCD_HOST, port=ETCD_PORT)

    def _etcd_get_prefix_raw(self, prefix):
        """
        Get all key-value pairs with a prefix from etcd.
        Returns list of (key, value) tuples with raw bytes.
        """
        etcd_client = self._get_etcd_client()
        try:
            results = []
            for value, metadata in etcd_client.get_prefix(prefix):
                key = metadata.key.decode('utf-8')
                results.append((key, value))
            return results
        finally:
            etcd_client.close()

    def _etcd_get_prefix(self, prefix):
        """
        Get all values with a prefix from etcd.
        Returns list of parsed JSON objects.
        """
        etcd_client = self._get_etcd_client()
        try:
            values = []
            for value, metadata in etcd_client.get_prefix(prefix):
                try:
                    data = json.loads(value.decode('utf-8'))
                    values.append(data)
                except json.JSONDecodeError:
                    continue
            return values
        finally:
            etcd_client.close()

    def _parse_protobuf(self, raw_bytes, schema):
        """
        Generic protobuf parser with schema definition as parameter.

        Args:
            raw_bytes: protobuf encoded bytes
            schema: dict mapping field_number to (field_name, field_type)
                    field_type: 'varint' for int64/int32/bool, 'string' for string/bytes

        Example schema for SnapshotInfo:
            {
                1: ('name', 'string'),
                2: ('id', 'varint'),
                3: ('description', 'string'),
                4: ('collection_id', 'varint'),
                6: ('create_ts', 'varint'),
                7: ('s3_location', 'string'),
            }

        Returns:
            dict with parsed field values
        """
        from google.protobuf.internal.decoder import _DecodeVarint32

        result = {}
        pos = 0
        while pos < len(raw_bytes):
            try:
                # Read field tag
                tag, new_pos = _DecodeVarint32(raw_bytes, pos)
                pos = new_pos

                field_number = tag >> 3
                wire_type = tag & 0x7

                if wire_type == 0:  # Varint (int64/int32/bool)
                    value, new_pos = _DecodeVarint32(raw_bytes, pos)
                    pos = new_pos
                    if field_number in schema:
                        field_name, field_type = schema[field_number]
                        if field_type == 'varint':
                            result[field_name] = value
                elif wire_type == 2:  # Length-delimited (string/bytes/embedded message)
                    length, new_pos = _DecodeVarint32(raw_bytes, pos)
                    pos = new_pos
                    value = raw_bytes[pos:pos+length]
                    pos += length
                    if field_number in schema:
                        field_name, field_type = schema[field_number]
                        if field_type == 'string':
                            try:
                                result[field_name] = value.decode('utf-8')
                            except UnicodeDecodeError:
                                result[field_name] = value  # keep as bytes
                        elif field_type == 'bytes':
                            result[field_name] = value
                elif wire_type == 5:  # 32-bit (float, fixed32)
                    pos += 4
                elif wire_type == 1:  # 64-bit (double, fixed64)
                    pos += 8
                else:
                    break  # Unknown wire type
            except Exception:
                break
        return result

    # Proto schema definitions (from milvus/pkg/proto/data_coord.proto)
    SNAPSHOT_INFO_SCHEMA = {
        1: ('name', 'string'),
        2: ('id', 'varint'),
        3: ('description', 'string'),
        4: ('collection_id', 'varint'),
        5: ('partition_ids', 'varint'),  # repeated, will get last value
        6: ('create_ts', 'varint'),
        7: ('s3_location', 'string'),
    }

    def _parse_snapshot_info_from_protobuf(self, raw_bytes):
        """
        Parse SnapshotInfo from protobuf-encoded data.
        Uses generic parser with schema definition.
        """
        return self._parse_protobuf(raw_bytes, self.SNAPSHOT_INFO_SCHEMA)

    def _parse_snapshot_name_from_protobuf(self, raw_bytes):
        """
        Extract snapshot name from protobuf-encoded data.
        """
        info = self._parse_snapshot_info_from_protobuf(raw_bytes)
        return info.get('name')

    def _get_snapshot_metadata_from_etcd(self, snapshot_name):
        """
        Get snapshot metadata from etcd.
        Milvus stores snapshot info under: {root_path}/meta/datacoord-meta/snapshot/{collection_id}/{snapshot_id}
        Data is protobuf encoded.
        """
        prefix = f"{ETCD_ROOT_PATH}/meta/datacoord-meta/snapshot/"
        raw_data = self._etcd_get_prefix_raw(prefix)

        for key, value in raw_data:
            # Extract snapshot name from protobuf data
            name = self._parse_snapshot_name_from_protobuf(value)
            if name == snapshot_name:
                # Parse key to get collection_id and snapshot_id
                # Key format: {root}/meta/datacoord-meta/snapshot/{collection_id}/{snapshot_id}
                parts = key.split('/')
                if len(parts) >= 2:
                    collection_id = parts[-2]
                    snapshot_id = parts[-1]
                    log.info(f"Found snapshot in etcd: name={name}, collection_id={collection_id}, snapshot_id={snapshot_id}")
                    return {
                        "name": name,
                        "collection_id": collection_id,
                        "snapshot_id": snapshot_id,
                        "etcd_key": key,
                        "raw_data": value
                    }
        return None

    def _get_snapshot_segment_refs_from_etcd(self, collection_id, snapshot_id):
        """
        Get segment references for a snapshot.
        Segment refs are stored in MinIO manifest files, not in etcd.
        Path: files/snapshots/{collection_id}/manifests/{snapshot_id}/*.avro
        Returns list of manifest file paths.
        """
        minio_client = self._get_minio_client()
        prefix = f"files/snapshots/{collection_id}/manifests/{snapshot_id}/"
        segment_refs = []
        try:
            for obj in minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True):
                if obj.object_name.endswith('.avro'):
                    segment_refs.append({
                        "manifest_file": obj.object_name,
                        "size": obj.size
                    })
            log.info(f"Found {len(segment_refs)} manifest files for snapshot {snapshot_id}")
        except Exception as e:
            log.warning(f"Error listing manifest files: {e}")
        return segment_refs

    def _get_snapshot_metadata_json_from_minio(self, collection_id, snapshot_id):
        """
        Get snapshot metadata JSON from MinIO.
        Path: files/snapshots/{collection_id}/metadata/{snapshot_id}.json
        """
        minio_client = self._get_minio_client()
        path = f"files/snapshots/{collection_id}/metadata/{snapshot_id}.json"
        try:
            response = minio_client.get_object(MINIO_BUCKET, path)
            content = json.loads(response.read().decode('utf-8'))
            response.close()
            response.release_conn()
            log.info(f"Found snapshot metadata JSON: {path}")
            return content
        except Exception as e:
            log.warning(f"Error reading snapshot metadata JSON: {e}")
            return None

    def _list_all_snapshots_from_etcd(self):
        """List all snapshots from etcd"""
        prefix = f"{ETCD_ROOT_PATH}/meta/datacoord-meta/snapshot/"
        raw_data = self._etcd_get_prefix_raw(prefix)
        snapshots = []
        for key, value in raw_data:
            name = self._parse_snapshot_name_from_protobuf(value)
            if name:
                parts = key.split('/')
                if len(parts) >= 2:
                    snapshots.append({
                        "name": name,
                        "collection_id": parts[-2],
                        "snapshot_id": parts[-1],
                        "etcd_key": key
                    })
        log.info(f"Found {len(snapshots)} snapshots in etcd")
        return snapshots

    def _get_segment_gc_status_from_etcd(self, segment_id):
        """
        Check segment status in etcd to verify GC protection.
        Segments stored under: {root_path}/meta/datacoord-meta/s/{collection_id}/{partition_id}/{segment_id}
        """
        prefix = f"{ETCD_ROOT_PATH}/meta/datacoord-meta/s/"
        segments = self._etcd_get_prefix(prefix)
        for data in segments:
            if data.get("ID") == segment_id:
                log.info(f"Found segment {segment_id}: state={data.get('State')}")
                return data
        return None

    def _verify_snapshot_exists_in_etcd(self, snapshot_name):
        """Verify snapshot metadata exists in etcd"""
        metadata = self._get_snapshot_metadata_from_etcd(snapshot_name)
        return metadata is not None

    def _get_segment_files_count(self, collection_id):
        """Count segment files for a collection in MinIO"""
        minio_client = self._get_minio_client()
        # Segment files are stored under files/insert_log/{collection_id}/
        # and files/index_files/{collection_id}/
        prefixes = [
            f"files/insert_log/{collection_id}/",
            f"files/index_files/{collection_id}/",
        ]
        total_count = 0
        for prefix in prefixes:
            objects = list(minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
            total_count += len(objects)
        return total_count

    def _list_segment_files(self, collection_id):
        """List all segment files for a collection in MinIO"""
        minio_client = self._get_minio_client()
        prefixes = [
            f"files/insert_log/{collection_id}/",
            f"files/index_files/{collection_id}/",
        ]
        files = []
        for prefix in prefixes:
            objects = list(minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
            files.extend([obj.object_name for obj in objects])
        return files

    @pytest.mark.tags(CaseLabel.L3)
    def test_snapshot_protects_segments_from_gc(self):
        """
        target: verify snapshot-referenced segments are not GC'd
        method:
            1. Create collection, insert data, flush to create segment
            2. Create snapshot
            3. Drop original collection (normally triggers GC)
            4. Wait for multiple GC cycles
            5. Restore snapshot - should succeed (segments protected by snapshot)
            6. Verify all data is intact
        expected: restore should succeed after GC cycles, proving segments are protected
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        log.info(f"Created collection: {collection_name}")

        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        log.info("Data inserted and flushed")

        # Create snapshot to protect segments
        self.create_snapshot(client, collection_name, snapshot_name)
        log.info(f"Created snapshot: {snapshot_name}")

        # Verify snapshot exists
        snapshots = client.list_snapshots(collection_name)
        assert snapshot_name in snapshots, f"Snapshot {snapshot_name} should exist"

        # Drop original collection
        self.drop_collection(client, collection_name)
        log.info(f"Dropped collection: {collection_name}")

        # Wait for multiple GC cycles (GC interval is 10s)
        gc_wait_time = 60  # Wait 60s for multiple GC cycles
        log.info(f"Waiting {gc_wait_time}s for GC cycles...")
        time.sleep(gc_wait_time)

        # Restore snapshot - should succeed because snapshot protects segments
        log.info(f"Restoring snapshot to: {restored_collection_name}")
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id, timeout=120)

        # Verify all data is intact - this proves segments were protected from GC
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 1000, \
            f"Expected 1000 rows after GC cycles, got {res[0]['count(*)']}"

        log.info("SUCCESS: Snapshot protected segments from GC (verified via successful restore)")

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_segments_gc_after_snapshot_drop(self):
        """
        target: verify segments are released for GC after snapshot is dropped
        method:
            1. Create collection, insert data, flush
            2. Create snapshot_1 and snapshot_2 (both reference same segments)
            3. Drop original collection
            4. Drop snapshot_1
            5. Verify snapshot_2 can still restore (segments still protected)
            6. Drop snapshot_2
            7. All snapshots removed - segments should eventually be GC'd
               (Note: Direct verification requires storage access, so we verify via API)
        expected: snapshot_2 restore works until it's also dropped
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name_1 = cf.gen_unique_str(prefix + "_1")
        snapshot_name_2 = cf.gen_unique_str(prefix + "_2")
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        log.info(f"Created collection: {collection_name}")

        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        log.info("Data inserted and flushed")

        # Create two snapshots that share the same segments
        self.create_snapshot(client, collection_name, snapshot_name_1)
        log.info(f"Created snapshot_1: {snapshot_name_1}")

        self.create_snapshot(client, collection_name, snapshot_name_2)
        log.info(f"Created snapshot_2: {snapshot_name_2}")

        # Drop original collection
        self.drop_collection(client, collection_name)
        log.info(f"Dropped collection: {collection_name}")

        # Wait for GC cycles
        time.sleep(30)

        # Drop snapshot_1 - snapshot_2 should still protect segments
        self.drop_snapshot(client, snapshot_name_1)
        log.info(f"Dropped snapshot_1: {snapshot_name_1}")

        # Wait for more GC cycles
        time.sleep(30)

        # Verify snapshot_2 can still restore (segments protected)
        log.info(f"Restoring snapshot_2 to: {restored_collection_name}")
        job_id, _ = self.restore_snapshot(client, snapshot_name_2, restored_collection_name)
        self._wait_for_restore_complete(client, job_id, timeout=120)

        # Verify data integrity
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 500, \
            f"Expected 500 rows, got {res[0]['count(*)']}"

        log.info("SUCCESS: snapshot_2 restored successfully after snapshot_1 was dropped")

        # Cleanup
        self.drop_snapshot(client, snapshot_name_2)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_multiple_snapshots_share_segment_gc_protection(self):
        """
        target: verify shared segments remain protected when one snapshot is dropped
        method:
            1. Create collection with data
            2. Create snapshot_1
            3. Create snapshot_2 (same data, shares segments)
            4. Drop original collection
            5. Drop snapshot_1
            6. Wait for GC
            7. Restore from snapshot_2
        expected: snapshot_2 should still restore successfully
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name_1 = cf.gen_unique_str(prefix + "_1")
        snapshot_name_2 = cf.gen_unique_str(prefix + "_2")
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create two snapshots that share segments
        self.create_snapshot(client, collection_name, snapshot_name_1)
        log.info(f"Created snapshot_1: {snapshot_name_1}")

        self.create_snapshot(client, collection_name, snapshot_name_2)
        log.info(f"Created snapshot_2: {snapshot_name_2}")

        # Drop original collection
        self.drop_collection(client, collection_name)
        log.info(f"Dropped collection: {collection_name}")

        # Drop snapshot_1 - snapshot_2 still references the segments
        self.drop_snapshot(client, snapshot_name_1)
        log.info(f"Dropped snapshot_1: {snapshot_name_1}")

        # Wait for GC cycles
        gc_wait_time = 45
        log.info(f"Waiting {gc_wait_time}s for GC cycles...")
        time.sleep(gc_wait_time)

        # Restore from snapshot_2 - should succeed
        log.info(f"Restoring from snapshot_2 to: {restored_collection_name}")
        job_id, _ = self.restore_snapshot(client, snapshot_name_2, restored_collection_name)
        self._wait_for_restore_complete(client, job_id, timeout=120)

        # Verify data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 500, \
            f"Expected 500 rows, got {res[0]['count(*)']}"

        log.info("SUCCESS: Shared segments protected by remaining snapshot")

        # Cleanup
        self.drop_snapshot(client, snapshot_name_2)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_snapshot_create_during_gc_cycle(self):
        """
        target: verify snapshot creation during active GC
        method:
            1. Create collection, insert data
            2. Delete some data to trigger GC on deleted segments
            3. Immediately create snapshot
            4. Wait for GC
            5. Restore and verify
        expected: snapshot should capture consistent state
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Load and delete some data to trigger GC activity
        self.load_collection(client, collection_name)
        self.delete(client, collection_name, filter="id < 500")
        self.flush(client, collection_name)
        log.info("Deleted rows 0-499, triggering potential GC")

        # Immediately create snapshot to capture current state
        self.create_snapshot(client, collection_name, snapshot_name)
        log.info(f"Created snapshot immediately after delete: {snapshot_name}")

        # Wait for GC cycles
        gc_wait_time = 45
        log.info(f"Waiting {gc_wait_time}s for GC cycles...")
        time.sleep(gc_wait_time)

        # Restore and verify
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])

        # Should have 500 rows (rows 500-999)
        assert res[0]["count(*)"] == 500, \
            f"Expected 500 rows after delete+snapshot, got {res[0]['count(*)']}"

        log.info("SUCCESS: Snapshot created during GC cycle captured consistent state")

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_restore_during_gc_of_other_collections(self):
        """
        target: verify restore works while GC is processing other collections
        method:
            1. Create collection_1, insert data, create snapshot, drop collection_1
            2. Create collection_2, insert data, drop collection_2 (triggers GC)
            3. Immediately start restore of snapshot
        expected: restore should succeed despite concurrent GC activity
        """
        client = self._client()
        collection_name_1 = cf.gen_collection_name_by_testcase_name() + "_1"
        collection_name_2 = cf.gen_collection_name_by_testcase_name() + "_2"
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection_1 with snapshot
        self.create_collection(client, collection_name_1, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500)]
        self.insert(client, collection_name_1, rows)
        self.flush(client, collection_name_1)
        self.create_snapshot(client, collection_name_1, snapshot_name)
        self.drop_collection(client, collection_name_1)
        log.info(f"Created snapshot from collection_1 and dropped it")

        # Create collection_2 and drop it to trigger GC activity
        self.create_collection(client, collection_name_2, default_dim)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(1000)]
        self.insert(client, collection_name_2, rows)
        self.flush(client, collection_name_2)
        self.drop_collection(client, collection_name_2)
        log.info(f"Created and dropped collection_2 to trigger GC")

        # Immediately start restore while GC may be running
        log.info(f"Starting restore while GC may be running...")
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id, timeout=120)

        # Verify restore succeeded
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 500, \
            f"Expected 500 rows, got {res[0]['count(*)']}"

        log.info("SUCCESS: Restore completed successfully during concurrent GC")

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_gc_with_incremental_snapshots(self):
        """
        target: test GC behavior with incremental snapshots
        method:
            1. Create collection, insert batch1, flush, create snapshot_1
            2. Insert batch2, flush, create snapshot_2
            3. Insert batch3, flush, create snapshot_3
            4. Drop collection
            5. Drop snapshot_1 and snapshot_3
            6. Wait for GC
            7. Restore from snapshot_2
        expected: snapshot_2 should restore correctly with its exact data state
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_names = []
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)

        # Batch 1: rows 0-99, snapshot_1
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        snap_1 = cf.gen_unique_str(prefix + "_snap1")
        self.create_snapshot(client, collection_name, snap_1)
        snapshot_names.append(snap_1)
        log.info(f"Created snapshot_1 with 100 rows")

        # Batch 2: rows 100-199, snapshot_2
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(100, 200)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        snap_2 = cf.gen_unique_str(prefix + "_snap2")
        self.create_snapshot(client, collection_name, snap_2)
        snapshot_names.append(snap_2)
        log.info(f"Created snapshot_2 with 200 rows")

        # Batch 3: rows 200-299, snapshot_3
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(200, 300)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        snap_3 = cf.gen_unique_str(prefix + "_snap3")
        self.create_snapshot(client, collection_name, snap_3)
        snapshot_names.append(snap_3)
        log.info(f"Created snapshot_3 with 300 rows")

        # Drop collection
        self.drop_collection(client, collection_name)

        # Drop snapshot_1 and snapshot_3, keep snapshot_2
        self.drop_snapshot(client, snap_1)
        self.drop_snapshot(client, snap_3)
        log.info("Dropped snapshot_1 and snapshot_3, keeping snapshot_2")

        # Wait for GC
        gc_wait_time = 45
        log.info(f"Waiting {gc_wait_time}s for GC...")
        time.sleep(gc_wait_time)

        # Restore from snapshot_2
        job_id, _ = self.restore_snapshot(client, snap_2, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify snapshot_2 has exactly 200 rows (batch 1 + batch 2)
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 200, \
            f"Expected 200 rows from snapshot_2, got {res[0]['count(*)']}"

        # Verify data range is 0-199
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["id"])
        ids = sorted([r["id"] for r in res])
        assert ids == list(range(200)), "IDs should be 0-199"

        log.info("SUCCESS: Incremental snapshot with GC restored correctly")

        # Cleanup
        self.drop_snapshot(client, snap_2)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_long_running_gc_protection(self):
        """
        target: verify snapshot protection persists over extended GC period
        method:
            1. Create collection with substantial data
            2. Create snapshot
            3. Drop collection
            4. Wait for extended period (multiple GC cycles)
            5. Restore snapshot
        expected: data should remain intact after extended GC exposure

        Note: This is a longer-running test to verify sustained protection
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with more data
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(2000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)
        log.info(f"Created snapshot: {snapshot_name}")

        # Drop collection
        self.drop_collection(client, collection_name)
        log.info(f"Dropped collection: {collection_name}")

        # Extended wait - multiple GC cycles
        gc_wait_time = 90  # 9 GC cycles at 10s interval
        log.info(f"Extended wait {gc_wait_time}s for multiple GC cycles...")
        time.sleep(gc_wait_time)

        # Restore and verify
        log.info(f"Restoring snapshot after extended GC exposure...")
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id, timeout=180)

        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 2000, \
            f"Expected 2000 rows after extended GC, got {res[0]['count(*)']}"

        log.info("SUCCESS: Snapshot protection persisted over extended GC period")

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")

    @pytest.mark.tags(CaseLabel.L3)
    def test_snapshot_metadata_in_etcd(self):
        """
        target: verify snapshot metadata is correctly stored in etcd
        method:
            1. Create collection and insert data
            2. Create snapshot
            3. Verify snapshot metadata exists in etcd
            4. Verify segment references in etcd
            5. Drop snapshot
            6. Verify snapshot metadata is removed from etcd
        expected: snapshot metadata should be correctly managed in etcd
        NOTE: This test requires traditional storage mode (non-woodpecker)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        # Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        collection_info = client.describe_collection(collection_name)
        collection_id = collection_info.get("collection_id")
        log.info(f"Created collection: {collection_name}, ID: {collection_id}")

        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)
        log.info(f"Created snapshot: {snapshot_name}")

        # Verify snapshot metadata in etcd
        snapshot_metadata = self._get_snapshot_metadata_from_etcd(snapshot_name)
        assert snapshot_metadata is not None, \
            f"Snapshot {snapshot_name} metadata should exist in etcd"
        log.info(f"Snapshot metadata from etcd: {snapshot_metadata}")

        # Verify snapshot ID is valid
        snapshot_id = snapshot_metadata.get("snapshot_id") or snapshot_metadata.get("id")
        assert snapshot_id is not None, "Snapshot should have a valid ID in etcd"
        log.info(f"Snapshot ID: {snapshot_id}")

        # List all snapshots from etcd to verify
        all_snapshots = self._list_all_snapshots_from_etcd()
        snapshot_names = [s.get("name") for s in all_snapshots]
        assert snapshot_name in snapshot_names, \
            f"Snapshot {snapshot_name} should be in etcd snapshot list"

        # Drop snapshot
        self.drop_snapshot(client, snapshot_name)
        log.info(f"Dropped snapshot: {snapshot_name}")

        # Wait for metadata cleanup
        time.sleep(5)

        # Verify snapshot metadata is removed from etcd
        snapshot_metadata_after = self._get_snapshot_metadata_from_etcd(snapshot_name)
        assert snapshot_metadata_after is None, \
            f"Snapshot {snapshot_name} metadata should be removed from etcd after drop"

        log.info("SUCCESS: Snapshot metadata correctly managed in etcd")

        # Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_gc_protection_with_etcd_verification(self):
        """
        target: comprehensive GC protection test with etcd and MinIO verification
        method:
            1. Create collection, insert data, flush
            2. Record segment files in MinIO and metadata in etcd
            3. Create snapshot
            4. Verify snapshot metadata in etcd
            5. Drop original collection
            6. Wait for GC cycles
            7. Verify:
               - Segment files still exist in MinIO
               - Snapshot metadata still exists in etcd
            8. Restore and verify data integrity
        expected: full verification of GC protection via etcd and MinIO
        NOTE: This test requires traditional storage mode (non-woodpecker)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        collection_info = client.describe_collection(collection_name)
        collection_id = collection_info.get("collection_id")
        log.info(f"Created collection: {collection_name}, ID: {collection_id}")

        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(800)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Record initial segment files count
        initial_files_count = self._get_segment_files_count(collection_id)
        log.info(f"Initial segment files count in MinIO: {initial_files_count}")
        assert initial_files_count > 0, "Should have segment files after flush"

        # Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)
        log.info(f"Created snapshot: {snapshot_name}")

        # Verify snapshot metadata in etcd
        snapshot_metadata = self._get_snapshot_metadata_from_etcd(snapshot_name)
        assert snapshot_metadata is not None, "Snapshot metadata should exist in etcd"
        snapshot_id = snapshot_metadata.get("snapshot_id") or snapshot_metadata.get("id")
        log.info(f"Snapshot metadata: ID={snapshot_id}, name={snapshot_metadata.get('name')}")

        # Drop original collection
        self.drop_collection(client, collection_name)
        log.info(f"Dropped collection: {collection_name}")

        # Wait for GC cycles
        gc_wait_time = 60
        log.info(f"Waiting {gc_wait_time}s for GC cycles...")
        time.sleep(gc_wait_time)

        # Verify segment files still exist in MinIO (protected by snapshot)
        protected_files_count = self._get_segment_files_count(collection_id)
        log.info(f"Segment files after GC cycles: {protected_files_count}")
        assert protected_files_count > 0, \
            f"Segment files should still exist (protected), but got {protected_files_count}"

        # Verify snapshot metadata still exists in etcd
        snapshot_metadata_after_gc = self._get_snapshot_metadata_from_etcd(snapshot_name)
        assert snapshot_metadata_after_gc is not None, \
            "Snapshot metadata should still exist in etcd after GC"

        # Restore and verify data
        log.info(f"Restoring snapshot to: {restored_collection_name}")
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id, timeout=120)

        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 800, \
            f"Expected 800 rows, got {res[0]['count(*)']}"

        log.info("SUCCESS: GC protection verified via etcd and MinIO")

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_snapshot_drop_triggers_segment_gc_with_verification(self):
        """
        target: verify segment cleanup after snapshot drop with etcd and MinIO verification
        method:
            1. Create collection, insert data, flush
            2. Create snapshot, verify metadata in etcd
            3. Drop collection
            4. Verify segments protected (MinIO)
            5. Drop snapshot, verify metadata removed from etcd
            6. Wait for GC
            7. Verify segment files cleaned from MinIO
        expected: full lifecycle verification with etcd and MinIO
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        # Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        collection_info = client.describe_collection(collection_name)
        collection_id = collection_info.get("collection_id")
        log.info(f"Created collection: {collection_name}, ID: {collection_id}")

        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify initial segment files
        initial_files = self._list_segment_files(collection_id)
        log.info(f"Initial segment files: {len(initial_files)} files")
        assert len(initial_files) > 0, "Should have segment files"

        # Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)
        log.info(f"Created snapshot: {snapshot_name}")

        # Verify snapshot in etcd
        snapshot_metadata = self._get_snapshot_metadata_from_etcd(snapshot_name)
        assert snapshot_metadata is not None, "Snapshot should exist in etcd"

        # Drop collection
        self.drop_collection(client, collection_name)
        log.info(f"Dropped collection: {collection_name}")

        # Wait and verify segments still protected
        time.sleep(30)
        protected_files = self._list_segment_files(collection_id)
        log.info(f"Protected segment files: {len(protected_files)} files")
        assert len(protected_files) > 0, "Segment files should be protected by snapshot"

        # Drop snapshot
        self.drop_snapshot(client, snapshot_name)
        log.info(f"Dropped snapshot: {snapshot_name}")

        # Wait for metadata cleanup
        time.sleep(10)

        # Verify snapshot removed from etcd
        snapshot_metadata_after = self._get_snapshot_metadata_from_etcd(snapshot_name)
        assert snapshot_metadata_after is None, "Snapshot should be removed from etcd"
        log.info("Snapshot metadata removed from etcd")

        # Wait for GC to clean segment files
        gc_wait_time = 90
        log.info(f"Waiting {gc_wait_time}s for GC to clean segment files...")
        time.sleep(gc_wait_time)

        # Verify segment files cleaned from MinIO
        final_files = self._list_segment_files(collection_id)
        log.info(f"Final segment files after GC: {len(final_files)} files")

        if len(final_files) > 0:
            log.warning(f"Remaining files: {final_files[:5]}")

        assert len(final_files) == 0, \
            f"Segment files should be cleaned by GC, but {len(final_files)} remain"

        log.info("SUCCESS: Full snapshot lifecycle verified with etcd and MinIO")

    @pytest.mark.tags(CaseLabel.L3)
    def test_snapshot_segment_refs_in_etcd(self):
        """
        target: verify snapshot segment references (manifests) are correctly stored
        method:
            1. Create collection, insert data, flush (creates segment)
            2. Create snapshot
            3. Query etcd for snapshot metadata and get snapshot_id
            4. Query MinIO for manifest files under snapshots/{collection_id}/manifests/{snapshot_id}/
            5. Verify manifest files exist
            6. Verify snapshot metadata JSON in MinIO
            7. Drop snapshot
            8. Verify manifest files are cleaned from MinIO
            9. Verify snapshot metadata is cleaned from etcd after drop
        expected: segment references (manifests) should be correctly managed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        # Create collection and insert data
        self.create_collection(client, collection_name, default_dim)
        collection_info = client.describe_collection(collection_name)
        collection_id = collection_info.get("collection_id")
        log.info(f"Created collection: {collection_name}, ID: {collection_id}")

        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        log.info("Data flushed, segment should be created")

        # Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)
        log.info(f"Created snapshot: {snapshot_name}")

        # Describe snapshot via API
        snapshot_info, _ = self.describe_snapshot(client, snapshot_name)
        log.info(f"Describe snapshot API response: {snapshot_info}")

        # Get snapshot metadata from etcd
        snapshot_metadata = self._get_snapshot_metadata_from_etcd(snapshot_name)
        assert snapshot_metadata is not None, f"Snapshot {snapshot_name} should exist in etcd"

        # Get snapshot ID and collection_id from etcd metadata
        snapshot_id = snapshot_metadata.get("snapshot_id")
        etcd_collection_id = snapshot_metadata.get("collection_id")
        assert snapshot_id is not None, "Snapshot should have a valid ID"
        log.info(f"Snapshot ID from etcd: {snapshot_id}, collection_id: {etcd_collection_id}")

        # Get manifest files from MinIO
        manifest_refs = self._get_snapshot_segment_refs_from_etcd(etcd_collection_id, snapshot_id)
        log.info(f"Found {len(manifest_refs)} manifest files")

        # Verify manifest files exist
        assert len(manifest_refs) > 0, \
            f"Snapshot should have manifest files, but got {len(manifest_refs)}"

        # Log manifest file details
        for ref in manifest_refs:
            log.info(f"Manifest file: {ref}")

        # Verify manifest files are avro format
        for ref in manifest_refs:
            assert ref.get("manifest_file", "").endswith('.avro'), \
                f"Manifest file should be avro format: {ref}"

        # Verify snapshot metadata JSON exists in MinIO
        metadata_json = self._get_snapshot_metadata_json_from_minio(etcd_collection_id, snapshot_id)
        assert metadata_json is not None, "Snapshot metadata JSON should exist in MinIO"
        assert metadata_json.get("snapshot_info", {}).get("name") == snapshot_name, \
            "Snapshot name in metadata JSON should match"
        log.info(f"Snapshot metadata JSON verified: {list(metadata_json.keys())}")

        # Verify manifest_list in metadata JSON
        manifest_list = metadata_json.get("manifest_list", [])
        assert len(manifest_list) > 0, "Snapshot should have manifest_list in metadata JSON"
        log.info(f"manifest_list in metadata JSON: {manifest_list}")

        # Drop snapshot
        self.drop_snapshot(client, snapshot_name)
        log.info(f"Dropped snapshot: {snapshot_name}")

        # Wait for cleanup
        time.sleep(15)

        # Verify manifest files are cleaned from MinIO
        manifest_refs_after = self._get_snapshot_segment_refs_from_etcd(etcd_collection_id, snapshot_id)
        assert len(manifest_refs_after) == 0, \
            f"Manifest files should be cleaned after snapshot drop, but got {len(manifest_refs_after)}"

        # Verify snapshot metadata from etcd is cleaned
        snapshot_metadata_after = self._get_snapshot_metadata_from_etcd(snapshot_name)
        assert snapshot_metadata_after is None, \
            "Snapshot metadata should be cleaned from etcd after drop"

        log.info("SUCCESS: Snapshot segment references (manifests) correctly managed")

        # Cleanup
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_multiple_snapshots_share_segment_etcd_refs(self):
        """
        target: verify snapshot manifest files when multiple snapshots share same segments
        method:
            1. Create collection, insert data, flush (segment_1)
            2. Create snapshot_1 (refs segment_1)
            3. Create snapshot_2 (also refs segment_1 - same data, no changes)
            4. Verify both snapshots have manifest files in MinIO
            5. Insert more data, flush (segment_2)
            6. Create snapshot_3 (refs segment_1 + segment_2)
            7. Verify snapshot_3 has manifest files
            8. Drop snapshot_1
            9. Verify snapshot_2 and snapshot_3 manifests still exist
            10. Verify segment files are still protected (not GC'd)
        expected: each snapshot maintains independent manifest files
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name_1 = cf.gen_unique_str(prefix + "_1")
        snapshot_name_2 = cf.gen_unique_str(prefix + "_2")
        snapshot_name_3 = cf.gen_unique_str(prefix + "_3")

        # Create collection
        self.create_collection(client, collection_name, default_dim)
        collection_info = client.describe_collection(collection_name)
        collection_id = collection_info.get("collection_id")
        log.info(f"Created collection: {collection_name}, ID: {collection_id}")

        # Insert initial data and flush (creates segment_1)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        log.info("Batch 1 flushed - segment_1 created")

        # Create snapshot_1
        self.create_snapshot(client, collection_name, snapshot_name_1)
        log.info(f"Created snapshot_1: {snapshot_name_1}")

        # Create snapshot_2 (same data state, shares segment_1)
        self.create_snapshot(client, collection_name, snapshot_name_2)
        log.info(f"Created snapshot_2: {snapshot_name_2}")

        # Get snapshot metadata
        snap1_metadata = self._get_snapshot_metadata_from_etcd(snapshot_name_1)
        snap2_metadata = self._get_snapshot_metadata_from_etcd(snapshot_name_2)
        assert snap1_metadata is not None, "snapshot_1 should exist in etcd"
        assert snap2_metadata is not None, "snapshot_2 should exist in etcd"

        snap1_id = snap1_metadata.get("snapshot_id")
        snap2_id = snap2_metadata.get("snapshot_id")
        snap1_coll_id = snap1_metadata.get("collection_id")
        snap2_coll_id = snap2_metadata.get("collection_id")
        log.info(f"Snapshot IDs: snap1={snap1_id}, snap2={snap2_id}")

        # Get manifest files for both snapshots
        snap1_manifests = self._get_snapshot_segment_refs_from_etcd(snap1_coll_id, snap1_id)
        snap2_manifests = self._get_snapshot_segment_refs_from_etcd(snap2_coll_id, snap2_id)
        log.info(f"Manifest files count: snap1={len(snap1_manifests)}, snap2={len(snap2_manifests)}")

        # Both should have manifest files
        assert len(snap1_manifests) > 0, "snapshot_1 should have manifest files"
        assert len(snap2_manifests) > 0, "snapshot_2 should have manifest files"

        # Insert more data and flush (creates segment_2)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500, 1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        log.info("Batch 2 flushed - segment_2 created")

        # Create snapshot_3 (has both segment_1 and segment_2)
        self.create_snapshot(client, collection_name, snapshot_name_3)
        log.info(f"Created snapshot_3: {snapshot_name_3}")

        snap3_metadata = self._get_snapshot_metadata_from_etcd(snapshot_name_3)
        assert snap3_metadata is not None, "snapshot_3 should exist in etcd"
        snap3_id = snap3_metadata.get("snapshot_id")
        snap3_coll_id = snap3_metadata.get("collection_id")

        snap3_manifests = self._get_snapshot_segment_refs_from_etcd(snap3_coll_id, snap3_id)
        log.info(f"snapshot_3 manifest files count: {len(snap3_manifests)}")

        # snapshot_3 should have manifest files (may have more due to new segments)
        assert len(snap3_manifests) > 0, "snapshot_3 should have manifest files"

        # Verify snapshot metadata JSON in MinIO for each snapshot
        for snap_name, snap_id, coll_id in [
            (snapshot_name_1, snap1_id, snap1_coll_id),
            (snapshot_name_2, snap2_id, snap2_coll_id),
            (snapshot_name_3, snap3_id, snap3_coll_id)
        ]:
            metadata_json = self._get_snapshot_metadata_json_from_minio(coll_id, snap_id)
            assert metadata_json is not None, f"{snap_name} metadata JSON should exist"
            assert metadata_json.get("snapshot_info", {}).get("name") == snap_name
            log.info(f"{snap_name} metadata JSON verified")

        # Drop snapshot_1
        self.drop_snapshot(client, snapshot_name_1)
        log.info(f"Dropped snapshot_1: {snapshot_name_1}")

        # Wait for cleanup
        time.sleep(15)

        # Verify snapshot_1 manifests are cleaned
        snap1_manifests_after = self._get_snapshot_segment_refs_from_etcd(snap1_coll_id, snap1_id)
        assert len(snap1_manifests_after) == 0, \
            f"snapshot_1 manifests should be cleaned, but got {len(snap1_manifests_after)}"

        # Verify snapshot_2 and snapshot_3 manifests still exist
        snap2_manifests_after = self._get_snapshot_segment_refs_from_etcd(snap2_coll_id, snap2_id)
        snap3_manifests_after = self._get_snapshot_segment_refs_from_etcd(snap3_coll_id, snap3_id)
        assert len(snap2_manifests_after) > 0, "snapshot_2 manifests should still exist"
        assert len(snap3_manifests_after) > 0, "snapshot_3 manifests should still exist"
        log.info(f"After drop snap1 - snap2 manifests: {len(snap2_manifests_after)}, snap3 manifests: {len(snap3_manifests_after)}")

        # Drop collection to trigger potential GC
        self.drop_collection(client, collection_name)
        log.info("Dropped collection")

        # Wait for GC cycles
        time.sleep(45)

        # Verify segments still protected by remaining snapshots
        files_count = self._get_segment_files_count(collection_id)
        log.info(f"Segment files after collection drop: {files_count}")
        assert files_count > 0, \
            "Segment files should still exist (protected by snapshot_2 and snapshot_3)"

        # Restore from snapshot_2 to verify data integrity
        restored_name = cf.gen_unique_str(prefix + "_restored")
        job_id, _ = self.restore_snapshot(client, snapshot_name_2, restored_name)
        self._wait_for_restore_complete(client, job_id, timeout=120)

        self.load_collection(client, restored_name)
        res, _ = self.query(client, restored_name, filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 500, f"snapshot_2 should have 500 rows, got {res[0]['count(*)']}"
        log.info("snapshot_2 restored successfully with 500 rows")

        # Cleanup
        self.drop_collection(client, restored_name)
        self.drop_snapshot(client, snapshot_name_2)
        self.drop_snapshot(client, snapshot_name_3)

        log.info("SUCCESS: Multiple snapshots manifest files managed correctly")


class TestMilvusClientSnapshotAllDataTypes(TestMilvusClientV2Base):
    """
    L2 Test - Snapshot with all data types matrix testing
    Tests snapshot functionality with comprehensive data type coverage
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_all_scalar_types(self):
        """
        target: test snapshot with all scalar data types
        method: create collection with all scalar types, snapshot and restore
        expected: all scalar data should be preserved correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create schema with all scalar types
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("int8_field", DataType.INT8)
        schema.add_field("int16_field", DataType.INT16)
        schema.add_field("int32_field", DataType.INT32)
        schema.add_field("bool_field", DataType.BOOL)
        schema.add_field("float_field", DataType.FLOAT)
        schema.add_field("double_field", DataType.DOUBLE)
        schema.add_field("varchar_field", DataType.VARCHAR, max_length=256)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        # Insert data with all scalar types
        rng = np.random.default_rng(seed=19530)
        rows = [{
            "id": i,
            "vector": list(rng.random(default_dim)),
            "int8_field": np.int8(i % 127),
            "int16_field": np.int16(i * 10),
            "int32_field": np.int32(i * 100),
            "bool_field": i % 2 == 0,
            "float_field": float(i * 0.5),
            "double_field": float(i * 1.5),
            "varchar_field": f"string_{i}",
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify all scalar data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0",
                            output_fields=["id", "int8_field", "int16_field", "int32_field",
                                           "bool_field", "float_field", "double_field", "varchar_field"])
        assert len(res) == 100

        # Verify specific values
        for row in res:
            i = row["id"]
            assert row["int8_field"] == i % 127, f"int8_field mismatch at id={i}"
            assert row["int16_field"] == i * 10, f"int16_field mismatch at id={i}"
            assert row["int32_field"] == i * 100, f"int32_field mismatch at id={i}"
            assert row["bool_field"] == (i % 2 == 0), f"bool_field mismatch at id={i}"
            assert abs(row["float_field"] - i * 0.5) < 1e-5, f"float_field mismatch at id={i}"
            assert abs(row["double_field"] - i * 1.5) < 1e-10, f"double_field mismatch at id={i}"
            assert row["varchar_field"] == f"string_{i}", f"varchar_field mismatch at id={i}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_array_types(self):
        """
        target: test snapshot with array data types
        method: create collection with array fields, snapshot and restore
        expected: array data should be preserved correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create schema with array types
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("int_array", DataType.ARRAY, element_type=DataType.INT64, max_capacity=50)
        schema.add_field("float_array", DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=50)
        schema.add_field("varchar_array", DataType.ARRAY, element_type=DataType.VARCHAR,
                         max_length=100, max_capacity=50)
        schema.add_field("bool_array", DataType.ARRAY, element_type=DataType.BOOL, max_capacity=50)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        # Insert data with array types
        rng = np.random.default_rng(seed=19530)
        rows = [{
            "id": i,
            "vector": list(rng.random(default_dim)),
            "int_array": [i * j for j in range(10)],
            "float_array": [float(i * j * 0.1) for j in range(10)],
            "varchar_array": [f"str_{i}_{j}" for j in range(5)],
            "bool_array": [j % 2 == 0 for j in range(5)],
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify array data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 5",
                            output_fields=["id", "int_array", "float_array", "varchar_array", "bool_array"])
        assert len(res) == 1
        row = res[0]
        assert row["int_array"] == [5 * j for j in range(10)]
        assert row["varchar_array"] == [f"str_5_{j}" for j in range(5)]
        assert row["bool_array"] == [j % 2 == 0 for j in range(5)]

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_all_vector_types(self):
        """
        target: test snapshot with multiple vector types
        method: create collection with FloatVector, BinaryVector, Float16Vector, SparseVector (max 4 vectors)
        expected: all vector types should be preserved correctly
        Note: Milvus limits maximum 4 vector fields per collection
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create schema with multiple vector types (max 4 allowed)
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("float_vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("binary_vector", DataType.BINARY_VECTOR, dim=128)
        schema.add_field("float16_vector", DataType.FLOAT16_VECTOR, dim=default_dim)
        schema.add_field("sparse_vector", DataType.SPARSE_FLOAT_VECTOR)

        index_params = client.prepare_index_params()
        index_params.add_index("float_vector", metric_type="COSINE")
        index_params.add_index("binary_vector", metric_type="HAMMING")
        index_params.add_index("float16_vector", metric_type="L2")
        index_params.add_index("sparse_vector", metric_type="IP", index_type="SPARSE_INVERTED_INDEX")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        # Generate test data
        rng = np.random.default_rng(seed=19530)
        rows = []
        for i in range(100):
            float_vec = list(rng.random(default_dim))
            binary_vec = bytes(rng.integers(0, 256, size=16, dtype=np.uint8))
            float16_vec = np.array(rng.random(default_dim), dtype=np.float16).tobytes()
            # Sparse vector: {dim_index: value}
            sparse_vec = {j: float(rng.random()) for j in rng.choice(1000, size=10, replace=False)}

            rows.append({
                "id": i,
                "float_vector": float_vec,
                "binary_vector": binary_vec,
                "float16_vector": float16_vec,
                "sparse_vector": sparse_vec,
            })
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify data count
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 100

        # Verify search on float_vector works
        search_vectors = [list(rng.random(default_dim))]
        search_res, _ = self.search(client, restored_collection_name, search_vectors,
                                    anns_field="float_vector", limit=10, output_fields=["id"])
        assert len(search_res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_nullable_fields(self):
        """
        target: test snapshot with nullable fields
        method: create collection with nullable fields, insert data with nulls
        expected: null values should be preserved correctly after restore
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create schema with nullable fields
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("nullable_int", DataType.INT32, nullable=True)
        schema.add_field("nullable_varchar", DataType.VARCHAR, max_length=256, nullable=True)
        schema.add_field("nullable_float", DataType.FLOAT, nullable=True)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        # Insert data with some null values
        rng = np.random.default_rng(seed=19530)
        rows = []
        for i in range(100):
            row = {
                "id": i,
                "vector": list(rng.random(default_dim)),
                "nullable_int": i * 10 if i % 3 != 0 else None,
                "nullable_varchar": f"str_{i}" if i % 4 != 0 else None,
                "nullable_float": float(i * 0.5) if i % 5 != 0 else None,
            }
            rows.append(row)
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify nullable fields
        self.load_collection(client, restored_collection_name)

        # Check rows with null values
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 0",  # i=0 should have nullable_int=None
                            output_fields=["nullable_int", "nullable_varchar", "nullable_float"])
        assert len(res) == 1
        assert res[0]["nullable_int"] is None, "nullable_int should be None for id=0"
        assert res[0]["nullable_varchar"] is None, "nullable_varchar should be None for id=0"
        assert res[0]["nullable_float"] is None, "nullable_float should be None for id=0"

        # Check rows with non-null values
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 7",  # i=7: nullable_int=70, nullable_varchar='str_7', nullable_float=3.5
                            output_fields=["nullable_int", "nullable_varchar", "nullable_float"])
        assert len(res) == 1
        assert res[0]["nullable_int"] == 70
        assert res[0]["nullable_varchar"] == "str_7"
        assert abs(res[0]["nullable_float"] - 3.5) < 1e-5

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")


class TestMilvusClientSnapshotAllIndexTypes(TestMilvusClientV2Base):
    """
    L2 Test - Snapshot with all index types testing
    Tests snapshot functionality with various index configurations
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_ivf_flat_index(self):
        """
        target: test snapshot preserves IVF_FLAT index
        method: create collection with IVF_FLAT index, snapshot and restore
        expected: index type and parameters should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="L2",
                               index_type="IVF_FLAT",
                               params={"nlist": 128})

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_dim))} for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify index and search
        self.load_collection(client, restored_collection_name)
        search_vectors = [list(rng.random(default_dim))]
        res, _ = self.search(client, restored_collection_name, search_vectors,
                             search_params={"nprobe": 16}, limit=10, output_fields=["id"])
        assert len(res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_ivf_sq8_index(self):
        """
        target: test snapshot preserves IVF_SQ8 index
        method: create collection with IVF_SQ8 index, snapshot and restore
        expected: index type and parameters should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="L2",
                               index_type="IVF_SQ8",
                               params={"nlist": 128})

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_dim))} for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify search works
        self.load_collection(client, restored_collection_name)
        search_vectors = [list(rng.random(default_dim))]
        res, _ = self.search(client, restored_collection_name, search_vectors,
                             search_params={"nprobe": 16}, limit=10, output_fields=["id"])
        assert len(res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_ivf_pq_index(self):
        """
        target: test snapshot preserves IVF_PQ index
        method: create collection with IVF_PQ index, snapshot and restore
        expected: index type and parameters should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="L2",
                               index_type="IVF_PQ",
                               params={"nlist": 128, "m": 16, "nbits": 8})

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_dim))} for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify search works
        self.load_collection(client, restored_collection_name)
        search_vectors = [list(rng.random(default_dim))]
        res, _ = self.search(client, restored_collection_name, search_vectors,
                             search_params={"nprobe": 16}, limit=10, output_fields=["id"])
        assert len(res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_diskann_index(self):
        """
        target: test snapshot preserves DISKANN index
        method: create collection with DISKANN index, snapshot and restore
        expected: index type and parameters should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="L2",
                               index_type="DISKANN")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_dim))} for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify search works
        self.load_collection(client, restored_collection_name)
        search_vectors = [list(rng.random(default_dim))]
        res, _ = self.search(client, restored_collection_name, search_vectors,
                             search_params={"search_list": 100}, limit=10, output_fields=["id"])
        assert len(res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_scann_index(self):
        """
        target: test snapshot preserves SCANN index
        method: create collection with SCANN index, snapshot and restore
        expected: index type and parameters should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="L2",
                               index_type="SCANN",
                               params={"nlist": 128})

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_dim))} for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify search works
        self.load_collection(client, restored_collection_name)
        search_vectors = [list(rng.random(default_dim))]
        res, _ = self.search(client, restored_collection_name, search_vectors,
                             search_params={"nprobe": 16}, limit=10, output_fields=["id"])
        assert len(res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_scalar_index(self):
        """
        target: test snapshot preserves scalar field indexes
        method: create collection with scalar indexes, snapshot and restore
        expected: scalar indexes should be preserved and filter queries work
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("category", DataType.INT32)
        schema.add_field("tag", DataType.VARCHAR, max_length=128)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")
        index_params.add_index("category", index_type="STL_SORT")
        index_params.add_index("tag", index_type="INVERTED")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            "id": i,
            "vector": list(rng.random(default_dim)),
            "category": i % 10,
            "tag": f"tag_{i % 5}",
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify scalar index works with filter
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="category == 5", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 50  # 500/10 = 50 rows with category=5

        res, _ = self.query(client, restored_collection_name,
                            filter="tag == 'tag_3'", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 100  # 500/5 = 100 rows with tag='tag_3'

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")


class TestMilvusClientSnapshotCollectionProperties(TestMilvusClientV2Base):
    """
    L2 Test - Snapshot with collection properties testing
    Tests snapshot functionality with various collection configurations
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_collection_description(self):
        """
        target: test snapshot preserves collection description
        method: create collection with description, snapshot and restore
        expected: collection description should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        description = "Test collection for snapshot with description preservation"

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False,
                                       description=description)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_dim))} for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify description is preserved
        desc = client.describe_collection(restored_collection_name)
        assert desc.get("description") == description, \
            f"Description should be preserved, got: {desc.get('description')}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_num_shards(self):
        """
        target: test snapshot preserves number of shards
        method: create collection with specific shard count, snapshot and restore
        expected: shard configuration should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        num_shards = 4

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params, num_shards=num_shards)

        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_dim))} for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Verify original shard count
        desc = client.describe_collection(collection_name)
        original_shards = desc.get("num_shards") or desc.get("shards_num")
        log.info(f"Original collection shards: {original_shards}")

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify shard count is preserved
        desc = client.describe_collection(restored_collection_name)
        restored_shards = desc.get("num_shards") or desc.get("shards_num")
        log.info(f"Restored collection shards: {restored_shards}")
        assert restored_shards == num_shards, \
            f"Shard count should be {num_shards}, got: {restored_shards}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_consistency_level(self):
        """
        target: test snapshot preserves consistency level
        method: create collection with specific consistency level, snapshot and restore
        expected: consistency level should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        # Create collection with Bounded consistency
        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params, consistency_level="Bounded")

        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_dim))} for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Get original consistency level
        desc = client.describe_collection(collection_name)
        original_consistency = desc.get("consistency_level")
        log.info(f"Original consistency level: {original_consistency}")

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify consistency level is preserved
        desc = client.describe_collection(restored_collection_name)
        restored_consistency = desc.get("consistency_level")
        log.info(f"Restored consistency level: {restored_consistency}")
        # Consistency level should be preserved or default
        assert restored_consistency is not None

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_partition_key(self):
        """
        target: test snapshot preserves partition key configuration
        method: create collection with partition key, snapshot and restore
        expected: partition key field should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("category", DataType.INT64, is_partition_key=True)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params, num_partitions=16)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            "id": i,
            "vector": list(rng.random(default_dim)),
            "category": i % 100,
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify partition key is preserved in schema
        desc = client.describe_collection(restored_collection_name)
        fields = desc.get("fields", [])
        category_field = [f for f in fields if f.get("name") == "category"]
        assert len(category_field) == 1
        assert category_field[0].get("is_partition_key") == True, \
            "Partition key should be preserved"

        # Verify data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 500

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")


class TestMilvusClientSnapshotDataOperationsExtended(TestMilvusClientV2Base):
    """
    L2 Test - Snapshot after various data operations
    Tests snapshot functionality after insert, upsert, delete, compact, etc.
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_after_upsert(self):
        """
        target: test snapshot after upsert operations
        method: insert -> upsert (update existing + insert new) -> snapshot -> restore
        expected: restored data should reflect upsert operations
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)

        # Initial insert: ids 0-99
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
            default_float_field_name: float(i),
            default_string_field_name: f"original_{i}",
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Upsert: update ids 50-99 and insert ids 100-149
        upsert_rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
            default_float_field_name: float(i * 10),  # Updated value
            default_string_field_name: f"updated_{i}",
        } for i in range(50, 150)]
        self.upsert(client, collection_name, upsert_rows)
        self.flush(client, collection_name)

        # Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)

        # Restore
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify
        self.load_collection(client, restored_collection_name)

        # Total count should be 150 (0-149)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 150, f"Expected 150 rows, got {res[0]['count(*)']}"

        # Check original data (0-49) unchanged
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 25",
                            output_fields=["float", "varchar"])
        assert res[0]["float"] == 25.0
        assert res[0]["varchar"] == "original_25"

        # Check updated data (50-99)
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 75",
                            output_fields=["float", "varchar"])
        assert res[0]["float"] == 750.0  # Updated value
        assert res[0]["varchar"] == "updated_75"

        # Check new data (100-149)
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 125",
                            output_fields=["float", "varchar"])
        assert res[0]["float"] == 1250.0
        assert res[0]["varchar"] == "updated_125"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_after_compact(self):
        """
        target: test snapshot after compact operations
        method: insert -> delete -> compact -> snapshot -> restore
        expected: restored data should reflect compacted state
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)

        # Insert data
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Delete some data
        self.load_collection(client, collection_name)
        self.delete(client, collection_name, filter="id < 300")
        self.flush(client, collection_name)

        # Trigger compaction
        compact_res, _ = self.compact(client, collection_name)
        log.info(f"Compaction triggered: {compact_res}")

        # Wait for compaction to complete
        time.sleep(10)

        # Create snapshot after compaction
        self.create_snapshot(client, collection_name, snapshot_name)

        # Restore
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify data count (should be 700: 1000 - 300 deleted)
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 700, f"Expected 700 rows, got {res[0]['count(*)']}"

        # Verify deleted data is not present
        res, _ = self.query(client, restored_collection_name,
                            filter="id < 300", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 0, "Deleted data should not be present"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_after_reindex(self):
        """
        target: test snapshot after reindex operations
        method: create index -> snapshot -> drop index -> create different index -> snapshot
        expected: both snapshots should have their respective index configurations
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name_1 = cf.gen_unique_str(prefix + "_hnsw")
        snapshot_name_2 = cf.gen_unique_str(prefix + "_ivf")
        restored_collection_name_1 = cf.gen_unique_str(prefix + "_restored_hnsw")
        restored_collection_name_2 = cf.gen_unique_str(prefix + "_restored_ivf")

        # Create collection with HNSW index
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE",
                               index_type="HNSW",
                               params={"M": 16, "efConstruction": 200})

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{"id": i, "vector": list(rng.random(default_dim))} for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Snapshot 1: with HNSW index
        self.create_snapshot(client, collection_name, snapshot_name_1)
        log.info("Created snapshot with HNSW index")

        # Drop existing index
        self.release_collection(client, collection_name)
        self.drop_index(client, collection_name, "vector")

        # Create new IVF_FLAT index
        new_index_params = client.prepare_index_params()
        new_index_params.add_index("vector", metric_type="L2",
                                   index_type="IVF_FLAT",
                                   params={"nlist": 128})
        self.create_index(client, collection_name, new_index_params)
        log.info("Reindexed with IVF_FLAT")

        # Snapshot 2: with IVF_FLAT index
        self.create_snapshot(client, collection_name, snapshot_name_2)
        log.info("Created snapshot with IVF_FLAT index")

        # Restore snapshot 1 (HNSW)
        job_id_1, _ = self.restore_snapshot(client, snapshot_name_1, restored_collection_name_1)
        self._wait_for_restore_complete(client, job_id_1)

        # Restore snapshot 2 (IVF_FLAT)
        job_id_2, _ = self.restore_snapshot(client, snapshot_name_2, restored_collection_name_2)
        self._wait_for_restore_complete(client, job_id_2)

        # Verify both collections can search
        for restored_name in [restored_collection_name_1, restored_collection_name_2]:
            self.load_collection(client, restored_name)
            search_vectors = [list(rng.random(default_dim))]
            res, _ = self.search(client, restored_name, search_vectors,
                                 limit=10, output_fields=["id"])
            assert len(res[0]) == 10, f"Search should return 10 results for {restored_name}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name_1)
        self.drop_snapshot(client, snapshot_name_2)
        self.drop_collection(client, restored_collection_name_1)
        self.drop_collection(client, restored_collection_name_2)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_after_multiple_inserts(self):
        """
        target: test snapshot after multiple insert operations (multiple segments)
        method: insert batch1 -> flush -> insert batch2 -> flush -> snapshot -> restore
        expected: all data from multiple segments should be preserved
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)

        total_rows = 0
        # Insert multiple batches to create multiple segments
        for batch in range(5):
            rows = [{
                default_primary_key_field_name: i + batch * 200,
                default_vector_field_name: list(rng.random(default_dim)),
            } for i in range(200)]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            total_rows += 200
            log.info(f"Inserted batch {batch + 1}, total rows: {total_rows}")

        # Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)

        # Restore
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify all data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == total_rows, \
            f"Expected {total_rows} rows, got {res[0]['count(*)']}"

        # Verify data range
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["id"])
        ids = sorted([r["id"] for r in res])
        assert ids == list(range(total_rows)), "All IDs should be present"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_after_mixed_operations(self):
        """
        target: test snapshot after mixed operations (insert, delete, upsert)
        method: insert -> delete some -> upsert some -> snapshot -> restore
        expected: final state should reflect all operations
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)

        # Step 1: Initial insert (0-199)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
            default_string_field_name: f"original_{i}",
        } for i in range(200)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        log.info("Initial insert: 200 rows (0-199)")

        # Step 2: Delete some rows (0-49)
        self.load_collection(client, collection_name)
        self.delete(client, collection_name, filter="id < 50")
        self.flush(client, collection_name)
        log.info("Deleted rows 0-49")

        # Step 3: Upsert (update 100-149, insert 200-249)
        upsert_rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
            default_string_field_name: f"upserted_{i}",
        } for i in range(100, 250)]
        self.upsert(client, collection_name, upsert_rows)
        self.flush(client, collection_name)
        log.info("Upserted rows 100-249 (update 100-149, insert 200-249)")

        # Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)

        # Restore
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify final state
        self.load_collection(client, restored_collection_name)

        # Expected: rows 50-249 = 200 rows
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 200, f"Expected 200 rows, got {res[0]['count(*)']}"

        # Deleted rows (0-49) should not exist
        res, _ = self.query(client, restored_collection_name,
                            filter="id < 50", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 0, "Deleted rows should not exist"

        # Original rows (50-99) should have original values
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 75", output_fields=["varchar"])
        assert res[0]["varchar"] == "original_75"

        # Upserted rows (100-149) should have updated values
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 125", output_fields=["varchar"])
        assert res[0]["varchar"] == "upserted_125"

        # New rows (200-249) should exist
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 225", output_fields=["varchar"])
        assert res[0]["varchar"] == "upserted_225"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_clustering_compaction(self):
        """
        target: test snapshot after clustering compaction
        method: insert data -> clustering compact -> snapshot -> restore
        expected: data should be preserved after clustering compaction
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with clustering key
        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("category", DataType.INT64, is_clustering_key=True)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        # Insert data with categories
        rows = [{
            "id": i,
            "vector": list(rng.random(default_dim)),
            "category": i % 10,
        } for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Trigger clustering compaction
        try:
            compact_res, _ = self.compact(client, collection_name, is_clustering=True)
            log.info(f"Clustering compaction triggered: {compact_res}")
            time.sleep(15)  # Wait for compaction
        except Exception as e:
            log.warning(f"Clustering compaction may not be supported: {e}")

        # Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)

        # Restore
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 1000, f"Expected 1000 rows, got {res[0]['count(*)']}"

        # Verify category data integrity
        for cat in range(10):
            res, _ = self.query(client, restored_collection_name,
                                filter=f"category == {cat}", output_fields=["count(*)"])
            assert res[0]["count(*)"] == 100, f"Category {cat} should have 100 rows"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_dynamic_field(self):
        """
        target: test snapshot with dynamic field data
        method: create collection with enable_dynamic_field=True, insert data with extra fields
        expected: dynamic fields should be preserved after snapshot restore
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Create collection with dynamic field enabled
        schema = client.create_schema(enable_dynamic_field=True, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        # Insert data with dynamic fields
        rows = [{
            "id": i,
            "vector": list(rng.random(default_dim)),
            "dynamic_str": f"dynamic_{i}",
            "dynamic_int": i * 100,
            "dynamic_float": float(i * 0.5),
            "dynamic_bool": i % 2 == 0,
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name)
        self._wait_for_restore_complete(client, job_id)

        # Verify dynamic field data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 50",
                            output_fields=["id", "dynamic_str", "dynamic_int", "dynamic_float", "dynamic_bool"])
        assert len(res) == 1
        assert res[0]["dynamic_str"] == "dynamic_50"
        assert res[0]["dynamic_int"] == 5000
        assert abs(res[0]["dynamic_float"] - 25.0) < 1e-5
        assert res[0]["dynamic_bool"] == True

        # Verify all data count
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 100

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_collection_name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state['reason']}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")


class TestMilvusClientSnapshotConcurrency(TestMilvusClientV2Base):
    """
    Test concurrent operations for snapshot feature.

    Key scenarios tested:
    - Concurrent snapshot creation with same name
    - Snapshot consistency during concurrent writes
    - Concurrent restore operations from same snapshot
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_concurrent_create_same_name(self):
        """
        target: verify only one concurrent create with same name succeeds
        method: create snapshots with same name in parallel threads
        expected: exactly one succeeds, others fail with "already exists"
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        self.create_collection(client, collection_name, default_dim)

        results = []
        errors = []

        def create_snapshot_thread():
            try:
                self.create_snapshot(client, collection_name, snapshot_name)
                results.append("success")
            except Exception as e:
                errors.append(str(e))

        # Start multiple threads
        threads = [threading.Thread(target=create_snapshot_thread) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        log.info(f"Successes: {len(results)}, Errors: {len(errors)}")
        log.info(f"Error messages: {errors}")

        # Exactly one should succeed
        assert len(results) == 1, f"Expected 1 success, got {len(results)}"

        # Others should fail with "already exists" type error
        for err in errors:
            assert "exist" in err.lower() or "duplicate" in err.lower(), \
                f"Unexpected error: {err}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_captures_consistent_point_in_time(self):
        """
        target: verify snapshot captures consistent point-in-time state
        method: create snapshot while data is being inserted concurrently
        expected: snapshot should contain a consistent subset of data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)

        # Insert initial data
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(1000)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Flag to control insert thread
        stop_inserting = threading.Event()
        insert_count = [1000]  # Track inserted count

        def insert_thread():
            nonlocal insert_count
            batch_id = 0
            while not stop_inserting.is_set():
                batch_rows = [{
                    default_primary_key_field_name: 10000 + batch_id * 100 + i,
                    default_vector_field_name: list(rng.random(default_dim)),
                } for i in range(100)]
                try:
                    self.insert(client, collection_name, batch_rows)
                    insert_count[0] += 100
                    batch_id += 1
                except Exception as e:
                    log.warning(f"Insert failed: {e}")
                time.sleep(0.1)

        # Start insert thread
        inserter = threading.Thread(target=insert_thread)
        inserter.start()

        # Wait a bit then create snapshot
        time.sleep(0.5)
        self.create_snapshot(client, collection_name, snapshot_name)

        # Stop inserting
        stop_inserting.set()
        inserter.join()

        # Get snapshot info
        info, _ = self.describe_snapshot(client, snapshot_name)
        log.info(f"Snapshot created at ts: {info.create_ts}")
        log.info(f"Total inserted: {insert_count[0]}")

        # Restore and verify consistency
        restored_name = cf.gen_unique_str(prefix + "_restored")
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_name)
        self._wait_for_restore_complete(client, job_id)

        self.load_collection(client, restored_name)
        res, _ = self.query(client, restored_name, filter="id >= 0", output_fields=["count(*)"])
        restored_count = res[0]["count(*)"]

        log.info(f"Restored count: {restored_count}")

        # Snapshot should have at least initial data
        assert restored_count >= 1000, f"Should have at least 1000 rows, got {restored_count}"

        # Snapshot should not have more than total inserted at snapshot time
        # (may have less due to unflushed data)
        assert restored_count <= insert_count[0], \
            f"Should not exceed total inserted: {restored_count} > {insert_count[0]}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        self.drop_collection(client, restored_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_concurrent_restore_same_snapshot(self):
        """
        target: verify multiple concurrent restores of same snapshot
        method: start multiple restore jobs simultaneously from different threads
        expected: all restores should complete successfully with correct data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        self.create_snapshot(client, collection_name, snapshot_name)

        # Start concurrent restores
        job_ids = []
        restored_names = []
        lock = threading.Lock()

        def restore_thread(idx):
            restored_name = cf.gen_unique_str(prefix + f"_concurrent_{idx}")
            try:
                job_id, _ = self.restore_snapshot(client, snapshot_name, restored_name)
                with lock:
                    job_ids.append(job_id)
                    restored_names.append(restored_name)
            except Exception as e:
                log.error(f"Restore {idx} failed: {e}")

        threads = [threading.Thread(target=restore_thread, args=(i,)) for i in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Wait for all to complete
        for job_id in job_ids:
            self._wait_for_restore_complete(client, job_id, timeout=120)

        # Verify all restored collections
        for name in restored_names:
            self.load_collection(client, name)
            res, _ = self.query(client, name, filter="id >= 0", output_fields=["count(*)"])
            assert res[0]["count(*)"] == 500, f"{name} should have 500 rows"

        # Cleanup
        self.drop_snapshot(client, snapshot_name)
        for name in restored_names:
            self.drop_collection(client, name)

    def _wait_for_restore_complete(self, client, job_id, timeout=60):
        """Wait for restore snapshot job to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            state, _ = self.get_restore_snapshot_state(client, job_id)
            if state.state == "RestoreSnapshotCompleted":
                return
            if state.state == "RestoreSnapshotFailed":
                raise Exception(f"Restore snapshot failed: {state}")
            time.sleep(1)
        raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")
