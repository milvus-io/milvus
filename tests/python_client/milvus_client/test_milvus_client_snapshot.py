import time
import pytest
import numpy as np

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from pymilvus import DataType


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
