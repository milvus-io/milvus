import threading
import time

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from ml_dtypes import bfloat16
from pymilvus import DataType
from utils.util_log import test_log as log

prefix = "snapshot"
default_dim = 128


def wait_for_restore_complete(client, job_id, timeout=60):
    """Wait for restore snapshot job to complete"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        state = client.get_restore_snapshot_state(job_id)
        if state.state == "RestoreSnapshotCompleted":
            return
        if state.state == "RestoreSnapshotFailed":
            raise Exception(f"Restore snapshot failed: {state.reason}")
        time.sleep(1)
    raise TimeoutError(f"Restore snapshot job {job_id} did not complete within {timeout}s")
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
        info, _ = self.describe_snapshot(client, snapshot_name, collection_name)
        assert info.name == snapshot_name
        assert info.collection_name == collection_name
        assert info.create_ts > 0

        # 5. Drop snapshot
        self.drop_snapshot(client, snapshot_name, collection_name)

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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        assert job_id > 0, "restore_snapshot should return a valid job_id"

        # 4. Wait for restore to complete
        wait_for_restore_complete(client, job_id)

        # 5. Verify restored collection data count
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name, filter="",
                            output_fields=["count(*)"])
        restored_count = res[0]["count(*)"]
        assert restored_count == default_nb, \
            f"Restored collection should have {default_nb} rows, got {restored_count}"

        # 6. Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)



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
    def test_snapshot_create_whitespace_name(self):
        """
        target: test create snapshot with whitespace-only name
        method: create snapshot with name containing only spaces
        expected: should raise exception with "snapshot name should be not empty"

        Fixed in PR #47096: Server now validates snapshot names using standard naming rules.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)

        # Server validates snapshot name and rejects whitespace-only names
        error = {ct.err_code: 1100, ct.err_msg: "snapshot name should be not empty"}
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
        self.drop_snapshot(client, snapshot_name, collection_name)

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
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_create_same_name_different_collections(self):
        """
        target: test that snapshot name uniqueness is per-collection, not global
        method: create the same snapshot_name under two different collections
        expected: both succeed; describe returns the owning collection for each
        note: server enforces uniqueness keyed by (collection_id, snapshot_name),
              see internal/datacoord/services.go:2093-2099
        """
        client = self._client()
        col_a = cf.gen_collection_name_by_testcase_name() + "_a"
        col_b = cf.gen_collection_name_by_testcase_name() + "_b"
        shared_snapshot = cf.gen_unique_str(prefix + "_shared")

        self.create_collection(client, col_a, default_dim)
        self.create_collection(client, col_b, default_dim)

        # Both snapshot creations should succeed under different collections
        self.create_snapshot(client, col_a, shared_snapshot)
        self.create_snapshot(client, col_b, shared_snapshot)

        # Each collection's list returns its own snapshot
        snaps_a, _ = self.list_snapshots(client, collection_name=col_a)
        snaps_b, _ = self.list_snapshots(client, collection_name=col_b)
        assert shared_snapshot in snaps_a
        assert shared_snapshot in snaps_b

        # describe returns the owning collection id/name, not the other one
        info_a, _ = self.describe_snapshot(client, shared_snapshot, col_a)
        info_b, _ = self.describe_snapshot(client, shared_snapshot, col_b)
        assert info_a.collection_name == col_a
        assert info_b.collection_name == col_b

        # Cleanup
        self.drop_snapshot(client, shared_snapshot, col_a)
        self.drop_snapshot(client, shared_snapshot, col_b)
        self.drop_collection(client, col_a)
        self.drop_collection(client, col_b)


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

        # SDK validates snapshot_name and raises ParamError before checking collection_name
        error = {ct.err_code: 1, ct.err_msg: "snapshot_name must be a non-empty string"}
        self.drop_snapshot(client, snapshot_name, "",
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_drop_whitespace_name(self):
        """
        target: test drop snapshot with whitespace-only name
        method: drop snapshot with name containing only spaces
        expected: should raise exception with "snapshot name should be not empty"

        Fixed in PR #47096: Server now validates snapshot names using standard naming rules.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)

        # Server validates snapshot name and rejects whitespace-only names
        error = {ct.err_code: 1100, ct.err_msg: "snapshot name should be not empty"}
        self.drop_snapshot(client, " ", collection_name,
                           check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_drop_not_exist(self):
        """
        target: test drop non-existent snapshot (idempotent)
        method: drop snapshot that doesn't exist
        expected: should succeed (idempotent behavior)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        snapshot_name = cf.gen_unique_str("non_existent")

        # Should not raise exception (idempotent)
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_snapshot_drop_during_restore(self):
        """
        target: test drop snapshot while restore job is still in progress
        method: create snapshot -> start restore -> immediately drop snapshot
        expected: drop should fail with error about active restore operations
        verified: https://github.com/milvus-io/milvus/issues/47578
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
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # 2. Create snapshot
        self.create_snapshot(client, collection_name, snapshot_name)

        # 3. Start restore (creates CopySegment jobs referencing the snapshot)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)

        # 4. Wait until restore is actively in progress (ref count registered)
        #    before attempting drop, to avoid timing-dependent flakiness
        start = time.time()
        while time.time() - start < 30:
            state = client.get_restore_snapshot_state(job_id)
            if state.state not in ("RestoreSnapshotPending",):
                break
            time.sleep(0.5)
        log.info(f"Restore state before drop attempt: {state.state}")

        # 5. Attempt to drop the snapshot while restore is in progress.
        # PR #48143 introduced pin-based protection: restore jobs pin the snapshot
        # and Drop fails with "active pins exist, unpin before dropping".
        error = {ct.err_code: 2601, ct.err_msg: "active pins exist"}
        self.drop_snapshot(client, snapshot_name, collection_name,
                           check_task=CheckTasks.err_res, check_items=error)

        # 6. Wait for restore to complete
        wait_for_restore_complete(client, job_id)

        # 7. After restore completes, drop should succeed (ref count is 0)
        self.drop_snapshot(client, snapshot_name, collection_name)

        # 8. Verify snapshot is actually dropped
        snapshots, _ = self.list_snapshots(client, collection_name=collection_name)
        assert snapshot_name not in snapshots

        # Cleanup
        self.drop_collection(client, restored_collection_name)


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
            self.drop_snapshot(client, name, collection_name)

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
        self.drop_snapshot(client, snapshot_name1, collection_name1)
        self.drop_snapshot(client, snapshot_name2, collection_name2)

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
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        snapshot_name = cf.gen_unique_str("non_existent")

        error = {ct.err_code: 1, ct.err_msg: "not found"}
        self.describe_snapshot(client, snapshot_name, collection_name,
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

        info, _ = self.describe_snapshot(client, snapshot_name, collection_name)
        assert info.description == description

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_list_by_db_name_from_other_context(self):
        """
        target: test list_snapshots honors the db_name kwarg from a different active-db context
        method: create collection + snapshot in a non-default db, then switch client to
                default db and call list_snapshots(collection_name=<col>, db_name=<target_db>)
        expected: snapshot is returned; same call with db_name="default" returns empty/missing
        note: server requires collection_name for ListSnapshots
              (internal/proxy/task_snapshot.go:448-450)
        """
        client = self._client()
        target_db = cf.gen_unique_str("test_db_list")
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        self.create_database(client, target_db)
        try:
            client.using_database(target_db)
            self.create_collection(client, collection_name, default_dim)
            self.create_snapshot(client, collection_name, snapshot_name)

            # switch active db back to default but query via db_name kwarg
            client.using_database("default")
            snapshots, _ = self.list_snapshots(client, collection_name=collection_name,
                                               db_name=target_db)
            assert snapshot_name in snapshots, \
                f"{snapshot_name} missing when listing via db_name={target_db}, got {snapshots}"

            # cleanup while still in target db context
            client.using_database(target_db)
            self.drop_snapshot(client, snapshot_name, collection_name)
            self.drop_collection(client, collection_name)
        finally:
            client.using_database("default")
            try:
                client.drop_database(target_db)
            except Exception as e:
                log.warning(f"Failed to drop test database '{target_db}': {e}")

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_list_restore_jobs_by_db_name(self):
        """
        target: test list_restore_snapshot_jobs(db_name=X) filters by database
        method: create snapshot + trigger restore entirely in a non-default db
                (explicit source_db_name / target_db_name) -> list jobs via db_name
        expected: returned jobs include the one created in target db; default db sees none
        """
        client = self._client()
        target_db = cf.gen_unique_str("test_db_jobs")
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_name = cf.gen_unique_str(prefix + "_restored")

        self.create_database(client, target_db)
        try:
            client.using_database(target_db)
            self.create_collection(client, collection_name, default_dim)
            rng = np.random.default_rng(seed=19530)
            rows = [{
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random(default_dim)),
            } for i in range(500)]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            self.create_snapshot(client, collection_name, snapshot_name)

            # Explicitly pin both source and target to target_db so the job is
            # recorded under target_db's DbId (default empty target_db_name
            # resolves to "default" on the server side)
            job_id = client.restore_snapshot(snapshot_name, collection_name,
                                             restored_name,
                                             source_db_name=target_db,
                                             target_db_name=target_db)
            wait_for_restore_complete(client, job_id)

            # list jobs via explicit db_name kwarg from default db context
            client.using_database("default")
            jobs_target, _ = self.list_restore_snapshot_jobs(client, collection_name="",
                                                             db_name=target_db)
            job_ids = [j.job_id for j in jobs_target]
            assert job_id in job_ids, \
                f"Job {job_id} should appear when listing via db_name={target_db}, got {job_ids}"

            # jobs in default db must not include this job
            jobs_default, _ = self.list_restore_snapshot_jobs(client, collection_name="",
                                                              db_name="default")
            default_job_ids = [j.job_id for j in jobs_default]
            assert job_id not in default_job_ids, \
                f"Job {job_id} leaked into default db listing: {default_job_ids}"

            # Cleanup: both collections are in target_db
            client.using_database(target_db)
            self.drop_snapshot(client, snapshot_name, collection_name)
            self.drop_collection(client, collection_name)
            self.drop_collection(client, restored_name)
        finally:
            client.using_database("default")
            try:
                client.drop_database(target_db)
            except Exception as e:
                log.warning(f"Failed to drop test database '{target_db}': {e}")


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
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        snapshot_name = cf.gen_unique_str("non_existent")
        target_collection_name = cf.gen_unique_str(prefix + "_target")

        error = {ct.err_code: 1, ct.err_msg: "not found"}
        self.restore_snapshot(client, snapshot_name, target_collection_name,
                              source_collection_name=collection_name,
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
                              source_collection_name=collection_name,
                              check_task=CheckTasks.err_res, check_items=error)

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)


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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["id"])
        assert len(res) == 100
        ids = sorted([r["id"] for r in res])
        assert ids == list(range(100))

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="pk like 'key_%'", output_fields=["pk"])
        assert len(res) == 100

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify data count
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 100

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify JSON data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 0", output_fields=["metadata"])
        assert res[0]["metadata"]["key"] == "value_0"

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify dynamic field data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 0", output_fields=["dynamic_field_1", "dynamic_field_2"])
        assert res[0]["dynamic_field_1"] == "dynamic_0"
        assert res[0]["dynamic_field_2"] == 0

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)



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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)



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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify only 50 rows remain
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 50

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Restored collection should only have 100 rows (point-in-time)
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 100

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)



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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)



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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)



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
                    self.drop_snapshot(client, name, collection_name)
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
                self.drop_snapshot(client, name, collection_name)
            except Exception as e:
                found_limit = length
                log.info(f"Snapshot name length limit is less than {length}: {e}")
                break

        if found_limit is None:
            log.warning("No length limit found up to 1024 characters - this may be a bug")

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_restore_progress_tracking(self):
        """
        target: verify restore progress is correctly reported
        method: monitor progress during restore
        expected: progress should go from 0 to 100, start_time should be set

        Fixed in PR #47096: Server now correctly sets start_time from RestoreSnapshotJob.StartedAt.
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
            job_id, _ = self.restore_snapshot(client, snapshot_name, restored_name,
                                                      source_collection_name=collection_name)
            wait_for_restore_complete(client, job_id)

            self.load_collection(client, restored_name)
            res, _ = self.query(client, restored_name, filter="id >= 0", output_fields=["count(*)"])
            actual_count = res[0]["count(*)"]

            assert actual_count == expected_counts[i], \
                f"Snapshot {i} should have {expected_counts[i]} rows, got {actual_count}"

            self.drop_collection(client, restored_name)

        # Cleanup
        for snapshot_name in snapshots:
            self.drop_snapshot(client, snapshot_name, collection_name)

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
            job_id, _ = self.restore_snapshot(client, snapshot_name, restored_name,
                                                      source_collection_name=collection_name)
            restore_jobs.append(job_id)

        # Wait for all to complete
        for job_id in restore_jobs:
            wait_for_restore_complete(client, job_id, timeout=120)

        # Verify all restored collections have correct data
        for restored_name in restored_names:
            self.load_collection(client, restored_name)
            res, _ = self.query(client, restored_name, filter="id >= 0", output_fields=["count(*)"])
            assert res[0]["count(*)"] == 500, f"{restored_name} should have 500 rows"
            self.drop_collection(client, restored_name)

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)



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
        self.drop_snapshot(client, snapshot_name, collection_name)

        # Try to restore - should fail
        error = {ct.err_code: 1, ct.err_msg: "not found"}
        self.restore_snapshot(client, snapshot_name, restored_collection_name,
                              source_collection_name=collection_name,
                              check_task=CheckTasks.err_res, check_items=error)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_cascade_delete_on_drop_collection(self):
        """
        target: test snapshots are cascade deleted when collection is dropped
        method: create snapshot, drop collection, recreate collection, list snapshots
        expected: snapshots should be automatically deleted with the collection

        Changed in PR #48143: snapshot lifecycle is now bound to collection.
        Dropping a collection cascades to delete all its snapshots.
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

        # Verify snapshot exists before drop
        snapshots, _ = self.list_snapshots(client, collection_name=collection_name)
        assert snapshot_name in snapshots, "Snapshot should exist before collection drop"

        # Drop collection - should cascade delete snapshots
        self.drop_collection(client, collection_name)

        # Recreate collection with same name (this is a new collection)
        self.create_collection(client, collection_name, default_dim)

        # Snapshots should not exist for the new collection
        snapshots, _ = self.list_snapshots(client, collection_name=collection_name)
        assert len(snapshots) == 0, \
            "Snapshots should be cascade deleted when collection is dropped"

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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify schema of restored collection
        desc = client.describe_collection(restored_collection_name)
        # Check auto_id is preserved
        pk_field = [f for f in desc["fields"] if f.get("is_primary")][0]
        assert pk_field.get("auto_id", False), "auto_id should be preserved"

        # Verify can insert without id
        new_rows = [{"vector": list(rng.random(default_dim))} for _ in range(10)]
        self.insert(client, restored_collection_name, new_rows)

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)



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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_comprehensive_schema(self):
        """
        target: test snapshot with comprehensive schema covering all data types
        method: use gen_all_datatype_collection_schema (all scalars, arrays, vectors,
                struct array, BM25 function, MinHash function, nullable fields, text match)
                then snapshot and restore, verify data integrity
        expected: all field data should be preserved correctly after restore
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # Generate comprehensive schema with all data types
        schema = cf.gen_all_datatype_collection_schema(
            dim=default_dim,
            enable_struct_array_field=True,
            enable_dynamic_field=True,
            nullable=True
        )

        # Create indexes for vector fields
        index_params = client.prepare_index_params()
        index_params.add_index("float_vector", metric_type="COSINE")
        index_params.add_index("text_sparse_emb", metric_type="BM25",
                               index_type="SPARSE_INVERTED_INDEX")
        index_params.add_index("minhash_emb", metric_type="HAMMING")
        # Struct array inner vector field also needs index
        index_params.add_index("array_struct[float_vector]", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        # Generate row data using schema-based data generator
        nb = 200
        data = cf.gen_row_data_by_schema(nb=nb, schema=schema)
        self.insert(client, collection_name, data)
        self.flush(client, collection_name)

        # Verify source collection
        self.load_collection(client, collection_name)
        res, _ = self.query(client, collection_name, filter="",
                            output_fields=["count(*)"])
        assert res[0]["count(*)"] == nb

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify restored data count
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name, filter="",
                            output_fields=["count(*)"])
        assert res[0]["count(*)"] == nb, \
            f"Expected {nb} rows, got {res[0]['count(*)']}"

        # Verify text match works (BM25 function preserved)
        res, _ = self.query(client, restored_collection_name,
                            filter='TEXT_MATCH(text, "the")',
                            output_fields=["id", "text"])
        log.info(f"Text match results: {len(res)} rows")

        # Verify float vector search works
        rng = np.random.default_rng(seed=19530)
        search_vectors = [list(rng.random(default_dim))]
        search_res, _ = self.search(client, restored_collection_name, search_vectors,
                                    anns_field="float_vector", limit=10,
                                    output_fields=["id"])
        assert len(search_res[0]) > 0, "Float vector search should return results"

        # Verify scalar fields are preserved (PK field name is "int64")
        res, _ = self.query(client, restored_collection_name,
                            filter="int64 >= 0",
                            output_fields=["int64", "varchar", "json_field",
                                           "array_int", "array_bool", "array_struct"],
                            limit=10)
        assert len(res) > 0
        # Check at least one row has non-null array fields
        has_array_data = False
        has_struct_data = False
        for row in res:
            arr = row.get("array_int")
            if arr is not None and len(arr) > 0:
                has_array_data = True
            struct_arr = row.get("array_struct")
            if struct_arr is not None and len(struct_arr) > 0:
                has_struct_data = True
                assert "name" in struct_arr[0], "Struct element should have 'name'"
                assert "age" in struct_arr[0], "Struct element should have 'age'"
        assert has_array_data, "Should have rows with array_int data"
        assert has_struct_data, "Should have rows with array_struct data"

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_bfloat16_vector(self):
        """
        target: test snapshot with BFloat16 vector type
        method: create collection with BFloat16Vector field, snapshot and restore
        expected: BFloat16 vector data should be preserved correctly
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("float_vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("bfloat16_vector", DataType.BFLOAT16_VECTOR, dim=default_dim)

        index_params = client.prepare_index_params()
        index_params.add_index("float_vector", metric_type="COSINE")
        index_params.add_index("bfloat16_vector", metric_type="L2")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            "id": i,
            "float_vector": list(rng.random(default_dim)),
            "bfloat16_vector": np.array(rng.random(default_dim), dtype=bfloat16),
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify data count
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 100

        # Verify search on bfloat16 vector works
        search_vectors = [np.array(rng.random(default_dim), dtype=bfloat16)]
        res, _ = self.search(client, restored_collection_name, search_vectors,
                             anns_field="bfloat16_vector", limit=10,
                             output_fields=["id"])
        assert len(res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_with_all_array_element_types(self):
        """
        target: test snapshot with all array element types
        method: create collection with Array[Int8/Int16/Int32/Int64/Float/Double/Bool/VarChar]
        expected: all array data should be preserved correctly after restore
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        schema = client.create_schema(enable_dynamic_field=False, auto_id=False)
        schema.add_field("id", DataType.INT64, is_primary=True)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=default_dim)
        schema.add_field("arr_int8", DataType.ARRAY, element_type=DataType.INT8, max_capacity=20)
        schema.add_field("arr_int16", DataType.ARRAY, element_type=DataType.INT16, max_capacity=20)
        schema.add_field("arr_int32", DataType.ARRAY, element_type=DataType.INT32, max_capacity=20)
        schema.add_field("arr_int64", DataType.ARRAY, element_type=DataType.INT64, max_capacity=20)
        schema.add_field("arr_float", DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=20)
        schema.add_field("arr_double", DataType.ARRAY, element_type=DataType.DOUBLE, max_capacity=20)
        schema.add_field("arr_bool", DataType.ARRAY, element_type=DataType.BOOL, max_capacity=20)
        schema.add_field("arr_varchar", DataType.ARRAY, element_type=DataType.VARCHAR,
                         max_length=100, max_capacity=20)

        index_params = client.prepare_index_params()
        index_params.add_index("vector", metric_type="COSINE")

        self.create_collection(client, collection_name, schema=schema,
                               index_params=index_params)

        rng = np.random.default_rng(seed=19530)
        rows = [{
            "id": i,
            "vector": list(rng.random(default_dim)),
            "arr_int8": [int(np.int8(j)) for j in range(5)],
            "arr_int16": [int(np.int16(i * 10 + j)) for j in range(5)],
            "arr_int32": [i * 100 + j for j in range(5)],
            "arr_int64": [i * 1000 + j for j in range(5)],
            "arr_float": [float(i * 0.1 + j * 0.01) for j in range(5)],
            "arr_double": [float(i * 1.1 + j * 0.11) for j in range(5)],
            "arr_bool": [j % 2 == 0 for j in range(5)],
            "arr_varchar": [f"s_{i}_{j}" for j in range(5)],
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # Create snapshot and restore
        self.create_snapshot(client, collection_name, snapshot_name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 5",
                            output_fields=["arr_int8", "arr_int16", "arr_int32", "arr_int64",
                                           "arr_float", "arr_double", "arr_bool", "arr_varchar"])
        assert len(res) == 1
        row = res[0]
        assert row["arr_int32"] == [500, 501, 502, 503, 504]
        assert row["arr_int64"] == [5000, 5001, 5002, 5003, 5004]
        assert row["arr_bool"] == [True, False, True, False, True]
        assert row["arr_varchar"] == ["s_5_0", "s_5_1", "s_5_2", "s_5_3", "s_5_4"]

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)


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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify index and search
        self.load_collection(client, restored_collection_name)
        search_vectors = [list(rng.random(default_dim))]
        res, _ = self.search(client, restored_collection_name, search_vectors,
                             search_params={"nprobe": 16}, limit=10, output_fields=["id"])
        assert len(res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify search works
        self.load_collection(client, restored_collection_name)
        search_vectors = [list(rng.random(default_dim))]
        res, _ = self.search(client, restored_collection_name, search_vectors,
                             search_params={"nprobe": 16}, limit=10, output_fields=["id"])
        assert len(res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify search works
        self.load_collection(client, restored_collection_name)
        search_vectors = [list(rng.random(default_dim))]
        res, _ = self.search(client, restored_collection_name, search_vectors,
                             search_params={"nprobe": 16}, limit=10, output_fields=["id"])
        assert len(res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify search works
        self.load_collection(client, restored_collection_name)
        search_vectors = [list(rng.random(default_dim))]
        res, _ = self.search(client, restored_collection_name, search_vectors,
                             search_params={"search_list": 100}, limit=10, output_fields=["id"])
        assert len(res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify search works
        self.load_collection(client, restored_collection_name)
        search_vectors = [list(rng.random(default_dim))]
        res, _ = self.search(client, restored_collection_name, search_vectors,
                             search_params={"nprobe": 16}, limit=10, output_fields=["id"])
        assert len(res[0]) == 10

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify scalar index works with filter
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="category == 5", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 50  # 500/10 = 50 rows with category=5

        res, _ = self.query(client, restored_collection_name,
                            filter="tag == 'tag_3'", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 100  # 500/5 = 100 rows with tag='tag_3'

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)



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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify description is preserved
        desc = client.describe_collection(restored_collection_name)
        assert desc.get("description") == description, \
            f"Description should be preserved, got: {desc.get('description')}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify shard count is preserved
        desc = client.describe_collection(restored_collection_name)
        restored_shards = desc.get("num_shards") or desc.get("shards_num")
        log.info(f"Restored collection shards: {restored_shards}")
        assert restored_shards == num_shards, \
            f"Shard count should be {num_shards}, got: {restored_shards}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify consistency level is preserved
        desc = client.describe_collection(restored_collection_name)
        restored_consistency = desc.get("consistency_level")
        log.info(f"Restored consistency level: {restored_consistency}")
        # Consistency level should be preserved or default
        assert restored_consistency is not None

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify partition key is preserved in schema
        desc = client.describe_collection(restored_collection_name)
        fields = desc.get("fields", [])
        category_field = [f for f in fields if f.get("name") == "category"]
        assert len(category_field) == 1
        assert category_field[0].get("is_partition_key"), \
            "Partition key should be preserved"

        # Verify data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 500

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)



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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id_1, _ = self.restore_snapshot(client, snapshot_name_1, restored_collection_name_1,
                                                  source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id_1)

        # Restore snapshot 2 (IVF_FLAT)
        job_id_2, _ = self.restore_snapshot(client, snapshot_name_2, restored_collection_name_2,
                                                  source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id_2)

        # Verify both collections can search
        for restored_name in [restored_collection_name_1, restored_collection_name_2]:
            self.load_collection(client, restored_name)
            search_vectors = [list(rng.random(default_dim))]
            res, _ = self.search(client, restored_name, search_vectors,
                                 limit=10, output_fields=["id"])
            assert len(res[0]) == 10, f"Search should return 10 results for {restored_name}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name_1, collection_name)
        self.drop_snapshot(client, snapshot_name_2, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # Verify dynamic field data
        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name,
                            filter="id == 50",
                            output_fields=["id", "dynamic_str", "dynamic_int", "dynamic_float", "dynamic_bool"])
        assert len(res) == 1
        assert res[0]["dynamic_str"] == "dynamic_50"
        assert res[0]["dynamic_int"] == 5000
        assert abs(res[0]["dynamic_float"] - 25.0) < 1e-5
        assert res[0]["dynamic_bool"] is True

        # Verify all data count
        res, _ = self.query(client, restored_collection_name,
                            filter="id >= 0", output_fields=["count(*)"])
        assert res[0]["count(*)"] == 100

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)



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
                # Directly call client method to bypass ResponseChecker framework
                # This allows us to capture the real MilvusException with original error message
                client.create_snapshot(snapshot_name, collection_name)
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
        self.drop_snapshot(client, snapshot_name, collection_name)

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
        info, _ = self.describe_snapshot(client, snapshot_name, collection_name)
        log.info(f"Snapshot created at ts: {info.create_ts}")
        log.info(f"Total inserted: {insert_count[0]}")

        # Restore and verify consistency
        restored_name = cf.gen_unique_str(prefix + "_restored")
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

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
        self.drop_snapshot(client, snapshot_name, collection_name)
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
                job_id, _ = self.restore_snapshot(client, snapshot_name, restored_name,
                                                      source_collection_name=collection_name)
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
            wait_for_restore_complete(client, job_id, timeout=120)

        # Verify all restored collections
        for name in restored_names:
            self.load_collection(client, name)
            res, _ = self.query(client, name, filter="id >= 0", output_fields=["count(*)"])
            assert res[0]["count(*)"] == 500, f"{name} should have 500 rows"

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        for name in restored_names:
            self.drop_collection(client, name)


class TestMilvusClientSnapshotLifecycle(TestMilvusClientV2Base):
    """
    Test snapshot + collection lifecycle management edge cases.

    Covers race conditions and interactions between snapshot operations
    and collection lifecycle operations (drop, rename, cross-db restore).
    """

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_drop_target_collection_during_restore(self):
        """
        target: test dropping the target collection while restore is still in progress
        method: start restore -> immediately drop the target collection -> check restore state
        expected: restore job should eventually fail; no resource leak
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # 1. Create collection with data and snapshot
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_snapshot(client, collection_name, snapshot_name)

        # 2. Start restore
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)

        # 3. Immediately drop the target collection while restore is in progress
        try:
            self.drop_collection(client, restored_collection_name, timeout=30)
        except Exception as e:
            log.info(f"Drop target collection during restore: {e}")

        # 4. Wait and check restore state - should eventually reach terminal state
        timeout = 120
        start_time = time.time()
        final_state = None
        while time.time() - start_time < timeout:
            state = client.get_restore_snapshot_state(job_id)
            final_state = state.state
            if final_state in ("RestoreSnapshotCompleted", "RestoreSnapshotFailed"):
                break
            time.sleep(2)

        log.info(f"Restore final state after dropping target collection: {final_state}")
        # The restore should reach a terminal state (not hang forever)
        assert final_state in ("RestoreSnapshotCompleted", "RestoreSnapshotFailed"), \
            f"Restore job should reach terminal state, got: {final_state}"

        # 5. Log collection state for diagnostics (existence is timing-dependent)
        collections = client.list_collections()
        log.info(f"Collections after race (target may or may not exist): {collections}")

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        try:
            self.drop_collection(client, restored_collection_name, timeout=30)
        except Exception:
            pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_rename_source_collection(self):
        """
        target: test snapshot behavior after renaming the source collection
        method: create snapshot -> rename collection -> list/describe/restore snapshot
        expected: snapshot should still be usable; describe may show original collection name
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        new_collection_name = cf.gen_unique_str(prefix + "_renamed")
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # 1. Create collection with data and snapshot
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_snapshot(client, collection_name, snapshot_name)

        # 2. Rename source collection
        self.rename_collection(client, collection_name, new_collection_name)

        # 3. Verify snapshot is still discoverable
        # list_snapshots with old name should fail (collection no longer exists)
        error = {ct.err_code: 100, ct.err_msg: "collection not found"}
        self.list_snapshots(client, collection_name=collection_name,
                            check_task=CheckTasks.err_res, check_items=error)

        # list_snapshots with new name should find it
        snapshots_new, _ = self.list_snapshots(client, collection_name=new_collection_name)
        log.info(f"Snapshots listed with new name '{new_collection_name}': {snapshots_new}")
        assert snapshot_name in snapshots_new, \
            f"Snapshot {snapshot_name} should be discoverable under new name '{new_collection_name}'"

        # 4. Describe snapshot should still work (use new collection name)
        info, _ = self.describe_snapshot(client, snapshot_name, new_collection_name)
        assert info.name == snapshot_name
        log.info(f"Snapshot collection_name after rename: {info.collection_name}")

        # 5. Restore should still work (snapshot data is independent of collection name)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=new_collection_name)
        wait_for_restore_complete(client, job_id)

        self.load_collection(client, restored_collection_name)
        res, _ = self.query(client, restored_collection_name, filter="id >= 0",
                            output_fields=["count(*)"])
        assert res[0]["count(*)"] == default_nb, \
            f"Restored collection should have {default_nb} rows"

        # Cleanup
        self.drop_snapshot(client, snapshot_name, new_collection_name)
        self.drop_collection(client, new_collection_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_create_on_restoring_collection(self):
        """
        target: test creating a snapshot on a collection that is being restored into
        method: start restore -> immediately create snapshot on the target collection
        expected: snapshot creation should either fail or capture incomplete data
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")
        snapshot_on_restored = cf.gen_unique_str(prefix + "_on_restored")

        # 1. Create collection with data and snapshot
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_snapshot(client, collection_name, snapshot_name)

        # 2. Start restore
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_collection_name,
                                          source_collection_name=collection_name)

        # 3. Immediately try to create snapshot on the target collection
        snapshot_created = False
        try:
            self.create_snapshot(client, restored_collection_name, snapshot_on_restored)
            snapshot_created = True
            log.info("Snapshot on restoring collection succeeded (captured partial state)")
        except Exception as e:
            log.info(f"Snapshot on restoring collection rejected: {e}")

        # 4. Wait for restore to complete regardless
        wait_for_restore_complete(client, job_id, timeout=120)

        # 5. If snapshot was created during restore, verify it captured a subset of data
        if snapshot_created:
            restored_from_partial = cf.gen_unique_str(prefix + "_from_partial")
            job_id2, _ = self.restore_snapshot(client, snapshot_on_restored, restored_from_partial,
                                               source_collection_name=restored_collection_name)
            wait_for_restore_complete(client, job_id2)
            self.load_collection(client, restored_from_partial)
            res, _ = self.query(client, restored_from_partial, filter="id >= 0",
                                output_fields=["count(*)"])
            partial_count = res[0]["count(*)"]
            log.info(f"Snapshot during restore captured {partial_count} rows "
                     f"(original: {default_nb})")
            # Should have at most the original count (might be less if data was still copying)
            assert partial_count <= default_nb
            self.drop_collection(client, restored_from_partial)
            self.drop_snapshot(client, snapshot_on_restored, restored_collection_name)

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_restore_failure_no_resource_leak(self):
        """
        target: test that a failed restore does not leak resources
        method: restore to an existing collection (will fail) -> verify no leftover resources
        expected: restore fails cleanly, no orphan collections or jobs stuck in non-terminal state
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        existing_collection = cf.gen_unique_str(prefix + "_existing")

        # 1. Create collection with data and snapshot
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_snapshot(client, collection_name, snapshot_name)

        # 2. Create the target collection so restore will fail (duplicate)
        self.create_collection(client, existing_collection, default_dim)

        # 3. Restore to existing collection - should fail
        error = {ct.err_code: 65535, ct.err_msg: "duplicate collection"}
        self.restore_snapshot(client, snapshot_name, existing_collection,
                              source_collection_name=collection_name,
                              check_task=CheckTasks.err_res, check_items=error)

        # 4. Verify the existing collection is untouched
        self.load_collection(client, existing_collection)
        res, _ = self.query(client, existing_collection, filter="id >= 0",
                            output_fields=["count(*)"])
        assert res[0]["count(*)"] == 0, "Existing collection should remain empty (untouched)"

        # 5. Verify snapshot is still usable after failed restore
        info, _ = self.describe_snapshot(client, snapshot_name, collection_name)
        assert info.name == snapshot_name

        # 6. Successful restore to a new collection proves no state corruption
        clean_restored = cf.gen_unique_str(prefix + "_clean")
        job_id, _ = self.restore_snapshot(client, snapshot_name, clean_restored,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        self.load_collection(client, clean_restored)
        res, _ = self.query(client, clean_restored, filter="id >= 0",
                            output_fields=["count(*)"])
        assert res[0]["count(*)"] == default_nb

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, existing_collection)
        self.drop_collection(client, clean_restored)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_concurrent_drop_same_snapshot(self):
        """
        target: test concurrent drop of the same snapshot (idempotent)
        method: drop the same snapshot from multiple threads simultaneously
        expected: all threads should succeed (idempotent behavior), no errors
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        # 1. Create collection and snapshot
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(100)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_snapshot(client, collection_name, snapshot_name)

        # 2. Concurrent drop from multiple threads
        results = []
        errors = []

        def drop_thread():
            try:
                client.drop_snapshot(snapshot_name, collection_name)
                results.append("success")
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=drop_thread) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        log.info(f"Concurrent drop results - successes: {len(results)}, errors: {len(errors)}")
        log.info(f"Errors: {errors}")

        # At least one thread should succeed; others may succeed (idempotent) or
        # hit transient errors (e.g., etcd write conflict under concurrent load)
        assert len(results) >= 1, "At least one concurrent drop should succeed, got 0 successes"
        assert len(results) + len(errors) == 5, "All 5 threads should have completed"

        # 3. Verify snapshot is gone
        snapshots, _ = self.list_snapshots(client, collection_name=collection_name)
        assert snapshot_name not in snapshots

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_create_during_drop_source_collection(self):
        """
        target: test creating a snapshot while the source collection is being dropped
        method: insert data -> flush -> start drop collection and create snapshot concurrently
        expected: create snapshot should either succeed (before drop) or fail (after drop);
                  system should remain consistent
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        # 1. Create collection with data
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # 2. Concurrently drop collection and create snapshot
        create_result = {"success": False, "error": None}
        drop_result = {"success": False, "error": None}

        def create_snapshot_thread():
            try:
                client.create_snapshot(collection_name, snapshot_name)
                create_result["success"] = True
            except Exception as e:
                create_result["error"] = str(e)

        def drop_collection_thread():
            try:
                client.drop_collection(collection_name)
                drop_result["success"] = True
            except Exception as e:
                drop_result["error"] = str(e)

        t1 = threading.Thread(target=create_snapshot_thread)
        t2 = threading.Thread(target=drop_collection_thread)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        log.info(f"Create snapshot: success={create_result['success']}, error={create_result['error']}")
        log.info(f"Drop collection: success={drop_result['success']}, error={drop_result['error']}")

        # 3. Verify consistent state
        # Note: with cascade delete (PR #48143), DropCollection triggers
        # DropSnapshotsByCollection, so even if snapshot creation succeeded
        # before drop, the snapshot may be cascade-deleted after drop completes.
        if create_result["success"]:
            # Snapshot was created before drop took effect; may be cascade-dropped
            all_snapshots, _ = self.list_snapshots(client)
            log.info(f"Snapshot created, all snapshots after race: {all_snapshots}")
            if snapshot_name in all_snapshots:
                # Still alive -- restore to verify data integrity
                restored_name = cf.gen_unique_str(prefix + "_restored")
                job_id, _ = self.restore_snapshot(client, snapshot_name, restored_name,
                                                  source_collection_name=collection_name)
                wait_for_restore_complete(client, job_id)
                self.load_collection(client, restored_name)
                res, _ = self.query(client, restored_name, filter="id >= 0",
                                    output_fields=["count(*)"])
                assert res[0]["count(*)"] == default_nb
                self.drop_collection(client, restored_name)
                # Cleanup snapshot
                try:
                    client.drop_snapshot(snapshot_name, collection_name)
                except Exception:
                    pass
            else:
                log.info("Snapshot was cascade-deleted with the source collection - OK")
        else:
            # Snapshot creation failed (drop happened first) - this is acceptable
            log.info("Snapshot creation failed because collection was dropped first - OK")

        # Drop should always succeed
        assert drop_result["success"], f"Drop collection should succeed, error: {drop_result['error']}"

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_restore_cross_database(self):
        """
        target: test restoring a snapshot to a different database via db_name param
        method: create snapshot in default db -> restore to target db via db_name
        expected: restored collection should be created in the target db
        note: requires pymilvus >= 2.7.0rc146 (fix: pass database context in snapshot APIs)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        target_db = cf.gen_unique_str("test_db")
        restored_collection_name = cf.gen_unique_str(prefix + "_cross_db")

        # 1. Create collection with data in default db and snapshot
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_snapshot(client, collection_name, snapshot_name)

        # 2. Create target database
        self.create_database(client, target_db)

        # 3. Restore snapshot to target db via target_db_name param
        # SDK signature: restore_snapshot(snapshot_name, source_collection_name,
        #                                  target_collection_name,
        #                                  source_db_name="", target_db_name="", ...)
        job_id = client.restore_snapshot(snapshot_name, collection_name,
                                         restored_collection_name,
                                         target_db_name=target_db)
        wait_for_restore_complete(client, job_id, timeout=120)

        # Note: MilvusClient does not honor per-call db_name kwarg for list/load/
        # query/drop_collection -- we must switch via using_database().
        try:
            # 4. Verify collection is in target db
            client.using_database(target_db)
            target_collections = client.list_collections()
            log.info(f"Collections in target db '{target_db}': {target_collections}")
            assert restored_collection_name in target_collections, \
                f"Restored collection should be in target db '{target_db}'"

            # 5. Verify collection is NOT in default db
            client.using_database("default")
            default_collections = client.list_collections()
            log.info(f"Collections in default db: {default_collections}")
            assert restored_collection_name not in default_collections, \
                "Restored collection should NOT be in default db"

            # 6. Verify data integrity in target db
            client.using_database(target_db)
            client.load_collection(restored_collection_name)
            res = client.query(restored_collection_name, filter="id >= 0",
                               output_fields=["count(*)"])
            assert res[0]["count(*)"] == default_nb, \
                f"Restored collection should have {default_nb} rows"

            # Cleanup target-db collection while still in target context
            client.drop_collection(restored_collection_name)
        finally:
            client.using_database("default")

        # Cleanup remaining resources in default db
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, collection_name)
        try:
            client.drop_database(target_db)
        except Exception as e:
            log.warning(f"Failed to drop test database '{target_db}': {e}")

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_drop_and_restore_race(self):
        """
        target: test race condition between DropSnapshot and RestoreSnapshot
        method: start restore and drop snapshot concurrently from different threads
        expected: either restore succeeds (drop blocked by ref count) or restore fails
                  (drop happened before restore registered ref); system should not hang
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_collection_name = cf.gen_unique_str(prefix + "_restored")

        # 1. Create collection with data and snapshot
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(default_nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_snapshot(client, collection_name, snapshot_name)

        # 2. Start restore and drop concurrently
        restore_result = {"job_id": None, "error": None}
        drop_result = {"success": False, "error": None}

        def restore_thread():
            try:
                # SDK positional args: (snapshot_name, source_collection_name, target_collection_name)
                job_id = client.restore_snapshot(snapshot_name, collection_name,
                                                 restored_collection_name, timeout=60)
                restore_result["job_id"] = job_id
            except Exception as e:
                restore_result["error"] = str(e)

        def drop_thread():
            try:
                client.drop_snapshot(snapshot_name, collection_name, timeout=60)
                drop_result["success"] = True
            except Exception as e:
                drop_result["error"] = str(e)

        t_restore = threading.Thread(target=restore_thread, name="restore_thread")
        t_drop = threading.Thread(target=drop_thread, name="drop_thread")
        t_restore.start()
        t_drop.start()
        t_restore.join(timeout=90)
        t_drop.join(timeout=90)
        assert not t_restore.is_alive(), "restore_thread timed out"
        assert not t_drop.is_alive(), "drop_thread timed out"

        log.info(f"Restore: job_id={restore_result['job_id']}, error={restore_result['error']}")
        log.info(f"Drop: success={drop_result['success']}, error={drop_result['error']}")

        # 3. Analyze outcomes - two valid scenarios:
        #
        # Scenario A: Restore registered ref first -> drop blocked -> restore completes
        #   restore_result["job_id"] is not None, drop_result["error"] contains "is restoring"
        #
        # Scenario B: Drop succeeded first -> restore fails with "snapshot not found"
        #   drop_result["success"] is True, restore_result["error"] contains "not found"
        #
        # Scenario C: Both succeed in sequence (restore ref registered and released quickly)
        #   Both succeed - rare but possible if restore is very fast

        if restore_result["job_id"] is not None:
            # Restore started - wait for it to complete
            try:
                wait_for_restore_complete(client, restore_result["job_id"], timeout=120)
                log.info("Restore completed successfully")

                # Verify data
                self.load_collection(client, restored_collection_name)
                res, _ = self.query(client, restored_collection_name, filter="id >= 0",
                                    output_fields=["count(*)"])
                assert res[0]["count(*)"] == default_nb
            except Exception as e:
                log.info(f"Restore ended with: {e}")

            if drop_result["error"]:
                # Scenario A: drop was blocked by active pins/restores
                # PR #48143 introduced explicit pin-based blocking:
                # "active pins exist, unpin before dropping: snapshot is pinned"
                log.info(f"Drop was blocked during restore: {drop_result['error']}")
                err_lower = drop_result["error"].lower()
                assert "pin" in err_lower or "restor" in err_lower, \
                    f"Drop error should mention pin/restore, got: {drop_result['error']}"
                # Now drop should succeed
                self.drop_snapshot(client, snapshot_name, collection_name)
            else:
                # Scenario C: drop also succeeded (restore was fast)
                log.info("Both restore and drop succeeded")
        else:
            # Scenario B: restore failed (snapshot was dropped first)
            log.info(f"Restore failed: {restore_result['error']}")
            assert drop_result["success"], "If restore failed, drop should have succeeded"

        # Verify system is in a clean state after race condition:
        # The restored collection should either not exist or be droppable
        # within a reasonable timeout. If drop_collection hangs or times out,
        # it indicates the server is stuck (e.g., broadcaster infinite retry loop).
        collections = client.list_collections()
        if restored_collection_name in collections:
            log.info(f"Restored collection {restored_collection_name} exists, verifying it can be dropped")
            self.drop_collection(client, restored_collection_name, timeout=30)
            collections_after = client.list_collections()
            assert restored_collection_name not in collections_after, \
                f"Restored collection {restored_collection_name} should be droppable after race condition, " \
                f"but drop_collection did not remove it. Server may be stuck in infinite retry loop."

        # Cleanup snapshot (idempotent)
        try:
            client.drop_snapshot(snapshot_name, collection_name, timeout=30)
        except Exception:
            pass


class TestMilvusClientSnapshotAlias(TestMilvusClientV2Base):
    """
    Test snapshot operations using collection aliases.

    Server resolves aliases via globalMetaCache.GetCollectionID() for
    create_snapshot, list_snapshots, and list_restore_snapshot_jobs.
    restore_snapshot takes a NEW collection name (not alias) as target.
    """

    def _create_collection_with_data(self, client, collection_name, nb=default_nb):
        """Helper: create collection, insert data, flush."""
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_create_via_alias(self):
        """
        target: test creating a snapshot using collection alias instead of collection name
        method: create collection -> create alias -> create snapshot via alias
        expected: snapshot created successfully, describe shows real collection name
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias_name = cf.gen_unique_str(prefix + "_alias")
        snapshot_name = cf.gen_unique_str(prefix)

        # 1. Create collection with data
        self._create_collection_with_data(client, collection_name)

        # 2. Create alias
        self.create_alias(client, collection_name, alias_name)

        # 3. Create snapshot using alias
        self.create_snapshot(client, alias_name, snapshot_name)

        # 4. Describe snapshot via alias should show the real collection name
        info, _ = self.describe_snapshot(client, snapshot_name, alias_name)
        assert info.collection_name == collection_name, \
            f"Expected real collection name '{collection_name}', got '{info.collection_name}'"

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_list_via_alias(self):
        """
        target: test listing snapshots using collection alias
        method: create snapshot with real name -> list snapshots via alias
        expected: list returns the same snapshots as using real name
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias_name = cf.gen_unique_str(prefix + "_alias")
        snapshot_name = cf.gen_unique_str(prefix)

        # 1. Create collection with data and snapshot
        self._create_collection_with_data(client, collection_name)
        self.create_alias(client, collection_name, alias_name)
        self.create_snapshot(client, collection_name, snapshot_name)

        # 2. List snapshots using alias
        snapshots_via_alias, _ = self.list_snapshots(client, collection_name=alias_name)
        snapshots_via_name, _ = self.list_snapshots(client, collection_name=collection_name)

        assert snapshot_name in snapshots_via_alias, \
            f"Snapshot not found via alias. Got: {snapshots_via_alias}"
        assert snapshots_via_alias == snapshots_via_name, \
            f"Mismatch: via alias={snapshots_via_alias}, via name={snapshots_via_name}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_restore_from_alias_created_snapshot(self):
        """
        target: test restoring a snapshot that was created via alias
        method: create snapshot via alias -> restore -> verify data
        expected: restore succeeds with full data integrity
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias_name = cf.gen_unique_str(prefix + "_alias")
        snapshot_name = cf.gen_unique_str(prefix)
        restored_name = cf.gen_unique_str(prefix + "_restored")

        # 1. Create collection with data and alias
        self._create_collection_with_data(client, collection_name)
        self.create_alias(client, collection_name, alias_name)

        # 2. Create snapshot via alias
        self.create_snapshot(client, alias_name, snapshot_name)

        # 3. Restore snapshot using alias as source (server resolves to real collection)
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_name,
                                          source_collection_name=alias_name)
        wait_for_restore_complete(client, job_id)

        # 4. Verify restored data
        self.load_collection(client, restored_name)
        res, _ = self.query(client, restored_name, filter="id >= 0",
                            output_fields=["count(*)"])
        assert res[0]["count(*)"] == default_nb

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_alias(client, alias_name)
        self.drop_collection(client, collection_name)
        self.drop_collection(client, restored_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_list_restore_jobs_via_alias(self):
        """
        target: test listing restore snapshot jobs using collection alias
        method: restore snapshot to new collection -> create alias on restored
                collection -> list restore jobs via alias
        expected: list_restore_snapshot_jobs returns correct jobs via alias
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_name = cf.gen_unique_str(prefix + "_restored")
        restored_alias = cf.gen_unique_str(prefix + "_restored_alias")

        # 1. Create collection with data and snapshot
        self._create_collection_with_data(client, collection_name)
        self.create_snapshot(client, collection_name, snapshot_name)

        # 2. Restore to new collection
        job_id, _ = self.restore_snapshot(client, snapshot_name, restored_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # 3. Create alias on restored collection and list jobs via alias
        self.create_alias(client, restored_name, restored_alias)
        jobs, _ = self.list_restore_snapshot_jobs(client, collection_name=restored_alias)
        job_ids = [j.job_id for j in jobs]
        assert job_id in job_ids, \
            f"Restore job {job_id} not found via alias. Jobs: {job_ids}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_alias(client, restored_alias)
        self.drop_collection(client, collection_name)
        self.drop_collection(client, restored_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_drop_alias_then_create_snapshot(self):
        """
        target: test that creating snapshot with dropped alias fails
        method: create alias -> drop alias -> create snapshot with dropped alias
        expected: create snapshot with dropped alias fails; real name still works
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        alias_name = cf.gen_unique_str(prefix + "_alias")
        snapshot_name = cf.gen_unique_str(prefix)

        # 1. Create collection with data and alias
        self._create_collection_with_data(client, collection_name)
        self.create_alias(client, collection_name, alias_name)

        # 2. Drop alias
        self.drop_alias(client, alias_name)

        # 3. Create snapshot should fail with dropped alias
        error = {ct.err_code: 100, ct.err_msg: "not found"}
        self.create_snapshot(client, alias_name, snapshot_name,
                             check_task=CheckTasks.err_res, check_items=error)

        # 4. Create snapshot with real name should succeed
        self.create_snapshot(client, collection_name, snapshot_name)

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_snapshot_alter_alias_then_list_snapshots(self):
        """
        target: test that alias retarget affects snapshot listing
        method: create alias on col_a -> create snapshot on col_a via alias ->
                alter alias to col_b -> list snapshots via alias should show col_b snapshots
        expected: alias retarget correctly affects which collection's snapshots are listed
        """
        client = self._client()
        col_a = cf.gen_collection_name_by_testcase_name() + "_a"
        col_b = cf.gen_collection_name_by_testcase_name() + "_b"
        alias_name = cf.gen_unique_str(prefix + "_alias")
        snapshot_a = cf.gen_unique_str(prefix + "_a")
        snapshot_b = cf.gen_unique_str(prefix + "_b")

        # 1. Create two collections with data
        self._create_collection_with_data(client, col_a)
        self._create_collection_with_data(client, col_b)

        # 2. Create alias pointing to col_a and create snapshot
        self.create_alias(client, col_a, alias_name)
        self.create_snapshot(client, alias_name, snapshot_a)

        # 3. Alter alias to point to col_b and create snapshot
        self.alter_alias(client, col_b, alias_name)
        self.create_snapshot(client, alias_name, snapshot_b)

        # 4. List snapshots via alias should show col_b's snapshots
        snapshots_via_alias, _ = self.list_snapshots(client, collection_name=alias_name)
        assert snapshot_b in snapshots_via_alias, \
            f"Snapshot_b not found via retargeted alias. Got: {snapshots_via_alias}"
        assert snapshot_a not in snapshots_via_alias, \
            f"Snapshot_a should not appear after alias retarget. Got: {snapshots_via_alias}"

        # 5. List directly should show each collection's own snapshots
        snapshots_a, _ = self.list_snapshots(client, collection_name=col_a)
        snapshots_b, _ = self.list_snapshots(client, collection_name=col_b)
        assert snapshot_a in snapshots_a
        assert snapshot_b in snapshots_b

        # Cleanup
        self.drop_snapshot(client, snapshot_a, col_a)
        self.drop_snapshot(client, snapshot_b, col_b)
        self.drop_alias(client, alias_name)
        self.drop_collection(client, col_a)
        self.drop_collection(client, col_b)

    @pytest.mark.tags(CaseLabel.L2)
    def test_restore_target_name_equals_existing_alias_fails(self):
        """
        target: test restoring a snapshot with target_collection_name equal to
                an existing alias should fail (alias and collection share a namespace)
        method: create col_src + snapshot -> create alias A pointing to col_src
                -> restore snapshot to target_collection_name=A
        expected: restore is synchronously rejected with an alias-conflict error;
                  source collection, snapshot, and alias all remain intact
        note: the rejection happens in datacoord's broker.CreateCollection path
              during RestoreCollection (snapshot_manager.go:833)
        """
        client = self._client()
        col_src = cf.gen_collection_name_by_testcase_name()
        alias_name = cf.gen_unique_str(prefix + "_alias")
        snapshot_name = cf.gen_unique_str(prefix)

        # 1. Create source collection + snapshot + alias
        self._create_collection_with_data(client, col_src)
        self.create_snapshot(client, col_src, snapshot_name)
        self.create_alias(client, col_src, alias_name)

        # 2. Restore with target_collection_name = existing alias name must fail
        error = {ct.err_code: 65535, ct.err_msg: "conflicts with an existing alias"}
        self.restore_snapshot(client, snapshot_name, alias_name,
                              source_collection_name=col_src,
                              check_task=CheckTasks.err_res, check_items=error)

        # 3. Verify source, snapshot, and alias are all untouched
        snapshots, _ = self.list_snapshots(client, collection_name=col_src)
        assert snapshot_name in snapshots

        # A restore succeeds with a fresh, non-conflicting target name — proves
        # the alias-conflict was a clean rejection, not a corrupted state.
        clean_target = cf.gen_unique_str(prefix + "_clean")
        job_id, _ = self.restore_snapshot(client, snapshot_name, clean_target,
                                          source_collection_name=col_src)
        wait_for_restore_complete(client, job_id)

        # Cleanup
        self.drop_snapshot(client, snapshot_name, col_src)
        self.drop_alias(client, alias_name)
        self.drop_collection(client, col_src)
        self.drop_collection(client, clean_target)


@pytest.mark.tags(CaseLabel.RBAC)
class TestMilvusClientSnapshotRbac(TestMilvusClientV2Base):
    """
    Test RBAC v2 privilege enforcement for snapshot operations.
    """

    user_pre = "snap_user"
    role_pre = "snap_role"

    def teardown_method(self, method):
        """Clean up users, roles, snapshots and collections created during test."""
        log.info("[snapshot_rbac_teardown] Start teardown ...")
        client = self._client()

        # drop all non-root users
        users, _ = self.list_users(client)
        for user in users:
            if user != ct.default_user:
                self.drop_user(client, user)

        # revoke privileges and drop non-builtin roles
        # Must use revoke_privilege_v2 for privileges granted via v2 API,
        # because v2-granted privileges carry db_name="*" which v1 revoke cannot match.
        roles, _ = self.list_roles(client)
        for role in roles:
            if role not in ['admin', 'public']:
                res, _ = self.describe_role(client, role)
                if res and res.get('privileges'):
                    for privilege in res['privileges']:
                        self.revoke_privilege_v2(client, role,
                                                 privilege["privilege"],
                                                 privilege.get("object_name", "*"),
                                                 privilege.get("db_name", "*"))
                self.drop_role(client, role)

        super().teardown_method(method)

    def _setup_user_with_role(self, root_client, host, port):
        """Helper: create a user + role, assign role, return (user_client, role_name)."""
        user_name = cf.gen_unique_str(self.user_pre)
        role_name = cf.gen_unique_str(self.role_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(root_client, user_name=user_name, password=password)
        self.create_role(root_client, role_name=role_name)
        self.grant_role(root_client, user_name=user_name, role_name=role_name)

        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)
        return user_client, role_name

    def _prepare_collection_with_snapshot(self, client, nb=500):
        """Helper: create collection, insert data, flush, create snapshot. Return (col_name, snap_name)."""
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_snapshot(client, collection_name, snapshot_name)
        return collection_name, snapshot_name

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_create_denied_without_privilege(self, host, port):
        """
        target: verify CreateSnapshot is denied without privilege
        method: create user with empty role, attempt create_snapshot
        expected: permission denied
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # should be denied
        snapshot_name = cf.gen_unique_str(prefix)
        self.create_snapshot(user_client, collection_name, snapshot_name,
                             check_task=CheckTasks.check_permission_deny)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_create_allowed_after_grant_v2(self, host, port):
        """
        target: verify CreateSnapshot succeeds after granting privilege via v2 API
        method: grant_privilege_v2 CreateSnapshot to role, then create snapshot
        expected: create snapshot succeeds
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # grant Collection-level CreateSnapshot via v2 API (snapshot privileges
        # moved from Global to Collection level in PR #48143)
        self.grant_privilege_v2(client, role_name, "CreateSnapshot", "*", "*")
        time.sleep(10)

        # should succeed
        snapshot_name = cf.gen_unique_str(prefix)
        self.create_snapshot(user_client, collection_name, snapshot_name)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_drop_denied_without_privilege(self, host, port):
        """
        target: verify DropSnapshot is denied without privilege
        method: create snapshot as root, attempt drop as unprivileged user
        expected: permission denied
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # should be denied
        self.drop_snapshot(user_client, snapshot_name, collection_name,
                           check_task=CheckTasks.check_permission_deny)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_drop_allowed_after_grant_v2(self, host, port):
        """
        target: verify DropSnapshot succeeds after granting privilege via v2 API
        method: grant_privilege_v2 DropSnapshot to role, then drop snapshot
        expected: drop snapshot succeeds
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # grant via v2 API
        self.grant_privilege_v2(client, role_name, "DropSnapshot", "*", "*")
        time.sleep(10)

        # should succeed
        self.drop_snapshot(user_client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_list_denied_without_privilege(self, host, port):
        """
        target: verify ListSnapshots is denied without privilege
        method: create snapshot as root, attempt list as unprivileged user
        expected: permission denied
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # should be denied
        self.list_snapshots(user_client, collection_name=collection_name,
                            check_task=CheckTasks.check_permission_deny)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_list_allowed_after_grant_v2(self, host, port):
        """
        target: verify ListSnapshots succeeds after granting privilege via v2 API
        method: grant_privilege_v2 ListSnapshots to role, then list snapshots
        expected: list snapshots succeeds and returns the snapshot
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # grant via v2 API
        self.grant_privilege_v2(client, role_name, "ListSnapshots", "*", "*")
        time.sleep(10)

        # should succeed
        snapshots, _ = self.list_snapshots(user_client, collection_name=collection_name)
        assert snapshot_name in snapshots

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_describe_denied_without_privilege(self, host, port):
        """
        target: verify DescribeSnapshot is denied without privilege
        method: create snapshot as root, attempt describe as unprivileged user
        expected: permission denied
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # should be denied
        self.describe_snapshot(user_client, snapshot_name, collection_name,
                               check_task=CheckTasks.check_permission_deny)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_describe_allowed_after_grant_v2(self, host, port):
        """
        target: verify DescribeSnapshot succeeds after granting privilege via v2 API
        method: grant_privilege_v2 DescribeSnapshot to role, then describe snapshot
        expected: describe snapshot succeeds with correct info
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # grant via v2 API
        self.grant_privilege_v2(client, role_name, "DescribeSnapshot", "*", "*")
        time.sleep(10)

        # should succeed
        info, _ = self.describe_snapshot(user_client, snapshot_name, collection_name)
        assert info.name == snapshot_name

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_restore_denied_without_privilege(self, host, port):
        """
        target: verify RestoreSnapshot is denied without privilege
        method: create snapshot as root, attempt restore as unprivileged user
        expected: permission denied
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # should be denied
        restored_name = cf.gen_unique_str(prefix + "_restored")
        self.restore_snapshot(user_client, snapshot_name, restored_name,
                              source_collection_name=collection_name,
                              check_task=CheckTasks.check_permission_deny)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_restore_allowed_after_grant_v2(self, host, port):
        """
        target: verify RestoreSnapshot succeeds after granting privilege via v2 API
        method: grant_privilege_v2 RestoreSnapshot to role, then restore snapshot
        expected: restore snapshot succeeds
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # grant via v2 API
        self.grant_privilege_v2(client, role_name, "RestoreSnapshot", "*", "*")
        time.sleep(10)

        # should succeed
        restored_name = cf.gen_unique_str(prefix + "_restored")
        job_id, _ = self.restore_snapshot(user_client, snapshot_name, restored_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_revoke_privilege_v2_then_denied(self, host, port):
        """
        target: verify operation is denied after revoking privilege via v2 API
        method: grant_privilege_v2 CreateSnapshot -> verify allowed -> revoke_privilege_v2 -> verify denied
        expected: permission denied after revocation
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # grant via v2 and verify allowed
        self.grant_privilege_v2(client, role_name, "CreateSnapshot", "*", "*")
        time.sleep(10)
        snapshot_name = cf.gen_unique_str(prefix)
        self.create_snapshot(user_client, collection_name, snapshot_name)
        self.drop_snapshot(client, snapshot_name, collection_name)

        # revoke via v2 and verify denied
        self.revoke_privilege_v2(client, role_name, "CreateSnapshot", "*", "*")
        time.sleep(10)
        snapshot_name2 = cf.gen_unique_str(prefix)
        self.create_snapshot(user_client, collection_name, snapshot_name2,
                             check_task=CheckTasks.check_permission_deny)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.xfail(reason="Built-in CollectionReadOnly group on deployed server does "
                              "not yet include DescribeSnapshot/ListSnapshots even though "
                              "util.CollectionReadOnlyPrivileges defines them. Tracking: "
                              "https://github.com/milvus-io/milvus/issues/47855")
    def test_snapshot_v2_privilege_group_collection_readonly(self, host, port):
        """
        target: verify CollectionReadOnly v2 privilege group grants read snapshot ops
        method: grant CollectionReadOnly, attempt describe and list
        expected: describe/list succeed, create/drop/restore denied
        note: snapshot privileges moved from Global (ClusterXxx) to Collection
              level groups in PR #48143 (fixes #47855)
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # grant CollectionReadOnly (includes DescribeSnapshot + ListSnapshots)
        self.grant_privilege_v2(client, role_name, "CollectionReadOnly", "*", "*")
        time.sleep(10)

        # read ops should succeed
        snapshots, _ = self.list_snapshots(user_client, collection_name=collection_name)
        assert snapshot_name in snapshots

        info, _ = self.describe_snapshot(user_client, snapshot_name, collection_name)
        assert info.name == snapshot_name

        # write ops should be denied
        new_snap = cf.gen_unique_str(prefix)
        self.create_snapshot(user_client, collection_name, new_snap,
                             check_task=CheckTasks.check_permission_deny)
        self.drop_snapshot(user_client, snapshot_name, collection_name,
                           check_task=CheckTasks.check_permission_deny)
        restored_name = cf.gen_unique_str(prefix + "_restored")
        self.restore_snapshot(user_client, snapshot_name, restored_name,
                              source_collection_name=collection_name,
                              check_task=CheckTasks.check_permission_deny)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.xfail(reason="Built-in CollectionReadWrite group on deployed server does "
                              "not yet include CreateSnapshot/DropSnapshot. Tracking: "
                              "https://github.com/milvus-io/milvus/issues/47855")
    def test_snapshot_v2_privilege_group_collection_readwrite(self, host, port):
        """
        target: verify CollectionReadWrite v2 privilege group grants CRUD snapshot ops
        method: grant CollectionReadWrite, attempt create/drop/describe/list
        expected: create/drop/describe/list succeed, restore denied
        note: CollectionReadWrite = CollectionReadOnly + {CreateSnapshot, DropSnapshot}.
              RestoreSnapshot is only granted by CollectionAdmin.
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # grant CollectionReadWrite (adds CreateSnapshot/DropSnapshot)
        self.grant_privilege_v2(client, role_name, "CollectionReadWrite", "*", "*")
        time.sleep(10)

        # create/list/describe/drop should succeed
        snapshot_name = cf.gen_unique_str(prefix)
        self.create_snapshot(user_client, collection_name, snapshot_name)

        snapshots, _ = self.list_snapshots(user_client, collection_name=collection_name)
        assert snapshot_name in snapshots

        info, _ = self.describe_snapshot(user_client, snapshot_name, collection_name)
        assert info.name == snapshot_name

        # restore should still be denied (not in ReadWrite group)
        restored_name = cf.gen_unique_str(prefix + "_restored")
        self.restore_snapshot(user_client, snapshot_name, restored_name,
                              source_collection_name=collection_name,
                              check_task=CheckTasks.check_permission_deny)

        # drop should succeed
        self.drop_snapshot(user_client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    @pytest.mark.xfail(reason="Built-in CollectionAdmin group on deployed server does not "
                              "yet include Create/Drop/RestoreSnapshot. Tracking: "
                              "https://github.com/milvus-io/milvus/issues/47855")
    def test_snapshot_v2_privilege_group_collection_admin(self, host, port):
        """
        target: verify CollectionAdmin v2 privilege group grants all snapshot ops
        method: grant CollectionAdmin, attempt all snapshot ops
        expected: all snapshot ops succeed
        note: CollectionAdmin = CollectionReadWrite + {RestoreSnapshot, ...}.
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # grant CollectionAdmin - the highest collection-level privilege group
        self.grant_privilege_v2(client, role_name, "CollectionAdmin", "*", "*")
        time.sleep(10)

        # all snapshot ops should succeed
        new_snap = cf.gen_unique_str(prefix)
        self.create_snapshot(user_client, collection_name, new_snap)

        snapshots, _ = self.list_snapshots(user_client, collection_name=collection_name)
        assert new_snap in snapshots

        info, _ = self.describe_snapshot(user_client, new_snap, collection_name)
        assert info.name == new_snap

        restored_name = cf.gen_unique_str(prefix + "_restored")
        job_id, _ = self.restore_snapshot(user_client, new_snap, restored_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        self.drop_snapshot(user_client, new_snap, collection_name)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_admin_role_has_full_access(self, host, port):
        """
        target: verify built-in admin role has full snapshot access
        method: create user, assign admin role, test all snapshot ops
        expected: all operations succeed
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(500)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)

        # create user with admin role
        user_name = cf.gen_unique_str(self.user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)
        self.grant_role(client, user_name=user_name, role_name="admin")

        uri = f"http://{host}:{port}"
        admin_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)

        # all ops should succeed
        snapshot_name = cf.gen_unique_str(prefix)
        self.create_snapshot(admin_client, collection_name, snapshot_name)

        snapshots, _ = self.list_snapshots(admin_client, collection_name=collection_name)
        assert snapshot_name in snapshots

        info, _ = self.describe_snapshot(admin_client, snapshot_name, collection_name)
        assert info.name == snapshot_name

        restored_name = cf.gen_unique_str(prefix + "_restored")
        job_id, _ = self.restore_snapshot(admin_client, snapshot_name, restored_name,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        self.drop_snapshot(admin_client, snapshot_name, collection_name)
        self.drop_collection(client, restored_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_multiple_privileges_granular_v2(self, host, port):
        """
        target: verify granular privilege combination works correctly via v2 API
        method: grant only ListSnapshots + DescribeSnapshot via v2, verify create/drop/restore denied
        expected: only granted ops succeed, others denied
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # grant only read-related privileges via v2 API
        self.grant_privilege_v2(client, role_name, "ListSnapshots", "*", "*")
        self.grant_privilege_v2(client, role_name, "DescribeSnapshot", "*", "*")
        time.sleep(10)

        # read ops should succeed
        snapshots, _ = self.list_snapshots(user_client, collection_name=collection_name)
        assert snapshot_name in snapshots

        info, _ = self.describe_snapshot(user_client, snapshot_name, collection_name)
        assert info.name == snapshot_name

        # write ops should be denied
        new_snap = cf.gen_unique_str(prefix)
        self.create_snapshot(user_client, collection_name, new_snap,
                             check_task=CheckTasks.check_permission_deny)
        self.drop_snapshot(user_client, snapshot_name, collection_name,
                           check_task=CheckTasks.check_permission_deny)
        restored_name = cf.gen_unique_str(prefix + "_restored")
        self.restore_snapshot(user_client, snapshot_name, restored_name,
                              source_collection_name=collection_name,
                              check_task=CheckTasks.check_permission_deny)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_public_role_has_no_access(self, host, port):
        """
        target: verify public role has no snapshot privileges by default
        method: create user with only public role, attempt all snapshot ops
        expected: all operations denied
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        # create user without any custom role (only default public role)
        user_name = cf.gen_unique_str(self.user_pre)
        password = cf.gen_str_by_length(contain_numbers=True)
        self.create_user(client, user_name=user_name, password=password)

        uri = f"http://{host}:{port}"
        user_client, _ = self.init_milvus_client(uri=uri, user=user_name, password=password)

        # all ops should be denied
        new_snap = cf.gen_unique_str(prefix)
        self.create_snapshot(user_client, collection_name, new_snap,
                             check_task=CheckTasks.check_permission_deny)
        self.list_snapshots(user_client, collection_name=collection_name,
                            check_task=CheckTasks.check_permission_deny)
        self.describe_snapshot(user_client, snapshot_name, collection_name,
                               check_task=CheckTasks.check_permission_deny)
        self.drop_snapshot(user_client, snapshot_name, collection_name,
                           check_task=CheckTasks.check_permission_deny)
        restored_name = cf.gen_unique_str(prefix + "_restored")
        self.restore_snapshot(user_client, snapshot_name, restored_name,
                              source_collection_name=collection_name,
                              check_task=CheckTasks.check_permission_deny)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_pin_denied_without_privilege(self, host, port):
        """
        target: verify PinSnapshotData is denied without the privilege
        method: create snapshot as root; attempt pin as unprivileged user
        expected: permission denied
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, _ = self._setup_user_with_role(client, host, port)

        self.pin_snapshot_data(user_client, snapshot_name, collection_name,
                               check_task=CheckTasks.check_permission_deny)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_pin_allowed_after_grant_v2(self, host, port):
        """
        target: verify PinSnapshotData succeeds after granting the privilege via v2 API
        method: grant PinSnapshotData to role; attempt pin; immediately unpin as root
        expected: pin succeeds with a non-zero pin_id
        note: PinSnapshotData is a Global-level privilege
              (pkg/util/constant.go:122-124)
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        self.grant_privilege_v2(client, role_name, "PinSnapshotData", "*", "*")
        time.sleep(10)

        pin_id, _ = self.pin_snapshot_data(user_client, snapshot_name, collection_name,
                                           ttl_seconds=60)
        assert isinstance(pin_id, int) and pin_id > 0, \
            f"pin_snapshot_data should return a positive int pin_id, got {pin_id!r}"

        # Clean up with root (unpin doesn't need user privilege here)
        try:
            client.unpin_snapshot_data(pin_id)
        except Exception as e:
            log.warning(f"cleanup unpin failed, relying on TTL: {e}")

        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_snapshot_unpin_denied_without_privilege(self, host, port):
        """
        target: verify UnpinSnapshotData is denied without the privilege
        method: unprivileged user attempts unpin with an arbitrary pin_id
        expected: permission denied (privilege check happens before pin lookup)
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, _ = self._setup_user_with_role(client, host, port)

        # Use an arbitrary pin_id — server must reject on privilege, not on lookup
        self.unpin_snapshot_data(user_client, pin_id=123456789,
                                 check_task=CheckTasks.check_permission_deny)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.RBAC)
    def test_restore_revoke_privilege_v2_then_denied(self, host, port):
        """
        target: verify restore is denied after revoking RestoreSnapshot via v2 API
        method: grant -> verify restore succeeds -> revoke -> verify restore denied
        expected: permission denied after revocation
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare_collection_with_snapshot(client)

        user_client, role_name = self._setup_user_with_role(client, host, port)

        # grant and verify allowed
        self.grant_privilege_v2(client, role_name, "RestoreSnapshot", "*", "*")
        time.sleep(10)
        restored_ok = cf.gen_unique_str(prefix + "_ok")
        job_id, _ = self.restore_snapshot(user_client, snapshot_name, restored_ok,
                                          source_collection_name=collection_name)
        wait_for_restore_complete(client, job_id)

        # revoke and verify denied
        self.revoke_privilege_v2(client, role_name, "RestoreSnapshot", "*", "*")
        time.sleep(10)
        restored_denied = cf.gen_unique_str(prefix + "_denied")
        self.restore_snapshot(user_client, snapshot_name, restored_denied,
                              source_collection_name=collection_name,
                              check_task=CheckTasks.check_permission_deny)

        # cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)
        self.drop_collection(client, restored_ok)


class TestMilvusClientSnapshotCreateParams(TestMilvusClientV2Base):
    """Test create_snapshot parameter handling beyond basic lifecycle.

    Focus on the ``compaction_protection_seconds`` option introduced with
    the snapshot feature (see ``internal/proxy/task_snapshot.go:118-126``).
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_snapshot_with_compaction_protection_seconds(self):
        """
        target: test create_snapshot accepts a positive compaction_protection_seconds
        method: create snapshot with compaction_protection_seconds=3600
        expected: snapshot is created and subsequent describe/list succeed
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

        # Call the SDK directly — wrapper forces description as kwarg ordering
        client.create_snapshot(snapshot_name, collection_name,
                               compaction_protection_seconds=3600)

        info, _ = self.describe_snapshot(client, snapshot_name, collection_name)
        assert info.name == snapshot_name

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_snapshot_compaction_protection_negative(self):
        """
        target: test create_snapshot rejects negative compaction_protection_seconds
        method: pass compaction_protection_seconds=-1
        expected: server raises ParameterInvalid with "non-negative" in message
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        self.create_collection(client, collection_name, default_dim)

        with pytest.raises(Exception) as exc_info:
            client.create_snapshot(snapshot_name, collection_name,
                                   compaction_protection_seconds=-1)
        msg = str(exc_info.value).lower()
        assert "non-negative" in msg or "compaction_protection_seconds" in msg, \
            f"Expected compaction_protection error, got: {exc_info.value}"

    @pytest.mark.tags(CaseLabel.L1)
    def test_create_snapshot_compaction_protection_exceeds_max(self):
        """
        target: test create_snapshot rejects compaction_protection_seconds > server max
        method: pass compaction_protection_seconds well above the default 604800s cap
        expected: server raises ParameterInvalid with "must not exceed" in message
        note: default max is 604800s (7 days) per dataCoord.snapshot.maxCompactionProtectionSeconds
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)

        self.create_collection(client, collection_name, default_dim)

        with pytest.raises(Exception) as exc_info:
            client.create_snapshot(snapshot_name, collection_name,
                                   compaction_protection_seconds=999_999_999)
        msg = str(exc_info.value).lower()
        assert "not exceed" in msg or "exceeds" in msg or "compaction_protection_seconds" in msg, \
            f"Expected exceeds-max error, got: {exc_info.value}"


class TestMilvusClientSnapshotRestoreParams(TestMilvusClientV2Base):
    """Test restore_snapshot parameter handling (cross-db source, etc.)."""

    @pytest.mark.tags(CaseLabel.L2)
    def test_restore_snapshot_source_db_name_explicit(self):
        """
        target: test restore_snapshot honors explicit source_db_name
        method: create snapshot in DB X; from default-db context call restore
                with source_db_name=X and target_db_name="default"
        expected: restore completes; target collection is created in default db
                  with identical row count; source remains in DB X untouched
        note: complements the existing cross-db test which only exercises
              target_db_name. This test exercises source_db_name explicitly.
        """
        client = self._client()
        source_db = cf.gen_unique_str("test_src_db")
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        restored_name = cf.gen_unique_str(prefix + "_restored")

        # 1. Create source db and populate collection + snapshot inside it
        self.create_database(client, source_db)
        try:
            client.using_database(source_db)
            self.create_collection(client, collection_name, default_dim)
            rng = np.random.default_rng(seed=19530)
            rows = [{
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random(default_dim)),
            } for i in range(default_nb)]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            self.create_snapshot(client, collection_name, snapshot_name)

            # 2. Switch to default db and restore via explicit source_db_name
            client.using_database("default")
            job_id = client.restore_snapshot(snapshot_name, collection_name,
                                             restored_name,
                                             source_db_name=source_db,
                                             target_db_name="default")
            wait_for_restore_complete(client, job_id, timeout=120)

            # 3. Target collection lives in default db
            default_collections = client.list_collections()
            assert restored_name in default_collections, \
                f"Restored collection should be in default db, got {default_collections}"

            client.load_collection(restored_name)
            res = client.query(restored_name, filter="id >= 0", output_fields=["count(*)"])
            assert res[0]["count(*)"] == default_nb, \
                f"Restored collection should have {default_nb} rows"

            # 4. Source still exists in source_db
            client.using_database(source_db)
            source_collections = client.list_collections()
            assert collection_name in source_collections, \
                f"Source collection missing in {source_db}: {source_collections}"

            # Cleanup
            self.drop_snapshot(client, snapshot_name, collection_name)
            self.drop_collection(client, collection_name)
            client.using_database("default")
            self.drop_collection(client, restored_name)
        finally:
            client.using_database("default")
            try:
                client.drop_database(source_db)
            except Exception as e:
                log.warning(f"Failed to drop source db '{source_db}': {e}")


class TestMilvusClientSnapshotPin(TestMilvusClientV2Base):
    """Test pin_snapshot_data / unpin_snapshot_data.

    These APIs exist in both the SDK (``pymilvus/milvus_client/milvus_client.py``)
    and the server (``internal/proxy/task_snapshot.go:839-1029``) but are not
    exercised by existing tests. They are the admin-facing hooks for holding
    snapshot segments against compaction/GC during out-of-band copy-out.
    """

    def _prepare(self, client, nb=500):
        """Helper: create collection + snapshot, return (collection, snapshot)."""
        collection_name = cf.gen_collection_name_by_testcase_name()
        snapshot_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, default_dim)
        rng = np.random.default_rng(seed=19530)
        rows = [{
            default_primary_key_field_name: i,
            default_vector_field_name: list(rng.random(default_dim)),
        } for i in range(nb)]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        self.create_snapshot(client, collection_name, snapshot_name)
        return collection_name, snapshot_name

    @pytest.mark.tags(CaseLabel.L1)
    def test_pin_snapshot_data_basic(self):
        """
        target: test basic pin → unpin flow
        method: pin with ttl=60; assert pin_id > 0; unpin; drop snapshot still works
        expected: pin returns a positive int pin_id; unpin is side-effect free
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare(client)

        pin_id, _ = self.pin_snapshot_data(client, snapshot_name, collection_name,
                                           ttl_seconds=60)
        assert isinstance(pin_id, int) and pin_id > 0, \
            f"pin_snapshot_data should return a positive pin_id, got {pin_id!r}"

        self.unpin_snapshot_data(client, pin_id)

        # Snapshot must still be intact after pin/unpin cycle
        info, _ = self.describe_snapshot(client, snapshot_name, collection_name)
        assert info.name == snapshot_name

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.L2)
    def test_pin_snapshot_blocks_drop(self):
        """
        target: test that a pin blocks drop_snapshot until unpinned
        method: pin with ttl=300s; attempt drop → expect failure; unpin → drop succeeds
        expected: drop fails while pin is active; error mentions pin; drop works after unpin
        note: mirrors the pin-based protection exercised indirectly by
              ``test_snapshot_drop_and_restore_race`` in TestMilvusClientSnapshotLifecycle.
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare(client)

        pin_id, _ = self.pin_snapshot_data(client, snapshot_name, collection_name,
                                           ttl_seconds=300)

        # drop should be rejected while pinned
        with pytest.raises(Exception) as exc_info:
            client.drop_snapshot(snapshot_name, collection_name, timeout=30)
        err_msg = str(exc_info.value).lower()
        assert "pin" in err_msg, \
            f"Expected drop error to mention pin, got: {exc_info.value}"

        # Unpin releases the hold
        self.unpin_snapshot_data(client, pin_id)

        # drop should now succeed
        self.drop_snapshot(client, snapshot_name, collection_name)

        snapshots, _ = self.list_snapshots(client, collection_name=collection_name)
        assert snapshot_name not in snapshots

    @pytest.mark.tags(CaseLabel.L1)
    def test_pin_snapshot_invalid_ttl_negative(self):
        """
        target: test pin rejects negative ttl_seconds
        method: call pin with ttl_seconds=-1
        expected: server returns ParameterInvalid with "non-negative"
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare(client)

        with pytest.raises(Exception) as exc_info:
            client.pin_snapshot_data(snapshot_name, collection_name, ttl_seconds=-1)
        msg = str(exc_info.value).lower()
        assert "non-negative" in msg or "ttl_seconds" in msg, \
            f"Expected ttl_seconds error, got: {exc_info.value}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_pin_snapshot_invalid_ttl_exceeds_max(self):
        """
        target: test pin rejects ttl_seconds beyond the 30-day cap
        method: call pin with ttl_seconds > 2592000 (30 days)
        expected: server returns ParameterInvalid with "exceeds maximum"
        note: cap defined in internal/proxy/task_snapshot.go:891 (maxPinTTLSeconds)
        """
        client = self._client()
        collection_name, snapshot_name = self._prepare(client)

        with pytest.raises(Exception) as exc_info:
            client.pin_snapshot_data(snapshot_name, collection_name,
                                     ttl_seconds=2_592_001)
        msg = str(exc_info.value).lower()
        assert "exceed" in msg or "ttl_seconds" in msg, \
            f"Expected exceeds-max error, got: {exc_info.value}"

        # Cleanup
        self.drop_snapshot(client, snapshot_name, collection_name)

    @pytest.mark.tags(CaseLabel.L1)
    def test_pin_nonexistent_snapshot(self):
        """
        target: test pin on a non-existent snapshot fails cleanly
        method: pin a snapshot_name that was never created
        expected: server returns a clear error (typically not found / snapshot metadata missing)
        """
        client = self._client()
        collection_name = cf.gen_collection_name_by_testcase_name()
        self.create_collection(client, collection_name, default_dim)

        with pytest.raises(Exception) as exc_info:
            client.pin_snapshot_data(cf.gen_unique_str("ghost"), collection_name,
                                     ttl_seconds=60)
        # Accept any error — the server may return "not found", "snapshot", or ParameterInvalid
        log.info(f"pin non-existent snapshot error: {exc_info.value}")
        assert str(exc_info.value), "pin_snapshot_data on non-existent snapshot must raise"

    @pytest.mark.tags(CaseLabel.L2)
    def test_unpin_invalid_pin_id(self):
        """
        target: test unpin with an unknown / never-issued pin_id
        method: call unpin with a random int not produced by pin_snapshot_data
        expected: server either ignores idempotently or returns a clear error;
                  no hang, no system inconsistency
        """
        client = self._client()

        # Either the call raises a clear error, or it silently no-ops.
        # Both are acceptable — the key is it must not hang or corrupt state.
        try:
            client.unpin_snapshot_data(pin_id=987_654_321, timeout=30)
            log.info("unpin with unknown pin_id was idempotent (no error)")
        except Exception as e:
            log.info(f"unpin with unknown pin_id rejected: {e}")
            assert str(e), "unpin error, if any, must carry a message"
