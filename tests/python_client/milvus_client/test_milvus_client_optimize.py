"""
Optimize API Test Cases

The optimize() method is a high-level sugar API wrapping force merge compaction.
It performs: wait for indexes -> force merge compaction -> wait for compaction ->
wait for index rebuild -> refresh load (if loaded).

L3 tests require Milvus configuration changes:
    dataCoord:
      segment:
        maxSize: 64  # MB, default is 1024
      compaction:
        enableAutoCompaction: false
"""

import time

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.constants import *  # noqa: F403
from pymilvus import DataType
from utils.util_log import test_log as log
from utils.util_pymilvus import *  # noqa: F403

prefix = "client_optimize"
epsilon = ct.epsilon
default_nb = ct.default_nb
default_nb_medium = ct.default_nb_medium
default_nq = ct.default_nq
default_dim = ct.default_dim
default_limit = ct.default_limit
default_search_exp = "id >= 0"
exp_res = "exp_res"
default_search_field = ct.default_float_vec_field_name
default_search_params = ct.default_search_params
default_primary_key_field_name = "id"
default_vector_field_name = "vector"
default_float_field_name = ct.default_float_field_name
default_string_field_name = ct.default_string_field_name


class TestMilvusClientOptimizeInvalid(TestMilvusClientV2Base):
    """Test cases for optimize() with invalid parameters"""

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target_size", ["abc", "1XB", "MB100", "1.2.3GB", "--1GB"])
    def test_optimize_invalid_target_size_format(self, target_size):
        """
        target: test optimize with invalid target_size string format
        method: create collection, call optimize with malformed target_size
        expected: Raise ParamError from client-side parse_target_size
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1, ct.err_msg: "Invalid"}
        self.optimize(
            client,
            collection_name,
            target_size=target_size,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target_size", ["0MB", "0GB", "0B", "100B", "500KB"])
    def test_optimize_target_size_too_small(self, target_size):
        """
        target: test optimize with target_size that resolves to less than 1MB
        method: create collection, call optimize with tiny target_size
        expected: Raise ParamError (target size too small)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1, ct.err_msg: "target size too small"}
        self.optimize(
            client,
            collection_name,
            target_size=target_size,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_optimize_nonexistent_collection(self):
        """
        target: test optimize on a non-existent collection
        method: call optimize on a collection that doesn't exist
        expected: Raise exception (collection not found)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 0, ct.err_msg: "can't find collection"}
        self.optimize(
            client,
            collection_name,
            target_size="1GB",
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_optimize_empty_collection_name(self):
        """
        target: test optimize with empty collection_name
        method: call optimize with empty string
        expected: Raise exception
        """
        client = self._client()
        error = {ct.err_code: 1, ct.err_msg: "collection_name"}
        self.optimize(
            client,
            "",
            target_size="1GB",
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_optimize_invalid_collection_name(self):
        """
        target: test optimize with invalid collection_name (blank space)
        method: call optimize with whitespace collection_name
        expected: Raise exception (invalid collection name)
        """
        client = self._client()
        error = {ct.err_code: 1100, ct.err_msg: "Invalid collection name"}
        self.optimize(
            client,
            " ",
            target_size="1GB",
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target_size", [[], {}, (1, 2)])
    def test_optimize_invalid_target_size_type(self, target_size):
        """
        target: test optimize with invalid target_size type
        method: call optimize with non-string/non-numeric target_size
        expected: Raise ParamError
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1, ct.err_msg: "target_size must be a string or number"}
        self.optimize(
            client,
            collection_name,
            target_size=target_size,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_optimize_oversized_target_size(self):
        """
        target: test the first target_size above the signed-int64-MB maximum
        method: call optimize with 9223372036854775808MB
        expected: Client-side parsing rejects the value before submission
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, default_dim)
        error = {ct.err_code: 1, ct.err_msg: "target size too large"}
        self.optimize(
            client,
            collection_name,
            target_size="9223372036854775808MB",
            check_task=CheckTasks.err_res,
            check_items=error,
        )


class TestMilvusClientOptimizeValid(TestMilvusClientV2Base):
    """Test cases for optimize() with valid parameters"""

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    def test_optimize_empty_collection(self):
        """
        target: test optimize on an empty collection
        method: create collection, call optimize with wait=True
        expected: Returns OptimizeResult with status="success"
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, default_dim)
        result = self.optimize(client, collection_name, target_size="1GB")[0]
        assert result.status == "success"
        assert result.collection_name == collection_name
        # Empty collection may return compaction_id=-1 (no segments to compact)
        assert isinstance(result.compaction_id, int)
        log.info(f"Optimize on empty collection completed: {result}")

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize(
        "target_size",
        [
            "1073741824B",
            "1048576 KB",
            "1024MB",
            "1GB",
            "1.5 gB",
            " 1 gb ",
            "1TB",
            "1PB",
        ],
        ids=["B", "KB", "MB", "GB", "decimal-mixed-case", "whitespace", "TB", "PB"],
    )
    def test_optimize_valid_target_size_formats(self, target_size):
        """
        target: test all supported units plus decimal, case, and whitespace handling
        method: optimize an empty collection with each valid representation
        expected: Each representation is accepted and preserved in OptimizeResult
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        self.create_collection(client, collection_name, default_dim)

        result = self.optimize(client, collection_name, target_size=target_size)[0]

        assert result.status == "success"
        assert result.collection_name == collection_name
        assert result.target_size == target_size

    @pytest.mark.tags(CaseLabel.L1)
    def test_optimize_max_target_size(self):
        """
        target: test the maximum accepted signed-int64-MB target_size
        method: optimize an empty collection with 9223372036854775807MB
        expected: The boundary value is accepted without overflow
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        target_size = "9223372036854775807MB"
        self.create_collection(client, collection_name, default_dim)

        result = self.optimize(client, collection_name, target_size=target_size)[0]

        assert result.status == "success"
        assert result.target_size == target_size

    @pytest.mark.tags(CaseLabel.L3)
    def test_optimize_default_target_size(self):
        """
        target: test optimize with default target_size (None)
        method: create collection, insert data, flush, optimize without target_size
        expected: Compaction completes successfully with auto-calculated size
        note: L3 - requires config change (segment.maxSize=64MB)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        self.create_collection(client, collection_name, dim)
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
            }
            for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        result = self.optimize(client, collection_name, timeout=300)[0]
        assert result.status == "success"
        assert result.collection_name == collection_name
        assert result.compaction_id > 0
        log.info(f"Optimize with default target_size completed: {result}")

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize("target_size", ["512MB", "1GB", "2GB"])
    def test_optimize_explicit_target_size(self, target_size):
        """
        target: test optimize with various explicit target_size strings
        method: create collection, insert data, flush, optimize with target_size
        expected: Compaction completes successfully
        note: L3 - requires config change (segment.maxSize=64MB)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        self.create_collection(client, collection_name, dim)
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
            }
            for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        result = self.optimize(client, collection_name, target_size=target_size, timeout=300)[0]
        assert result.status == "success"
        assert result.collection_name == collection_name
        assert result.target_size == target_size
        log.info(f"Optimize with target_size={target_size} completed: {result}")

    @pytest.mark.tags(CaseLabel.L3)
    def test_optimize_result_fields(self):
        """
        target: test optimize result contains all expected fields
        method: create collection, insert data, flush, optimize, verify result fields
        expected: OptimizeResult has status, collection_name, compaction_id, target_size, progress
        note: L3 - requires config change (segment.maxSize=64MB)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        self.create_collection(client, collection_name, dim)
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
            }
            for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        target_size = "1GB"
        result = self.optimize(client, collection_name, target_size=target_size, timeout=300)[0]
        # Verify all result fields
        assert result.status == "success"
        assert result.collection_name == collection_name
        assert isinstance(result.compaction_id, int)
        assert result.compaction_id > 0
        assert result.target_size == target_size
        assert isinstance(result.progress, list)
        assert len(result.progress) > 0
        log.info(f"Optimize result fields verified: progress={result.progress}")

    @pytest.mark.tags(CaseLabel.L3)
    def test_optimize_with_multiple_segments(self):
        """
        target: test optimize merges multiple segments
        method: create collection, insert in batches with flush to create segments, optimize
        expected: Compaction completes, segment count reduced
        note: L3 - requires config change (segment.maxSize=64MB)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        self.create_collection(client, collection_name, dim)
        rng = np.random.default_rng(seed=19530)
        num_batches = 5
        batch_size = default_nb
        for batch in range(num_batches):
            rows = [
                {
                    default_primary_key_field_name: batch * batch_size + i,
                    default_vector_field_name: list(rng.random((1, dim))[0]),
                }
                for i in range(batch_size)
            ]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            log.info(f"Inserted batch {batch + 1}/{num_batches}")

        result = self.optimize(client, collection_name, target_size="2GB", timeout=600)[0]
        assert result.status == "success"
        log.info(f"Optimize with {num_batches} batches completed: compaction_id={result.compaction_id}")

    @pytest.mark.tags(CaseLabel.L3)
    def test_optimize_search_after(self):
        """
        target: test search works correctly after optimize
        method: create collection, insert data, flush, optimize, search
        expected: Search returns correct results
        note: L3 - requires config change (segment.maxSize=64MB)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        nb = default_nb
        self.create_collection(client, collection_name, dim)
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
            }
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # Optimize (includes refresh_load if loaded)
        result = self.optimize(client, collection_name, target_size="2GB", timeout=300)[0]
        assert result.status == "success"
        # Search after optimize
        search_vectors = rng.random((1, dim))
        search_res = self.search(
            client,
            collection_name,
            list(search_vectors),
            limit=10,
            output_fields=[default_primary_key_field_name],
        )[0]
        log.info(f"Search results: {search_res}")
        assert len(search_res) == 1
        assert len(search_res[0]) == 10
        log.info(f"Search after optimize returned {len(search_res[0])} results")

    @pytest.mark.tags(CaseLabel.L3)
    def test_optimize_wait_false_task_tracking(self):
        """
        target: test optimize with wait=False returns OptimizeTask with progress tracking
        method: create collection, insert data, flush, optimize with wait=False, track progress
        expected: Task completes, progress stages are recorded
        note: L3 - requires config change (segment.maxSize=64MB)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        self.create_collection(client, collection_name, dim)
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
            }
            for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # Call optimize with wait=False to get OptimizeTask
        task = self.optimize(client, collection_name, target_size="1GB", wait=False)[0]
        # Track progress
        cost = 300
        start = time.time()
        while not task.done():
            progress = task.progress()
            log.info(f"Optimize progress: {progress}")
            time.sleep(2)
            if time.time() - start > cost:
                raise Exception(f"Optimize task cost more than {cost}s")
        # Get result
        result = task.result(timeout=10)
        assert result.status == "success"
        assert result.collection_name == collection_name
        # Verify progress history
        history = task.progress_history()
        assert len(history) > 0
        log.info(f"Optimize task completed with progress history: {history}")

    @pytest.mark.tags(CaseLabel.L3)
    def test_optimize_wait_false_cancel(self):
        """
        target: test cancelling an optimize task
        method: create collection, insert data, flush, start optimize with wait=False, cancel it
        expected: Task is cancelled, result raises MilvusException
        note: L3 - requires config change (segment.maxSize=64MB)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        self.create_collection(client, collection_name, dim)
        rng = np.random.default_rng(seed=19530)
        num_batches = 5
        for batch in range(num_batches):
            rows = [
                {
                    default_primary_key_field_name: batch * 1000 + i,
                    default_vector_field_name: list(rng.random((1, dim))[0]),
                }
                for i in range(1000)
            ]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
        # Start optimize with wait=False
        task = self.optimize(client, collection_name, target_size="2GB", wait=False)[0]
        # Wait a moment then cancel
        time.sleep(2)
        cancelled = task.cancel()
        log.info(f"Task cancel result: {cancelled}")
        assert task.cancelled()
        log.info("Optimize task cancelled successfully")

    @pytest.mark.tags(CaseLabel.L3)
    def test_optimize_refreshes_loaded_segments(self):
        """
        target: test optimize refreshes an already-loaded collection after compaction
        method: record loaded IDs, optimize, then poll loaded IDs without release/load
        expected: Old IDs disappear, segment count decreases, and all rows remain queryable
        note: L3 - requires config change (segment.maxSize=64MB)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        self.create_collection(client, collection_name, dim)
        rng = np.random.default_rng(seed=19530)
        num_batches = 5
        batch_size = default_nb
        for batch in range(num_batches):
            rows = [
                {
                    default_primary_key_field_name: batch * batch_size + i,
                    default_vector_field_name: list(rng.random((1, dim))[0]),
                }
                for i in range(batch_size)
            ]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)

        # Establish the loaded precondition and a stable pre-optimize view.
        assert self.wait_for_index_ready(client, collection_name, default_vector_field_name, timeout=300)
        self.refresh_load(client, collection_name, timeout=300)
        segments_before = client.list_loaded_segments(collection_name)
        segment_ids_before = {segment.segment_id for segment in segments_before}
        assert len(segment_ids_before) > 1, f"Expected multiple loaded inputs, got {segments_before}"
        log.info(f"Loaded segments before optimize: {segments_before}")

        # optimize() must rebuild indexes and refresh the loaded view itself.
        result = self.optimize(client, collection_name, target_size="64MB", timeout=600)[0]
        assert result.status == "success"
        progress = {getattr(stage, "value", stage) for stage in result.progress}
        assert "refreshing load" in progress, f"Loaded optimize skipped refresh_load: {result.progress}"

        deadline = time.time() + 300
        segments_after = []
        while time.time() < deadline:
            segments_after = client.list_loaded_segments(collection_name)
            segment_ids_after = {segment.segment_id for segment in segments_after}
            if len(segment_ids_after) < len(segment_ids_before) and segment_ids_after.isdisjoint(segment_ids_before):
                break
            time.sleep(2)
        else:
            pytest.fail(
                f"Loaded segments did not converge after optimize without release/load: "
                f"before={segments_before}, after={segments_after}"
            )

        count_result = self.query(
            client,
            collection_name,
            filter="",
            output_fields=["count(*)"],
        )[0]
        assert count_result[0]["count(*)"] == num_batches * batch_size
        log.info(f"Loaded segments converged after optimize: before={segments_before}, after={segments_after}")

    @pytest.mark.tags(CaseLabel.L3)
    def test_optimize_numeric_target_size(self):
        """
        target: test optimize with numeric target_size (bytes)
        method: create collection, insert data, flush, optimize with int target_size
        expected: Compaction completes, parse_target_size treats number as bytes
        note: L3 - requires config change (segment.maxSize=64MB)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        self.create_collection(client, collection_name, dim)
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
            }
            for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 1GB in bytes
        target_size_bytes = 1024 * 1024 * 1024
        result = self.optimize(client, collection_name, target_size=target_size_bytes, timeout=300)[0]
        assert result.status == "success"
        log.info(f"Optimize with numeric target_size={target_size_bytes} completed")

    @pytest.mark.tags(CaseLabel.L3)
    def test_optimize_with_clustering_key(self):
        """
        target: test optimize on collection with clustering key
        method: create collection with clustering key, insert data, optimize
        expected: Optimize completes successfully
        note: L3 - requires config change (segment.maxSize=64MB)
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        schema = self.create_schema(client, enable_dynamic_field=False)[0]
        schema.add_field(
            default_primary_key_field_name,
            DataType.INT64,
            is_primary=True,
            auto_id=False,
        )
        schema.add_field(default_vector_field_name, DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field(
            default_string_field_name,
            DataType.VARCHAR,
            max_length=64,
            is_clustering_key=True,
        )
        index_params = self.prepare_index_params(client)[0]
        index_params.add_index(default_vector_field_name, metric_type="COSINE")
        self.create_collection(
            client,
            collection_name,
            dimension=dim,
            schema=schema,
            index_params=index_params,
        )
        rng = np.random.default_rng(seed=19530)
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
                default_string_field_name: f"str_{i}",
            }
            for i in range(default_nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        result = self.optimize(client, collection_name, target_size="2GB", timeout=300)[0]
        assert result.status == "success"
        log.info(f"Optimize with clustering key completed: {result}")


class TestAsyncMilvusClientOptimizeValid(TestMilvusClientV2Base):
    """Live AsyncMilvusClient optimize coverage."""

    @pytest.mark.asyncio
    @pytest.mark.tags(CaseLabel.L3)
    async def test_async_optimize_loaded_collection(self):
        """
        target: test AsyncMilvusClient.optimize against a loaded multi-segment collection
        method: insert and flush five batches, optimize, inspect progress and persistent segments
        expected: Async optimize refreshes load, reduces segments, and preserves the row count
        """
        self.init_async_milvus_client()
        async_client = self.async_milvus_client_wrap
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        num_batches = 5
        batch_size = default_nb
        collection_created = False

        try:
            await async_client.create_collection(collection_name, dimension=dim)
            collection_created = True
            rng = np.random.default_rng(seed=19530)
            for batch in range(num_batches):
                vectors = rng.random((batch_size, dim), dtype=np.float32)
                rows = [
                    {
                        default_primary_key_field_name: batch * batch_size + index,
                        default_vector_field_name: vectors[index].tolist(),
                    }
                    for index in range(batch_size)
                ]
                await async_client.insert(collection_name, rows)
                await async_client.flush(collection_name)

            await async_client.load_collection(collection_name)
            segments_before, _ = await async_client.list_persistent_segments(collection_name)
            assert len(segments_before) > 1, f"Expected multiple async optimize inputs: {segments_before}"

            result, _ = await async_client.optimize(
                collection_name,
                target_size="64MB",
                timeout=600,
            )

            assert result.status == "success"
            assert result.collection_name == collection_name
            assert result.target_size == "64MB"
            progress = {getattr(stage, "value", stage) for stage in result.progress}
            assert "compacting" in progress
            assert "waiting for index rebuild" in progress
            assert "refreshing load" in progress

            segments_after, _ = await async_client.list_persistent_segments(collection_name)
            assert len(segments_after) < len(segments_before), (
                f"Async optimize did not reduce persistent segments: before={segments_before}, after={segments_after}"
            )
            count_result, _ = await async_client.query(
                collection_name,
                filter="",
                output_fields=["count(*)"],
            )
            assert count_result[0]["count(*)"] == num_batches * batch_size
        finally:
            if collection_created:
                await async_client.drop_collection(collection_name)
            await async_client.close()
