"""
ForceMerge Compaction Test Cases

L3 tests require Milvus configuration changes to trigger actual force merge:
    dataCoord:
      segment:
        maxSize: 64  # MB, default is 1024
      compaction:
        enableAutoCompaction: false  # Disable auto compaction

With maxSize=64MB and auto compaction disabled, small data volumes can trigger
force merge compaction manually without interference from auto compaction.
"""

import math
import os
import time
from collections import Counter

import numpy as np
import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from common.constants import *  # noqa: F403
from minio import Minio
from pymilvus import DataType
from utils.util_log import test_log as log
from utils.util_pymilvus import *  # noqa: F403

prefix = "client_force_merge"
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

# ForceMerge specific constants
max_int64 = (1 << 63) - 1
auto_target_size_mb = max_int64 // (1024 * 1024) + 1  # Triggers server auto target-size mode.
default_max_size_mb = 1024  # Default segment max size in MB
actual_output_size_tolerance = 0.10


def minio_endpoint(minio_host):
    endpoint = (minio_host or "localhost").strip()
    if "://" in endpoint:
        endpoint = endpoint.split("://", 1)[1]
    endpoint = endpoint.split("/", 1)[0]
    if ":" not in endpoint:
        endpoint = f"{endpoint}:9000"
    return endpoint


def new_minio_client(minio_host):
    return Minio(
        minio_endpoint(minio_host),
        access_key=os.getenv("MILVUS_MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MILVUS_MINIO_SECRET_KEY", "minioadmin"),
        secure=os.getenv("MILVUS_MINIO_SECURE", "false").lower() in ["1", "true", "yes"],
    )


def get_insert_log_sizes(minio_client, bucket, collection_id, segment_ids):
    root_path = os.getenv("MILVUS_MINIO_ROOT_PATH", "files").strip("/")
    prefix = f"{root_path}/insert_log/{collection_id}/"
    sizes = {str(segment_id): 0 for segment_id in segment_ids}

    for item in minio_client.list_objects(bucket, prefix=prefix, recursive=True):
        relative_parts = item.object_name[len(prefix) :].split("/")
        if len(relative_parts) < 3:
            continue
        segment_id = relative_parts[1]
        if segment_id in sizes:
            sizes[segment_id] += item.size

    missing = [segment_id for segment_id, size in sizes.items() if size == 0]
    assert not missing, f"No insert-log objects found for segments {missing} under {prefix}"
    return {int(segment_id): size for segment_id, size in sizes.items()}


class TestMilvusClientForceMergeInvalid(TestMilvusClientV2Base):
    """Test cases for ForceMerge with invalid parameters"""

    """
    ******************************************************************
    #  The following are invalid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target_size", [-1, -100])
    def test_force_merge_invalid_target_size_negative(self, target_size):
        """
        target: test ForceMerge with negative target_size
        method: create collection, call compact with negative target_size
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. compact with invalid target_size
        error = {ct.err_code: 1, ct.err_msg: "target_size"}
        self.compact(
            client,
            collection_name,
            target_size=target_size,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("target_size", [100, 512, 1000])
    def test_force_merge_target_size_less_than_max_size(self, target_size):
        """
        target: test ForceMerge with target_size less than config maxSize
        method: create collection, call compact with target_size < 1024 MB
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        # 1. create collection
        self.create_collection(client, collection_name, default_dim)
        # 2. compact with target_size less than maxSize
        error = {ct.err_code: 1100, ct.err_msg: "targetSize"}
        self.compact(
            client,
            collection_name,
            target_size=target_size,
            check_task=CheckTasks.err_res,
            check_items=error,
        )

    @pytest.mark.tags(CaseLabel.L1)
    def test_force_merge_nonexistent_collection(self):
        """
        target: test ForceMerge with non-existent collection
        method: call compact on a collection that doesn't exist
        expected: Raise exception
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        error = {ct.err_code: 100, ct.err_msg: "collection not found"}
        self.compact(
            client,
            collection_name,
            target_size=2048,
            check_task=CheckTasks.err_res,
            check_items=error,
        )


class TestMilvusClientForceMergeValid(TestMilvusClientV2Base):
    """Test cases for ForceMerge with valid parameters"""

    """
    ******************************************************************
    #  The following are valid base cases
    ******************************************************************
    """

    @pytest.mark.tags(CaseLabel.L3)
    def test_manual_compaction_without_target_size(self):
        """
        target: test ordinary manual compaction when target_size is omitted
        method: create collection, insert data, flush, compact without target_size
        expected: Ordinary manual compaction completes successfully
        note: Omitting target_size does not select Force Merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data
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
        # 3. compact without target_size; this is ordinary manual compaction
        compact_id = self.compact(client, collection_name)[0]
        # 4. wait for compaction to complete
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info("Manual compaction without target_size completed successfully")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_explicit_target_size(self):
        """
        target: test ForceMerge with explicit target_size (2048 MB)
        method: create collection, insert data, flush, compact with target_size=2048
        expected: Compaction completes successfully
        note: L3 - requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data
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
        # 3. compact with explicit target_size
        target_size = 2048  # 2GB
        compact_id = self.compact(client, collection_name, target_size=target_size)[0]
        # 4. wait for compaction to complete
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info(f"ForceMerge with target_size={target_size}MB completed successfully")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_auto_target_size(self):
        """
        target: test ForceMerge with automatic target_size
        method: create collection, insert data, flush, compact with target_size above int64-safe MB threshold
        expected: Compaction completes successfully with auto-calculated optimal size
        note: L3 - requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data
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
        # 3. compact with auto target_size
        compact_id = self.compact(client, collection_name, target_size=auto_target_size_mb)[0]
        # 4. wait for compaction to complete
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info("ForceMerge with auto target_size (max_int64) completed successfully")

    @pytest.mark.tags(CaseLabel.L1)
    def test_force_merge_empty_collection(self):
        """
        target: test ForceMerge on empty collection
        method: create collection, compact with target_size
        expected: Compaction completes successfully
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection (empty)
        self.create_collection(client, collection_name, dim)
        # 2. compact with target_size on empty collection
        compact_id = self.compact(client, collection_name, target_size=2048)[0]
        # 3. wait for compaction to complete
        cost = 60
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info("ForceMerge on empty collection completed successfully")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_with_multiple_segments(self):
        """
        target: test ForceMerge with multiple segments
        method: create collection, insert data in batches to create multiple segments,
                flush, compact with target_size
        expected: Segments merged, fewer segments after compaction
        note: L3 - requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data in multiple batches to create multiple segments
        rng = np.random.default_rng(seed=19530)
        batch_size = default_nb
        num_batches = 5
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

        # 3. compact with target_size
        target_size = 2048  # 2GB
        compact_id = self.compact(client, collection_name, target_size=target_size)[0]
        # 4. wait for compaction to complete
        cost = 300
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info("ForceMerge with multiple segments completed successfully")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_search_after_merge(self):
        """
        target: test search works correctly after ForceMerge
        method: create collection, insert data, flush, compact, load, search
        expected: Search returns correct results
        note: L3 - requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data
        rng = np.random.default_rng(seed=19530)
        nb = default_nb
        rows = [
            {
                default_primary_key_field_name: i,
                default_vector_field_name: list(rng.random((1, dim))[0]),
            }
            for i in range(nb)
        ]
        self.insert(client, collection_name, rows)
        self.flush(client, collection_name)
        # 3. compact with target_size
        target_size = 2048
        compact_id = self.compact(client, collection_name, target_size=target_size)[0]
        # 4. wait for compaction to complete
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        # 5. search
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
        log.info(f"Search after ForceMerge completed successfully with {len(search_res[0])} results")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_with_clustering_key(self):
        """
        target: test ForceMerge with clustering key
        method: create collection with clustering key, insert data, compact with is_clustering=True
        expected: Compaction completes successfully
        note: L3 - requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection with clustering key
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
        # 2. insert data
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
        # 3. compact with is_clustering=True and target_size
        target_size = 2048
        compact_id = self.compact(client, collection_name, is_clustering=True, target_size=target_size)[0]
        # 4. wait for compaction to complete
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id, is_clustering=True)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info("ForceMerge with clustering key completed successfully")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_verify_segment_count(self):
        """
        target: test ForceMerge reduces segment count
        method: create collection, insert data in batches, verify segment count before/after
        expected: Fewer segments after ForceMerge
        note: L3 - requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data in multiple batches
        rng = np.random.default_rng(seed=19530)
        batch_size = default_nb
        num_batches = 5
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

        # 3. reload and get stable segment count before compaction
        assert self.wait_for_index_ready(client, collection_name, default_vector_field_name, timeout=300)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        segments_before = client.list_loaded_segments(collection_name)
        segment_count_before = len(segments_before)
        log.info(f"Segment count before ForceMerge: {segment_count_before}")

        # 4. compact with target_size
        target_size = 2048
        compact_id = self.compact(client, collection_name, target_size=target_size)[0]
        # 5. wait for compaction to complete
        cost = 300
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")

        # 6. wait for the compacted segment index, then reload to get updated segment info
        assert self.wait_for_index_ready(client, collection_name, default_vector_field_name, timeout=300)
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        segments_after = client.list_loaded_segments(collection_name)
        segment_count_after = len(segments_after)
        log.info(f"Segment count after ForceMerge: {segment_count_after}")

        # 7. verify segment count reduced
        assert segment_count_after < segment_count_before, (
            f"Expected fewer segments after ForceMerge, got {segment_count_after} >= {segment_count_before}"
        )
        log.info(f"ForceMerge reduced segments from {segment_count_before} to {segment_count_after}")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_target_size_controls_output_segments(self, minio_host, minio_bucket):
        """
        target: prove explicit target_size controls Force Merge output count and size
        method: create a 1.25x-2x target input scope, compact, inspect plans and insert logs
        expected: rows and sources are conserved; output count matches target and sizes stay within tolerance
        note: L3 - requires segment.maxSize=64MB, auto compaction disabled, and MinIO access
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 1024
        batch_size = 2000
        num_batches = 18
        total_rows = batch_size * num_batches
        target_size_mb = 80
        target_size_bytes = target_size_mb * 1024 * 1024
        minio_client = new_minio_client(minio_host)
        assert minio_client.bucket_exists(minio_bucket), f"MinIO bucket {minio_bucket!r} does not exist"

        self.create_collection(client, collection_name, dim)
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
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)

        segments_before = self.list_persistent_segments(client, collection_name)[0]
        source_ids = {segment.segment_id for segment in segments_before}
        assert len(source_ids) >= num_batches, (
            f"Expected at least {num_batches} sealed inputs, got {len(source_ids)}: {segments_before}"
        )
        description = self.describe_collection(client, collection_name)[0]
        collection_id = description["collection_id"]
        input_sizes = get_insert_log_sizes(minio_client, minio_bucket, collection_id, source_ids)
        total_input_size = sum(input_sizes.values())
        assert target_size_bytes * 1.25 < total_input_size < target_size_bytes * 2, (
            f"Fixture must produce between 1.25x and 2x target bytes; "
            f"input={total_input_size}, target={target_size_bytes}, sizes={input_sizes}"
        )

        compact_id = self.compact(client, collection_name, target_size=target_size_mb)[0]
        assert self.wait_for_compaction_ready(client, compact_id, timeout=600)

        plans = client.get_compaction_plans(compact_id).plans
        planned_source_counts = Counter(segment_id for plan in plans for segment_id in plan.sources)
        planned_source_ids = set(planned_source_counts)
        assert planned_source_ids == source_ids, (
            f"Force Merge plans must cover every input: "
            f"expected={source_ids}, actual={planned_source_ids}, plans={plans}"
        )
        assert all(count == 1 for count in planned_source_counts.values()), (
            f"Force Merge source IDs must occur in exactly one plan: {planned_source_counts}"
        )

        segments_after = self.list_persistent_segments(client, collection_name)[0]
        output_ids = {segment.segment_id for segment in segments_after}
        assert output_ids.isdisjoint(source_ids), (
            f"Completed Force Merge must replace all source segments: sources={source_ids}, outputs={output_ids}"
        )
        assert sum(segment.num_rows for segment in segments_after) == total_rows

        output_sizes = get_insert_log_sizes(minio_client, minio_bucket, collection_id, output_ids)
        total_output_size = sum(output_sizes.values())
        rewrite_delta = abs(total_output_size - total_input_size) / total_input_size
        assert rewrite_delta <= actual_output_size_tolerance, (
            f"Rewrite changed total insert-log bytes by {rewrite_delta:.2%}: "
            f"before={total_input_size}, after={total_output_size}"
        )

        expected_output_count = math.ceil(total_input_size / target_size_bytes)
        assert len(output_sizes) == expected_output_count == 2, (
            f"Expected two outputs for target={target_size_bytes}, got sizes={output_sizes}"
        )
        max_output_size = max(output_sizes.values())
        assert target_size_bytes * (1 - actual_output_size_tolerance) <= max_output_size
        assert all(size <= target_size_bytes * (1 + actual_output_size_tolerance) for size in output_sizes.values()), (
            f"Output insert-log size exceeded target tolerance: target={target_size_bytes}, sizes={output_sizes}"
        )
        log.info(f"Force Merge target-size evidence: input={input_sizes}, output={output_sizes}, plans={plans}")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_max_int64_overflow(self):
        """
        target: test ForceMerge with huge targetSize doesn't cause overflow
        method: create collection, insert data, compact with target_size above int64-safe MB threshold
        expected: Compaction completes successfully (auto-calculate mode)
        note: L3 - This verifies PR #47327 fix for integer overflow when
              targetSizeBytes = targetSize * 1024 * 1024
              Requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data in batches to create segments
        rng = np.random.default_rng(seed=19530)
        for batch in range(3):
            rows = [
                {
                    default_primary_key_field_name: batch * 1000 + i,
                    default_vector_field_name: list(rng.random((1, dim))[0]),
                }
                for i in range(1000)
            ]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            log.info(f"Inserted batch {batch + 1}/3")
        # 3. compact with huge target_size (should trigger auto-calculate mode)
        # Before fix: would fail with overflow error
        # After fix: should succeed
        compact_id = self.compact(client, collection_name, target_size=auto_target_size_mb)[0]
        # 4. wait for compaction to complete
        cost = 180
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info("ForceMerge with max_int64 target_size completed (overflow fix verified)")
