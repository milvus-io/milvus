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
import pytest
import time
import numpy as np

from base.client_v2_base import TestMilvusClientV2Base
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel, CheckTasks
from utils.util_pymilvus import *  # noqa: F403
from common.constants import *  # noqa: F403
from pymilvus import DataType


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
max_int64 = (1 << 63) - 1  # For automatic target_size calculation
default_max_size_mb = 1024  # Default segment max size in MB


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
        error = {ct.err_code: 1, ct.err_msg: "target_size"}
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
    def test_force_merge_default_target_size(self):
        """
        target: test ForceMerge with default target_size (0 or not passed)
        method: create collection, insert data, flush, compact without target_size
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
        # 3. compact with default target_size (not passed)
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
        log.info("ForceMerge with default target_size completed successfully")

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
        target: test ForceMerge with automatic target_size (max_int64)
        method: create collection, insert data, flush, compact with target_size=max_int64
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
        compact_id = self.compact(client, collection_name, target_size=max_int64)[0]
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
        log.info(
            f"Search after ForceMerge completed successfully with {len(search_res[0])} results"
        )

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
        compact_id = self.compact(
            client, collection_name, is_clustering=True, target_size=target_size
        )[0]
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

        # 3. load and get segment count before compaction using MilvusClient method
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

        # 6. release and reload to get updated segment info
        self.release_collection(client, collection_name)
        self.load_collection(client, collection_name)
        segments_after = client.list_loaded_segments(collection_name)
        segment_count_after = len(segments_after)
        log.info(f"Segment count after ForceMerge: {segment_count_after}")

        # 7. verify segment count reduced (or at least not increased)
        assert segment_count_after <= segment_count_before, (
            f"Expected fewer segments after ForceMerge, got {segment_count_after} >= {segment_count_before}"
        )
        log.info(
            f"ForceMerge reduced segments from {segment_count_before} to {segment_count_after}"
        )

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_max_int64_overflow(self):
        """
        target: test ForceMerge with max_int64 targetSize doesn't cause overflow
        method: create collection, insert data, compact with target_size=max_int64
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
        # 3. compact with max_int64 target_size (should trigger auto-calculate mode)
        # Before fix: would fail with overflow error
        # After fix: should succeed
        compact_id = self.compact(client, collection_name, target_size=max_int64)[0]
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

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_algorithm_selection_under_threshold(self):
        """
        target: test ForceMerge uses maxFullSegmentsGrouping when segment count <= threshold
        method: create collection, insert data to create 5 segments (< threshold 10),
                trigger force merge with target_size
        expected: Compaction completes, algorithm selection logged as maxFullSegmentsGrouping
        note: L3 - Check Loki logs for 'using maxFullSegmentsGrouping algorithm'
              Requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        num_segments = 5  # Under threshold (default 10)
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data in batches to create multiple segments
        rng = np.random.default_rng(seed=19530)
        for batch in range(num_segments):
            rows = [
                {
                    default_primary_key_field_name: batch * 1000 + i,
                    default_vector_field_name: list(rng.random((1, dim))[0]),
                }
                for i in range(1000)
            ]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            log.info(f"Inserted batch {batch + 1}/{num_segments}")
        # 3. compact with target_size to trigger ForceMerge
        target_size = 2048
        compact_id = self.compact(client, collection_name, target_size=target_size)[0]
        log.info(f"ForceMerge triggered with {num_segments} segments (expect maxFullSegmentsGrouping)")
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
        log.info(f"ForceMerge with {num_segments} segments completed (check logs for algorithm)")

    @pytest.mark.tags(CaseLabel.L3)
    def test_force_merge_algorithm_selection_over_threshold(self):
        """
        target: test ForceMerge uses largerGroupingSegments when segment count > threshold
        method: create collection, insert data to create 15 segments (> threshold 10),
                trigger force merge with target_size
        expected: Compaction completes, algorithm selection logged as largerGroupingSegments
        note: L3 - Check Loki logs for 'using largerGroupingSegments algorithm'
              Requires config change (segment.maxSize=64MB) to trigger actual force merge
        """
        client = self._client()
        collection_name = cf.gen_unique_str(prefix)
        dim = 128
        num_segments = 15  # Over threshold (default 10)
        # 1. create collection
        self.create_collection(client, collection_name, dim)
        # 2. insert data in batches to create multiple segments
        rng = np.random.default_rng(seed=19530)
        for batch in range(num_segments):
            rows = [
                {
                    default_primary_key_field_name: batch * 1000 + i,
                    default_vector_field_name: list(rng.random((1, dim))[0]),
                }
                for i in range(1000)
            ]
            self.insert(client, collection_name, rows)
            self.flush(client, collection_name)
            log.info(f"Inserted batch {batch + 1}/{num_segments}")
        # 3. compact with target_size to trigger ForceMerge
        target_size = 2048
        compact_id = self.compact(client, collection_name, target_size=target_size)[0]
        log.info(f"ForceMerge triggered with {num_segments} segments (expect largerGroupingSegments)")
        # 4. wait for compaction to complete
        cost = 600  # Longer timeout for more segments
        start = time.time()
        while True:
            time.sleep(1)
            res = self.get_compaction_state(client, compact_id)[0]
            log.info(f"Compaction state: {res}")
            if res == "Completed":
                break
            if time.time() - start > cost:
                raise Exception(f"Compaction cost more than {cost}s")
        log.info(f"ForceMerge with {num_segments} segments completed (check logs for algorithm)")
