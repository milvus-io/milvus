import pytest

from base.client_base import TestcaseBase
from common.common_type import CaseLabel


@pytest.mark.skip(reason="Waiting for development")
class TestCompactionParams(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_without_connection(self):
        """
        target: test compact without connection
        method: compact after remove connection
        expected: raise exception
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_twice(self):
        """
        target: test compact twice
        method: compact twice
        expected: No exception
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_partition(self):
        """
        target: test compact partition
        method: compact partition
        expected: Verify partition segments merged
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_growing_segment(self):
        """
        target: test compact growing data
        method: 1.insert into multi segments without flush
                2.compact
        expected: No compaction (compact just for sealed data)
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_empty_collection(self):
        """
        target: test compact an empty collection
        method: compact an empty collection
        expected: No exception
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_after_delete_single(self):
        """
        target: test delete one entity and compact
        method: 1.create with shard_num=1
                2.delete one sealed entity
                2.compact
        expected: Verify compact result todo
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_after_delete_half(self):
        """
        target: test delete half entity and compact
        method: 1.create with shard_num=1
                2.insert and flush
                3.delete half of nb
                4.compact
        expected: collection num_entities decrease
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_after_delete_all(self):
        """
        target: test delete all and compact
        method: 1.create with shard_num=1
                2.delete all sealed data
                3.compact
        expected: collection num_entities is close to 0
        """
        pass


@pytest.mark.skip(reason="Waiting for development")
class TestCompactionOperation(TestcaseBase):

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_after_index(self):
        """
        target: test compact after create index
        method: 1.insert data into two segments
                2.create index
                3.compact
                4.search
        expected: Verify segment info and index info
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_after_binary_index(self):
        """
        target: test compact after create index
        method: 1.insert binary data into two segments
                2.create binary index
                3.compact
                4.search
        expected: Verify segment info and index info
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_and_index(self):
        """
        target: test compact and create index
        method: 1.insert data into two segments
                2.compact
                3.create index
                4.load and search
        expected: Verify search result and index info
        """
        pass

    @pytest.mark.tags(CaseLabel.L0)
    def test_compact_and_search(self):
        """
        target: test compact and search
        method: 1.insert data into two segments
                2.compact
                3.load and search
        expected: Verify search result
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_search_after_delete_channel(self):
        """
        target: test search after compact, and queryNode get delete request from channel,
                rather than compacted delta log
        method: 1.insert, flush and load
                2.delete half
                3.compact
                4.search
        expected: No exception
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_delete_inside_time_travel(self):
        """
        target: test compact inside time_travel range
        method: 1.insert data and get ts
                2.delete ids
                3.search with ts
                4.compact
                5.search with ts
        expected: Both search are successful
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_delete_outside_time_travel(self):
        """
        target: test compact outside time_travel range
        method: todo
        expected: Verify compact result
        """
        pass

    @pytest.mark.tags(CaseLabel.L0)
    def test_compact_merge_two_segments(self):
        """
        target: test compact merge two segments
        method: 1.create with shard_num=1
                2.insert half nb and flush
                3.insert half nb and flush
                4.compact
                5.search
        expected: Verify segments are merged
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_no_merge(self):
        """
        target: test compact when no segments merge
        method: 1.create with shard_num=1
                2.insert and flush
                3.compact and search
        expected: No exception
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_merge_multi_segments(self):
        """
        target: test compact and merge multi small segments
        method: 1.create with shard_num=1
                2.insert one and flush (multi times)
                3.compact
                4.load and search
        expected: Verify segments info
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_merge_inside_time_travel(self):
        """
        target: test compact and merge segments inside time_travel range
        method: todo
        expected: Verify segments inside time_travel merged
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_threshold_auto_merge(self):
        """
        target: test num (segment_size < 1/2Max) reaches auto-merge threshold
        method: todo
        expected: Auto-merge segments
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_less_threshold_no_merge(self):
        """
        target: test compact the num of segments that size less than 1/2Max, does not reach the threshold
        method: todo
        expected: No auto-merge
        """
        pass

    @pytest.mark.tags(CaseLabel.L2)
    def test_compact_multi_collections(self):
        """
        target: test compact multi collections with merge
        method: create 50 collections, add entities into them and compact in turn
        expected: No exception
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_and_insert(self):
        """
        target: test insert after compact
        method: 1.create and insert with flush
                2.delete and compact
                3.insert new data
                4.load and search
        expected: Verify search result and segment info
        """
        pass

    @pytest.mark.tags(CaseLabel.L1)
    def test_compact_and_delete(self):
        """
        target: test delete after compact
        method: 1.delete half and compact
                2.load and query
                3.delete and query
        expected: Verify deleted ids
        """
        pass
