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
    pass
