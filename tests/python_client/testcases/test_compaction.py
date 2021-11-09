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
    def test_compact_empty_collection(self):
        """
        target: test compact an empty collection
        method: compact an empty collection
        expected: No exception
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
    def test_compact_before_index(self):
        """
        target: test compact and create index
        method: 1.insert data into two segments
                2.compact
                3.create index
                4.search
        expected: Verify search result and index info
        """
        pass
