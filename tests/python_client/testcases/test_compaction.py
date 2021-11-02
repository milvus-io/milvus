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


@pytest.mark.skip(reason="Waiting for development")
class TestCompactionOperation(TestcaseBase):
    pass
