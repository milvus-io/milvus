import pytest

from base.client_request import ApiReq
from utils.util_log import test_log as log
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel


class TestInsertParams(ApiReq):
    """ Test case of Insert interface """

    @pytest.mark.tags(CaseLabel.L0)
    @pytest.mark.xfail(reason="issue #5421")
    def test_collection_numpy_insert_data(self):
        """
        target: test collection create and insert list-like data
        method: 1. create by schema 2. insert data
        expected: assert num_entities
        """
        self._connect()
        nb = 10
        collection = self._collection()
        data = cf.gen_numpy_data(nb)
        ex, _ = self.collection.insert(data=data)
        log.error(str(ex))
