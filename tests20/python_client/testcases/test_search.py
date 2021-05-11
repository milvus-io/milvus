import pytest
from base.ClientRequest import ApiReq
from utils.util_log import my_log as log
from common.common_type import *


class TestSearch(ApiReq):
    """ Test case of search interface """

    @pytest.mark.tags(CaseLabel.L3)
    def test_case(self):
        log.info("Test case of search interface")
