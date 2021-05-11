import pytest
from base.ClientRequest import ApiReq
from utils.util_log import my_log as log
from common.common_type import *


class TestConnection(ApiReq):
    """ Test case of connections interface """

    @pytest.mark.tags(CaseLabel.L3)
    def test_case(self):
        self.connection.configure(check_res='', default={"host": "192.168.1.240", "port": "19530"})
        res_ = self.connection.get_connection(alias='default')
        log.info("res : %s" % str(res_))
        log.info("self.connection : %s" % str(self.connection))
