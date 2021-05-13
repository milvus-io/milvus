import pytest
from base.client_request import ApiReq
from utils.util_log import test_log as log
from common.common_type import *


class TestConnection(ApiReq):
    """ Test case of connections interface """

    @pytest.mark.tags(CaseLabel.L3)
    def test_case(self):
        self.connection.configure(check_res='', default={"host": "192.168.1.240", "port": "19530"})
        res_ = self.connection.get_connection(alias='default')
        log.info("res : %s" % str(res_))
        log.info("self.connection : %s" % str(self.connection))
        res_list = self.connection.list_connections()
        log.info(res_list)

    def test_connection_kwargs_param_check(self):
        res_configure = self.connection.configure(check_res='', default={"host": "192.168.1.240", "port": "19530"})
        log.info(res_configure)
        res_list = self.connection.list_connections()
        log.info(res_list)

