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

    def test_wt(self):
        res_configure = self.connection.configure(check_res='', default={"host": "192.168.1.240", "port": "19530"})
        log.info(res_configure)
        res_list, che = self.connection.list_connections(check_res='')
        log.info(res_list)
        for i in res_list:
            log.info("!" * 20)
            log.info(self.connection.remove_connection(alias=i))
        res_list = self.connection.list_connections(check_res='')
        log.info(res_list)
        log.info("!" * 20)
        log.info(self.utility.list_collections())

    def test_check_res(self):
        self.connection.configure(check_res='', check_params=None, default={"host": "192.168.1.240", "port": "19530"})
        self.connection.get_connection(alias='default')
        self.connection.list_connections(check_res=CheckParams.list_count, check_params={"list_count": 1})


