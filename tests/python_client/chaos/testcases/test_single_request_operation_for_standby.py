import pytest
import threading
from time import sleep
from pymilvus import connections
from chaos.checker import (CreateChecker,
                           InsertChecker,
                           FlushChecker, 
                           SearchChecker,
                           QueryChecker,
                           IndexChecker,
                           DeleteChecker,
                           Op)
from utils.util_log import test_log as log
from chaos import chaos_commons as cc
from common.common_type import CaseLabel
from common.milvus_sys import MilvusSys
from chaos.chaos_commons import assert_statistic
from chaos import constants
from delayed_assert import assert_expectations
from utils.util_k8s import (get_milvus_instance_name,
                            get_milvus_deploy_tool,
                            reset_healthy_checker_after_standby_activated)



class TestBase:
    expect_create = constants.SUCC
    expect_insert = constants.SUCC
    expect_flush = constants.SUCC
    expect_index = constants.SUCC
    expect_search = constants.SUCC
    expect_query = constants.SUCC
    host = '127.0.0.1'
    port = 19530
    _chaos_config = None
    health_checkers = {}


class TestOperations(TestBase):

    @pytest.fixture(scope="function", autouse=True)
    def connection(self, host, port, user, password, milvus_ns):
        if user and password:
            # log.info(f"connect to {host}:{port} with user {user} and password {password}")
            connections.connect('default', host=host, port=port, user=user, password=password, secure=True)
        else:
            connections.connect('default', host=host, port=port)
        if connections.has_connection("default") is False:
            raise Exception("no connections")
        log.info("connect to milvus successfully")
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.milvus_sys = MilvusSys(alias='default')
        self.chaos_ns = constants.CHAOS_NAMESPACE
        self.milvus_ns = milvus_ns
        self.release_name = get_milvus_instance_name(self.milvus_ns, milvus_sys=self.milvus_sys)
        self.deploy_by = get_milvus_deploy_tool(self.milvus_ns, self.milvus_sys)

    def init_health_checkers(self, collection_name=None):
        c_name = collection_name
        checkers = {
            Op.create: CreateChecker(collection_name=c_name),
            Op.insert: InsertChecker(collection_name=c_name),
            Op.flush: FlushChecker(collection_name=c_name),
            Op.index: IndexChecker(collection_name=c_name),
            Op.search: SearchChecker(collection_name=c_name),
            Op.query: QueryChecker(collection_name=c_name),
            Op.delete: DeleteChecker(collection_name=c_name),
        }
        self.health_checkers = checkers

    @pytest.mark.tags(CaseLabel.L3)
    def test_operations(self, request_duration, target_component, is_check):
        # start the monitor threads to check the milvus ops
        log.info("*********************Test Start**********************")
        log.info(connections.get_connection_addr('default'))
        c_name = None
        self.init_health_checkers(collection_name=c_name)
        cc.start_monitor_threads(self.health_checkers)
        log.info("*********************Load Start**********************")
        # wait request_duration
        request_duration = request_duration.replace("h", "*3600+").replace("m", "*60+").replace("s", "")
        if request_duration[-1] == "+":
            request_duration = request_duration[:-1]
        request_duration = eval(request_duration)
        # start a thread to reset health_checkers when standby is activated.
        t = threading.Thread(target=reset_healthy_checker_after_standby_activated,
                             args=(self.milvus_ns, self.release_name, target_component, self.health_checkers),
                             kwargs={"timeout": request_duration//2},
                             daemon=True)
        t.start()
        # t.join()
        log.info('start a thread to reset health_checkers when standby is activated')
        for i in range(10):
            sleep(request_duration//10)
            for k, v in self.health_checkers.items():
                v.check_result()
        if is_check:
            assert_statistic(self.health_checkers)
            assert_expectations()
        log.info("*********************Chaos Test Completed**********************")
