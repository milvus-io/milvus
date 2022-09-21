import pytest
from time import sleep
from collections import defaultdict
from pymilvus import connections
from chaos.checker import (InsertChecker,
                           FlushChecker, 
                           SearchChecker,
                           QueryChecker,
                           IndexChecker,
                           DeleteChecker,
                           Op)
from utils.util_log import test_log as log
from chaos import chaos_commons as cc
from chaos.chaos_commons import assert_statistic
from common.common_type import CaseLabel
from chaos import constants
from utils.util_common import get_collections
from delayed_assert import assert_expectations


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


def get_collections_with_limit():
    collections = get_collections()
    m = defaultdict(list)
    for c_name in collections:
        prefix = c_name.split("_")[0]
        if len(m[prefix]) <= 5:
            m[prefix].append(c_name)
    selected_collections = []
    for v in m.values():
        selected_collections.extend(v)
    if len(selected_collections) == 0:
        selected_collections = [None]
    return selected_collections


class TestOperations(TestBase):

    @pytest.fixture(scope="function", params=get_collections_with_limit())
    def collection_name(self, request):
        if request.param == [] or request.param == "":
            pytest.skip("The collection name is invalid")
        yield request.param

    @pytest.fixture(scope="function", autouse=True)
    def connection(self, host, port, user, password):
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

    def init_health_checkers(self, collection_name=None):
        c_name = collection_name
        checkers = {
            Op.insert: InsertChecker(collection_name=c_name),
            Op.flush: FlushChecker(collection_name=c_name),
            Op.index: IndexChecker(collection_name=c_name),
            Op.search: SearchChecker(collection_name=c_name),
            Op.query: QueryChecker(collection_name=c_name),
            Op.delete: DeleteChecker(collection_name=c_name),
        }
        self.health_checkers = checkers

    @pytest.mark.tags(CaseLabel.L3)
    def test_operations(self,collection_name, request_duration):
        # start the monitor threads to check the milvus ops
        log.info("*********************Test Start**********************")
        log.info(connections.get_connection_addr('default'))
        c_name = collection_name
        self.init_health_checkers(collection_name=c_name)
        cc.start_monitor_threads(self.health_checkers)
        log.info("*********************Request Load Start**********************")
        # wait request_duration for the load request to be finished
        request_duration = request_duration.replace("h","*3600+").replace("m","*60+").replace("s","")
        if request_duration[-1] == "+":
            request_duration = request_duration[:-1]
        request_duration = eval(request_duration)
        for i in range(10):
            sleep(request_duration//10)
            for k,v in self.health_checkers.items():
                v.check_result()
        log.info("******assert after chaos deleted: ")
        assert_statistic(self.health_checkers)
        assert_expectations()
        log.info("*********************Test Completed**********************")
