import logging

import pytest
import sys
import threading
from time import sleep

from base.client_request import ApiReq
from pymilvus_orm import connections
from checker import CreateChecker, SearchChecker, InsertChecker
from base.client_request import ApiCollection
from common import common_func as cf
from common import common_type as ct
from utils.util_log import test_log as log


class TestsChaos:
    @pytest.fixture(scope="function", autouse=True)
    def coll_wrapper_4_insert(self):
        connections.configure(default={"host": "192.168.1.239", "port": 19530})
        res = connections.create_connection(alias='default')
        if res is None:
            raise Exception("no connections")
        c_wrapper = ApiCollection()
        c_wrapper.collection_init(name=cf.gen_unique_str(),
                                  schema=cf.gen_default_collection_schema(),
                                  check_res="check_nothing")
        return c_wrapper

    @pytest.fixture(scope="function", autouse=True)
    def coll_wrapper_4_search(self):
        connections.configure(default={"host": "192.168.1.239", "port": 19530})
        res = connections.create_connection(alias='default')
        if res is None:
            raise Exception("no connections")
        c_wrapper = ApiCollection()
        _, result = c_wrapper.collection_init(name=cf.gen_unique_str(),
                                  schema=cf.gen_default_collection_schema(),
                                  check_res="check_nothing")
        if result is False:
            log.log("result: ")
        # for _ in range(10):
        #     c_wrapper.insert(data=cf.gen_default_list_data(nb=ct.default_nb*10),
        #                     check_res="check_nothing")
        return c_wrapper

    @pytest.fixture(scope="function", autouse=True)
    def health_checkers(self, coll_wrapper_4_insert, coll_wrapper_4_search):
        checkers = {}
        # search_ch = SearchChecker(collection_wrapper=coll_wrapper_4_search)
        # checkers["search"] = search_ch
        # insert_ch = InsertChecker(collection_wrapper=coll_wrapper_4_insert)
        # checkers["insert"] = insert_ch
        create_ch = CreateChecker(collection_wrapper=coll_wrapper_4_insert)
        checkers["create"] = create_ch

        return checkers

    '''
    def teardown(self, health_checkers):
        for ch in health_checkers.values():
            ch.terminate()
        pass
    '''

    def test_chaos(self, health_checkers):
        # query_t = threading.Thread(target=health_checkers['create'].keep_searching, args=())
        # query_t.start()
        # insert_t = threading.Thread(target=health_checkers['create'].keep_inserting, args=())
        # insert_t.start()
        create_t = threading.Thread(target=health_checkers['create'].keep_creating, args=())
        create_t.start()

        # parse chaos object
        # find the testcase by chaos ops in testcases
        # parse the test expectations
        # wait 120s
        print("test_chaos starting...")
        sleep(2)
        print(f"succ count1: {health_checkers['create']._succ}")
        print(f"succ rate1: {health_checkers['create'].statics()}")
        # assert statistic:all ops 100% succ
        # reset counting
        # apply chaos object
        # wait 300s (varies by chaos)
        health_checkers["create"].reset()
        print(f"succ count2: {health_checkers['create']._succ}")
        print(f"succ rate2: {health_checkers['create'].statics()}")
        sleep(2)
        print(f"succ count3: {health_checkers['create']._succ}")
        print(f"succ rate3: {health_checkers['create'].statics()}")

        # assert statistic: the target ops succ <50% and the other keep 100% succ
        # delete chaos
        # wait 300s (varies by feature)
        # assert statistic: the target ops succ >90% and the other keep 100% succ
        # terminate thread
        for ch in health_checkers.values():
            ch.terminate()
        pass



