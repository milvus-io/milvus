import logging

import pytest
import sys
import threading
from time import sleep

from pymilvus_orm import connections, utility
from checker import CreateChecker, SearchChecker, InsertAndFlushChecker
from base.collection_wrapper import ApiCollectionWrapper
from common import common_func as cf
from utils.util_log import test_log as log
from checker import Op


def reset_counting(checkers={}):
    for ch in checkers.values():
        ch.reset()


class TestsChaos:
    @pytest.fixture(scope="function", autouse=True)
    def connection(self):
        connections.add_connection(default={"host": "192.168.1.239", "port": 19530})
        conn = connections.connect(alias='default')
        if conn is None:
            raise Exception("no connections")
        return conn

    @pytest.fixture(scope="function", autouse=True)
    def collection_wrap_4_insert(self, connection):
        c_wrap = ApiCollectionWrapper()
        c_wrap.init_collection(name=cf.gen_unique_str("collection_4_insert"),
                               schema=cf.gen_default_collection_schema(),
                               check_task="check_nothing")
        return c_wrap

    @pytest.fixture(scope="function", autouse=True)
    def collection_wrap_4_search(self, connection):
        c_wrap = ApiCollectionWrapper()
        _, result = c_wrap.init_collection(name=cf.gen_unique_str("collection_4_search_"),
                                           schema=cf.gen_default_collection_schema(),
                                           check_task="check_nothing")
        if result is False:
            log.log("result: ")
        return c_wrap

    @pytest.fixture(scope="function", autouse=True)
    def h_chk(self, connection, collection_wrap_4_insert, collection_wrap_4_search):
        checkers = {}
        # search_ch = SearchChecker(collection_wrap=collection_wrap_4_search)
        # checkers["search"] = search_ch
        insert_n_flush_ch = InsertAndFlushChecker(connection=connection,
                                                  collection_wrap=collection_wrap_4_insert)
        checkers[Op.insert_n_flush] = insert_n_flush_ch
        create_ch = CreateChecker()
        checkers[Op.create] = create_ch

        return checkers

    '''
    def teardown(self, health_checkers):
        for ch in health_checkers.values():
            ch.terminate()
        pass
    '''

    def test_chaos(self, h_chk):
        # start the monitor threads to check the milvus ops
        log.debug("***********************Test_chaos Starting.**************")
        for k in h_chk.keys():
            v = h_chk[k]
            t = threading.Thread(target=v.keep_running, args=())
            t.start()
            log.debug("checker %s start..." % k)

        # parse chaos object
        # find the testcase by chaos ops in testcases
        # parse the test expectations
        # wait 120s
        sleep(10)
        log.debug("create succ/total count1: %s, %s "
                  % (str(h_chk[Op.create]._succ), str(h_chk[Op.create].total())))
        log.debug("insert succ/total count1: %s, %s "
                  % (str(h_chk[Op.insert_n_flush]._succ),
                     str(h_chk[Op.insert_n_flush].total())))
        log.debug("flush succ/total count1: %s, %s "
                  % (str(h_chk[Op.insert_n_flush]._flush_succ),
                     str(h_chk[Op.insert_n_flush].flush_total())))
        # assert statistic:all ops 100% succ
        # reset counting
        # apply chaos object
        # wait 300s (varies by chaos)
        reset_counting(h_chk)
        log.debug("reset !!!!!!!")
        log.debug("create succ/total count2: %s, %s "
                  % (str(h_chk[Op.create]._succ), str(h_chk[Op.create].total())))
        log.debug("insert succ/total count2: %s, %s "
                  % (str(h_chk[Op.insert_n_flush]._succ), str(h_chk[Op.insert_n_flush].total())))
        log.debug("flush succ/total count2: %s, %s "
                  % (str(h_chk[Op.insert_n_flush]._flush_succ), str(h_chk[Op.insert_n_flush].flush_total())))
        sleep(10)
        log.debug("after a few seconds....")
        log.debug("create succ/total count3: %s, %s "
                  % (str(h_chk[Op.create]._succ), str(h_chk[Op.create].total())))
        log.debug("insert succ/total count3: %s, %s "
                  % (str(h_chk[Op.insert_n_flush]._succ), str(h_chk[Op.insert_n_flush].total())))
        log.debug("flush succ/total count3: %s, %s "
                  % (str(h_chk[Op.insert_n_flush]._flush_succ), str(h_chk[Op.insert_n_flush].flush_total())))

        # assert statistic: the target ops succ <50% and the other keep 100% succ
        # delete chaos
        # wait 300s (varies by feature)
        # assert statistic: the target ops succ >90% and the other keep 100% succ
        # terminate thread
        for ch in h_chk.values():
            ch.terminate()
        log.debug("*******************Test Completed.*******************")
        '''
        for c_name in utility.list_collections():
            if "CreateChecker_" in c_name:
                c_wrap = ApiCollectionWrapper()
                c_wrap.init_collection(c_name).drop()
        pass
        '''
