import pytest
from time import sleep

from pymilvus_orm import connections
from checker import CreateChecker, Op
from chaos_opt import ChaosOpt
from utils.util_log import test_log as log
from base.collection_wrapper import ApiCollectionWrapper
from common import common_func as cf
from chaos_commons import *
import constants


class TestChaosBase:
    expect_create = constants.SUCC
    expect_insert = constants.SUCC
    expect_flush = constants.SUCC
    expect_index = constants.SUCC
    expect_search = constants.SUCC
    expect_query = constants.SUCC
    chaos_location = ''
    health_checkers = {}

    def parser_testcase_config(self, chaos_yaml):
        tests_yaml = constants.TESTS_CONFIG_LOCATION + 'testcases.yaml'
        tests_config = gen_experiment_config(tests_yaml)
        test_collections = tests_config.get('Collections', None)
        for t in test_collections:
            test_chaos = t.get('testcase', {}).get('chaos', {})
            if test_chaos in chaos_yaml:
                expects = t.get('testcase', {}).get('expectation', {}) \
                    .get('cluster_1_node', {})
                self.expect_create = expects.get(Op.create, constants.SUCC)
                self.expect_insert = expects.get(Op.insert, constants.SUCC)
                self.expect_flush = expects.get(Op.flush, constants.SUCC)
                self.expect_index = expects.get(Op.index, constants.SUCC)
                self.expect_search = expects.get(Op.search, constants.SUCC)
                self.expect_query = expects.get(Op.query, constants.SUCC)
                return True

        return False


class TestChaos(TestChaosBase):

    @pytest.fixture(scope="function", autouse=True)
    def connection(self):
        connections.add_connection(default={"host": "192.168.1.239", "port": 19530})
        conn = connections.connect(alias='default')
        if conn is None:
            raise Exception("no connections")
        return conn

    @pytest.fixture(scope="function")
    def collection_wrap_4_insert(self, connection):
        c_wrap = ApiCollectionWrapper()
        c_wrap.init_collection(name=cf.gen_unique_str("collection_4_insert"),
                               schema=cf.gen_default_collection_schema(),
                               check_task="check_nothing")
        return c_wrap

    @pytest.fixture(scope="function")
    def collection_wrap_4_flush(self, connection):
        c_wrap = ApiCollectionWrapper()
        c_wrap.init_collection(name=cf.gen_unique_str("collection_4_insert"),
                               schema=cf.gen_default_collection_schema(),
                               check_task="check_nothing")
        return c_wrap

    @pytest.fixture(scope="function")
    def collection_wrap_4_search(self, connection):
        c_wrap = ApiCollectionWrapper()
        c_wrap.init_collection(name=cf.gen_unique_str("collection_4_search_"),
                               schema=cf.gen_default_collection_schema(),
                               check_task="check_nothing")
        c_wrap.insert(data=cf.gen_default_dataframe_data(nb=10000))
        return c_wrap

    @pytest.fixture(scope="function", autouse=True)
    def init_health_checkers(self, connection, collection_wrap_4_insert,
                             collection_wrap_4_flush, collection_wrap_4_search):
        checkers = {}
        # search_ch = SearchChecker(collection_wrap=collection_wrap_4_search)
        # checkers[Op.search] = search_ch
        # insert_ch = InsertFlushChecker(connection=connection,
        #                                collection_wrap=collection_wrap_4_insert)
        # checkers[Op.insert] = insert_ch
        # flush_ch = InsertFlushChecker(connection=connection,
        #                               collection_wrap=collection_wrap_4_flush,
        #                               do_flush=True)
        # checkers[Op.flush] = flush_ch
        create_ch = CreateChecker()
        checkers[Op.create] = create_ch

        self.health_checkers = checkers

    def teardown(self):
        for ch in self.health_checkers.values():
            ch.terminate()
        pass

    @pytest.mark.parametrize('chaos_yaml', get_chaos_yamls())
    def test_chaos(self, chaos_yaml):
        # start the monitor threads to check the milvus ops
        start_monitor_threads(self.health_checkers)

        # parse chaos object
        print("test.start")
        chaos_config = gen_experiment_config(chaos_yaml)
        log.debug(chaos_config)

        # parse the test expectations in testcases.yaml
        self.parser_testcase_config(chaos_yaml)

        # wait 120s
        sleep(1)

        # assert statistic:all ops 100% succ
        assert_statistic(self.health_checkers)

        # reset counting
        reset_counting(self.health_checkers)

        # apply chaos object
        # chaos_opt = ChaosOpt(chaos_config['kind'])
        # chaos_opt.create_chaos_object(chaos_config)

        # wait 120s
        sleep(1)

        # assert statistic
        assert_statistic(self.health_checkers, expectations={Op.create: self.expect_create,
                                                             Op.insert: self.expect_insert,
                                                             Op.flush: self.expect_flush,
                                                             Op.index: self.expect_index,
                                                             Op.search: self.expect_search,
                                                             Op.query: self.expect_query
                                                             })
        #
        # delete chaos
        # meta_name = chaos_config.get('metadata', None).get('name', None)
        # chaos_opt.delete_chaos_object(meta_name)

        # reset counting again
        reset_counting(self.health_checkers)

        # wait 300s (varies by feature)
        sleep(1)

        # assert statistic: all ops success again
        assert_statistic(self.health_checkers)

        # terminate thread
        for ch in self.health_checkers.values():
            ch.terminate()
        # log.debug("*******************Test Completed.*******************")


