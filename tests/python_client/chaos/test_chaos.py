import threading
import pytest
import os
import time
import json
from time import sleep

from pymilvus import connections
from chaos.checker import (CreateChecker, InsertFlushChecker,
                           SearchChecker, QueryChecker, IndexChecker, Op)
from common.cus_resource_opts import CustomResourceOperations as CusResource
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, get_pod_list
from utils.util_common import findkeys
from chaos import chaos_commons as cc
from chaos.chaos_commons import assert_statistic
from common.common_type import CaseLabel
from chaos import constants
from delayed_assert import assert_expectations


def check_cluster_nodes(chaos_config):
     
    # if all pods will be effected, the expect is all fail. 
    # Even though the replicas is greater than 1, it can not provide HA, so cluster_nodes is set as 1 for this situation.
    if "all" in chaos_config["metadata"]["name"]:
        return 1
    
    selector = findkeys(chaos_config, "selector")
    selector = list(selector)
    log.info(f"chaos target selector: {selector}")
    # assert len(selector) == 1
    selector = selector[0] # chaos yaml file must place the effected pod selector in the first position
    namespace = selector["namespaces"][0]
    labels_dict = selector["labelSelectors"]
    labels_list = []
    for k,v in labels_dict.items():
        labels_list.append(k+"="+v)
    labels_str = ",".join(labels_list)
    pods = get_pod_list(namespace, labels_str)
    return len(pods)

def record_results(checkers):
    res = ""
    for k in checkers.keys():
        check_result = checkers[k].check_result()
        res += f"{str(k):10} {check_result}\n"
    return res


class TestChaosBase:
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

    def parser_testcase_config(self, chaos_yaml, chaos_config):
        cluster_nodes = check_cluster_nodes(chaos_config)
        tests_yaml = constants.TESTS_CONFIG_LOCATION + 'testcases.yaml'
        tests_config = cc.gen_experiment_config(tests_yaml)
        test_collections = tests_config.get('Collections', None)
        for t in test_collections:
            test_chaos = t.get('testcase', {}).get('chaos', {})
            if test_chaos in chaos_yaml:
                expects = t.get('testcase', {}).get('expectation', {}).get('cluster_1_node', {})
                # for the cluster_n_node
                if cluster_nodes > 1:
                    expects = t.get('testcase', {}).get('expectation', {}).get('cluster_n_node', {})
                log.info(f"yaml.expects: {expects}")
                self.expect_create = expects.get(Op.create.value, constants.SUCC)
                self.expect_insert = expects.get(Op.insert.value, constants.SUCC)
                self.expect_flush = expects.get(Op.flush.value, constants.SUCC)
                self.expect_index = expects.get(Op.index.value, constants.SUCC)
                self.expect_search = expects.get(Op.search.value, constants.SUCC)
                self.expect_query = expects.get(Op.query.value, constants.SUCC)
                log.info(f"self.expects: create:{self.expect_create}, insert:{self.expect_insert}, "
                         f"flush:{self.expect_flush}, index:{self.expect_index}, "
                         f"search:{self.expect_search}, query:{self.expect_query}")
                return True

        return False


class TestChaos(TestChaosBase):

    @pytest.fixture(scope="function", autouse=True)
    def connection(self, host, port):
        connections.add_connection(default={"host": host, "port": port})
        connections.connect(alias='default')
        
        if connections.has_connection("default") is False:
            raise Exception("no connections")
        self.host = host
        self.port = port

    @pytest.fixture(scope="function", autouse=True)
    def init_health_checkers(self):
        checkers = {
            Op.create: CreateChecker(),
            Op.insert: InsertFlushChecker(),
            Op.flush: InsertFlushChecker(flush=True),
            Op.index: IndexChecker(),
            Op.search: SearchChecker(),
            Op.query: QueryChecker()
        }
        self.health_checkers = checkers

    def teardown(self):
        chaos_res = CusResource(kind=self._chaos_config['kind'],
                                group=constants.CHAOS_GROUP,
                                version=constants.CHAOS_VERSION,
                                namespace=constants.CHAOS_NAMESPACE)
        meta_name = self._chaos_config.get('metadata', None).get('name', None)
        chaos_res.delete(meta_name, raise_ex=False)
        sleep(2)
        log.info(f'Alive threads: {threading.enumerate()}')

    @pytest.mark.tags(CaseLabel.L3)
    @pytest.mark.parametrize('chaos_yaml', cc.get_chaos_yamls())
    def test_chaos(self, chaos_yaml):
        # start the monitor threads to check the milvus ops
        log.info("*********************Chaos Test Start**********************")
        log.info(f"chaos_yaml: {chaos_yaml}")
        log.info(connections.get_connection_addr('default'))
        cc.start_monitor_threads(self.health_checkers)

        # parse chaos object
        chaos_config = cc.gen_experiment_config(chaos_yaml)
        release_name = constants.RELEASE_NAME
        log.info(f"release_name: {release_name}")
        chaos_config['metadata']['name'] = release_name
        kind = chaos_config['kind']
        meta_name = chaos_config.get('metadata', None).get('name', None)
        chaos_config_str = json.dumps(chaos_config)
        chaos_config_str = chaos_config_str.replace("milvus-chaos", release_name)
        chaos_config = json.loads(chaos_config_str)
        self._chaos_config = chaos_config  # cache the chaos config for tear down
        log.info(f"chaos_config: {chaos_config}")
        # parse the test expectations in testcases.yaml
        if self.parser_testcase_config(chaos_yaml, chaos_config) is False:
            log.error("Fail to get the testcase info in testcases.yaml")
            assert False

        # init report
        dir_name = "./reports"
        file_name = f"./reports/{meta_name}.log"
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        # wait 20s
        sleep(constants.WAIT_PER_OP * 2)

        # assert statistic:all ops 100% succ
        log.info("******1st assert before chaos: ")
        assert_statistic(self.health_checkers)
        try:
            with open(file_name, "a+") as f:
                ts = time.strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{meta_name}-{ts}\n")
                f.write("1st assert before chaos:\n")
                f.write(record_results(self.health_checkers))
        except Exception as e:
            log.info(f"Fail to write to file: {e}")
        # apply chaos object
        chaos_res = CusResource(kind=chaos_config['kind'],
                                group=constants.CHAOS_GROUP,
                                version=constants.CHAOS_VERSION,
                                namespace=constants.CHAOS_NAMESPACE)
        chaos_res.create(chaos_config)
        log.info("chaos injected")
        # verify the chaos is injected
        log.info(f"kubectl get {kind} {meta_name} -n {constants.CHAOS_NAMESPACE}")
        os.system(f"kubectl get {kind} {meta_name} -n {constants.CHAOS_NAMESPACE}")
        sleep(constants.WAIT_PER_OP * 2)
        # reset counting
        cc.reset_counting(self.health_checkers)

        # wait 120s
        sleep(constants.CHAOS_DURATION)
        log.info(f'Alive threads: {threading.enumerate()}')

        # assert statistic
        log.info("******2nd assert after chaos injected: ")
        assert_statistic(self.health_checkers,
                         expectations={Op.create: self.expect_create,
                                       Op.insert: self.expect_insert,
                                       Op.flush: self.expect_flush,
                                       Op.index: self.expect_index,
                                       Op.search: self.expect_search,
                                       Op.query: self.expect_query
                                       })
        try:
            with open(file_name, "a+") as f:
                f.write("2nd assert after chaos injected:\n")
                f.write(record_results(self.health_checkers))
        except Exception as e:
            log.error(f"Fail to write the report: {e}")
        # delete chaos
        chaos_res.delete(meta_name)
        log.info("chaos deleted")
        # verify the chaos is deleted
        log.info(f"kubectl get {kind} {meta_name} -n {constants.CHAOS_NAMESPACE}")
        os.system(f"kubectl get {kind} {meta_name} -n {constants.CHAOS_NAMESPACE}")
        log.info(f'Alive threads: {threading.enumerate()}')
        sleep(2)
        # wait all pods ready
        log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label app.kubernetes.io/instance={meta_name}")
        wait_pods_ready(constants.CHAOS_NAMESPACE, f"app.kubernetes.io/instance={meta_name}")
        log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label release={meta_name}")
        wait_pods_ready(constants.CHAOS_NAMESPACE, f"release={meta_name}")
        log.info("all pods are ready")
        # reconnect if needed
        sleep(constants.WAIT_PER_OP * 2)
        cc.reconnect(connections, alias='default')
        # reset counting again
        cc.reset_counting(self.health_checkers)
        # wait 50s (varies by feature)
        sleep(constants.WAIT_PER_OP * 5)
        # assert statistic: all ops success again
        log.info("******3rd assert after chaos deleted: ")
        assert_statistic(self.health_checkers)
        try:
            with open(file_name, "a+") as f:
                f.write("3rd assert after chaos deleted:\n")
                f.write(record_results(self.health_checkers))
        except Exception as e:
            log.info(f"Fail to write the report: {e}")
        # assert all expectations
        assert_expectations()

        log.info("*********************Chaos Test Completed**********************")