
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
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, get_pod_list, get_pod_ip_name_pairs
from utils.util_common import findkeys
from chaos import chaos_commons as cc
from common.common_type import CaseLabel
from chaos import constants
from delayed_assert import expect, assert_expectations


def assert_statistic(checkers, expectations={}):
    for k in checkers.keys():
        # expect succ if no expectations
        succ_rate = checkers[k].succ_rate()
        total = checkers[k].total()
        average_time = checkers[k].average_time
        if expectations.get(k, '') == constants.FAIL:
            log.info(
                f"Expect Fail: {str(k)} succ rate {succ_rate}, total: {total}, average time: {average_time:.4f}")
            expect(succ_rate < 0.49 or total < 2,
                   f"Expect Fail: {str(k)} succ rate {succ_rate}, total: {total}, average time: {average_time:.4f}")
        else:
            log.info(
                f"Expect Succ: {str(k)} succ rate {succ_rate}, total: {total}, average time: {average_time:.4f}")
            expect(succ_rate > 0.90 and total > 2,
                   f"Expect Succ: {str(k)} succ rate {succ_rate}, total: {total}, average time: {average_time:.4f}")


def check_cluster_nodes(chaos_config):

    # if all pods will be effected, the expect is all fail.
    # Even though the replicas is greater than 1, it can not provide HA, so cluster_nodes is set as 1 for this situation.
    if "all" in chaos_config["metadata"]["name"]:
        return 1

    selector = findkeys(chaos_config, "selector")
    selector = list(selector)
    log.info(f"chaos target selector: {selector}")
    # assert len(selector) == 1
    # chaos yaml file must place the effected pod selector in the first position
    selector = selector[0]
    namespace = selector["namespaces"][0]
    labels_dict = selector["labelSelectors"]
    labels_list = []
    for k, v in labels_dict.items():
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
                expects = t.get('testcase', {}).get(
                    'expectation', {}).get('cluster_1_node', {})
                # for the cluster_n_node
                if cluster_nodes > 1:
                    expects = t.get('testcase', {}).get(
                        'expectation', {}).get('cluster_n_node', {})
                log.info(f"yaml.expects: {expects}")
                self.expect_create = expects.get(
                    Op.create.value, constants.SUCC)
                self.expect_insert = expects.get(
                    Op.insert.value, constants.SUCC)
                self.expect_flush = expects.get(Op.flush.value, constants.SUCC)
                self.expect_index = expects.get(Op.index.value, constants.SUCC)
                self.expect_search = expects.get(
                    Op.search.value, constants.SUCC)
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
            Op.search: SearchChecker(replica_number=2),
            Op.query: QueryChecker(replica_number=2)
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
    # @pytest.mark.parametrize('chaos_yaml', "chaos/chaos_objects/template/pod-failure-by-pod-list.yaml")
    def test_chaos(self):
        # start the monitor threads to check the milvus ops
        log.info("*********************Chaos Test Start**********************")
        # log.info(f"chaos_yaml: {chaos_yaml}")
        log.info(connections.get_connection_addr('default'))
        cc.start_monitor_threads(self.health_checkers)

        # get replicas info
        release_name = "milvus-multi-querynode"
        replicas_info, _ = self.health_checkers[Op.search].c_wrap.get_replicas()
        querynode_ip_pod_pair = get_pod_ip_name_pairs(
            "chaos-testing", "app.kubernetes.io/instance=milvus-multi-querynode, component=querynode")
        querynode_id_pod_pair = {}
        ms = MilvusSys()
        for node in ms.query_nodes:
            ip = node["infos"]['hardware_infos']["ip"].split(":")[0]
            querynode_id_pod_pair[node["identifier"]
                                  ] = querynode_ip_pod_pair[ip]
        group_list = []
        for g in replicas_info.groups:
            group_list.append(list(g.group_nodes))
        # keep only one group in healthy status, other groups all are unhealthy by injecting chaos,
        # In the effected groups, each group has one pod is in pod failure status 
        target_pod_list = []
        for g in group_list[1:]:
            pod = querynode_id_pod_pair[g[0]]
            target_pod_list.append(pod)
        chaos_config = cc.gen_experiment_config("chaos/chaos_objects/template/pod-failure-by-pod-list.yaml")
        chaos_config['metadata']['name'] = f"test-multi-replicase-{int(time.time())}"
        kind = chaos_config['kind']
        meta_name = chaos_config.get('metadata', None).get('name', None)
        chaos_config['spec']['selector']['pods']['chaos-testing'] = target_pod_list
        self._chaos_config = chaos_config  # cache the chaos config for tear down
        
        log.info(f"chaos_config: {chaos_config}")
        # wait 20s
        sleep(constants.WAIT_PER_OP * 2)
        # replicas info
        replicas_info, _ = self.health_checkers[Op.search].c_wrap.get_replicas()
        log.info(f"replicas_info for collection {self.health_checkers[Op.search].c_wrap.name}: {replicas_info}")

        replicas_info, _ = self.health_checkers[Op.query].c_wrap.get_replicas()
        log.info(f"replicas_info for collection {self.health_checkers[Op.query].c_wrap.name}: {replicas_info}")        

        # assert statistic:all ops 100% succ
        log.info("******1st assert before chaos: ")
        assert_statistic(self.health_checkers)
        # apply chaos object
        chaos_res = CusResource(kind=chaos_config['kind'],
                                group=constants.CHAOS_GROUP,
                                version=constants.CHAOS_VERSION,
                                namespace=constants.CHAOS_NAMESPACE)
        chaos_res.create(chaos_config)
        log.info("chaos injected")
        sleep(constants.WAIT_PER_OP * 2)
        # reset counting
        cc.reset_counting(self.health_checkers)

        # wait 120s
        sleep(constants.CHAOS_DURATION)
        log.info(f'Alive threads: {threading.enumerate()}')
        # replicas info
        replicas_info, _ = self.health_checkers[Op.search].c_wrap.get_replicas()
        log.info(f"replicas_info for collection {self.health_checkers[Op.search].c_wrap.name}: {replicas_info}")

        replicas_info, _ = self.health_checkers[Op.query].c_wrap.get_replicas()
        log.info(f"replicas_info for collection {self.health_checkers[Op.query].c_wrap.name}: {replicas_info}") 
        # assert statistic
        log.info("******2nd assert after chaos injected: ")
        assert_statistic(self.health_checkers,
                         expectations={
                                       Op.search: constants.SUCC,
                                       Op.query: constants.SUCC
                                       })
        # delete chaos
        chaos_res.delete(meta_name)
        log.info("chaos deleted")
        sleep(2)
        # wait all pods ready
        log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label app.kubernetes.io/instance={release_name}")
        wait_pods_ready(constants.CHAOS_NAMESPACE,f"app.kubernetes.io/instance={release_name}")
        log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label release={release_name}")
        wait_pods_ready(constants.CHAOS_NAMESPACE, f"release={release_name}")
        log.info("all pods are ready")
        # reconnect if needed
        sleep(constants.WAIT_PER_OP * 2)
        cc.reconnect(connections, alias='default')
        # reset counting again
        cc.reset_counting(self.health_checkers)
        # wait 50s (varies by feature)
        sleep(constants.WAIT_PER_OP * 5)
        # replicas info
        replicas_info, _ = self.health_checkers[Op.search].c_wrap.get_replicas()
        log.info(f"replicas_info for collection {self.health_checkers[Op.search].c_wrap.name}: {replicas_info}")

        replicas_info, _ = self.health_checkers[Op.query].c_wrap.get_replicas()
        log.info(f"replicas_info for collection {self.health_checkers[Op.query].c_wrap.name}: {replicas_info}")         
        # assert statistic: all ops success again
        log.info("******3rd assert after chaos deleted: ")
        assert_statistic(self.health_checkers)
        # assert all expectations
        assert_expectations()

        log.info("*********************Chaos Test Completed**********************")
