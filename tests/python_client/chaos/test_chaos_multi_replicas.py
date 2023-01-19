
import threading

import pytest
import time
import random
from pathlib import Path
from time import sleep

from pymilvus import connections
from chaos.checker import (InsertFlushChecker, SearchChecker, QueryChecker, Op)
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, get_pod_ip_name_pairs, get_milvus_instance_name
from chaos import chaos_commons as cc
from common.common_type import CaseLabel
from common import common_func as cf
from chaos import constants
from delayed_assert import expect, assert_expectations

config_file_name = f"{str(Path(__file__).absolute().parent)}/config/multi_replicas_chaos.yaml"

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


def record_results(checkers):
    res = ""
    for k in checkers.keys():
        check_result = checkers[k].check_result()
        res += f"{str(k):10} {check_result}\n"
    return res


def get_querynode_info(release_name):
    querynode_id_pod_pair = {}
    querynode_ip_pod_pair = get_pod_ip_name_pairs(
        "chaos-testing", f"app.kubernetes.io/instance={release_name}, component=querynode")
    ms = MilvusSys()
    for node in ms.query_nodes:
        ip = node["infos"]['hardware_infos']["ip"].split(":")[0]
        querynode_id_pod_pair[node["identifier"]] = querynode_ip_pod_pair[ip]
    return querynode_id_pod_pair


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


class TestChaos(TestChaosBase):

    @pytest.fixture(scope="function", autouse=True)
    def connection(self, host, port):
        connections.add_connection(default={"host": host, "port": port})
        connections.connect(alias='default')

        if connections.has_connection("default") is False:
            raise Exception("no connections")
        self.host = host
        self.port = port
        self.instance_name = get_milvus_instance_name(constants.CHAOS_NAMESPACE, host)

    @pytest.fixture(scope="function", autouse=True)
    def init_health_checkers(self):
        c_name = cf.gen_unique_str('MultiReplicasChecker_')
        replicas_num = 2
        shards_num = 2
        checkers = {
            Op.insert: InsertFlushChecker(collection_name=c_name, shards_num=shards_num),
            Op.search: SearchChecker(collection_name=c_name, shards_num=shards_num, replica_number=replicas_num),
            Op.query: QueryChecker(collection_name=c_name, shards_num=shards_num, replica_number=replicas_num)
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
    @pytest.mark.parametrize("is_streaming", cc.gen_experiment_config(config_file_name)['is_streaming'])  # [False, True]
    @pytest.mark.parametrize("failed_group_scope", cc.gen_experiment_config(config_file_name)['failed_group_scope'])  # ["one", "except_one" "all"]
    @pytest.mark.parametrize("failed_node_type", cc.gen_experiment_config(config_file_name)['failed_node_type'])  # ["non_shard_leader", "shard_leader"]
    @pytest.mark.parametrize("chaos_type", cc.gen_experiment_config(config_file_name)['chaos_type'])  # ["pod-failure", "pod-kill"]
    def test_multi_replicas_with_only_one_group_available(self, chaos_type, failed_node_type, failed_group_scope, is_streaming):
        # start the monitor threads to check the milvus ops
        log.info("*********************Chaos Test Start**********************")
        log.info("Test config")
        log.info(cc.gen_experiment_config(config_file_name))
        # log.info(f"chaos_yaml: {chaos_yaml}")
        log.info(connections.get_connection_addr('default'))
        if is_streaming is False:
            del self.health_checkers[Op.insert]
        cc.start_monitor_threads(self.health_checkers)
        # get replicas info
        release_name = self.instance_name
        querynode_id_pod_pair = get_querynode_info(release_name)
        log.info(querynode_id_pod_pair)
        group_list = []
        shard_leader_list = []
        replicas_info, _ = self.health_checkers[Op.search].c_wrap.get_replicas()
        for g in replicas_info.groups:
            group_list.append(list(g.group_nodes))
            for shard in g.shards:
                shard_leader_list.append(shard.shard_leader)
        # keep only one group in healthy status, other groups will be unhealthy by injecting pod failure chaos,
        # In the effected groups, each group has one pod is in pod failure status 
        target_pod_list = []
        target_group = []
        group_list = sorted(group_list, key=lambda x: -len(x))
        if failed_group_scope == "one":
            target_group = random.sample(group_list, 1)
        if failed_group_scope == "except_one":
            target_group = random.sample(group_list, len(group_list)-1)
        if failed_group_scope == "all":
            target_group = group_list[:]
        for g in target_group:
            target_nodes = []
            if failed_node_type == "shard_leader":
                target_nodes = list(set(g) & set(shard_leader_list))
            if failed_node_type == "non_shard_leader":
                target_nodes = list(set(g) - set(shard_leader_list))
            if len(target_nodes) == 0:
                log.info("there is no node satisfied, chose one randomly")
                target_nodes = [random.choice(g)]
            for target_node in target_nodes:
                pod = querynode_id_pod_pair[target_node]
                target_pod_list.append(pod)
        log.info(f"target_pod_list: {target_pod_list}")
        chaos_config = cc.gen_experiment_config(f"{str(Path(__file__).absolute().parent)}/chaos_objects/template/{chaos_type}-by-pod-list.yaml")
        chaos_config['metadata']['name'] = f"test-multi-replicase-{int(time.time())}"
        meta_name = chaos_config.get('metadata', None).get('name', None)
        chaos_config['spec']['selector']['pods']['chaos-testing'] = target_pod_list
        self._chaos_config = chaos_config  # cache the chaos config for tear down
        
        log.info(f"chaos_config: {chaos_config}")
        # wait 20s
        sleep(constants.WAIT_PER_OP * 2)
        # replicas info
        replicas_info, _ = self.health_checkers[Op.search].c_wrap.get_replicas()
        log.info(f"replicas_info for search collection {self.health_checkers[Op.search].c_wrap.name}: {replicas_info}")

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
        # node info
        querynode_id_pod_pair = get_querynode_info(release_name)
        log.info(querynode_id_pod_pair)
        # replicas info
        replicas_info, _ = self.health_checkers[Op.search].c_wrap.get_replicas()
        log.info(f"replicas_info for search collection {self.health_checkers[Op.search].c_wrap.name}: {replicas_info}")

        replicas_info, _ = self.health_checkers[Op.query].c_wrap.get_replicas()
        log.info(f"replicas_info for query collection {self.health_checkers[Op.query].c_wrap.name}: {replicas_info}")
        # assert statistic
        log.info("******2nd assert after chaos injected: ")
        expectations = {
            Op.search: constants.SUCC,
            Op.query: constants.SUCC
        }
        if failed_group_scope == "all":
            expectations = {
                Op.search: constants.FAIL,
                Op.query: constants.FAIL
            }
        assert_statistic(self.health_checkers, expectations=expectations)
        # delete chaos
        chaos_res.delete(meta_name)
        log.info("chaos deleted")
        sleep(2)
        # wait all pods ready
        log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label app.kubernetes.io/instance={release_name}")
        ready_1 = wait_pods_ready(constants.CHAOS_NAMESPACE, f"app.kubernetes.io/instance={release_name}")
        log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label release={release_name}")
        ready_2 = wait_pods_ready(constants.CHAOS_NAMESPACE, f"release={release_name}")
        if ready_1 and ready_2:
            log.info("all pods are ready")
        # reconnect if needed
        sleep(constants.WAIT_PER_OP * 2)
        # cc.reconnect(connections, alias='default')
        # reset counting again
        cc.reset_counting(self.health_checkers)
        # wait 50s (varies by feature)
        sleep(constants.WAIT_PER_OP * 5)
        # node info
        querynode_id_pod_pair = get_querynode_info(release_name)
        log.info(querynode_id_pod_pair)
        sleep(30)
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
