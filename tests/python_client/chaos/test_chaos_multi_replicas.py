
import threading

import pytest
import time
import random
from pathlib import Path
from time import sleep

from pymilvus import connections, Collection
from chaos.checker import (InsertFlushChecker, SearchChecker, QueryChecker, Op)
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, get_querynode_id_pod_pairs, get_milvus_instance_name
from chaos import chaos_commons as cc
from common.common_type import CaseLabel
from common import common_func as cf
from chaos import constants
from delayed_assert import expect, assert_expectations

config_file_name = f"{str(Path(__file__).absolute().parent)}/config/multi_replicas_chaos.yaml"


def assert_statistic(checkers, expectations={}):
    for k in checkers.keys():
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
        shards_num = 2
        # replica_number is managed by cluster-level config (clusterLevelLoadReplicaNumber)
        # via replicaResourceGroups in helm chart, no need to pass explicitly
        checkers = {
            Op.insert: InsertFlushChecker(collection_name=c_name, shards_num=shards_num),
            Op.search: SearchChecker(collection_name=c_name, shards_num=shards_num),
            Op.query: QueryChecker(collection_name=c_name, shards_num=shards_num)
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
    @pytest.mark.parametrize("is_streaming", cc.gen_experiment_config(config_file_name)['is_streaming'])
    @pytest.mark.parametrize("failed_group_scope", cc.gen_experiment_config(config_file_name)['failed_group_scope'])
    @pytest.mark.parametrize("failed_node_type", cc.gen_experiment_config(config_file_name)['failed_node_type'])
    @pytest.mark.parametrize("chaos_type", cc.gen_experiment_config(config_file_name)['chaos_type'])
    def test_multi_replicas_with_only_one_group_available(self, chaos_type, failed_node_type, failed_group_scope, is_streaming):
        log.info("*********************Chaos Test Start**********************")
        log.info("Test config")
        log.info(cc.gen_experiment_config(config_file_name))
        log.info(connections.get_connection_addr('default'))

        if is_streaming is False:
            del self.health_checkers[Op.insert]
        cc.start_monitor_threads(self.health_checkers)

        # get replicas info via ORM Collection
        release_name = self.instance_name
        querynode_id_pod_pair = get_querynode_id_pod_pairs(
            constants.CHAOS_NAMESPACE,
            f"app.kubernetes.io/instance={release_name}, component=querynode"
        )
        log.info(f"querynode_id_pod_pair: {querynode_id_pod_pair}")

        # use a checker's collection to get replica info
        search_c_name = self.health_checkers[Op.search].c_name
        col = Collection(name=search_c_name, using='default')
        replicas_info = col.get_replicas()
        log.info(f"replicas_info: {replicas_info}")

        group_list = []
        shard_leader_list = []
        for g in replicas_info.groups:
            group_list.append(list(g.group_nodes))
            for shard in g.shards:
                shard_leader_list.append(shard.shard_leader)

        log.info(f"group_list: {group_list}")
        log.info(f"shard_leader_list: {shard_leader_list}")

        # select target pods based on parameters
        target_pod_list = []
        target_group = []
        group_list = sorted(group_list, key=lambda x: -len(x))
        if failed_group_scope == "one":
            target_group = random.sample(group_list, 1)
        if failed_group_scope == "except_one":
            target_group = random.sample(group_list, len(group_list) - 1)
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

        # prepare chaos config
        chaos_config = cc.gen_experiment_config(
            f"{str(Path(__file__).absolute().parent)}/chaos_objects/template/{chaos_type}-by-pod-list.yaml"
        )
        chaos_config['metadata']['name'] = f"test-multi-replicas-{int(time.time())}"
        meta_name = chaos_config.get('metadata', None).get('name', None)
        chaos_config['spec']['selector']['pods']['chaos-testing'] = target_pod_list
        self._chaos_config = chaos_config

        log.info(f"chaos_config: {chaos_config}")
        sleep(constants.WAIT_PER_OP * 2)

        # replicas info before chaos
        replicas_info = col.get_replicas()
        log.info(f"replicas_info for search collection {search_c_name}: {replicas_info}")

        # 1st assert: all ops 100% succ before chaos
        log.info("******1st assert before chaos: ")
        assert_statistic(self.health_checkers)

        # apply chaos
        chaos_res = CusResource(kind=chaos_config['kind'],
                                group=constants.CHAOS_GROUP,
                                version=constants.CHAOS_VERSION,
                                namespace=constants.CHAOS_NAMESPACE)
        chaos_res.create(chaos_config)
        log.info("chaos injected")
        sleep(constants.WAIT_PER_OP * 2)

        # reset counting
        cc.reset_counting(self.health_checkers)

        # wait for chaos duration
        sleep(constants.CHAOS_DURATION)
        log.info(f'Alive threads: {threading.enumerate()}')

        # node info during chaos
        querynode_id_pod_pair = get_querynode_id_pod_pairs(
            constants.CHAOS_NAMESPACE,
            f"app.kubernetes.io/instance={release_name}, component=querynode"
        )
        log.info(f"querynode_id_pod_pair during chaos: {querynode_id_pod_pair}")

        # replicas info during chaos
        try:
            replicas_info = col.get_replicas()
            log.info(f"replicas_info for search collection {search_c_name}: {replicas_info}")
        except Exception as e:
            log.warning(f"get_replicas failed during chaos: {e}")

        # 2nd assert: check expectations based on failed_group_scope
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

        # wait for recovery
        sleep(constants.WAIT_PER_OP * 2)
        cc.reset_counting(self.health_checkers)
        sleep(constants.WAIT_PER_OP * 5)

        # node info after recovery
        querynode_id_pod_pair = get_querynode_id_pod_pairs(
            constants.CHAOS_NAMESPACE,
            f"app.kubernetes.io/instance={release_name}, component=querynode"
        )
        log.info(f"querynode_id_pod_pair after recovery: {querynode_id_pod_pair}")

        sleep(30)

        # replicas info after recovery
        replicas_info = col.get_replicas()
        log.info(f"replicas_info for collection {search_c_name}: {replicas_info}")

        # 3rd assert: all ops success again after chaos deleted
        log.info("******3rd assert after chaos deleted: ")
        assert_statistic(self.health_checkers)
        assert_expectations()

        log.info("*********************Chaos Test Completed**********************")
