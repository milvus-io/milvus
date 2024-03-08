import pytest
import time
from time import sleep
from pathlib import Path
from datetime import datetime
import json
from pymilvus import connections
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.milvus_sys import MilvusSys
from chaos import chaos_commons as cc
import logging as log
from utils.util_k8s import (wait_pods_ready, get_milvus_instance_name,
                            get_milvus_deploy_tool, get_etcd_leader, get_etcd_followers)
from utils.util_common import wait_signal_to_apply_chaos
import constants


class TestChaosApply:

    @pytest.fixture(scope="function", autouse=True)
    def init_env(self, host, port, user, password, milvus_ns):
        if user and password:
            # log.info(f"connect to {host}:{port} with user {user} and password {password}")
            connections.connect('default', host=host, port=port, user=user, password=password, secure=True)
        else:
            connections.connect('default', host=host, port=port)
        if connections.has_connection("default") is False:
            raise Exception("no connections")
        #
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.milvus_sys = MilvusSys(alias='default')
        self.chaos_ns = constants.CHAOS_NAMESPACE
        self.milvus_ns = milvus_ns
        self.release_name = get_milvus_instance_name(self.milvus_ns, milvus_sys=self.milvus_sys)
        self.deploy_by = get_milvus_deploy_tool(self.milvus_ns, self.milvus_sys)

    def reconnect(self):
        if self.user and self.password:
            connections.connect('default', host=self.host, port=self.port,
                                user=self.user,
                                password=self.password,
                                secure=True)
        else:
            connections.connect('default', host=self.host, port=self.port)
        if connections.has_connection("default") is False:
            raise Exception("no connections")

    def teardown(self):
        chaos_res = CusResource(kind=self.chaos_config['kind'],
                                group=constants.CHAOS_GROUP,
                                version=constants.CHAOS_VERSION,
                                namespace=constants.CHAOS_NAMESPACE)
        meta_name = self.chaos_config.get('metadata', None).get('name', None)
        chaos_res.delete(meta_name, raise_ex=False)
        sleep(2)

    def test_chaos_apply(self, chaos_type, target_pod, chaos_duration, chaos_interval, wait_signal):
        # start the monitor threads to check the milvus ops
        log.info("*********************Chaos Test Start**********************")
        if wait_signal:
            log.info("need wait signal to start chaos")
            ready_for_chaos = wait_signal_to_apply_chaos()
            if not ready_for_chaos:
                log.info("did not get the signal to apply chaos")
            else:
                log.info("get the signal to apply chaos")
        log.info(connections.get_connection_addr('default'))
        release_name = self.release_name
        deploy_tool = get_milvus_deploy_tool(self.milvus_ns, self.milvus_sys)
        target_pod_list = []
        if target_pod in ['etcd_leader', 'etcd-leader']:
            etcd_leader = get_etcd_leader(release_name, deploy_tool)
            if etcd_leader is None:
                raise Exception("no etcd leader")
            target_pod_list.append(etcd_leader)
        if target_pod in ['etcd_followers', 'etcd-followers']:
            etcd_followers = get_etcd_followers(release_name, deploy_tool)
            if etcd_followers is None:
                raise Exception("no etcd followers")
            target_pod_list.extend(etcd_followers)
            if len(target_pod_list) >=2:
                # only choose one follower to apply chaos
                target_pod_list = target_pod_list[:1]
        log.info(f"target_pod_list: {target_pod_list}")
        chaos_type = chaos_type.replace('_', '-')
        chaos_config = cc.gen_experiment_config(f"{str(Path(__file__).absolute().parent)}/chaos_objects/template/{chaos_type}-by-pod-list.yaml")
        chaos_config['metadata']['name'] = f"test-{target_pod.replace('_', '-')}-{chaos_type}-{int(time.time())}"
        meta_name = chaos_config.get('metadata', None).get('name', None)
        chaos_config['spec']['selector']['pods']['chaos-testing'] = target_pod_list
        self.chaos_config = chaos_config  # cache the chaos config for tear down  # cache the chaos config for tear down
        log.info(f"chaos_config: {chaos_config}")
        # apply chaos object
        chaos_res = CusResource(kind=chaos_config['kind'],
                                group=constants.CHAOS_GROUP,
                                version=constants.CHAOS_VERSION,
                                namespace=constants.CHAOS_NAMESPACE)
        chaos_res.create(chaos_config)
        create_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
        log.info("chaos injected")
        res = chaos_res.list_all()
        chaos_list = [r['metadata']['name'] for r in res['items']]
        assert meta_name in chaos_list
        res = chaos_res.get(meta_name)
        log.info(f"chaos inject result: {res['kind']}, {res['metadata']['name']}")
        chaos_duration = chaos_duration.replace('h', '*3600+').replace('m', '*60+').replace('s', '*1+') + '+0'
        chaos_duration = eval(chaos_duration)
        sleep(chaos_duration)
        # delete chaos
        chaos_res.delete(meta_name)
        delete_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
        log.info("chaos deleted")
        res = chaos_res.list_all()
        chaos_list = [r['metadata']['name'] for r in res['items']]
        # verify the chaos is deleted
        sleep(10)
        res = chaos_res.list_all()
        chaos_list = [r['metadata']['name'] for r in res['items']]
        assert meta_name not in chaos_list
        # wait all pods ready
        t0 = time.time()
        log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label app.kubernetes.io/instance={release_name}")
        wait_pods_ready(constants.CHAOS_NAMESPACE, f"app.kubernetes.io/instance={release_name}")
        log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label release={release_name}")
        wait_pods_ready(constants.CHAOS_NAMESPACE, f"release={release_name}")
        log.info("all pods are ready")
        pods_ready_time = time.time() - t0
        log.info(f"pods ready time: {pods_ready_time}")

        recovery_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
        event_records = {
            "chaos_type": chaos_type,
            "target_component": target_pod,
            "meta_name": meta_name,
            "create_time": create_time,
            "delete_time": delete_time,
            "recovery_time": recovery_time
        }
        # save event records to json file
        with open(constants.CHAOS_INFO_SAVE_PATH, 'w') as f:
            json.dump(event_records, f)
        # reconnect to test the service healthy
        start_time = time.time()
        end_time = start_time + 120
        while time.time() < end_time:
            try:
                self.reconnect()
                break
            except Exception as e:
                log.error(e)
                sleep(2)
        recovery_time = time.time() - start_time
        log.info(f"recovery time: {recovery_time}")
        log.info("*********************Chaos Test Completed**********************")
