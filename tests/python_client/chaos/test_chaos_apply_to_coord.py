import threading
import pytest
import time
from time import sleep
from pathlib import Path
from pymilvus import connections
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.milvus_sys import MilvusSys
import logging as log
from utils.util_k8s import wait_pods_ready, get_milvus_instance_name, get_milvus_deploy_tool, find_activate_standby_coord_pod
from utils.util_common import update_key_value, update_key_name, gen_experiment_config
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

    def test_chaos_apply(self, chaos_type, role_type, target_component, chaos_duration, chaos_interval):
        # start the monitor threads to check the milvus ops
        log.info("*********************Chaos Test Start**********************")
        log.info(connections.get_connection_addr('default'))

        activate_pod_list, standby_pod_list = find_activate_standby_coord_pod(self.milvus_ns, self.release_name,
                                                                              target_component)
        log.info(f"activated pod list: {activate_pod_list}, standby pod list: {standby_pod_list}")
        target_pod_list = activate_pod_list + standby_pod_list
        if role_type == "standby":
            target_pod_list = standby_pod_list
        if role_type == "activated":
            target_pod_list = activate_pod_list
        chaos_type = chaos_type.replace("_", "-")
        chaos_config = gen_experiment_config(f"{str(Path(__file__).absolute().parent)}/"
                                             f"chaos_objects/template/{chaos_type}-by-pod-list.yaml")
        chaos_config['metadata']['name'] = f"test-{target_component}-standby-{int(time.time())}"

        meta_name = chaos_config.get('metadata', None).get('name', None)
        chaos_config['spec']['selector']['pods']['chaos-testing'] = target_pod_list
        self.chaos_config = chaos_config
        # update chaos_duration from string to int with unit second
        chaos_duration = chaos_duration.replace('h', '*3600+').replace('m', '*60+').replace('s', '*1+') + '+0'
        chaos_duration = eval(chaos_duration)
        if self.deploy_by == "milvus-operator":
            update_key_name(chaos_config, "component", "app.kubernetes.io/component")
        self._chaos_config = chaos_config  # cache the chaos config for tear down
        log.info(f"chaos_config: {chaos_config}")
        # apply chaos object
        chaos_res = CusResource(kind=chaos_config['kind'],
                                group=constants.CHAOS_GROUP,
                                version=constants.CHAOS_VERSION,
                                namespace=constants.CHAOS_NAMESPACE)
        chaos_res.create(chaos_config)
        log.info("chaos injected")
        res = chaos_res.list_all()
        chaos_list = [r['metadata']['name'] for r in res['items']]
        assert meta_name in chaos_list
        res = chaos_res.get(meta_name)
        log.info(f"chaos inject result: {res['kind']}, {res['metadata']['name']}")
        sleep(chaos_duration)
        # delete chaos
        chaos_res.delete(meta_name)
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
        log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label app.kubernetes.io/instance={meta_name}")
        wait_pods_ready(constants.CHAOS_NAMESPACE, f"app.kubernetes.io/instance={meta_name}")
        log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label release={meta_name}")
        wait_pods_ready(constants.CHAOS_NAMESPACE, f"release={meta_name}")
        log.info("all pods are ready")
        pods_ready_time = time.time() - t0
        log.info(f"pods ready time: {pods_ready_time}")
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
        time.sleep(30)
        activate_pod_list_after_chaos, standby_pod_list_after_chaos = find_activate_standby_coord_pod(self.milvus_ns, self.release_name,
                                                                                                      target_component)
        log.info(f"activated pod list: {activate_pod_list_after_chaos}, standby pod list: {standby_pod_list_after_chaos}")
        if role_type == "standby":
            # if the standby pod is injected, the activated pod should not be changed
            assert activate_pod_list_after_chaos[0] == activate_pod_list[0]
        if role_type == "activated":
            # if the activated pod is injected, the one of standby pods should be changed to activated
            assert activate_pod_list_after_chaos[0] in standby_pod_list
        log.info("*********************Chaos Test Completed**********************")
