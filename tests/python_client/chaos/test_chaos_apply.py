import threading
import pytest
import time
from time import sleep
from pathlib import Path
from pymilvus import connections
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.milvus_sys import MilvusSys
import logging as log
from utils.util_k8s import wait_pods_ready, get_milvus_instance_name, get_milvus_deploy_tool
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

    def test_chaos_apply(self, chaos_type, target_component, chaos_duration, chaos_interval):
        # start the monitor threads to check the milvus ops
        log.info("*********************Chaos Test Start**********************")
        log.info(connections.get_connection_addr('default'))
        release_name = self.release_name
        chaos_config = gen_experiment_config(
            f"{str(Path(__file__).absolute().parent)}/chaos_objects/{chaos_type.replace('-', '_')}/chaos_{target_component}_{chaos_type.replace('-', '_')}.yaml")
        chaos_config['metadata']['name'] = f"test-{target_component}-{chaos_type.replace('_','-')}-{int(time.time())}"
        chaos_config['metadata']['namespace'] = self.chaos_ns
        meta_name = chaos_config.get('metadata', None).get('name', None)
        update_key_value(chaos_config, "release", release_name)
        update_key_value(chaos_config, "app.kubernetes.io/instance", release_name)
        update_key_value(chaos_config, "namespaces", [self.milvus_ns])
        self.chaos_config = chaos_config
        if "s" in chaos_interval:
            schedule = f"*/{chaos_interval[:-1]} * * * * *"
        if "m" in chaos_interval:
            schedule = f"00 */{chaos_interval[:-1]} * * * *"
        update_key_value(chaos_config, "schedule", schedule)
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
        log.info("*********************Chaos Test Completed**********************")
