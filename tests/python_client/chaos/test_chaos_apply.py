import copy
import json
import random
import time
from datetime import datetime
from pathlib import Path
from time import sleep

import constants
import pytest
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.milvus_sys import MilvusSys
from pymilvus import connections
from utils.util_common import gen_experiment_config, update_key_name, update_key_value, wait_signal_to_apply_chaos
from utils.util_k8s import (
    get_milvus_deploy_tool,
    get_milvus_instance_name,
    get_pod_container_names,
    get_pod_list,
    wait_pods_ready,
)
from utils.util_log import test_log as log


def get_pod_chaos_spec(chaos_config):
    spec = chaos_config.get("spec", {})
    return spec.get("podChaos", spec)


def normalize_container_kill_config(chaos_config):
    target = get_pod_chaos_spec(chaos_config)
    if target.get("action") != "container-kill":
        return chaos_config
    if "containerName" in target:
        target["containerNames"] = [target.pop("containerName")]
    return chaos_config


def build_label_selector(label_selectors):
    return ",".join([f"{key}={value}" for key, value in label_selectors.items()])


def get_selector_namespace(selector, default_namespace):
    namespaces = selector.get("namespaces") or [default_namespace]
    return namespaces[0]


def split_container_kill_configs_by_pod_containers(chaos_config, default_namespace):
    normalize_container_kill_config(chaos_config)
    target = get_pod_chaos_spec(chaos_config)
    if target.get("action") != "container-kill":
        return [chaos_config]

    selector = target.get("selector", {})
    label_selectors = selector.get("labelSelectors")
    if not label_selectors:
        return [chaos_config]

    namespace = get_selector_namespace(selector, default_namespace)
    pods = get_pod_list(namespace, build_label_selector(label_selectors))
    pod_names = [pod.metadata.name for pod in pods]
    if not pod_names:
        raise Exception(f"no pods matched selector {label_selectors} in namespace {namespace}")

    pod_container_names = get_pod_container_names(namespace, pod_names)
    configured_container_names = target.get("containerNames")
    if configured_container_names:
        configured = set(configured_container_names)
        invalid_pods = {
            pod_name: container_names
            for pod_name, container_names in pod_container_names.items()
            if not configured.intersection(container_names)
        }
        if invalid_pods:
            raise Exception(
                f"configured containerNames {configured_container_names} do not match containers "
                f"in selected pods: {invalid_pods}"
            )
        return [chaos_config]

    pods_by_containers = {}
    for pod_name, container_names in pod_container_names.items():
        pods_by_containers.setdefault(tuple(container_names), []).append(pod_name)

    if len(pods_by_containers) == 1:
        target["containerNames"] = list(next(iter(pods_by_containers.keys())))
        return [chaos_config]

    if target.get("mode") == "fixed" and target.get("value") == "1":
        pod_name = random.choice(pod_names)
        target["containerNames"] = pod_container_names[pod_name]
        target["selector"] = {"pods": {namespace: [pod_name]}}
        return [chaos_config]

    configs = []
    for idx, (container_names, grouped_pods) in enumerate(pods_by_containers.items(), start=1):
        grouped_config = copy.deepcopy(chaos_config)
        grouped_target = get_pod_chaos_spec(grouped_config)
        grouped_target["containerNames"] = list(container_names)
        grouped_target["selector"] = {"pods": {namespace: grouped_pods}}
        grouped_config["metadata"]["name"] = f"{chaos_config['metadata']['name']}-{idx}"
        configs.append(grouped_config)
    return configs


class TestChaosApply:
    @pytest.fixture(scope="function", autouse=True)
    def init_env(self, host, port, user, password, milvus_ns):
        if user and password:
            connections.connect("default", host=host, port=port, user=user, password=password)
        else:
            connections.connect("default", host=host, port=port)
        if connections.has_connection("default") is False:
            raise Exception("no connections")
        #
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.milvus_sys = MilvusSys(alias="default")
        self.chaos_ns = constants.CHAOS_NAMESPACE
        self.milvus_ns = milvus_ns
        self.release_name = get_milvus_instance_name(self.milvus_ns, milvus_sys=self.milvus_sys)
        self.deploy_by = get_milvus_deploy_tool(self.milvus_ns, self.milvus_sys)

    def reconnect(self):
        if self.user and self.password:
            connections.connect("default", host=self.host, port=self.port, user=self.user, password=self.password)
        else:
            connections.connect("default", host=self.host, port=self.port)
        if connections.has_connection("default") is False:
            raise Exception("no connections")

    def teardown(self):
        for chaos_config in getattr(self, "chaos_configs", [getattr(self, "chaos_config", {})]):
            if not chaos_config:
                continue
            chaos_res = CusResource(
                kind=chaos_config["kind"],
                group=constants.CHAOS_GROUP,
                version=constants.CHAOS_VERSION,
                namespace=constants.CHAOS_NAMESPACE,
            )
            meta_name = chaos_config.get("metadata", None).get("name", None)
            chaos_res.delete(meta_name, raise_ex=False)
        sleep(2)

    def test_chaos_apply(
        self, chaos_type, target_component, target_scope, target_number, chaos_duration, chaos_interval, wait_signal
    ):
        # start the monitor threads to check the milvus ops
        log.info("*********************Chaos Test Start**********************")
        if wait_signal:
            log.info("need wait signal to start chaos")
            ready_for_chaos = wait_signal_to_apply_chaos()
            if not ready_for_chaos:
                log.info("get the signal to apply chaos timeout")
            else:
                log.info("get the signal to apply chaos")
        log.info(connections.get_connection_addr("default"))
        release_name = self.release_name
        chaos_config = gen_experiment_config(
            f"{str(Path(__file__).absolute().parent)}/chaos_objects/{chaos_type.replace('-', '_')}/chaos_{target_component}_{chaos_type.replace('-', '_')}.yaml"
        )
        chaos_config["metadata"]["name"] = f"test-{target_component}-{chaos_type.replace('_', '-')}-{int(time.time())}"
        chaos_config["metadata"]["namespace"] = self.chaos_ns
        update_key_value(chaos_config, "release", release_name)
        update_key_value(chaos_config, "app.kubernetes.io/instance", release_name)
        update_key_value(chaos_config, "namespaces", [self.milvus_ns])
        update_key_value(chaos_config, "value", target_number)
        update_key_value(chaos_config, "mode", target_scope)
        self.chaos_config = chaos_config
        if "s" in chaos_interval:
            schedule = f"*/{chaos_interval[:-1]} * * * * *"
        if "m" in chaos_interval:
            schedule = f"00 */{chaos_interval[:-1]} * * * *"
        update_key_value(chaos_config, "schedule", schedule)
        # update chaos_duration from string to int with unit second
        chaos_duration = chaos_duration.replace("h", "*3600+").replace("m", "*60+").replace("s", "*1+") + "+0"
        chaos_duration = eval(chaos_duration)
        update_key_value(chaos_config, "duration", f"{chaos_duration // 60}m")
        if self.deploy_by == "milvus-operator":
            update_key_name(chaos_config, "component", "app.kubernetes.io/component")
        chaos_configs = split_container_kill_configs_by_pod_containers(chaos_config, self.milvus_ns)
        self.chaos_configs = chaos_configs
        self.chaos_config = chaos_configs[0]
        self._chaos_config = self.chaos_config  # cache the chaos config for tear down
        log.info(f"chaos_configs: {chaos_configs}")
        # apply chaos object
        for item in chaos_configs:
            chaos_res = CusResource(
                kind=item["kind"],
                group=constants.CHAOS_GROUP,
                version=constants.CHAOS_VERSION,
                namespace=constants.CHAOS_NAMESPACE,
            )
            chaos_res.create(item)
        create_time = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S.%f")
        log.info("chaos injected")
        meta_names = [item.get("metadata", None).get("name", None) for item in chaos_configs]
        for item in chaos_configs:
            chaos_res = CusResource(
                kind=item["kind"],
                group=constants.CHAOS_GROUP,
                version=constants.CHAOS_VERSION,
                namespace=constants.CHAOS_NAMESPACE,
            )
            res = chaos_res.list_all()
            chaos_list = [r["metadata"]["name"] for r in res["items"]]
            assert item["metadata"]["name"] in chaos_list
            res = chaos_res.get(item["metadata"]["name"])
            log.info(f"chaos inject result: {res['kind']}, {res['metadata']['name']}")
        sleep(chaos_duration)
        # delete chaos
        for item in chaos_configs:
            chaos_res = CusResource(
                kind=item["kind"],
                group=constants.CHAOS_GROUP,
                version=constants.CHAOS_VERSION,
                namespace=constants.CHAOS_NAMESPACE,
            )
            chaos_res.delete(item["metadata"]["name"])
        delete_time = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S.%f")
        log.info("chaos deleted")
        # verify the chaos is deleted in 60s
        t0 = time.time()
        deleted = False
        while not deleted and time.time() - t0 < 60:
            deleted = True
            for item in chaos_configs:
                chaos_res = CusResource(
                    kind=item["kind"],
                    group=constants.CHAOS_GROUP,
                    version=constants.CHAOS_VERSION,
                    namespace=constants.CHAOS_NAMESPACE,
                )
                res = chaos_res.list_all()
                chaos_list = [r["metadata"]["name"] for r in res["items"]]
                if item["metadata"]["name"] in chaos_list:
                    deleted = False
                    break
            if not deleted:
                sleep(10)
        assert deleted
        # wait all pods ready
        t0 = time.time()
        log.info(
            f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label app.kubernetes.io/instance={release_name}"
        )
        wait_pods_ready(constants.CHAOS_NAMESPACE, f"app.kubernetes.io/instance={release_name}")
        log.info(f"wait for pods in namespace {constants.CHAOS_NAMESPACE} with label release={release_name}")
        wait_pods_ready(constants.CHAOS_NAMESPACE, f"release={release_name}")
        log.info("all pods are ready")
        pods_ready_time = time.time() - t0
        log.info(f"pods ready time: {pods_ready_time}")
        recovery_time = datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S.%f")
        event_records = {
            "chaos_type": chaos_type,
            "target_component": target_component,
            "meta_name": ",".join(meta_names),
            "create_time": create_time,
            "delete_time": delete_time,
            "recovery_time": recovery_time,
        }
        # save event records to json file
        with open(constants.CHAOS_INFO_SAVE_PATH, "w") as f:
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
        log.info(f"recovery time from pod ready to can be connected: {recovery_time}")

        log.info("*********************Chaos Test Completed**********************")
