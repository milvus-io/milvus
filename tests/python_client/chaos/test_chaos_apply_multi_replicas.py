import pytest
import time
import json
from time import sleep
from datetime import datetime
from pymilvus import connections
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, get_milvus_instance_name, get_milvus_deploy_tool
from utils.util_common import wait_signal_to_apply_chaos
import constants


def build_rg_chaos_config(chaos_type, release_name, namespace, target_rgs, duration="2m"):
    """Build a chaos config that targets querynode pods by resource group labels.

    Uses expressionSelectors with 'In' operator to target one or more RGs,
    combined with labelSelectors for instance and component filtering.

    Args:
        chaos_type: 'pod-failure' or 'pod-kill'
        release_name: milvus helm release name
        namespace: k8s namespace
        target_rgs: list of resource group names to target (e.g. ['rg1', 'rg2'])
        duration: chaos duration string (e.g. '2m')
    """
    action = chaos_type  # pod-failure or pod-kill

    config = {
        "apiVersion": constants.CHAOS_API_VERSION,
        "kind": "PodChaos",
        "metadata": {
            "name": f"test-multi-replicas-rg-{int(time.time())}",
            "namespace": namespace,
        },
        "spec": {
            "selector": {
                "namespaces": [namespace],
                "labelSelectors": {
                    "app.kubernetes.io/instance": release_name,
                    "component": "querynode",
                },
                "expressionSelectors": [
                    {
                        "key": "milvus.io/resource-group",
                        "operator": "In",
                        "values": list(target_rgs),
                    }
                ],
            },
            "mode": "all",
            "action": action,
            "gracePeriod": 0,
        },
    }

    if action == "pod-failure":
        config["spec"]["duration"] = duration

    return config


class TestChaosApplyMultiReplicas:

    @pytest.fixture(scope="function", autouse=True)
    def init_env(self, host, port, user, password, milvus_ns):
        if user and password:
            connections.connect('default', host=host, port=port, user=user, password=password)
        else:
            connections.connect('default', host=host, port=port)
        if connections.has_connection("default") is False:
            raise Exception("no connections")
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
                                user=self.user, password=self.password)
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

    def test_chaos_apply(self, chaos_type, target_rgs, chaos_duration, wait_signal):
        """Apply chaos to specific resource groups by label selector.

        Args:
            chaos_type: pod-failure or pod-kill (from --chaos_type)
            target_rgs: comma-separated RG names (from --target_rgs, e.g. "rg1,rg2")
            chaos_duration: duration string (from --chaos_duration, e.g. "7m")
            wait_signal: whether to wait for signal before applying chaos
        """
        log.info("*********************Multi-Replica Chaos Test Start**********************")
        if wait_signal:
            log.info("need wait signal to start chaos")
            ready_for_chaos = wait_signal_to_apply_chaos()
            if not ready_for_chaos:
                log.info("get the signal to apply chaos timeout")
            else:
                log.info("get the signal to apply chaos")

        log.info(connections.get_connection_addr('default'))
        release_name = self.release_name

        # Parse target resource groups
        rg_list = [rg.strip() for rg in target_rgs.split(',') if rg.strip()]
        assert len(rg_list) > 0, "target_rgs must not be empty, e.g. 'rg1,rg2'"
        log.info(f"target resource groups: {rg_list}")

        # Parse chaos duration
        chaos_duration_str = chaos_duration
        chaos_duration_seconds = chaos_duration.replace('h', '*3600+').replace('m', '*60+').replace('s', '*1+') + '+0'
        chaos_duration_seconds = eval(chaos_duration_seconds)
        duration_for_spec = f"{chaos_duration_seconds // 60}m" if chaos_duration_seconds >= 60 else f"{chaos_duration_seconds}s"

        # Build chaos config with RG label selectors
        chaos_config = build_rg_chaos_config(
            chaos_type=chaos_type,
            release_name=release_name,
            namespace=self.milvus_ns,
            target_rgs=rg_list,
            duration=duration_for_spec,
        )
        meta_name = chaos_config['metadata']['name']
        self.chaos_config = chaos_config

        log.info(f"chaos_config: {json.dumps(chaos_config, indent=2)}")

        # Apply chaos object
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

        # Wait for chaos duration
        sleep(chaos_duration_seconds)

        # Delete chaos
        chaos_res.delete(meta_name)
        delete_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
        log.info("chaos deleted")

        # Verify the chaos is deleted
        res = chaos_res.list_all()
        chaos_list = [r['metadata']['name'] for r in res['items']]
        t0 = time.time()
        while meta_name in chaos_list and time.time() - t0 < 60:
            sleep(10)
            res = chaos_res.list_all()
            chaos_list = [r['metadata']['name'] for r in res['items']]
        assert meta_name not in chaos_list

        # Wait all pods ready
        t0 = time.time()
        log.info(f"wait for pods in namespace {self.milvus_ns} with label app.kubernetes.io/instance={release_name}")
        wait_pods_ready(self.milvus_ns, f"app.kubernetes.io/instance={release_name}")
        log.info(f"wait for pods in namespace {self.milvus_ns} with label release={release_name}")
        wait_pods_ready(self.milvus_ns, f"release={release_name}")
        log.info("all pods are ready")
        pods_ready_time = time.time() - t0
        log.info(f"pods ready time: {pods_ready_time}")
        recovery_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')

        # Save event records
        event_records = {
            "chaos_type": chaos_type,
            "target_component": "querynode",
            "target_rgs": rg_list,
            "meta_name": meta_name,
            "create_time": create_time,
            "delete_time": delete_time,
            "recovery_time": recovery_time,
        }
        with open(constants.CHAOS_INFO_SAVE_PATH, 'w') as f:
            json.dump(event_records, f)

        # Reconnect to verify service is healthy
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

        log.info("*********************Multi-Replica Chaos Test Completed**********************")
