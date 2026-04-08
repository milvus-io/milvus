import pytest
import time
import json
import random
import yaml
from time import sleep
from datetime import datetime
from pymilvus import connections
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.milvus_sys import MilvusSys
from utils.util_log import test_log as log
from utils.util_k8s import wait_pods_ready, get_milvus_instance_name, get_milvus_deploy_tool
from utils.util_common import wait_signal_to_apply_chaos
import constants


def parse_duration(duration_str):
    """Parse duration string like '24h', '10m', '30s' to seconds."""
    s = duration_str.strip()
    s = s.replace('h', '*3600+').replace('m', '*60+').replace('s', '*1+') + '+0'
    return eval(s)


def build_rg_chaos_config(chaos_type, release_name, namespace, target_rgs,
                          component=None, mode="one", duration="2m"):
    """Build a chaos config that targets pods in target RGs.

    Args:
        chaos_type: pod-failure or pod-kill
        release_name: milvus helm release name
        namespace: k8s namespace
        target_rgs: list of RG names to target
        component: optional component filter (e.g. 'querynode', 'streamingnode').
                   If None, targets all pods in the RG.
        mode: 'one' (random single pod) or 'all' (all matching pods)
        duration: chaos duration string (e.g. '2m')
    """
    action = chaos_type
    component_suffix = f"-{component}" if component else ""

    label_selectors = {
        "app.kubernetes.io/instance": release_name,
    }
    if component:
        label_selectors["component"] = component

    config = {
        "apiVersion": constants.CHAOS_API_VERSION,
        "kind": "PodChaos",
        "metadata": {
            "name": f"test-multi-rg{component_suffix}-{int(time.time())}",
            "namespace": namespace,
        },
        "spec": {
            "selector": {
                "namespaces": [namespace],
                "labelSelectors": label_selectors,
                "expressionSelectors": [
                    {
                        "key": "milvus.io/resource-group",
                        "operator": "In",
                        "values": list(target_rgs),
                    }
                ],
            },
            "mode": mode,
            "action": action,
            "gracePeriod": 0,
        },
    }

    if action == "pod-failure":
        config["spec"]["duration"] = duration

    return config


def load_chaos_template(template_path, namespace, release_name):
    """Load external ChaosMesh YAML template and override metadata."""
    with open(template_path, 'r') as f:
        config = yaml.safe_load(f)
    # Override metadata to avoid name collision across cycles
    config["metadata"]["name"] = f"{config['metadata'].get('name', 'custom-chaos')}-{int(time.time())}"
    config["metadata"]["namespace"] = namespace
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
        self.chaos_configs = []

    def reconnect(self):
        if self.user and self.password:
            connections.connect('default', host=self.host, port=self.port,
                                user=self.user, password=self.password)
        else:
            connections.connect('default', host=self.host, port=self.port)
        if connections.has_connection("default") is False:
            raise Exception("no connections")

    def teardown(self):
        for chaos_config in self.chaos_configs:
            chaos_res = CusResource(kind=chaos_config['kind'],
                                    group=constants.CHAOS_GROUP,
                                    version=constants.CHAOS_VERSION,
                                    namespace=constants.CHAOS_NAMESPACE)
            meta_name = chaos_config.get('metadata', {}).get('name', None)
            if meta_name:
                chaos_res.delete(meta_name, raise_ex=False)
        sleep(2)

    def _apply_single_chaos(self, chaos_config, chaos_duration_seconds):
        """Apply one chaos CR, wait for duration, delete and wait recovery.
        Returns event record dict.
        """
        meta_name = chaos_config['metadata']['name']
        self.chaos_configs.append(chaos_config)

        log.info(f"applying chaos: {meta_name}")
        log.info(f"chaos spec: {json.dumps(chaos_config['spec'], indent=2)}")

        chaos_res = CusResource(kind=chaos_config['kind'],
                                group=constants.CHAOS_GROUP,
                                version=constants.CHAOS_VERSION,
                                namespace=constants.CHAOS_NAMESPACE)
        chaos_res.create(chaos_config)
        create_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
        log.info(f"chaos injected: {meta_name}")

        # Wait for chaos duration
        sleep(chaos_duration_seconds)

        # Delete chaos
        chaos_res.delete(meta_name)
        delete_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
        log.info(f"chaos deleted: {meta_name}")

        # Verify deletion
        t0 = time.time()
        while time.time() - t0 < 60:
            res = chaos_res.list_all()
            chaos_list = [r['metadata']['name'] for r in res['items']]
            if meta_name not in chaos_list:
                break
            sleep(5)

        return {
            "meta_name": meta_name,
            "create_time": create_time,
            "delete_time": delete_time,
        }

    def _wait_recovery(self):
        """Wait for all pods to be ready and reconnect."""
        release_name = self.release_name
        t0 = time.time()
        wait_pods_ready(self.milvus_ns, f"app.kubernetes.io/instance={release_name}")
        wait_pods_ready(self.milvus_ns, f"release={release_name}")
        pods_ready_time = time.time() - t0
        log.info(f"all pods ready, recovery took {pods_ready_time:.1f}s")

        recovery_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')

        # Reconnect to verify service
        t0 = time.time()
        while time.time() - t0 < 120:
            try:
                self.reconnect()
                break
            except Exception as e:
                log.error(f"reconnect failed: {e}")
                sleep(2)
        log.info(f"service reconnected, took {time.time() - t0:.1f}s")

        return recovery_time, pods_ready_time

    def _apply_and_wait_chaos(self, chaos_type, target_rg, chaos_duration_seconds,
                              mode="one", components=None, template_path=None):
        """Apply chaos to one RG with per-component injection, wait and recover.

        If template_path is provided, uses the external template directly.
        Otherwise builds chaos configs per component with the given mode.

        Args:
            chaos_type: pod-failure or pod-kill
            target_rg: single RG name to target
            chaos_duration_seconds: duration in seconds
            mode: 'one' or 'all'
            components: list of components to inject sequentially (e.g. ['querynode', 'streamingnode'])
            template_path: optional path to external ChaosMesh YAML template

        Returns:
            Event record dict with per-component details.
        """
        release_name = self.release_name
        duration_str = f"{chaos_duration_seconds // 60}m" if chaos_duration_seconds >= 60 else f"{chaos_duration_seconds}s"

        record = {
            "target_rg": target_rg,
            "chaos_type": chaos_type,
            "mode": mode,
            "steps": [],
        }

        if template_path:
            # Use external template directly
            log.info(f"using external template: {template_path}")
            chaos_config = load_chaos_template(template_path, self.milvus_ns, release_name)
            step_record = self._apply_single_chaos(chaos_config, chaos_duration_seconds)
            step_record["source"] = "template"
            record["steps"].append(step_record)
        else:
            # Per-component sequential injection
            if not components:
                components = [None]  # No component filter, target all pods in RG

            random.shuffle(components)
            log.info(f"injection order: {components} (mode={mode})")

            for component in components:
                log.info(f"injecting {chaos_type} to RG={target_rg}, component={component or 'all'}, mode={mode}")
                chaos_config = build_rg_chaos_config(
                    chaos_type=chaos_type,
                    release_name=release_name,
                    namespace=self.milvus_ns,
                    target_rgs=[target_rg],
                    component=component,
                    mode=mode,
                    duration=duration_str,
                )
                step_record = self._apply_single_chaos(chaos_config, chaos_duration_seconds)
                step_record["component"] = component or "all"
                step_record["mode"] = mode
                record["steps"].append(step_record)

        # Wait recovery after all injections in this cycle
        recovery_time, pods_ready_time = self._wait_recovery()
        record["recovery_time"] = recovery_time
        record["pods_ready_time"] = pods_ready_time

        return record

    def test_chaos_apply(self, chaos_type, target_rgs, chaos_duration, chaos_mode,
                         target_components, chaos_template, wait_signal):
        """One-shot chaos injection to specific RGs (for quick testing)."""
        log.info("*********************Multi-Replica Chaos Test Start**********************")
        if wait_signal:
            ready_for_chaos = wait_signal_to_apply_chaos()
            if not ready_for_chaos:
                log.info("get the signal to apply chaos timeout")
            else:
                log.info("get the signal to apply chaos")

        log.info(connections.get_connection_addr('default'))
        rg_list = [rg.strip() for rg in target_rgs.split(',') if rg.strip()]
        assert len(rg_list) > 0, "target_rgs must not be empty"

        components = [c.strip() for c in target_components.split(',') if c.strip()] if target_components else None
        template_path = chaos_template if chaos_template else None

        chaos_duration_seconds = parse_duration(chaos_duration)
        record = self._apply_and_wait_chaos(
            chaos_type, rg_list[0], chaos_duration_seconds,
            mode=chaos_mode, components=components, template_path=template_path,
        )

        with open(constants.CHAOS_INFO_SAVE_PATH, 'w') as f:
            json.dump(record, f, indent=2)

        log.info("*********************Multi-Replica Chaos Test Completed**********************")

    def test_chaos_apply_periodic(self, chaos_type, target_rgs, chaos_duration,
                                  chaos_interval, request_duration, chaos_mode,
                                  target_components, chaos_template, wait_signal):
        """Periodically inject chaos to RGs with per-component injection.

        Each cycle: pick next RG -> for each component, inject chaos (mode=one/all)
        -> wait duration -> delete -> recover -> wait interval.
        Runs for total request_duration (e.g. 24h).

        Args:
            chaos_type: pod-failure or pod-kill
            target_rgs: comma-separated RG names to rotate through
            chaos_duration: duration of each chaos injection per component
            chaos_interval: total interval between cycle starts
            request_duration: total test duration
            chaos_mode: 'one' (single random pod) or 'all' (all matching pods)
            target_components: comma-separated components (e.g. 'querynode,streamingnode')
            chaos_template: optional path to external ChaosMesh YAML
            wait_signal: whether to wait for signal before starting
        """
        log.info("*********************Periodic Chaos Test Start**********************")
        if wait_signal:
            ready_for_chaos = wait_signal_to_apply_chaos()
            if not ready_for_chaos:
                log.info("get the signal to apply chaos timeout")
            else:
                log.info("get the signal to apply chaos")

        log.info(connections.get_connection_addr('default'))

        rg_list = [rg.strip() for rg in target_rgs.split(',') if rg.strip()]
        assert len(rg_list) > 0, "target_rgs must not be empty"

        components = [c.strip() for c in target_components.split(',') if c.strip()] if target_components else None
        template_path = chaos_template if chaos_template else None

        total_seconds = parse_duration(request_duration)
        interval_seconds = parse_duration(chaos_interval)
        chaos_dur_seconds = parse_duration(chaos_duration)

        log.info(f"periodic chaos config:")
        log.info(f"  target RGs (round-robin): {rg_list}")
        log.info(f"  chaos type: {chaos_type}")
        log.info(f"  chaos mode: {chaos_mode}")
        log.info(f"  target components: {components or 'all (no filter)'}")
        log.info(f"  chaos template: {template_path or 'none (built-in)'}")
        log.info(f"  chaos duration per component: {chaos_duration} ({chaos_dur_seconds}s)")
        log.info(f"  interval between cycles: {chaos_interval} ({interval_seconds}s)")
        log.info(f"  total duration: {request_duration} ({total_seconds}s)")
        log.info(f"  expected cycles: ~{total_seconds // interval_seconds}")

        start_time = time.time()
        round_num = 0
        all_records = []

        while time.time() - start_time < total_seconds:
            round_num += 1
            cycle_start = time.time()
            elapsed = cycle_start - start_time
            remaining = total_seconds - elapsed

            # Round-robin: pick RG by index
            target_rg = rg_list[(round_num - 1) % len(rg_list)]
            log.info(f"===== Round {round_num} | elapsed={elapsed/3600:.1f}h | remaining={remaining/3600:.1f}h | target={target_rg} =====")

            # Don't start a new cycle if remaining time < chaos duration
            if remaining < chaos_dur_seconds:
                log.info(f"remaining time ({remaining:.0f}s) < chaos duration ({chaos_dur_seconds}s), stopping")
                break

            try:
                record = self._apply_and_wait_chaos(
                    chaos_type, target_rg, chaos_dur_seconds,
                    mode=chaos_mode, components=components, template_path=template_path,
                )
                record["round"] = round_num
                all_records.append(record)
                log.info(f"round {round_num} completed: target={target_rg}, recovery={record['pods_ready_time']:.1f}s")
            except Exception as e:
                log.error(f"round {round_num} failed: {e}")
                all_records.append({
                    "round": round_num,
                    "target_rg": target_rg,
                    "error": str(e),
                    "time": datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'),
                })

            # Wait for next interval
            cycle_elapsed = time.time() - cycle_start
            wait_time = interval_seconds - cycle_elapsed
            if wait_time > 0 and time.time() - start_time + wait_time < total_seconds:
                log.info(f"waiting {wait_time:.0f}s until next cycle")
                sleep(wait_time)

        # Save all event records
        summary = {
            "total_rounds": round_num,
            "total_duration_hours": (time.time() - start_time) / 3600,
            "chaos_type": chaos_type,
            "chaos_mode": chaos_mode,
            "target_components": components,
            "rg_list": rg_list,
            "records": all_records,
        }
        with open(constants.CHAOS_INFO_SAVE_PATH, 'w') as f:
            json.dump(summary, f, indent=2)

        log.info(f"*********************Periodic Chaos Test Completed**********************")
        log.info(f"total rounds: {round_num}, duration: {(time.time() - start_time)/3600:.1f}h")
