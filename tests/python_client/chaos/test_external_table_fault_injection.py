import random
import re
import threading
import time
from pathlib import Path
from time import sleep

import pyarrow as pa
import pytest
from chaos import constants
from common import common_func as cf
from common import common_type as ct
from common.common_type import CaseLabel
from common.cus_resource_opts import CustomResourceOperations as CusResource
from common.external_table_common import (
    REFRESH_TIMEOUT,
    build_external_source,
    build_external_spec,
    cleanup_minio_prefix,
    get_minio_config,
    new_minio_client,
    query_count,
    write_basic_format_dataset,
    write_vortex_table,
)
from common.milvus_sys import MilvusSys
from pymilvus import DataType, MilvusClient, connections
from utils.util_common import gen_experiment_config, update_key_name, update_key_value
from utils.util_k8s import get_milvus_deploy_tool, get_milvus_instance_name, get_pod_list, get_svc_ip, wait_pods_ready
from utils.util_log import test_log as log

_TERMINAL_STATES = ("RefreshCompleted", "RefreshFailed")
_CHAOS_OBJECT_DIR = Path(__file__).resolve().parent / "chaos_objects"
_FAULT_FILES = 6
_FAULT_ROWS_PER_FILE = 10_000
_FAULT_ROWS = _FAULT_FILES * _FAULT_ROWS_PER_FILE
_CHAOS_FORMATS = ("parquet", "lance-table", "iceberg-table", "vortex")
_CHAOS_FORMAT_IDS = ("parquet", "lance", "iceberg", "vortex")
_POD_KILL_REFRESH_TIMEOUT = max(REFRESH_TIMEOUT, 900)
_MINIO_PARTITION_WINDOW_SECONDS = 180
_REFRESH_SUBMIT_TIMEOUT_SECONDS = _MINIO_PARTITION_WINDOW_SECONDS + 60
_MINIO_IO_LATENCY_DELAY = "100ms"
_POST_REFRESH_RECOVERY_TIMEOUT = 180
_IN_PROGRESS_REFRESH_JOB_RE = re.compile(r"refresh job (\d+) is already in progress")


def _build_basic_schema(client, external_source, external_spec):
    schema = client.create_schema(external_source=external_source, external_spec=external_spec)
    schema.add_field("id", DataType.INT64, external_field="id")
    schema.add_field("value", DataType.FLOAT, external_field="value")
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=ct.default_dim, external_field="embedding")
    return schema


@pytest.fixture(scope="function", params=_CHAOS_FORMATS, ids=_CHAOS_FORMAT_IDS)
def external_table_format(request):
    return request.param


@pytest.fixture(scope="function")
def external_table_minio_env(minio_host, minio_bucket):
    cfg = get_minio_config(minio_host=minio_host, minio_bucket=minio_bucket)
    minio_client = new_minio_client(cfg)
    assert minio_client.bucket_exists(cfg["bucket"]), f"MinIO bucket {cfg['bucket']} not accessible at {cfg['address']}"
    return minio_client, cfg


@pytest.fixture(scope="function")
def external_table_prefix(external_table_minio_env, request):
    minio_client, cfg = external_table_minio_env
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", request.node.name)
    key = f"external-table-chaos/{safe}-{int(time.time() * 1000)}-{random.randint(1000, 9999)}"
    yield {"key": key, "url": build_external_source(cfg, key)}
    try:
        cleanup_minio_prefix(minio_client, cfg["bucket"], f"{key}/")
    except Exception as exc:
        log.warning(f"cleanup_minio_prefix({key}) failed: {exc}")


def _unwrap_single(value):
    if isinstance(value, (list, tuple)) and len(value) == 1:
        return value[0]
    return value


def _progress_state(progress):
    if isinstance(progress, dict):
        return progress.get("state")
    return getattr(progress, "state", None)


def _progress_reason(progress):
    if isinstance(progress, dict):
        return progress.get("reason") or ""
    return getattr(progress, "reason", None) or ""


def _poll_refresh_terminal(client, job_id, timeout=REFRESH_TIMEOUT):
    deadline = time.time() + timeout
    progress = None
    last_error = None
    while time.time() < deadline:
        try:
            progress = _unwrap_single(client.get_refresh_external_collection_progress(job_id=job_id, timeout=30))
            if _progress_state(progress) in _TERMINAL_STATES:
                return progress
        except Exception as exc:
            last_error = exc
            log.warning(f"refresh progress is temporarily unavailable after chaos, job_id={job_id}: {exc}")
        sleep(2)
    raise TimeoutError(
        f"refresh did not reach terminal state in {timeout}s, job_id={job_id}, last={progress}, last_error={last_error}"
    )


def _assert_refresh_terminal(progress, context):
    state = _progress_state(progress)
    assert state in _TERMINAL_STATES, f"{context}: refresh stuck in {state}, progress={progress}"
    if state == "RefreshFailed":
        assert _progress_reason(progress), f"{context}: RefreshFailed without reason, progress={progress}"


def _assert_refresh_completed(progress, context):
    state = _progress_state(progress)
    assert state == "RefreshCompleted", (
        f"{context}: expected RefreshCompleted, got {state}, reason={_progress_reason(progress)}"
    )


def _create_milvus_client():
    uri = cf.param_info.param_uri or f"http://{cf.param_info.param_host}:{cf.param_info.param_port}"
    token = cf.param_info.param_token or f"{cf.param_info.param_user}:{cf.param_info.param_password}"
    return MilvusClient(uri=uri, token=token)


def _external_dataset_batches(num_rows=_FAULT_ROWS):
    assert num_rows % _FAULT_FILES == 0, f"num_rows={num_rows} must be divisible by {_FAULT_FILES}"
    rows_per_file = num_rows // _FAULT_FILES
    return [(file_idx * rows_per_file, rows_per_file) for file_idx in range(_FAULT_FILES)]


def _basic_arrow_table(num_rows, start_id, dim=ct.default_dim):
    ids = list(range(start_id, start_id + num_rows))
    vectors = [float(i) * 0.1 + d for i in ids for d in range(dim)]
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "value": pa.array([float(i) * 1.5 for i in ids], type=pa.float32()),
            "embedding": pa.FixedSizeListArray.from_arrays(pa.array(vectors, type=pa.float32()), list_size=dim),
        }
    )


def _write_vortex_dataset(minio_client, cfg, external_key, batches):
    for idx, (start_id, num_rows) in enumerate(batches):
        write_vortex_table(
            minio_client,
            cfg["bucket"],
            f"{external_key}/part-{idx:03d}.vortex",
            _basic_arrow_table(num_rows, start_id),
        )


def _replace_source_address(source, old_address, new_address):
    old_prefix = f"s3://{old_address}/"
    new_prefix = f"s3://{new_address}/"
    if source.startswith(old_prefix):
        return new_prefix + source[len(old_prefix) :]
    return source


def _create_external_collection(client, collection_name, external_source, external_spec):
    schema = _build_basic_schema(client, external_source, external_spec)
    client.create_collection(
        collection_name=collection_name,
        schema=schema,
        consistency_level="Strong",
        timeout=120,
    )


def _submit_refresh(client, collection_name, timeout=_REFRESH_SUBMIT_TIMEOUT_SECONDS):
    return _unwrap_single(client.refresh_external_collection(collection_name=collection_name, timeout=timeout))


def _extract_in_progress_refresh_job_id(exc):
    match = _IN_PROGRESS_REFRESH_JOB_RE.search(str(exc))
    if match:
        return int(match.group(1))
    return None


def _refresh_and_wait_completed(client, collection_name, context):
    job_id = _submit_refresh(client, collection_name)
    progress = _poll_refresh_terminal(client, job_id)
    _assert_refresh_completed(progress, context)
    return job_id


def _refresh_after_recovery_or_wait_existing(client, collection_name, context):
    try:
        return _refresh_and_wait_completed(client, collection_name, context)
    except Exception as exc:
        existing_job_id = _extract_in_progress_refresh_job_id(exc)
        if existing_job_id is None:
            raise

    log.info(f"{context}: refresh already in progress, polling job_id={existing_job_id}")
    progress = _poll_refresh_terminal(client, existing_job_id, timeout=_POD_KILL_REFRESH_TIMEOUT)
    _assert_refresh_terminal(progress, f"{context} existing refresh")
    if _progress_state(progress) == "RefreshCompleted":
        return existing_job_id
    return _refresh_and_wait_completed(client, collection_name, context)


def _create_index_with_retry(client, collection_name, timeout=_POST_REFRESH_RECOVERY_TIMEOUT):
    index_params = client.prepare_index_params()
    index_params.add_index(field_name="embedding", index_type="AUTOINDEX", metric_type="L2")

    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            client.create_index(collection_name, index_params, timeout=120)
            return
        except Exception as exc:
            if "index already" in str(exc).lower():
                return
            last_error = exc
            log.warning(f"external table post-chaos create_index not ready yet: {exc}")
            sleep(5)
    else:
        raise AssertionError(f"external table create_index did not recover in {timeout}s: {last_error}")


def _load_collection_with_retry(client, collection_name, timeout=_POST_REFRESH_RECOVERY_TIMEOUT):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            client.load_collection(collection_name, timeout=120)
            return
        except Exception as exc:
            last_error = exc
            log.warning(f"external table post-chaos load not ready yet: {exc}")
            sleep(5)
    else:
        raise AssertionError(f"external table load did not recover in {timeout}s: {last_error}")


def _assert_queryable(client, collection_name, expected_rows):
    count = query_count(client, collection_name)
    assert count == expected_rows, f"expected {expected_rows} rows, got {count}"

    search_res = client.search(
        collection_name=collection_name,
        data=[[float(i) for i in range(ct.default_dim)]],
        anns_field="embedding",
        search_params={"metric_type": "L2", "params": {}},
        limit=1,
        output_fields=["id"],
        timeout=30,
    )
    assert search_res and search_res[0], f"search should return at least one row, got {search_res}"


def _assert_queryable_with_retry(client, collection_name, expected_rows, timeout=_POST_REFRESH_RECOVERY_TIMEOUT):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            _assert_queryable(client, collection_name, expected_rows)
            return
        except Exception as exc:
            last_error = exc
            log.warning(f"external table post-chaos query path not ready yet: {exc}")
            sleep(5)
    raise AssertionError(f"external table query path did not recover in {timeout}s: {last_error}")


def _index_load_and_assert_queryable(client, collection_name, expected_rows):
    _create_index_with_retry(client, collection_name)
    _load_collection_with_retry(client, collection_name)
    _assert_queryable(client, collection_name, expected_rows)


def _drop_collection(client, collection_name):
    try:
        if client.has_collection(collection_name):
            client.drop_collection(collection_name=collection_name, timeout=120)
    except Exception as exc:
        log.warning(f"drop collection {collection_name} failed: {exc}")


class TestExternalTableFaultInjection:
    @pytest.fixture(scope="function", autouse=True)
    def init_env(self, host, port, user, password, milvus_ns):
        if user and password:
            connections.connect("default", host=host, port=port, user=user, password=password)
        else:
            connections.connect("default", host=host, port=port)
        if connections.has_connection("default") is False:
            raise Exception("no connections")

        self.milvus_ns = milvus_ns
        self.chaos_ns = constants.CHAOS_NAMESPACE
        self.milvus_sys = MilvusSys(alias="default")
        self.release_name = get_milvus_instance_name(self.milvus_ns, milvus_sys=self.milvus_sys)
        self.deploy_by = get_milvus_deploy_tool(self.milvus_ns, self.milvus_sys)

    def _build_chaos_config(
        self,
        chaos_type,
        target_component,
        target_scope="fixed",
        target_number="1",
        schedule="*/1 * * * * *",
        delay=None,
    ):
        chaos_dir = chaos_type.replace("-", "_")
        yaml_path = _CHAOS_OBJECT_DIR / chaos_dir / f"chaos_{target_component}_{chaos_dir}.yaml"
        chaos_config = gen_experiment_config(str(yaml_path))
        chaos_config["metadata"]["name"] = (
            f"external-table-{target_component}-{chaos_type.replace('_', '-')}-{int(time.time())}"
        )
        chaos_config["metadata"]["namespace"] = self.chaos_ns
        update_key_value(chaos_config, "release", self.release_name)
        update_key_value(chaos_config, "app.kubernetes.io/instance", self.release_name)
        update_key_value(chaos_config, "namespaces", [self.milvus_ns])
        update_key_value(chaos_config, "mode", target_scope)
        update_key_value(chaos_config, "value", target_number)
        if schedule:
            update_key_value(chaos_config, "schedule", schedule)
        if delay:
            update_key_value(chaos_config, "delay", delay)
        if self.deploy_by == "milvus-operator":
            update_key_name(chaos_config, "component", "app.kubernetes.io/component")
        if chaos_type == "pod_kill" and chaos_config.get("kind") == "Schedule":
            chaos_config["kind"] = "PodChaos"
            chaos_config["spec"] = chaos_config["spec"]["podChaos"]
        if target_component == "minio":
            self._set_minio_dependency_selector(chaos_config)
        return chaos_config

    def _set_minio_dependency_selector(self, chaos_config):
        minio_release = f"{self.release_name}-minio"

        def fix_selector(selector):
            labels = selector.get("labelSelectors", {})
            if labels.get("app") == "minio":
                labels["release"] = minio_release

        spec = chaos_config.get("spec", {})
        if "selector" in spec:
            fix_selector(spec["selector"])
        if "target" in spec and "selector" in spec["target"]:
            fix_selector(spec["target"]["selector"])

    def _limit_milvus_source_selector_to_component(self, chaos_config, component):
        selector = chaos_config.get("spec", {}).get("selector", {})
        labels = selector.setdefault("labelSelectors", {})
        component_key = "app.kubernetes.io/component" if self.deploy_by == "milvus-operator" else "component"
        labels[component_key] = component

    def _apply_chaos(self, chaos_config):
        chaos_res = CusResource(
            kind=chaos_config["kind"],
            group=constants.CHAOS_GROUP,
            version=constants.CHAOS_VERSION,
            namespace=constants.CHAOS_NAMESPACE,
        )
        meta_name = chaos_config["metadata"]["name"]
        chaos_res.create(chaos_config)
        chaos_names = [item["metadata"]["name"] for item in chaos_res.list_all()["items"]]
        assert meta_name in chaos_names, f"chaos object {meta_name} was not created"
        return chaos_res, meta_name

    def _wait_chaos_injected(self, chaos_res, meta_name, timeout=60):
        deadline = time.time() + timeout
        last_item = None
        while time.time() < deadline:
            items = chaos_res.list_all()["items"]
            last_item = next((item for item in items if item["metadata"]["name"] == meta_name), None)
            conditions = (last_item or {}).get("status", {}).get("conditions", [])
            injected = any(c.get("type") == "AllInjected" and c.get("status") == "True" for c in conditions)
            if injected:
                return
            sleep(1)
        raise AssertionError(f"chaos object {meta_name} was not injected in {timeout}s, last={last_item}")

    def _delete_chaos_and_wait_ready(self, chaos_res, meta_name, wait_minio=False):
        chaos_res.delete(meta_name, raise_ex=False)
        deadline = time.time() + 60
        while time.time() < deadline:
            chaos_names = [item["metadata"]["name"] for item in chaos_res.list_all()["items"]]
            if meta_name not in chaos_names:
                break
            sleep(5)
        else:
            raise AssertionError(f"chaos object {meta_name} was not deleted in 60s")

        log.info(f"wait pods ready for release {self.release_name}")
        wait_pods_ready(self.milvus_ns, f"app.kubernetes.io/instance={self.release_name}")
        wait_pods_ready(self.milvus_ns, f"release={self.release_name}")
        if wait_minio:
            wait_pods_ready(self.milvus_ns, f"release={self.release_name}-minio")

    def _prepare_collection(self, external_table_minio_env, external_table_prefix, external_format):
        minio_client, cfg = external_table_minio_env
        collection_name = cf.gen_collection_name_by_testcase_name()
        external_key = external_table_prefix["key"]
        external_source, external_spec = self._write_external_dataset(
            external_format,
            minio_client,
            cfg,
            external_key,
        )
        client = _create_milvus_client()
        _create_external_collection(
            client,
            collection_name,
            external_source,
            external_spec,
        )
        return client, collection_name

    def _build_cluster_ip_minio_cfg(self, cfg):
        in_cluster_cfg = dict(cfg)
        minio_cluster_ip = get_svc_ip(self.milvus_ns, f"app=minio,release={self.release_name}-minio")
        in_cluster_cfg["address"] = f"{minio_cluster_ip}:9000"
        in_cluster_cfg["secure"] = False
        return in_cluster_cfg

    def _build_cluster_ip_minio_source(self, cfg, external_key):
        in_cluster_cfg = self._build_cluster_ip_minio_cfg(cfg)
        return build_external_source(in_cluster_cfg, external_key)

    def _write_external_dataset(self, external_format, minio_client, cfg, external_key):
        batches = _external_dataset_batches()
        in_cluster_cfg = self._build_cluster_ip_minio_cfg(cfg)
        in_cluster_source = build_external_source(in_cluster_cfg, external_key)

        if external_format == "vortex":
            _write_vortex_dataset(minio_client, cfg, external_key, batches)
            return in_cluster_source, build_external_spec(in_cluster_cfg, fmt=external_format)

        source, external_spec = write_basic_format_dataset(
            external_format,
            minio_client,
            cfg,
            in_cluster_source,
            external_key,
            batches,
        )
        source = _replace_source_address(source, cfg["address"], in_cluster_cfg["address"])
        return source, external_spec

    def _milvus_component_selector(self, component):
        component_key = "app.kubernetes.io/component" if self.deploy_by == "milvus-operator" else "component"
        return (
            f"app.kubernetes.io/instance={self.release_name},app.kubernetes.io/name=milvus,{component_key}={component}"
        )

    def _skip_if_component_absent(self, component):
        pods = get_pod_list(self.milvus_ns, self._milvus_component_selector(component))
        if not pods:
            pytest.skip(f"milvus component {component} has no dedicated pod in release {self.release_name}")

    def _kill_component_once(self, target_component, require_component=True):
        if require_component:
            self._skip_if_component_absent(target_component)

        chaos_res = None
        meta_name = None
        try:
            chaos_config = self._build_chaos_config("pod_kill", target_component)
            chaos_res, meta_name = self._apply_chaos(chaos_config)
            self._wait_chaos_injected(chaos_res, meta_name)
        finally:
            if chaos_res is not None:
                self._delete_chaos_and_wait_ready(chaos_res, meta_name, wait_minio=target_component == "minio")

    def _submit_refresh_and_kill_component(self, client, collection_name, target_component):
        chaos_res = None
        meta_name = None
        job_id = _submit_refresh(client, collection_name)
        try:
            chaos_config = self._build_chaos_config("pod_kill", target_component)
            chaos_res, meta_name = self._apply_chaos(chaos_config)
            self._wait_chaos_injected(chaos_res, meta_name)
        finally:
            if chaos_res is not None:
                self._delete_chaos_and_wait_ready(chaos_res, meta_name, wait_minio=target_component == "minio")
        return job_id

    def _submit_refresh_during_minio_partition(self, client, collection_name):
        chaos_res = None
        meta_name = None
        job_id = None
        progress = None
        self._skip_if_component_absent("datanode")
        try:
            chaos_config = self._build_chaos_config(
                "network_partition",
                "minio",
                target_scope="all",
                schedule=None,
            )
            self._limit_milvus_source_selector_to_component(chaos_config, "datanode")
            chaos_res, meta_name = self._apply_chaos(chaos_config)
            self._wait_chaos_injected(chaos_res, meta_name)
            job_id = _submit_refresh(client, collection_name)
            try:
                progress = _poll_refresh_terminal(client, job_id, timeout=_MINIO_PARTITION_WINDOW_SECONDS)
            except TimeoutError:
                log.info(
                    f"refresh job {job_id} did not finish during {_MINIO_PARTITION_WINDOW_SECONDS}s "
                    "datanode-to-minio partition window; continue after recovery"
                )
        finally:
            if chaos_res is not None:
                self._delete_chaos_and_wait_ready(chaos_res, meta_name, wait_minio=True)
        return job_id, progress

    def _submit_refresh_during_minio_network_latency(self, client, collection_name):
        chaos_res = None
        meta_name = None
        progress = None
        try:
            chaos_config = self._build_chaos_config(
                "network_latency",
                "minio",
                target_scope="all",
                schedule=None,
            )
            chaos_res, meta_name = self._apply_chaos(chaos_config)
            self._wait_chaos_injected(chaos_res, meta_name)
            job_id = _submit_refresh(client, collection_name)
            progress = _poll_refresh_terminal(client, job_id, timeout=_POD_KILL_REFRESH_TIMEOUT)
        finally:
            if chaos_res is not None:
                self._delete_chaos_and_wait_ready(chaos_res, meta_name, wait_minio=True)
        return progress

    def _submit_refresh_while_minio_unavailable(self, client, collection_name):
        chaos_res = None
        meta_name = None
        recovery_thread = None
        recovery_error = []
        job_id = None
        submit_error = None

        def recover_minio_after_window():
            try:
                sleep(_MINIO_PARTITION_WINDOW_SECONDS)
                self._delete_chaos_and_wait_ready(chaos_res, meta_name, wait_minio=True)
            except Exception as exc:
                recovery_error.append(exc)

        try:
            chaos_config = self._build_chaos_config(
                "network_partition",
                "minio",
                target_scope="all",
                schedule=None,
            )
            chaos_res, meta_name = self._apply_chaos(chaos_config)
            self._wait_chaos_injected(chaos_res, meta_name)
            recovery_thread = threading.Thread(
                target=recover_minio_after_window,
                name=f"{meta_name}-recovery",
                daemon=True,
            )
            recovery_thread.start()
            try:
                job_id = _submit_refresh(client, collection_name)
            except Exception as exc:
                submit_error = exc
        finally:
            if recovery_thread is not None:
                recovery_thread.join(timeout=_MINIO_PARTITION_WINDOW_SECONDS + 120)
                if recovery_thread.is_alive():
                    raise AssertionError(f"chaos object {meta_name} recovery thread did not finish")
                if recovery_error:
                    raise AssertionError(f"chaos object {meta_name} recovery failed: {recovery_error[0]}")
            elif chaos_res is not None:
                self._delete_chaos_and_wait_ready(chaos_res, meta_name, wait_minio=True)
        return job_id, submit_error

    def _create_index_with_component_kill(self, client, collection_name, target_component):
        self._skip_if_component_absent(target_component)
        chaos_res = None
        meta_name = None
        create_error = None
        try:
            chaos_config = self._build_chaos_config("pod_kill", target_component)
            chaos_res, meta_name = self._apply_chaos(chaos_config)
            self._wait_chaos_injected(chaos_res, meta_name)
            try:
                _create_index_with_retry(client, collection_name, timeout=60)
                return
            except Exception as exc:
                create_error = exc
                log.warning(f"create_index during {target_component} pod kill failed before recovery: {exc}")
        finally:
            if chaos_res is not None:
                self._delete_chaos_and_wait_ready(chaos_res, meta_name)

        log.info(f"retry create_index after {target_component} recovery, previous_error={create_error}")
        _create_index_with_retry(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_external_table_refresh_datacoord_pod_kill_after_job_accepted(
        self, external_table_minio_env, external_table_prefix, external_table_format
    ):
        """
        target: external table refresh with datacoord pod kill after job accepted
        method: submit refresh, immediately schedule datacoord pod kill, then poll the exact job
        expected: original job reaches terminal state; if it fails, it exposes reason and refresh succeeds after recovery
        """
        client, collection_name = self._prepare_collection(
            external_table_minio_env, external_table_prefix, external_table_format
        )
        try:
            # DataCoord is embedded in mixcoord in current cluster deployments.
            job_id = self._submit_refresh_and_kill_component(client, collection_name, "mixcoord")
            progress = _poll_refresh_terminal(client, job_id, timeout=_POD_KILL_REFRESH_TIMEOUT)
            _assert_refresh_terminal(progress, "mixcoord/datacoord pod kill after refresh accepted")

            if _progress_state(progress) == "RefreshCompleted":
                _index_load_and_assert_queryable(client, collection_name, _FAULT_ROWS)
            else:
                _refresh_and_wait_completed(client, collection_name, "datacoord recovery refresh")
                _index_load_and_assert_queryable(client, collection_name, _FAULT_ROWS)
        finally:
            _drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_external_table_refresh_datanode_pod_kill_after_job_accepted(
        self, external_table_minio_env, external_table_prefix, external_table_format
    ):
        """
        target: external table refresh with datanode pod kill after job accepted
        method: submit refresh, immediately schedule datanode pod kill, then poll the exact job
        expected: original job reaches terminal state; if it fails, it exposes reason and refresh succeeds after recovery
        """
        client, collection_name = self._prepare_collection(
            external_table_minio_env, external_table_prefix, external_table_format
        )
        try:
            job_id = self._submit_refresh_and_kill_component(client, collection_name, "datanode")
            progress = _poll_refresh_terminal(client, job_id, timeout=_POD_KILL_REFRESH_TIMEOUT)
            _assert_refresh_terminal(progress, "datanode pod kill after refresh accepted")

            if _progress_state(progress) == "RefreshCompleted":
                _index_load_and_assert_queryable(client, collection_name, _FAULT_ROWS)
            else:
                _refresh_and_wait_completed(client, collection_name, "datanode recovery refresh")
                _index_load_and_assert_queryable(client, collection_name, _FAULT_ROWS)
        finally:
            _drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_external_table_refresh_minio_pod_kill_after_job_accepted(
        self, external_table_minio_env, external_table_prefix, external_table_format
    ):
        """
        target: external table refresh with MinIO pod kill after job accepted
        method: submit refresh, kill the MinIO pod once, wait MinIO/Milvus recovery, then poll the exact job
        expected: original job reaches terminal state; if it fails, it exposes reason and refresh succeeds after recovery
        """
        client, collection_name = self._prepare_collection(
            external_table_minio_env, external_table_prefix, external_table_format
        )
        try:
            job_id = self._submit_refresh_and_kill_component(client, collection_name, "minio")
            progress = _poll_refresh_terminal(client, job_id, timeout=_POD_KILL_REFRESH_TIMEOUT)
            _assert_refresh_terminal(progress, "minio pod kill after refresh accepted")

            if _progress_state(progress) == "RefreshCompleted":
                _index_load_and_assert_queryable(client, collection_name, _FAULT_ROWS)
            else:
                _refresh_and_wait_completed(client, collection_name, "minio recovery refresh")
                _index_load_and_assert_queryable(client, collection_name, _FAULT_ROWS)
        finally:
            _drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_external_table_refresh_minio_network_partition_window(
        self, external_table_minio_env, external_table_prefix, external_table_format
    ):
        """
        target: external table refresh while DataNode cannot reach MinIO for a full fault window
        method: apply DataNode-to-MinIO NetworkChaos before refresh submit, hold it for the window, then poll the exact job
        expected: original job reaches terminal state; if it fails, it exposes reason and refresh succeeds after recovery
        """
        client, collection_name = self._prepare_collection(
            external_table_minio_env, external_table_prefix, external_table_format
        )
        try:
            job_id, progress = self._submit_refresh_during_minio_partition(client, collection_name)
            if progress is None:
                progress = _poll_refresh_terminal(client, job_id, timeout=_POD_KILL_REFRESH_TIMEOUT)
            _assert_refresh_terminal(progress, "minio network partition during refresh")

            if _progress_state(progress) == "RefreshCompleted":
                _index_load_and_assert_queryable(client, collection_name, _FAULT_ROWS)
            else:
                _refresh_and_wait_completed(client, collection_name, "minio partition recovery refresh")
                _index_load_and_assert_queryable(client, collection_name, _FAULT_ROWS)
        finally:
            _drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_external_table_refresh_minio_network_latency_window(
        self, external_table_minio_env, external_table_prefix, external_table_format
    ):
        """
        target: external table refresh while Milvus-to-MinIO network latency is active
        method: apply MinIO NetworkChaos delay before refresh submit and remove it after the exact job reaches terminal state
        expected: refresh completes and the loaded external collection is queryable with the expected row count
        """
        client, collection_name = self._prepare_collection(
            external_table_minio_env, external_table_prefix, external_table_format
        )
        try:
            progress = self._submit_refresh_during_minio_network_latency(client, collection_name)
            _assert_refresh_completed(progress, "minio network latency refresh")
            _index_load_and_assert_queryable(client, collection_name, _FAULT_ROWS)
        finally:
            _drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_external_table_refresh_submit_when_minio_unavailable(
        self, external_table_minio_env, external_table_prefix, external_table_format
    ):
        """
        target: external table refresh submitted while Milvus cannot reach MinIO
        method: apply Milvus-to-MinIO NetworkChaos before refresh submit, remove chaos, then verify failure or recovery
        expected: submit fails clearly or the accepted job reaches terminal state; refresh succeeds after recovery
        """
        client, collection_name = self._prepare_collection(
            external_table_minio_env, external_table_prefix, external_table_format
        )
        try:
            job_id, submit_error = self._submit_refresh_while_minio_unavailable(client, collection_name)
            if submit_error is not None:
                assert str(submit_error) or repr(submit_error), "refresh submit failed without useful error text"
                _refresh_after_recovery_or_wait_existing(client, collection_name, "minio unavailable recovery refresh")
            else:
                assert job_id is not None, "refresh submit returned neither job id nor error"
                progress = _poll_refresh_terminal(client, job_id, timeout=_POD_KILL_REFRESH_TIMEOUT)
                _assert_refresh_terminal(progress, "minio unavailable refresh submit")
                if _progress_state(progress) == "RefreshFailed":
                    _refresh_after_recovery_or_wait_existing(
                        client, collection_name, "minio unavailable recovery refresh"
                    )

            _index_load_and_assert_queryable(client, collection_name, _FAULT_ROWS)
        finally:
            _drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_external_table_refresh_minio_io_latency_window(
        self, external_table_minio_env, external_table_prefix, external_table_format
    ):
        """
        target: external table refresh while MinIO IO latency is active
        method: apply MinIO IOChaos before refresh submit and remove it after the exact job reaches terminal state
        expected: refresh completes and the loaded external collection is queryable with the expected row count
        """
        client, collection_name = self._prepare_collection(
            external_table_minio_env, external_table_prefix, external_table_format
        )
        chaos_res = None
        meta_name = None
        progress = None
        try:
            try:
                chaos_config = self._build_chaos_config(
                    "io_latency",
                    "minio",
                    target_scope="all",
                    schedule=None,
                    delay=_MINIO_IO_LATENCY_DELAY,
                )
                chaos_res, meta_name = self._apply_chaos(chaos_config)
                self._wait_chaos_injected(chaos_res, meta_name)
                job_id = _submit_refresh(client, collection_name)
                progress = _poll_refresh_terminal(client, job_id)
            finally:
                if chaos_res is not None:
                    self._delete_chaos_and_wait_ready(chaos_res, meta_name, wait_minio=True)

            _assert_refresh_completed(progress, "minio io latency refresh")
            _index_load_and_assert_queryable(client, collection_name, _FAULT_ROWS)
        finally:
            _drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_external_table_query_path_querynode_pod_kill_after_refresh(
        self, external_table_minio_env, external_table_prefix, external_table_format
    ):
        """
        target: external table query path after querynode pod kill
        method: refresh/index/load the external table, kill querynode once, then retry query/search after recovery
        expected: loaded external collection remains queryable with the expected row count
        """
        client, collection_name = self._prepare_collection(
            external_table_minio_env, external_table_prefix, external_table_format
        )
        try:
            _refresh_and_wait_completed(client, collection_name, "querynode post-refresh baseline refresh")
            _create_index_with_retry(client, collection_name)
            _load_collection_with_retry(client, collection_name)

            self._kill_component_once("querynode")
            _assert_queryable_with_retry(client, collection_name, _FAULT_ROWS)
        finally:
            _drop_collection(client, collection_name)

    @pytest.mark.tags(CaseLabel.L3)
    def test_external_table_index_path_datanode_pod_kill_after_refresh(
        self, external_table_minio_env, external_table_prefix, external_table_format
    ):
        """
        target: external table index path after refresh with datanode pod kill
        method: refresh the external table, kill datanode while creating index, then load/query after recovery
        expected: index creation recovers and the loaded external collection is queryable with the expected row count
        """
        client, collection_name = self._prepare_collection(
            external_table_minio_env, external_table_prefix, external_table_format
        )
        try:
            _refresh_and_wait_completed(client, collection_name, "datanode post-refresh baseline refresh")
            self._create_index_with_component_kill(client, collection_name, "datanode")
            _load_collection_with_retry(client, collection_name)
            _assert_queryable_with_retry(client, collection_name, _FAULT_ROWS)
        finally:
            _drop_collection(client, collection_name)
