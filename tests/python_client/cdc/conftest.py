import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, wait

import pytest
from pymilvus import MilvusClient

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


CDC_UPDATE_REPLICATE_TIMEOUT_SECONDS = 600


def apply_replicate_configuration(tasks, timeout=CDC_UPDATE_REPLICATE_TIMEOUT_SECONDS):
    # Fan out in parallel: the server blocks non-primary clusters in
    # waitUntilPrimaryChangeOrConfigurationSame until the primary's broadcast
    # propagates via CDC, so a sequential call where the first client happens
    # to be a replica deadlocks on the client's RPC timeout.
    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        futures = [
            executor.submit(client.update_replicate_configuration, timeout=timeout, **config)
            for client, config in tasks
        ]
        wait(futures)
    for f in futures:
        f.result()


def pytest_addoption(parser):
    """Add command line options for pytest."""
    parser.addoption(
        "--upstream-uri",
        action="store",
        default="http://10.104.17.154:19530",
        help="Upstream Milvus uri",
    )
    parser.addoption(
        "--upstream-token",
        action="store",
        default="root:Milvus",
        help="Upstream Milvus token",
    )
    parser.addoption(
        "--downstream-uri",
        action="store",
        default="http://10.104.17.156:19530",
        help="Downstream Milvus uri",
    )
    parser.addoption(
        "--downstream-token",
        action="store",
        default="root:Milvus",
        help="Downstream Milvus token",
    )
    parser.addoption("--sync-timeout", action="store", default="30", help="Sync timeout in seconds")
    parser.addoption(
        "--source-cluster-id",
        action="store",
        default="cdc-test-source-0930",
        help="Source cluster ID for CDC topology",
    )
    parser.addoption(
        "--target-cluster-id",
        action="store",
        default="cdc-test-target-0930",
        help="Target cluster ID for CDC topology",
    )
    parser.addoption(
        "--pchannel-num",
        action="store",
        default="16",
        help="Number of physical channels for CDC",
    )
    parser.addoption(
        "--request-duration",
        action="store",
        default="30m",
        help="Duration for test operations (e.g., 30m, 1h, 60s)",
    )
    parser.addoption(
        "--is-check",
        action="store",
        default="true",
        help="Whether to assert on checker statistics",
    )
    parser.addoption(
        "--milvus-ns",
        action="store",
        default="chaos-testing",
        help="Kubernetes namespace for Milvus deployment",
    )


@pytest.fixture(scope="session")
def upstream_client(request):
    """Create upstream MilvusClient."""
    uri = request.config.getoption("--upstream-uri")
    token = request.config.getoption("--upstream-token")
    client = MilvusClient(uri=uri, token=token)
    yield client
    client.close()


@pytest.fixture(scope="session")
def downstream_client(request):
    """Create downstream MilvusClient."""
    uri = request.config.getoption("--downstream-uri")
    token = request.config.getoption("--downstream-token")
    client = MilvusClient(uri=uri, token=token)
    yield client
    client.close()


@pytest.fixture(scope="session")
def sync_timeout(request):
    """Get sync timeout from command line."""
    return int(request.config.getoption("--sync-timeout"))


@pytest.fixture(scope="session")
def upstream_uri(request):
    """Get upstream uri from command line."""
    return request.config.getoption("--upstream-uri")


@pytest.fixture(scope="session")
def upstream_token(request):
    """Get upstream token from command line."""
    return request.config.getoption("--upstream-token")


@pytest.fixture(scope="session")
def downstream_uri(request):
    """Get downstream uri from command line."""
    return request.config.getoption("--downstream-uri")


@pytest.fixture(scope="session")
def downstream_token(request):
    """Get downstream token from command line."""
    return request.config.getoption("--downstream-token")


@pytest.fixture(scope="session")
def source_cluster_id(request):
    """Get source cluster id from command line."""
    return request.config.getoption("--source-cluster-id")


@pytest.fixture(scope="session")
def target_cluster_id(request):
    """Get target cluster id from command line."""
    return request.config.getoption("--target-cluster-id")


@pytest.fixture(scope="session")
def pchannel_num(request):
    """Get pchannel num from command line."""
    return int(request.config.getoption("--pchannel-num"))


@pytest.fixture(scope="session")
def request_duration(request):
    """Get request duration from command line."""
    return request.config.getoption("--request-duration")


@pytest.fixture(scope="session")
def is_check(request):
    # The root tests/python_client/conftest.py registers --is_check (underscore)
    # with type=bool, which argparse maps to the same dest (is_check) as our
    # --is-check (hyphen). The root's bool wins in chaos runs, so accept either.
    val = request.config.getoption("--is-check")
    return val if isinstance(val, bool) else str(val).lower() == "true"


@pytest.fixture(scope="session")
def milvus_ns(request):
    return request.config.getoption("--milvus-ns")


@pytest.fixture(scope="session")
def switchover_helper(request, upstream_client, downstream_client):
    """Returns a callable that performs CDC topology switchover."""
    upstream_uri = request.config.getoption("--upstream-uri")
    upstream_token = request.config.getoption("--upstream-token")
    downstream_uri = request.config.getoption("--downstream-uri")
    downstream_token = request.config.getoption("--downstream-token")
    pchannel_num = int(request.config.getoption("--pchannel-num"))
    original_source = request.config.getoption("--source-cluster-id")
    original_target = request.config.getoption("--target-cluster-id")

    # Map cluster IDs to their URIs/tokens
    cluster_map = {
        original_source: {"uri": upstream_uri, "token": upstream_token},
        original_target: {"uri": downstream_uri, "token": downstream_token},
    }

    def do_switchover(new_source_id, new_target_id):
        logger.info(f"Performing switchover: {new_source_id} -> {new_target_id}")
        config = {
            "clusters": [
                {
                    "cluster_id": new_source_id,
                    "connection_param": cluster_map[new_source_id],
                    "pchannels": [f"{new_source_id}-rootcoord-dml_{i}" for i in range(pchannel_num)],
                },
                {
                    "cluster_id": new_target_id,
                    "connection_param": cluster_map[new_target_id],
                    "pchannels": [f"{new_target_id}-rootcoord-dml_{i}" for i in range(pchannel_num)],
                },
            ],
            "cross_cluster_topology": [{"source_cluster_id": new_source_id, "target_cluster_id": new_target_id}],
        }
        # Dedicated short-lived clients so switchover RPCs don't share a
        # gRPC channel with concurrent DML on the session-scoped clients.
        # pymilvus's connection manager closes a channel on UNAVAILABLE /
        # STREAMING_CODE_REPLICATE_VIOLATION to trigger recovery; if a DML
        # on the session client triggers that close while our sibling
        # update_replicate_configuration RPC is in flight on the same
        # channel, the latter surfaces "Cannot invoke RPC on closed
        # channel!". Separate clients = separate channels = no race.
        up_tmp = MilvusClient(uri=upstream_uri, token=upstream_token)
        dn_tmp = MilvusClient(uri=downstream_uri, token=downstream_token)
        try:
            apply_replicate_configuration([(up_tmp, config), (dn_tmp, config)])
        finally:
            up_tmp.close()
            dn_tmp.close()
        logger.info("Switchover completed, waiting 10s for stabilization...")
        time.sleep(10)

    return do_switchover


@pytest.fixture(scope="session")
def kubectl_helper(milvus_ns):
    """Helpers for container-kill failover scenarios."""
    import subprocess as _sp
    import time as _time

    def get_pods(instance_label):
        cmd = [
            "kubectl",
            "get",
            "pods",
            "-l",
            f"app.kubernetes.io/instance={instance_label}",
            "-n",
            milvus_ns,
            "-o",
            "json",
        ]
        result = _sp.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"failed to list pods for {instance_label}: stdout={result.stdout!r}, stderr={result.stderr!r}"
            )
        pods = json.loads(result.stdout).get("items", [])
        if not pods:
            raise RuntimeError(f"no pods matched instance {instance_label}")
        return pods

    def snapshot_containers(pods):
        snapshot = {}
        for pod in pods:
            metadata = pod["metadata"]
            pod_name = metadata["name"]
            statuses = {status["name"]: status for status in pod.get("status", {}).get("containerStatuses", [])}
            containers = {}
            for container in pod.get("spec", {}).get("containers", []):
                container_name = container["name"]
                containers[container_name] = statuses.get(container_name, {}).get("restartCount", -1)
            snapshot[pod_name] = {
                "uid": metadata["uid"],
                "containers": containers,
            }
        return snapshot

    def kill_containers(instance_label, timeout=120):
        """Kill every matching container without deleting its Pod object."""
        baseline = snapshot_containers(get_pods(instance_label))
        missing_status = [
            f"{pod_name}/{container_name}"
            for pod_name, pod in baseline.items()
            for container_name, restart_count in pod["containers"].items()
            if restart_count < 0
        ]
        if missing_status:
            raise RuntimeError(f"containers have no initial status: {missing_status}")

        pods_by_containers = {}
        for pod_name, pod in baseline.items():
            container_names = tuple(sorted(pod["containers"]))
            pods_by_containers.setdefault(container_names, []).append(pod_name)

        safe_instance = re.sub(r"[^a-z0-9-]", "-", instance_label.lower()).strip("-")[:20]
        run_id = str(_time.time_ns())[-10:]
        chaos_names = []
        try:
            for index, (container_names, pod_names) in enumerate(pods_by_containers.items(), start=1):
                chaos_name = f"cdc-ck-{safe_instance}-{run_id}-{index}"
                chaos = {
                    "apiVersion": "chaos-mesh.org/v1alpha1",
                    "kind": "PodChaos",
                    "metadata": {"name": chaos_name, "namespace": milvus_ns},
                    "spec": {
                        "selector": {"pods": {milvus_ns: sorted(pod_names)}},
                        "mode": "all",
                        "action": "container-kill",
                        "containerNames": list(container_names),
                    },
                }
                result = _sp.run(
                    ["kubectl", "create", "-f", "-"],
                    input=json.dumps(chaos),
                    capture_output=True,
                    text=True,
                    check=False,
                )
                logger.info(
                    f"[CONTAINER_KILL] create {chaos_name}: rc={result.returncode}, "
                    f"pods={sorted(pod_names)}, containers={list(container_names)}, "
                    f"stdout={result.stdout!r}, stderr={result.stderr!r}"
                )
                if result.returncode != 0:
                    raise RuntimeError(
                        f"failed to create PodChaos {chaos_name}: stdout={result.stdout!r}, stderr={result.stderr!r}"
                    )
                chaos_names.append(chaos_name)

            deadline = _time.time() + timeout
            pending = []
            while _time.time() < deadline:
                current = snapshot_containers(get_pods(instance_label))
                pending = []
                for pod_name, original in baseline.items():
                    observed = current.get(pod_name)
                    if observed is None:
                        raise RuntimeError(
                            f"Pod {pod_name} disappeared during container-kill; the fault must preserve Pod objects"
                        )
                    if observed["uid"] != original["uid"]:
                        raise RuntimeError(
                            f"Pod {pod_name} was recreated during container-kill: "
                            f"old UID={original['uid']}, new UID={observed['uid']}"
                        )
                    for container_name, restart_count in original["containers"].items():
                        observed_count = observed["containers"].get(container_name, -1)
                        if observed_count <= restart_count:
                            pending.append(f"{pod_name}/{container_name}({restart_count}->{observed_count})")
                if not pending:
                    logger.info(
                        f"[CONTAINER_KILL] all containers restarted for {instance_label}; "
                        f"Pod UIDs unchanged: {sorted(baseline)}"
                    )
                    return baseline
                _time.sleep(1)

            raise TimeoutError(
                f"container-kill was not observed for {instance_label} within {timeout}s; pending={pending}"
            )
        finally:
            for chaos_name in chaos_names:
                result = _sp.run(
                    [
                        "kubectl",
                        "delete",
                        "podchaos",
                        chaos_name,
                        "-n",
                        milvus_ns,
                        "--ignore-not-found",
                        "--wait=false",
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                logger.info(f"[CONTAINER_KILL] delete {chaos_name}: rc={result.returncode}")

    def wait_for_pods_ready(instance_label, timeout=300):
        """Wait for pods matching the label to be Ready.

        Keep the existence poll so this helper also handles an unexpected Pod
        recreation without letting `kubectl wait` fail with "no matching
        resources found". The expected container-kill path preserves Pod UIDs.
        So:
          1. Poll until at least one pod matches.
          2. Then `kubectl wait` for Ready on the remaining time budget.
        """
        deadline = _time.time() + timeout
        existence = None
        while _time.time() < deadline:
            existence = _sp.run(
                [
                    "kubectl",
                    "get",
                    "pods",
                    "-l",
                    f"app.kubernetes.io/instance={instance_label}",
                    "-n",
                    milvus_ns,
                    "--no-headers",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if existence.stdout.strip():
                break
            _time.sleep(5)
        else:
            raise TimeoutError(f"no pods matched {instance_label} within {timeout}s")

        remaining = max(1, int(deadline - _time.time()))
        cmd = [
            "kubectl",
            "wait",
            "--for=condition=Ready",
            "pods",
            "-l",
            f"app.kubernetes.io/instance={instance_label}",
            "-n",
            milvus_ns,
            f"--timeout={remaining}s",
        ]
        result = _sp.run(cmd, capture_output=True, text=True, check=False)
        logger.info(f"[KUBECTL] wait pods {instance_label}: rc={result.returncode}")
        if result.returncode != 0:
            raise TimeoutError(
                f"pods matching {instance_label} did not become Ready within {remaining}s: "
                f"stdout={result.stdout!r}, stderr={result.stderr!r}"
            )
        return result

    class KubectlHelper:
        pass

    KubectlHelper.kill_containers = staticmethod(kill_containers)
    KubectlHelper.wait_for_pods_ready = staticmethod(wait_for_pods_ready)

    return KubectlHelper()


@pytest.fixture(scope="session", autouse=True)
def cdc_topology_setup(request, upstream_client, downstream_client):
    """Setup CDC topology at the beginning of test session."""
    upstream_uri = request.config.getoption("--upstream-uri")
    downstream_uri = request.config.getoption("--downstream-uri")
    source_cluster_id = request.config.getoption("--source-cluster-id")
    target_cluster_id = request.config.getoption("--target-cluster-id")
    pchannel_num = int(request.config.getoption("--pchannel-num"))

    logger.info(f"Setting up CDC topology: {source_cluster_id} -> {target_cluster_id} (channels: {pchannel_num})...")

    # Create CDC replication configuration
    config = {
        "clusters": [
            {
                "cluster_id": source_cluster_id,
                "connection_param": {
                    "uri": upstream_uri,
                    "token": request.config.getoption("--upstream-token"),
                },
                "pchannels": [f"{source_cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)],
            },
            {
                "cluster_id": target_cluster_id,
                "connection_param": {
                    "uri": downstream_uri,
                    "token": request.config.getoption("--downstream-token"),
                },
                "pchannels": [f"{target_cluster_id}-rootcoord-dml_{i}" for i in range(pchannel_num)],
            },
        ],
        "cross_cluster_topology": [
            {
                "source_cluster_id": source_cluster_id,
                "target_cluster_id": target_cluster_id,
            }
        ],
    }

    try:
        # Dedicated clients for the control-plane update_replicate_configuration
        # RPC, mirroring switchover_helper. Keeps the session-scoped clients'
        # channels clean of any recovery side effects from the initial setup.
        up_tmp = MilvusClient(uri=upstream_uri, token=request.config.getoption("--upstream-token"))
        dn_tmp = MilvusClient(uri=downstream_uri, token=request.config.getoption("--downstream-token"))
        try:
            apply_replicate_configuration([(up_tmp, config), (dn_tmp, config)])
        finally:
            up_tmp.close()
            dn_tmp.close()
        logger.info("CDC topology setup completed successfully")

        # Allow some time for CDC to initialize
        time.sleep(5)

    except Exception as e:
        logger.error(f"Failed to setup CDC topology: {e}")
        raise

    yield

    # Cleanup can be added here if needed
    logger.info("CDC topology teardown completed")
