"""Real PAI-L3-13 driver for a dedicated Kubernetes standalone test-hook image.

Reads one control-protocol JSON request on stdin. All Kubernetes commands bind
an explicit kubeconfig or CI service account and verify its API server. Only two explicit
etcd config keys and lease-owned hook files are changed. No shell is used.
"""

import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit

from pymilvus import MilvusClient
from pymilvus.grpc_gen import common_pb2

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.paimon_etcd import EtcdKV, decode_fields, scalar  # noqa: E402

HOOK_DIR = "/tmp/milvus-paimon-e2e-hooks"
SOURCE_COMMIT = "4f95cb61d849bd4ecb03fa1f641179825a58b05d"
WAIT = "dataCoord.externalCollection.refreshWaitForIndex"
TIMEOUT = "dataCoord.externalCollectionJobTimeout"
STATES = {1: "RefreshInit", 2: "RefreshInProgress", 3: "RefreshCompleted", 4: "RefreshFailed", 5: "RefreshRetry"}


def save(path, value):
    temporary = path.with_suffix(".writing")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, sort_keys=True)
    temporary.replace(path)


def validate_service_url(value, service, namespace, port):
    parsed = urlsplit(value)
    hosts = {f"{service}.{namespace}", f"{service}.{namespace}.svc", f"{service}.{namespace}.svc.cluster.local"}
    assert (
        parsed.scheme == "http"
        and parsed.hostname in hosts
        and parsed.port == port
        and parsed.path in {"", "/"}
        and not parsed.query
        and not parsed.fragment
        and not parsed.username
        and not parsed.password
    ), "CI service endpoint must identify the selected deployment and namespace"


def in_cluster_kubeconfig(
    path, cluster, namespace, service_account=Path("/var/run/secrets/kubernetes.io/serviceaccount")
):
    host, port = os.environ["KUBERNETES_SERVICE_HOST"], os.environ["KUBERNETES_SERVICE_PORT"]
    if ":" in host:
        host = f"[{host}]"
    assert cluster.rstrip("/") in {
        f"https://{host}:{port}",
        "https://kubernetes.default.svc",
        "https://kubernetes.default.svc:443",
    }, "CI profile Kubernetes API does not match the in-cluster service account"
    # Preserve the stable projected-volume symlinks across kubelet rotations.
    token_file = service_account.absolute() / "token"
    ca_file = service_account.absolute() / "ca.crt"
    assert os.access(token_file, os.R_OK) and os.access(ca_file, os.R_OK), "CI service-account files are unreadable"
    # Reference the projected token file; never copy its bytes into state or argv.
    save(
        path,
        {
            "apiVersion": "v1",
            "kind": "Config",
            "current-context": "paimon-l3",
            "clusters": [{"name": "selected", "cluster": {"server": cluster, "certificate-authority": str(ca_file)}}],
            "users": [{"name": "ci", "user": {"tokenFile": str(token_file)}}],
            "contexts": [
                {"name": "paimon-l3", "context": {"cluster": "selected", "user": "ci", "namespace": namespace}}
            ],
        },
    )
    return str(path)


def decode_job(data):
    fields = decode_fields(data)
    for number in [1, 2, 6, 8, 13, 15]:
        if not isinstance(scalar(fields, number), int):
            raise ValueError("job evidence has a non-integer protobuf field")
    if scalar(fields, 15) not in {0, 1}:
        raise ValueError("job applied marker is not boolean")
    spec = json.loads(scalar(fields, 5, b"{}").decode())
    snapshot = spec.get("snapshot_id")
    return {
        "job_id": scalar(fields, 1),
        "collection_id": scalar(fields, 2),
        "collection_name": scalar(fields, 3, b"").decode(),
        "state": STATES.get(scalar(fields, 6), "unknown"),
        "start_time": scalar(fields, 8),
        "index_wait_started_time": scalar(fields, 13),
        "segments_applied": bool(scalar(fields, 15)),
        "snapshot_id": int(snapshot) if snapshot is not None else None,
    }


class Driver:
    def __init__(self, request):
        assert request["protocol"] == 1
        self.request = request
        self.lease = request["lease"]
        assert re.fullmatch(r"[a-f0-9]{32}", self.lease)
        self.target = request["target"]
        self.mode = self.target.get("access_mode", "cell")
        assert self.mode in {"cell", "service"}, "unknown Paimon deployment access mode"
        for key in ["namespace", "deployment"]:
            assert re.fullmatch(r"[a-z0-9][a-z0-9-]*", self.target[key])
        self.uri = request["milvus_uri"]
        self.directory = Path(tempfile.gettempdir()) / "milvus-paimon-l3" / self.lease
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.state_path = self.directory / "state.json"
        self.state = json.loads(self.state_path.read_bytes()) if self.state_path.exists() else {}
        if self.state:
            assert self.state["target"] == self.target and self.state["uri"] == self.uri
        if self.mode == "service":
            for key in ["milvus_service", "etcd_service"]:
                assert re.fullmatch(r"[a-z0-9][a-z0-9-]*", self.target.get(key, "")), (
                    "CI profile requires Service names"
                )
            validate_service_url(self.uri, self.target["milvus_service"], self.target["namespace"], 19530)
            validate_service_url(
                self.target["etcd_endpoint"], self.target["etcd_service"], self.target["namespace"], 2379
            )
        if self.target.get("kubeconfig"):
            self.kubeconfig = str(Path(self.target["kubeconfig"]).expanduser().resolve(strict=True))
        else:
            assert self.mode == "service", "Cell access requires an explicit kubeconfig"
            self.kubeconfig = in_cluster_kubeconfig(
                self.directory / "ci-kubeconfig.json", self.target["cluster"], self.target["namespace"]
            )
        self.base = ["kubectl", "--kubeconfig", self.kubeconfig]
        cluster = self._run(self.base + ["config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}"])
        assert cluster.rstrip("/") == self.target["cluster"].rstrip("/"), "wrong Kubernetes API server"
        self.base += ["--namespace", self.target["namespace"]]
        self.kv = EtcdKV(self.target["etcd_endpoint"])
        self.root = self.target["etcd_root_prefix"]
        assert re.fullmatch(r"[A-Za-z0-9_-]+", self.root), "unexpected etcd root prefix"
        self._client = None

    @property
    def client(self):
        # Restore hooks/config first even if Milvus RPC is currently unavailable.
        if self._client is None:
            self._client = MilvusClient(
                uri=self.uri, token=os.environ.get("MILVUS_L3_TOKEN", "root:Milvus"), timeout=10
            )
        return self._client

    @staticmethod
    def _run(command, input_text=None):
        result = subprocess.run(command, input=input_text, text=True, capture_output=True, timeout=15, check=False)
        assert result.returncode == 0, f"driver command failed: {result.stderr[:1000]}"
        return result.stdout

    def kube(self, *args):
        return self._run(self.base + list(args))

    def pod(self, *args, input_text=None):
        command = self.base + ["exec"] + (["-i"] if input_text is not None else [])
        command += [self.state["pod"], "--", *args]
        return self._run(command, input_text)

    def key(self, name):
        assert name in {WAIT, TIMEOUT}, "only the two reviewed L3 configuration keys may change"
        return self.root + "/config/" + name.replace(".", "/")

    def effective(self):
        values = dict(self.state["baseline"])
        for name in [WAIT, TIMEOUT]:
            rows = self.kv.range(self.key(name))
            if rows:
                assert len(rows) == 1
                values[name] = json.loads(rows[0][1])
        return values

    def runtime_effective(self):
        url = "http://127.0.0.1:9091/management/config/get?keys=" + WAIT + "," + TIMEOUT
        response = json.loads(self.pod("curl", "-fsS", "--max-time", "5", url))
        values = {row["key"]: json.loads(row["value"]) for row in response["configs"] if not row.get("error")}
        assert set(values) == {WAIT, TIMEOUT}, "component did not expose both runtime settings"
        return values

    def await_runtime(self, expected):
        deadline = time.monotonic() + 20
        while True:
            actual = self.runtime_effective()
            if actual == expected:
                return actual
            assert time.monotonic() < deadline, f"runtime config did not converge: {actual} != {expected}"
            time.sleep(0.5)

    @property
    def lease_key(self):
        return self.root + "/paimon-e2e/control-lease"

    def owns_lease(self):
        rows = self.kv.range(self.lease_key)
        return bool(rows) and rows[0][1].decode() == self.lease

    def require_lease(self):
        assert self.owns_lease(), "this deployment is not owned by this L3 control lease"

    def jobs(self):
        return [
            decode_job(data)
            for _, data in self.kv.range(
                self.root + "/meta/datacoord-meta/external-collection-refresh-job/", prefix=True
            )
        ]

    def live_segments(self, include_manifest=False):
        rows = self.kv.range(self.root + f"/meta/datacoord-meta/s/{self.state['collection_id']}/", prefix=True)
        result = []
        for _, data in rows:
            fields = decode_fields(data)
            assert scalar(fields, 2) == self.state["collection_id"]
            if common_pb2.SegmentState.Name(scalar(fields, 6)) == "Flushed":
                row = {"id": scalar(fields, 1), "rows": scalar(fields, 5)}
                if include_manifest:
                    row["manifest_path"] = scalar(fields, 32, b"").decode()
                result.append(row)
        return sorted(result, key=lambda row: row["id"])

    def files(self):
        return self.pod("ls", "-1", HOOK_DIR).splitlines()

    def event_files(self, kind):
        prefix = f"seen-{kind}-{self.lease}-"
        names = [name for name in self.files() if name.startswith(prefix)]
        assert all(re.fullmatch(r"[a-z0-9-]+", name) for name in names)
        return [json.loads(self.pod("cat", HOOK_DIR + "/" + name)) for name in names]

    def verify_service(self, service):
        deployment = self.target["deployment"]
        milvus_service = self.target.get("milvus_service", deployment + "-milvus")
        etcd_service = self.target.get("etcd_service", deployment + "-etcd")
        assert service in {milvus_service, etcd_service}
        releases = {deployment} if service == milvus_service else {deployment, deployment + "-etcd"}

        def verify_release(labels):
            identities = [labels[key] for key in ["app.kubernetes.io/instance", "release"] if key in labels]
            assert identities and all(identity in releases for identity in identities), (
                "Service/Pod belongs to another release"
            )

        selected = json.loads(self.kube("get", "service", service, "-o", "json"))
        verify_release(selected["metadata"].get("labels", {}))
        owner = selected["metadata"].get("annotations", {}).get("meta.helm.sh/release-name")
        assert owner is None or owner in releases, "Service belongs to another Helm release"
        selector = selected["spec"]["selector"]
        verify_release(selector)
        assert selector and all(
            re.fullmatch(r"[A-Za-z0-9_./-]+", key) and re.fullmatch(r"[A-Za-z0-9_.-]+", value)
            for key, value in selector.items()
        )
        endpoints = json.loads(self.kube("get", "endpoints", service, "-o", "json"))
        addresses = [address for subset in endpoints.get("subsets", []) for address in subset.get("addresses", [])]
        labels = ",".join(f"{key}={value}" for key, value in sorted(selector.items()))
        pods = json.loads(self.kube("get", "pods", "-l", labels, "-o", "json"))
        owned = {pod["metadata"]["name"]: pod for pod in pods["items"]}
        assert addresses, "selected service has no ready endpoints"
        active = {}
        for address in addresses:
            ref = address["targetRef"]
            assert ref["kind"] == "Pod" and ref["name"] in owned
            pod = owned[ref["name"]]
            verify_release(pod["metadata"].get("labels", {}))
            assert all(pod["metadata"]["labels"].get(key) == value for key, value in selector.items())
            assert ref.get("namespace", self.target["namespace"]) == self.target["namespace"]
            assert ref["uid"] == pod["metadata"]["uid"] and address["ip"] == pod["status"]["podIP"]
            active[ref["name"]] = pod
        return list(active.values())

    def verify_etcd_sessions(self, pod):
        sessions = []
        for key, value in self.kv.range(self.root + "/meta/session/", prefix=True):
            if key.rsplit("/", 1)[-1] not in {"rootcoord", "datacoord"}:
                continue
            record = json.loads(value)
            assert isinstance(record, dict), "malformed coordinator session"
            sessions.append(record)
        bound = {}
        for role in ["rootcoord", "datacoord"]:
            matches = [record for record in sessions if record.get("ServerName") == role and not record.get("Stopping")]
            assert matches and all(
                record.get("Address", "").rsplit(":", 1)[0].strip("[]") == pod["status"]["podIP"]
                and record.get("HostName", pod["metadata"]["name"]) == pod["metadata"]["name"]
                for record in matches
            ), "selected etcd root does not belong to this standalone pod"
            bound[role] = [record["Address"] for record in matches]
        return bound

    def prepare(self):
        assert not self.state.get("original_config"), "lease already prepared; restore it before starting another case"
        deployment = self.target["deployment"]
        if self.mode == "cell":
            # Cell's external routes are resolved by its own control plane.
            resolved = json.loads(
                self._run(["milvus-dev-cli", "deploy", "status", deployment, "--namespace", self.target["namespace"]])
            )
            assert resolved["status"] == "Healthy" and resolved["updated"] is True
            assert resolved["cluster"].rstrip("/") == self.target["cluster"].rstrip("/")
            assert resolved["milvus_endpoint"].rstrip("/") == self.uri.rstrip("/")
            assert resolved["etcd_endpoint"] == self.target["etcd_endpoint"]
            assert resolved["image"] == self.target["image"] and resolved["namespace"] == self.target["namespace"]
            assert resolved["etcd_root_prefix"] == self.root
        pods = self.verify_service(self.target.get("milvus_service", deployment + "-milvus"))
        self.verify_service(self.target.get("etcd_service", deployment + "-etcd"))
        assert len(pods) == 1, "Paimon phase driver supports one dedicated standalone pod"
        pod = pods[0]
        assert any(container["image"] == self.target["image"] for container in pod["spec"]["containers"])
        assert pod["status"]["phase"] == "Running"
        assert any(
            condition["type"] == "Ready" and condition["status"] == "True" for condition in pod["status"]["conditions"]
        )
        self.state.update(target=self.target, uri=self.uri, pod=pod["metadata"]["name"])
        session_binding = self.verify_etcd_sessions(pod)
        marker = json.loads(self.pod("cat", HOOK_DIR + "/ready.json"))
        assert marker == {"protocol": 1, "source_commit": "4f95cb61", "test_image_only": True}
        self.state["baseline"] = self.runtime_effective()
        save(self.state_path, self.state)
        assert self.kv.acquire(self.lease_key, self.lease), "another L3 controller owns the deployment"
        assert self.client.list_databases(timeout=10) == ["default"], "dedicated deployment contains other databases"
        assert not self.client.list_collections(timeout=10), "dedicated L3 deployment already contains collections"
        assert not any(name.startswith(("apply-", "index-")) for name in self.files()), "another hook lease is active"
        self.state["original_overrides"] = {
            name: (self.kv.range(self.key(name))[0][1].decode() if self.kv.range(self.key(name)) else None)
            for name in [WAIT, TIMEOUT]
        }
        original = self.await_runtime(self.effective())
        self.state["original_config"] = original
        save(self.state_path, self.state)
        return {
            "exclusive_deployment": True,
            "milvus_uri": self.uri,
            "resolved_target": self.target,
            "original_config": original,
            "access_mode": self.mode,
            "etcd_session_binding": session_binding,
        }

    def arm(self):
        assert self.state.get("original_config"), "prepare must precede arm"
        self.require_lease()
        name, snapshot = self.request["collection"], int(self.request["snapshot_id"])
        description = self.client.describe_collection(collection_name=name, timeout=10)
        assert description["external_source"], "hook target is not an external collection"
        collection_id = int(description["collection_id"])
        # Bind the public URI to this exact etcd root using the unique baseline
        # collection and its completed job before changing configuration.
        assert any(
            job["collection_id"] == collection_id
            and job["collection_name"] == name
            and job["state"] == "RefreshCompleted"
            and job["segments_applied"]
            for job in self.jobs()
        ), "public Milvus URI and selected etcd do not share this baseline collection"
        phase = self.request["phase"]
        assert phase in {"before-apply", "after-apply", "wait-disabled"}
        expected_rows = int(self.request["expected_rows"])
        assert expected_rows >= 1024 and snapshot >= 0
        assert self.client.list_indexes(collection_name=name, timeout=10) == ["embedding"]
        baseline_index = self.client.describe_index(collection_name=name, index_name="embedding", timeout=10)
        assert baseline_index["field_name"] == "embedding"
        assert int(baseline_index["indexed_rows"]) == expected_rows
        assert int(baseline_index["pending_index_rows"]) == 0
        self.state.update(
            collection=name,
            collection_id=collection_id,
            snapshot_id=snapshot,
            phase=phase,
            expected_rows=expected_rows,
            armed_at=int(time.time() * 1000),
        )
        save(self.state_path, self.state)  # Recovery information precedes every mutation.
        gate = {
            "lease": self.lease,
            "collection_id": collection_id,
            "snapshot_id": snapshot,
            "phase": phase,
            "field_name": "embedding",
            "index_name": "embedding",
        }
        kind = "apply" if phase == "before-apply" else "index"
        path = f"{HOOK_DIR}/{kind}-{collection_id}"
        # A failed tee can leave invalid JSON. Encode ownership in the staging
        # filename so recovery can remove our partial write without parsing it.
        temporary = path + ".writing-" + self.lease
        self.pod("tee", temporary, input_text=json.dumps(gate))
        self.pod("mv", temporary, path)
        self.kv.put(self.key(WAIT), json.dumps(phase != "wait-disabled"))
        self.kv.put(self.key(TIMEOUT), "20")
        assert self.effective() == {WAIT: phase != "wait-disabled", TIMEOUT: 20}
        subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--monitor", str(self.state_path)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        effective = self.await_runtime(self.effective())
        return {
            "keyed_collection": name,
            "keyed_snapshot_id": snapshot,
            "hook_ready": True,
            "effective_config": effective,
            "effective_job_timeout_seconds": 20,
        }

    def inspect(self):
        self.require_lease()
        jobs = self.jobs()
        matches = [job for job in jobs if job["job_id"] == int(self.request["job_id"])]
        assert len(matches) == 1
        job = matches[0]
        assert job["collection_id"] == self.state["collection_id"] and job["snapshot_id"] == self.state["snapshot_id"]
        observations = json.loads((self.directory / "observations.json").read_bytes())
        assert not observations.get("error"), observations.get("error")
        seen = observations.get("jobs", {}).get(str(job["job_id"]), {})
        apply_events = self.event_files("apply")
        live = self.live_segments()
        names = self.files()
        gate_active = f"index-{self.state['collection_id']}" in names
        deadline = time.monotonic() + 10
        while True:
            relevant_index = [
                event
                for event in self.event_files("index")
                if event["collection_id"] == self.state["collection_id"]
                and event["target_snapshot_id"] == self.state["snapshot_id"]
                and event["field_name"] == "embedding"
                and event["index_name"] == "embedding"
            ]
            # Completed with wait disabled may precede index task scheduling.
            if relevant_index or not gate_active or not job["segments_applied"] or time.monotonic() >= deadline:
                break
            time.sleep(0.2)
        if relevant_index:
            assert {event["segment_id"] for event in relevant_index} <= {row["id"] for row in live}
            assert all(
                event["build_id"] > 0 and event["field_id"] > 0 and event["index_id"] > 0 for event in relevant_index
            )
            if gate_active:
                manifests = {row["id"]: row["manifest_path"] for row in self.live_segments(include_manifest=True)}
                assert all(event["manifest_path"] == manifests[event["segment_id"]] for event in relevant_index)
        if job["segments_applied"] and "applied_segments" not in self.state:
            self.state["applied_segments"] = live
            save(self.state_path, self.state)
        applied_jobs = [
            record
            for record in jobs
            if record["collection_id"] == self.state["collection_id"]
            and record["snapshot_id"] == self.state["snapshot_id"]
            and record["segments_applied"]
        ]
        duplicate = (
            live != self.state["applied_segments"] if "applied_segments" in self.state else len(applied_jobs) > 1
        )
        stats = self.client.describe_index(collection_name=self.state["collection"], index_name="embedding", timeout=10)
        complete = (
            int(stats.get("indexed_rows", -1)) == self.state["expected_rows"]
            and int(stats.get("pending_index_rows", -1)) == 0
        )
        background = (
            "unblocked_job_ids" in self.state
            and sorted(job["job_id"] for job in jobs) == self.state["unblocked_job_ids"]
            and complete
        )
        return {
            "job_id": job["job_id"],
            "raw_job_state": job["state"],
            "segments_applied": job["segments_applied"],
            "index_wait_started_time": job["index_wait_started_time"],
            "observed_in_progress_with_index_wait": bool(seen.get("wait_seen")),
            "s2_indexes_blocked": gate_active and bool(relevant_index),
            "apply_blocked_before_publication": any(
                event["job_id"] == job["job_id"]
                and event["collection_id"] == self.state["collection_id"]
                and event["target_snapshot_id"] == self.state["snapshot_id"]
                for event in apply_events
            ),
            "background_index_completed_without_refresh_retry": background,
            "duplicate_logical_application": duplicate,
            "live_segments": live,
            "index_stats": stats,
            "phase_observations": seen,
            "actual_index_tasks": relevant_index,
        }

    def unblock(self):
        self.require_lease()
        errors = []
        for kind in ["apply", "index"]:
            name = f"{kind}-{self.state['collection_id']}"
            temporary = name + ".writing-" + self.lease
            for candidate in [temporary, name, name + ".writing"]:
                if candidate not in self.files():
                    continue
                try:
                    if candidate != temporary:
                        # Legacy staging files have no owner in their name.
                        gate = json.loads(self.pod("cat", HOOK_DIR + "/" + candidate))
                        assert gate["lease"] == self.lease, "gate belongs to another lease"
                    self.pod("rm", "-f", HOOK_DIR + "/" + candidate)
                except Exception as error:
                    errors.append(f"{candidate}: {type(error).__name__}: {error}")
        assert not errors, errors
        self.kv.put(self.key(TIMEOUT), "600")
        self.state["unblocked_job_ids"] = sorted(job["job_id"] for job in self.jobs())
        save(self.state_path, self.state)
        runtime = self.await_runtime(self.effective())
        return {"hook_cleared": True, "timeout_restored_for_retry": runtime[TIMEOUT] == 600}

    def restore(self):
        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_bytes())
        if not self.state.get("original_config"):
            # prepare can fail after CAS but before any hook/config mutation.
            if self.owns_lease():
                assert self.kv.release(self.lease_key, self.lease)
            commit = self.client.get_server_version(detail=True, timeout=10)["git_commit"]
            assert len(commit) >= 7 and SOURCE_COMMIT.startswith(commit)
            return {"hooks_cleared": True, "healthy": True, "effective_config": {}}
        self.require_lease()
        errors = []
        if self.state.get("collection_id"):
            try:
                self.unblock()
            except Exception as error:
                errors.append(f"hook cleanup: {type(error).__name__}: {error}")
        (self.directory / "stop").touch(mode=0o600)
        for name, original in self.state["original_overrides"].items():
            try:
                if original is None:
                    self.kv.delete(self.key(name))
                else:
                    self.kv.put(self.key(name), original)
            except Exception as error:
                errors.append(f"config restore {name}: {type(error).__name__}: {error}")
        effective = self.await_runtime(self.effective())
        assert effective == self.state["original_config"]
        assert not any(name.startswith(("apply-", "index-")) for name in self.files())
        version = self.client.get_server_version(detail=True, timeout=10)
        assert len(version["git_commit"]) >= 7 and SOURCE_COMMIT.startswith(version["git_commit"])
        assert not errors, errors
        assert self.kv.release(self.lease_key, self.lease), "could not release own L3 deployment lease"
        return {"hooks_cleared": True, "healthy": True, "effective_config": effective}

    def close(self):
        self.kv.close()
        if self._client is not None:
            self._client.close()


def monitor(state_path):
    state = json.loads(Path(state_path).read_bytes())
    directory = Path(state_path).parent
    kv = EtcdKV(state["target"]["etcd_endpoint"])
    observations = {"jobs": {}}
    deadline = time.monotonic() + 600
    try:
        while time.monotonic() < deadline and not (directory / "stop").exists():
            prefix = state["target"]["etcd_root_prefix"] + "/meta/datacoord-meta/external-collection-refresh-job/"
            for _, data in kv.range(prefix, prefix=True):
                job = decode_job(data)
                if job["collection_id"] != state["collection_id"] or job["snapshot_id"] != state["snapshot_id"]:
                    continue
                record = observations["jobs"].setdefault(str(job["job_id"]), {"samples": []})
                sample = {**job, "observed_at": int(time.time() * 1000)}
                if len(record["samples"]) < 100:
                    record["samples"].append(sample)
                if job["state"] == "RefreshInProgress" and job["index_wait_started_time"] > 0:
                    record["wait_seen"] = sample
            save(directory / "observations.json", observations)
            time.sleep(0.5)
    except Exception as error:
        observations["error"] = str(error)
        save(directory / "observations.json", observations)
    finally:
        kv.close()


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--monitor":
        monitor(sys.argv[2])
        return
    request = json.load(sys.stdin)
    action = request["action"]
    assert action in {"prepare", "arm", "inspect", "unblock", "restore"}
    driver = Driver(request)
    try:
        evidence = getattr(driver, action)()
        print(json.dumps({"ok": True, "evidence": evidence}, sort_keys=True))
    finally:
        driver.close()


if __name__ == "__main__":
    main()
