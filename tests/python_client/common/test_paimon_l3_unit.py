"""Offline checks of the opt-in L3 protocol, wire evidence and restoration."""

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from common.paimon_etcd import EtcdKV, bytes_field, decode_fields, integer_field, scalar, varint
from common.paimon_l3_control import PaimonL3Control


@pytest.mark.parametrize("value,encoded", [(0, b"\x00"), (300, b"\xac\x02"), (2**64 - 1, b"\xff" * 9 + b"\x01")])
def test_uint64_wire_encoding_has_known_bytes(value, encoded):
    """
    target: wire integers
    method: independent known encodings
    expected: exact bytes without float precision loss.
    """
    assert varint(value) == encoded
    assert integer_field(13, value) == b"\x68" + encoded
    assert scalar(decode_fields(b"\x68" + encoded), 13) == value


@pytest.mark.parametrize("data", [b"\x00", b"\x08\x80", b"\x0a\x02x", b"\x08" + b"\xff" * 10, b"\x0b"])
def test_invalid_phase_wire_evidence_fails_closed(data):
    """
    target: malformed evidence
    method: truncated/overflow/invalid field data
    expected: rejection.
    """
    with pytest.raises(ValueError):
        decode_fields(data)


def test_etcd_range_uses_real_kv_field_numbers(monkeypatch):
    """
    target: etcd RPC protocol
    method: independently encoded response and captured request
    expected: exact key/value.
    """
    requests = []
    # Literal rpc.proto RangeResponse/kv.proto KeyValue wire bytes; no production encoder.
    response = b"\x12\x17\x0a\x0eown/config/key\x2a\x05false\x20\x01"

    class Channel:
        def unary_unary(self, method, request_serializer, response_deserializer):
            def call(request, timeout):
                requests.append((method, request_serializer(request)))
                return response_deserializer(response)

            return call

        def close(self):
            pass

    monkeypatch.setattr("common.paimon_etcd.grpc.insecure_channel", lambda address: Channel())
    client = EtcdKV("http://dedicated-etcd:80")
    assert client.range("own/config/", prefix=True) == [("own/config/key", b"false")]
    method, request = requests[0]
    assert method == "/etcdserverpb.KV/Range"
    assert request == b"\x0a\x0bown/config/\x12\x0bown/config0"
    client.close()


def test_control_transmits_and_protects_selected_target(tmp_path, monkeypatch):
    """
    target: deployment isolation
    method: capture driver input
    expected: fixed URI/target and no override.
    """
    target = {"cluster": "https://vc-own", "namespace": "tests", "deployment": "paimon-l3"}
    profile = tmp_path / "profile.json"
    profile.write_text(
        json.dumps(
            {
                "protocol": 1,
                "dedicated": True,
                "milvus_uri": "http://milvus:19530",
                "target": target,
                "control_command": ["driver"],
            }
        )
    )
    captured = []

    def run(command, **kwargs):
        captured.append(json.loads(kwargs["input"]))
        return SimpleNamespace(returncode=0, stdout='{"ok":true,"evidence":{"read_only":true}}', stderr="")

    monkeypatch.setattr("common.paimon_l3_control.subprocess.run", run)
    control = PaimonL3Control(profile, "http://milvus:19530")
    assert control.call("prepare") == {"read_only": True}
    assert captured[0]["milvus_uri"] == "http://milvus:19530" and captured[0]["target"] == target
    with pytest.raises(ValueError, match="cannot override"):
        control.call("arm", target={"deployment": "other"})
    with pytest.raises(ValueError, match="must match"):
        PaimonL3Control(profile, "http://other:19530")
    assert len(captured) == 1


def load_driver():
    path = Path(__file__).parents[1] / "scripts/paimon/k8s_phase_driver.py"
    spec = importlib.util.spec_from_file_location("paimon_k8s_phase_driver", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_live_segment_evidence_preserves_actual_manifest():
    """
    target: index task lineage
    method: literal SegmentInfo wire with manifest field 32
    expected: manifest survives decoding; stable identity excludes mutable path.
    """
    module = load_driver()
    driver = module.Driver.__new__(module.Driver)
    driver.root = "own"
    driver.state = {"collection_id": 41}
    # id=11, collection=41, rows=4096, state=Flushed(4), manifest_path="manifest-s2".
    data = b"\x08\x0b\x10\x29\x28\x80\x20\x30\x04\x82\x02\x0bmanifest-s2"
    driver.kv = SimpleNamespace(range=lambda *args, **kwargs: [("owned", data)])
    assert driver.live_segments() == [{"id": 11, "rows": 4096}]
    assert driver.live_segments(include_manifest=True) == [{"id": 11, "rows": 4096, "manifest_path": "manifest-s2"}]


def test_internal_job_marker_is_decoded_from_fixed_proto():
    """
    target: publication proof
    method: fixed data_coord.proto tags
    expected: exact IDs, applied and wait markers.
    """
    driver = load_driver()
    data = b"\x08\xfb" + b"\x80" * 7 + b"\x10\x10\x29\x30\x04"
    data += b'\x2a\x13{"snapshot_id":"4"}\x68\xe8\x07\x78\x01'
    job = driver.decode_job(data)
    assert job["job_id"] == 2**60 + 123 and job["collection_id"] == 41
    assert job["state"] == "RefreshFailed" and job["segments_applied"] is True
    assert job["index_wait_started_time"] == 1000 and job["snapshot_id"] == 4
    with pytest.raises(ValueError):
        driver.decode_job(data + bytes_field(15, b"true"))


def test_failed_hook_cleanup_still_restores_configuration(tmp_path):
    """
    target: restore safety
    method: fail hook removal
    expected: both original config overrides still restored.
    """
    module = load_driver()
    driver = module.Driver.__new__(module.Driver)
    driver.directory = tmp_path
    driver.state_path = tmp_path / "state.json"
    original = {module.WAIT: False, module.TIMEOUT: 600}
    driver.state_path.write_text(
        json.dumps(
            {"collection_id": 41, "original_config": original, "original_overrides": {key: None for key in original}}
        )
    )
    driver.root = "owned"
    restored = []
    driver.kv = SimpleNamespace(delete=lambda key: restored.append(key))
    driver.effective = lambda: original
    driver.await_runtime = lambda expected: expected
    driver.require_lease = lambda: None
    driver.files = lambda: []
    driver._client = SimpleNamespace(get_server_version=lambda **kwargs: {"git_commit": "4f95cb61"})

    def broken_hook():
        raise OSError("pod unavailable")

    driver.unblock = broken_hook
    with pytest.raises(AssertionError, match="hook cleanup"):
        driver.restore()
    assert set(restored) == {"owned/config/" + name.replace(".", "/") for name in original}


def test_etcd_lease_txn_has_literal_compare_and_mutation_fields():
    """
    target: deployment mutex
    method: compare literal etcd Txn wire requests
    expected: acquire/release use CAS.
    """
    client = EtcdKV.__new__(EtcdKV)
    captured = []
    client._call = lambda method, data: captured.append((method, data)) or {2: [1]}
    assert client.acquire("k", "v") is True
    assert captured[-1] == ("Txn", b"\x0a\x05\x1a\x01k\x20\x00\x12\x08\x12\x06\x0a\x01k\x12\x01v")
    assert client.release("k", "v") is True
    assert captured[-1] == ("Txn", b"\x0a\x08\x10\x03\x1a\x01k\x3a\x01v\x12\x05\x1a\x03\x0a\x01k")


def test_runtime_confirmation_does_not_accept_only_etcd_readback(monkeypatch):
    """
    target: actual config effect
    method: stale then current component values
    expected: wait for the component.
    """
    module = load_driver()
    driver = module.Driver.__new__(module.Driver)
    expected = {module.WAIT: True, module.TIMEOUT: 20}
    values = iter([{module.WAIT: False, module.TIMEOUT: 600}, expected])
    calls = []
    driver.runtime_effective = lambda: calls.append(1) or next(values)
    monkeypatch.setattr(module.time, "sleep", lambda duration: None)
    assert driver.await_runtime(expected) == expected
    assert len(calls) == 2


def test_restore_initializes_without_milvus_rpc_and_restores_before_health_check(tmp_path, monkeypatch):
    """
    target: restore when Milvus RPC is unavailable
    method: real driver construction with a failing RPC constructor and isolated control transports
    expected: construction succeeds; config restoration precedes the failed health connection
    """
    module = load_driver()
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.touch()
    calls = []
    restored = []
    transport = SimpleNamespace(delete=lambda key: restored.append(key), close=lambda: calls.append("etcd-close"))
    monkeypatch.setattr(module, "EtcdKV", lambda endpoint: transport)
    monkeypatch.setattr(module.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(module.Driver, "_run", staticmethod(lambda *args, **kwargs: "https://vc-owned"))

    def unavailable_rpc(**kwargs):
        calls.append("rpc-connect")
        raise OSError("Milvus RPC unavailable")

    monkeypatch.setattr(module, "MilvusClient", unavailable_rpc)
    request = {
        "protocol": 1,
        "lease": "0" * 32,
        "milvus_uri": "http://owned:19530",
        "target": {
            "cluster": "https://vc-owned",
            "namespace": "tests",
            "deployment": "owned",
            "kubeconfig": str(kubeconfig),
            "etcd_endpoint": "http://etcd:80",
            "etcd_root_prefix": "owned",
        },
    }
    driver = module.Driver(request)
    assert calls == []
    original = {module.WAIT: False, module.TIMEOUT: 600}
    driver.state_path.write_text(
        json.dumps(
            {"collection_id": 41, "original_config": original, "original_overrides": {key: None for key in original}}
        )
    )
    driver.require_lease = lambda: None
    driver.unblock = lambda: None
    driver.effective = lambda: original
    driver.await_runtime = lambda expected: expected
    driver.files = lambda: []
    with pytest.raises(OSError, match="RPC unavailable"):
        driver.restore()
    assert set(restored) == {"owned/config/" + name.replace(".", "/") for name in original}
    driver.close()
    assert calls == ["rpc-connect", "etcd-close"]


@pytest.mark.parametrize(
    "url",
    [
        "http://other-milvus.tests:19530",
        "http://owned-milvus.other:19530",
        "http://owned-milvus.tests:80",
        "http://owned-milvus.tests:19530/path",
        "http://owned-milvus.tests:19530?target=other",
        "http://user@owned-milvus.tests:19530",
    ],
)
def test_ci_service_endpoint_rejects_unbound_routes(url):
    """
    target: CI target binding
    method: vary host/namespace/port/path
    expected: no fallback route.
    """
    module = load_driver()
    module.validate_service_url("http://owned-milvus.tests.svc.cluster.local:19530", "owned-milvus", "tests", 19530)
    with pytest.raises(AssertionError, match="selected deployment"):
        module.validate_service_url(url, "owned-milvus", "tests", 19530)


def test_in_cluster_config_references_projected_token_without_copying_it(tmp_path, monkeypatch):
    """
    target: CI credentials
    method: projected service-account files
    expected: tokenFile and strict cluster binding.
    """
    module = load_driver()
    account = tmp_path / "serviceaccount"
    account.mkdir()
    (account / "token").write_text("synthetic-token-never-copy")
    (account / "ca.crt").write_text("synthetic-ca")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", "443")
    path = tmp_path / "kubeconfig.json"
    with pytest.raises(AssertionError, match="in-cluster service account"):
        module.in_cluster_kubeconfig(path, "https://other-cluster", "tests", account)
    assert not path.exists()
    module.in_cluster_kubeconfig(path, "https://10.0.0.1:443", "tests", account)
    value = json.loads(path.read_text())
    assert value["users"][0]["user"] == {"tokenFile": str(account / "token")}
    assert "synthetic-token-never-copy" not in path.read_text()
    assert path.stat().st_mode & 0o777 == 0o600


def test_service_endpoint_must_reference_current_pod_uid_and_ip():
    """
    target: stale/misdirected endpoint
    method: mismatch UID/IP
    expected: only a current owned pod is accepted.
    """
    module = load_driver()
    driver = module.Driver.__new__(module.Driver)
    driver.target = {"deployment": "owned", "namespace": "tests"}
    pod = {
        "metadata": {
            "name": "owned-standalone",
            "uid": "new-uid",
            "labels": {"release": "owned", "component": "standalone"},
        },
        "status": {"podIP": "10.0.0.2"},
    }
    address = {"ip": "10.0.0.2", "targetRef": {"name": "owned-standalone", "kind": "Pod", "uid": "old-uid"}}

    def kube(*args):
        if args[1] == "service":
            return json.dumps(
                {
                    "metadata": {"labels": {"release": "owned"}},
                    "spec": {"selector": {"release": "owned", "component": "standalone"}},
                }
            )
        if args[1] == "endpoints":
            return json.dumps({"subsets": [{"addresses": [address]}]})
        assert args[1] == "pods" and "component=standalone,release=owned" in args
        return json.dumps({"items": [pod]})

    driver.kube = kube
    with pytest.raises(AssertionError):
        driver.verify_service("owned-milvus")
    address["targetRef"]["uid"] = "new-uid"
    assert driver.verify_service("owned-milvus") == [pod]
    address["ip"] = "10.0.0.3"
    with pytest.raises(AssertionError):
        driver.verify_service("owned-milvus")


@pytest.mark.parametrize("wrong_session", [False, True])
def test_ci_prepare_binds_role_sessions_before_lease_without_cell_cli(tmp_path, wrong_session):
    """
    target: shared-root isolation
    method: real prepare with foreign session
    expected: reject before CAS/config writes.
    """
    module = load_driver()
    driver = module.Driver.__new__(module.Driver)
    driver.mode = "service"
    driver.target = {"deployment": "owned", "namespace": "tests", "image": "test-hook-image"}
    driver.uri = "http://owned-milvus.tests:19530"
    driver.root, driver.lease = "owned", "0" * 32
    driver.state, driver.state_path = {}, tmp_path / "state.json"
    acquired = []
    pod = {
        "metadata": {"name": "owned-standalone", "uid": "current"},
        "spec": {"containers": [{"image": "test-hook-image"}]},
        "status": {"phase": "Running", "podIP": "10.0.0.2", "conditions": [{"type": "Ready", "status": "True"}]},
    }
    driver.verify_service = lambda service: [pod]

    def forbidden_command(*args, **kwargs):
        raise AssertionError("CI service mode must not call the Cell CLI or read a Milvus CR")

    driver._run = forbidden_command
    driver.kube = forbidden_command
    driver.pod = lambda *args, **kwargs: json.dumps(
        {"protocol": 1, "source_commit": "4f95cb61", "test_image_only": True}
    )
    driver.files = lambda: ["ready.json"]
    original = {module.WAIT: False, module.TIMEOUT: 600}
    driver.runtime_effective = lambda: original
    driver.await_runtime = lambda values: values
    driver._client = SimpleNamespace(list_databases=lambda **kwargs: ["default"], list_collections=lambda **kwargs: [])

    def rows(key, **kwargs):
        if key.endswith("/meta/session/"):
            ip = "10.0.0.3" if wrong_session else "10.0.0.2"
            return [
                (
                    f"owned/meta/session/{role}",
                    json.dumps({"ServerName": role, "Address": ip + ":22125", "HostName": "owned-standalone"}).encode(),
                )
                for role in ["rootcoord", "datacoord"]
            ]
        return []

    driver.kv = SimpleNamespace(range=rows, acquire=lambda *args: acquired.append(args) or True)
    if wrong_session:
        with pytest.raises(AssertionError, match="etcd root"):
            driver.prepare()
        assert acquired == [] and not driver.state_path.exists()
    else:
        result = driver.prepare()
        assert result["access_mode"] == "service" and result["original_config"] == original
        assert result["etcd_session_binding"]["datacoord"] == ["10.0.0.2:22125"]
        assert acquired == [("owned/paimon-e2e/control-lease", "0" * 32)]


def test_projected_service_account_rotation_keeps_kubeconfig_paths_valid(tmp_path, monkeypatch):
    """
    target: projected volume rotation
    method: replace ..data and retire old generation
    expected: fresh token/CA.
    """
    import shutil

    module = load_driver()
    account = tmp_path / "serviceaccount"
    account.mkdir()
    for name in ["old", "new"]:
        generation = account / (".." + name)
        generation.mkdir()
        (generation / "token").write_text(name + "-synthetic-token")
        (generation / "ca.crt").write_text(name + "-synthetic-ca")
    (account / "..data").symlink_to("..old")
    (account / "token").symlink_to("..data/token")
    (account / "ca.crt").symlink_to("..data/ca.crt")
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", "443")
    path = tmp_path / "kubeconfig.json"
    module.in_cluster_kubeconfig(path, "https://10.0.0.1:443", "tests", account)
    value = json.loads(path.read_text())
    (account / "..data").unlink()
    (account / "..data").symlink_to("..new")
    shutil.rmtree(account / "..old")
    assert Path(value["users"][0]["user"]["tokenFile"]).read_text() == "new-synthetic-token"
    assert Path(value["clusters"][0]["cluster"]["certificate-authority"]).read_text() == "new-synthetic-ca"


def test_partial_prepare_releases_own_lease_before_unavailable_health_check(tmp_path):
    """
    target: failed prepare recovery
    method: state saved before CAS then unavailable RPC
    expected: own lease released.
    """
    module = load_driver()
    driver = module.Driver.__new__(module.Driver)
    driver.root, driver.lease = "owned", "0" * 32
    driver.state_path = tmp_path / "state.json"
    driver.state_path.write_text(json.dumps({"baseline": {module.WAIT: False, module.TIMEOUT: 600}}))
    driver.state = {}
    released = []
    driver.kv = SimpleNamespace(
        range=lambda key: [(key, driver.lease.encode())],
        release=lambda *args: released.append(args) or True,
    )

    def unhealthy(**kwargs):
        assert released == [("owned/paimon-e2e/control-lease", "0" * 32)]
        raise OSError("unavailable health RPC")

    driver._client = SimpleNamespace(get_server_version=unhealthy)
    with pytest.raises(OSError, match="health RPC"):
        driver.restore()
    assert released == [("owned/paimon-e2e/control-lease", "0" * 32)]


def test_helm_service_name_can_equal_release_name():
    """
    target: Helm fullname
    method: explicitly selected service named milvus
    expected: no invented suffix.
    """
    module = load_driver()
    module.validate_service_url("http://milvus.tests.svc:19530", "milvus", "tests", 19530)


def test_service_ownership_rejects_colliding_release_prefix():
    """
    target: wrong release binding
    method: real UID/IP but owned-other release
    expected: reject before endpoint use.
    """
    module = load_driver()
    driver = module.Driver.__new__(module.Driver)
    driver.target = {"deployment": "owned", "namespace": "tests"}
    service = {"metadata": {"labels": {"release": "owned-other"}}, "spec": {"selector": {"release": "owned-other"}}}
    driver.kube = lambda *args: json.dumps(service)
    with pytest.raises(AssertionError, match="another release"):
        driver.verify_service("owned-milvus")


@pytest.mark.parametrize("phase", ["before-apply", "after-apply"])
@pytest.mark.parametrize("failure", ["partial-write", "rename", "legacy-rename", "late-rename"])
def test_arm_interruption_removes_owned_staging_gate_and_releases_lease(tmp_path, phase, failure):
    """
    target: interrupted arm recovery
    method: run real arm, interrupt staging write/rename, then run real restore
    expected: own partial/legacy staging files removed, both config overrides restored and CAS released once
    """
    module = load_driver()
    driver = module.Driver.__new__(module.Driver)
    driver.root, driver.lease = "owned", "0" * 32
    driver.directory, driver.state_path = tmp_path, tmp_path / "state.json"
    original = {module.WAIT: False, module.TIMEOUT: 600}
    driver.state = {
        "baseline": original,
        "original_config": original,
        "original_overrides": {key: None for key in original},
    }
    module.save(driver.state_path, driver.state)
    driver.request = {"collection": "source", "snapshot_id": 2, "expected_rows": 2048, "phase": phase}
    driver._client = SimpleNamespace(
        describe_collection=lambda **kwargs: {"external_source": "s3://own/table", "collection_id": 41},
        list_indexes=lambda **kwargs: ["embedding"],
        describe_index=lambda **kwargs: {"field_name": "embedding", "indexed_rows": 2048, "pending_index_rows": 0},
        get_server_version=lambda **kwargs: {"git_commit": "4f95cb61"},
    )
    driver.jobs = lambda: [
        {
            "job_id": 1,
            "collection_id": 41,
            "collection_name": "source",
            "state": "RefreshCompleted",
            "segments_applied": True,
        }
    ]
    values = {driver.lease_key: driver.lease.encode()}
    deleted, released, files = [], [], {}
    driver.kv = SimpleNamespace(
        range=lambda key: [(key, values[key])] if key in values else [],
        put=lambda key, value: values.__setitem__(key, value.encode()),
        delete=lambda key: (deleted.append(key), values.pop(key, None)),
        release=lambda key, value: released.append((key, value)) or values.pop(key) == value.encode(),
    )

    def pod(command, *args, input_text=None):
        if command == "tee":
            files[args[0].rsplit("/", 1)[-1]] = input_text[:8] if failure == "partial-write" else input_text
            if failure == "partial-write":
                raise OSError("injected incomplete staging write")
            return input_text
        if command == "mv":
            if failure == "legacy-rename":
                temporary = args[0].rsplit("/", 1)[-1]
                files[args[1].rsplit("/", 1)[-1] + ".writing"] = files.pop(temporary)
            raise OSError("injected staging rename failure")
        name = args[-1].rsplit("/", 1)[-1]
        if command == "cat":
            return files[name]
        assert command == "rm"
        assert args[0] == "-f", "cleanup must tolerate a rename completing after ls"
        if failure == "late-rename" and ".writing-" in name:
            files[name.split(".writing-", 1)[0]] = files.pop(name)
        files.pop(name, None)
        return ""

    driver.pod = pod
    driver.files = lambda: list(files)
    driver.await_runtime = lambda expected: expected
    with pytest.raises(OSError, match="injected"):
        driver.arm()
    assert files, "fault injection did not leave a staging gate"
    result = driver.restore()
    assert result == {"hooks_cleared": True, "healthy": True, "effective_config": original}
    assert files == {}
    assert set(deleted) == {"owned/config/" + key.replace(".", "/") for key in original}
    assert released == [("owned/paimon-e2e/control-lease", "0" * 32)]
    assert values == {}


@pytest.mark.parametrize("suffix", ["", ".writing"])
def test_unblock_preserves_foreign_gate_but_cleans_own_partial_write(tmp_path, suffix):
    """
    target: staging gate ownership
    method: own incomplete staging file alongside another lease's final/legacy gate
    expected: own partial write removed; foreign gate retained and cleanup reports failure
    """
    module = load_driver()
    driver = module.Driver.__new__(module.Driver)
    driver.lease = "0" * 32
    driver.state = {"collection_id": 41}
    driver.require_lease = lambda: None
    own = "apply-41.writing-" + driver.lease
    foreign = "apply-41" + suffix
    files = {own: "{partial", foreign: json.dumps({"lease": "1" * 32})}
    driver.files = lambda: list(files)

    def pod(command, *args):
        name = args[-1].rsplit("/", 1)[-1]
        if command == "cat":
            return files[name]
        assert command == "rm"
        files.pop(name)

    driver.pod = pod
    with pytest.raises(AssertionError, match="another lease"):
        driver.unblock()
    assert set(files) == {foreign}
