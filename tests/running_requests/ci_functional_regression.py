"""Run all functional pytest phases and retain separate CI results."""
import json
from contextlib import closing
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from uuid import uuid4
import xml.etree.ElementTree as ET

from pymilvus import MilvusClient
from running_requests_topology import ProxyAuthorizationTopology, run

ROOT = Path(__file__).resolve().parent
T04_RUNNING = (
    "test_running_requests_t04_snapshot.py::test_t04_cancel_and_repeat_while_exit_is_held[Running]"
)


def pytest_phase(artifacts, name, env, *selection):
    with (artifacts / f"{name}.log").open("w") as report:
        command = [sys.executable, "-m", "pytest", "-ra", "--tb=short",
                   f"--junitxml={artifacts / (name + '.xml')}", *selection]
        with subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, bufsize=1) as process:
            for line in process.stdout:
                report.write(line)
                print(line, end="", flush=True)
            return process.wait()


def prepare_rbac(uri, env):
    users = {
        "NONE": (), "LIST": ("ListRunningRequests",), "CANCEL": ("CancelRequests",),
        "BOTH": ("ListRunningRequests", "CancelRequests"), "SELF": ("Search",),
    }
    with closing(MilvusClient(uri=uri, token=env["MILVUS_TOKEN"], timeout=15)) as client:
        for name, privileges in users.items():
            username = f"rr_{name.lower()}_{uuid4().hex[:8]}"
            password = f"Qa7{uuid4().hex}"
            client.create_user(username, password, timeout=15)
            if privileges:
                client.create_role(username, timeout=15)
                client.grant_role(username, username, timeout=15)
                for privilege in privileges:
                    client.grant_privilege_v2(username, privilege, "*", "*", timeout=15)
            env[f"MILVUS_L1_RBAC_{name}_TOKEN"] = f"{username}:{password}"


def authorization_config(topology):
    deployment = json.loads(run([*topology.kube, "get", "deployment", topology.deployments[0], "-o", "json"]))
    for volume in deployment["spec"]["template"]["spec"].get("volumes", []):
        name = volume.get("configMap", {}).get("name")
        if not name:
            continue
        config = json.loads(run([*topology.kube, "get", "configmap", name, "-o", "json"]))
        for key, value in config.get("data", {}).items():
            if "authorizationEnabled: true" in value:
                return name, key
    return topology.configmap, "user.yaml"


def main():
    env = dict(os.environ)
    artifacts = Path(env["RR_ARTIFACTS"]).resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    results = {}
    topology = ProxyAuthorizationTopology(shared=True)
    with TemporaryDirectory(prefix="rr-ci-") as work, topology.endpoints(require_auth_config=False) as endpoints:
        (env["PROXY_A_URI"], a), (env["PROXY_B_URI"], b) = endpoints
        env.update(PROXY_A_ID=str(a), PROXY_B_ID=str(b),
                   MILVUS_L0_BASELINE=str(Path(work) / "baseline.npz"))
        print(f"Functional suite using distinct Proxy IDs {a}, {b}", flush=True)
        extended = env.get("RR_CI_EXTENDED_CHECKPOINTS") == "1"
        if extended:
            env["RR_CI_T04_CHECKPOINT"] = "1"
            owner_pod = topology.proxy_pods()[a]
            capabilities = json.loads(run([*topology.kube, "exec", owner_pod, "-c", "proxy", "--", "cat", "/milvus/checkpoint-capabilities.json"]))
            assert all(capabilities.get(name) for name in ("t04_running", "t04_queued", "t14_controlled_wait")), capabilities
            assert set(capabilities.get("t12_internal_experimental", [])) >= {"requery", "search_by_id", "retry"}, capabilities
            (artifacts / "checkpoint-capabilities.json").write_text(json.dumps(capabilities, indent=2))
        if env.get("RR_CI_T04_CHECKPOINT") == "1":
            checkpoint_env = dict(env, MILVUS_L1_CHECKPOINT_PROVIDER=("running_requests_checkpoint_provider" if extended else "running_requests_t04_provider"),
                                  MILVUS_L0_ROWS="80000")
            checkpoint_env.pop("MILVUS_L0_COLLECTION", None)
            checkpoint_env.pop("MILVUS_L0_BASELINE", None)
            status = pytest_phase(artifacts, "t04-running-checkpoint", checkpoint_env, T04_RUNNING)
            report = artifacts / "t04-running-checkpoint.xml"
            cases = list(ET.parse(report).iter("testcase")) if report.is_file() else []
            if status or len(cases) != 1 or cases[0].find("skipped") is not None:
                print("T04 Running checkpoint did not complete successfully", flush=True)
                return 1
        if env.get("RR_CI_T04_CHECKPOINT") == "1":
            results["t04-provider-transports"] = pytest_phase(
                artifacts, "t04-provider-transports", checkpoint_env,
                *[f"test_running_requests_t04_transport_parity.py::test_t04_transport_cancel_and_repeat_while_exit_is_held[{language}-Running]"
                  for language in ("java", "go", "rest")],
            )
        if extended:
            queued = ["test_running_requests_t04_snapshot.py::test_t04_cancel_and_repeat_while_exit_is_held[Queued]"]
            queued.extend(f"test_running_requests_t04_transport_parity.py::test_t04_transport_cancel_and_repeat_while_exit_is_held[{lang}-Queued]" for lang in ("java", "go", "rest"))
            results["t04-queued"] = pytest_phase(artifacts, "t04-queued", checkpoint_env, *queued)
            stages = [f"test_running_requests_t12_internal_tasks.py::test_t12_cancel_across_internal_stages[{path}]" for path in ("requery", "search_by_id", "retry")]
            stages.extend(f"test_running_requests_t10_t12_transport_parity.py::test_t12_transport_cancel_across_internal_stages[{lang}-{path}]" for lang in ("java", "go", "rest") for path in ("requery", "search_by_id", "retry"))
            results["t12-checkpoints"] = pytest_phase(artifacts, "t12-checkpoints", checkpoint_env, *stages)
            results["t14-controlled-wait"] = pytest_phase(artifacts, "t14-controlled-wait", checkpoint_env, "test_running_requests_t14_controlled_wait.py")
            for phase, expected in (("t04-queued", 4), ("t12-checkpoints", 12), ("t14-controlled-wait", 1)):
                report = artifacts / f"{phase}.xml"
                cases = list(ET.parse(report).iter("testcase")) if report.exists() else []
                if len(cases) != expected or any(case.find("skipped") is not None for case in cases):
                    results[phase] = 1
        subprocess.run([sys.executable, str(ROOT / "setup_running_requests_l0.py")],
                       env=env, check=True, cwd=ROOT)
        try:
            prepare_rbac(env["PROXY_A_URI"], env)
        except Exception as exc:
            print(f"RBAC fixture setup failed: {type(exc).__name__}: {exc}", flush=True)
            results["rbac-setup"] = 1
        results["l0"] = pytest_phase(artifacts, "l0", env, "test_running_requests_l0.py")
        trace_env = dict(env, MILVUS_T12_HYBRID_TRACE="1", MILVUS_L0_ROWS="80000",
                         MILVUS_L1_HYBRID_NQ="4096")
        results["t12-trace"] = pytest_phase(
            artifacts, "t12-trace", trace_env, "-s",
            "test_running_requests_t12_internal_tasks.py::test_t12_cancel_hybrid_with_trace",
        )
        if results["t12-trace"] == 0:
            cases = list(ET.parse(artifacts / "t12-trace.xml").iter("testcase"))
            if len(cases) != 1 or cases[0].find("skipped") is not None:
                print("T12 trace phase requires one executed case with zero skips", flush=True)
                results["t12-trace"] = 1
        results["functional"] = pytest_phase(
            artifacts, "functional", dict(env, MILVUS_T12_HYBRID_TRACE="1"), ".", "--ignore=test_running_requests_l0.py",
            "--ignore=test_running_requests_t11_mixed_proxy.py", "--ignore=native_sdk",
            "--ignore=standalone",
            "-k", "not test_t08_auth_disabled_smoke and not test_t12_cancel_hybrid_with_trace "
            "and not test_t12_cancel_across_internal_stages[hybrid]"
            + (" and not test_t04_cancel_and_repeat_while_exit_is_held[Running]"
               " and not test_t04_transport_cancel_and_repeat_while_exit_is_held[java-Running]"
               " and not test_t04_transport_cancel_and_repeat_while_exit_is_held[go-Running]"
               " and not test_t04_transport_cancel_and_repeat_while_exit_is_held[rest-Running]"
               if env.get("RR_CI_T04_CHECKPOINT") == "1" else "")
            + (" and not test_t04_cancel_and_repeat_while_exit_is_held[Queued]"
               " and not test_t04_transport_cancel_and_repeat_while_exit_is_held[java-Queued]"
               " and not test_t04_transport_cancel_and_repeat_while_exit_is_held[go-Queued]"
               " and not test_t04_transport_cancel_and_repeat_while_exit_is_held[rest-Queued]"
               " and not test_t14_controlled_segment_wait"
               + "".join(f" and not test_t12_cancel_across_internal_stages[{path}]" for path in ("requery", "search_by_id", "retry"))
               + "".join(f" and not test_t12_transport_cancel_across_internal_stages[{lang}-{path}]" for lang in ("java", "go", "rest") for path in ("requery", "search_by_id", "retry"))
               if extended else ""),
        )
        name, key = authorization_config(topology)
        env.update(MILVUS_L1_AUTH_CONFIGMAP=name, MILVUS_L1_AUTH_CONFIG_KEY=key)
        results["auth-lifecycle"] = pytest_phase(
            artifacts, "auth-lifecycle", env,
            "test_running_requests_t08_rbac.py::test_t08_auth_disabled_smoke",
            "test_running_requests_t08_rbac.py::test_t08_auth_disabled_smoke_native",
        )
    (artifacts / "exit-codes.json").write_text(json.dumps(results, indent=2) + "\n")
    return int(any(results.values()))


if __name__ == "__main__":
    raise SystemExit(main())
