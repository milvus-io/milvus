"""Run Python or all four transports against a prepared checkpoint deployment."""
import argparse
import importlib
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import xml.etree.ElementTree as ET

from ci_functional_regression import ROOT, pytest_phase
from running_requests_topology import ProxyAuthorizationTopology

PYTHON_CASES = (
    "test_running_requests_t04_snapshot.py::test_t04_cancel_and_repeat_while_exit_is_held",
    "test_running_requests_t10_merge.py::test_t10_cancel_only_selected_group_member",
    "test_running_requests_t12_internal_tasks.py::test_t12_cancel_across_internal_stages",
)


TRANSPORT_CASES = (
    "test_running_requests_t04_transport_parity.py::test_t04_transport_cancel_and_repeat_while_exit_is_held",
    "test_running_requests_t10_t12_transport_parity.py::test_t10_transport_cancel_only_selected_group_member",
    "test_running_requests_t10_t12_transport_parity.py::test_t12_transport_cancel_across_internal_stages",
)


def selection(scope):
    if scope == "all":
        return PYTHON_CASES + TRANSPORT_CASES, 32
    return PYTHON_CASES, 8


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", choices=("python", "all"), default="python",
                        help="Run Python only (8 cases) or Python, Java, Go and REST (32 cases)")
    args = parser.parse_args()
    cases, expected = selection(args.scope)

    provider_name = os.environ.get("MILVUS_L1_CHECKPOINT_PROVIDER")
    if not provider_name:
        raise SystemExit("Checkpoint lane requires MILVUS_L1_CHECKPOINT_PROVIDER")
    provider = importlib.import_module(provider_name)
    for method in ("hold_request_exit", "hold_search_group", "hold_internal_stage"):
        if not callable(getattr(provider, method, None)):
            raise SystemExit(f"{provider_name} does not implement {method}")

    if args.scope == "all":
        sdk = ROOT / "native_sdk"
        if not os.access(sdk / "go-worker", os.X_OK) or not (sdk / "java-runtime.json").is_file():
            raise SystemExit("Build the Java and Go workers before running --scope all")

    env = dict(os.environ)
    # This entrypoint exercises the provider-backed HybridSearch branch.
    env.pop("MILVUS_T12_HYBRID_TRACE", None)
    artifacts = Path(env["RR_ARTIFACTS"]).resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    topology = ProxyAuthorizationTopology(shared=True)
    with TemporaryDirectory(prefix="rr-checkpoint-") as work:
        with topology.endpoints(require_auth_config=False) as endpoints:
            (env["PROXY_A_URI"], a), (env["PROXY_B_URI"], b) = endpoints
            env.update(PROXY_A_ID=str(a), PROXY_B_ID=str(b),
                       MILVUS_L0_BASELINE=str(Path(work) / "baseline.npz"))
            subprocess.run([sys.executable, str(ROOT / "setup_running_requests_l0.py")],
                           cwd=ROOT, env=env, check=True)
            status = pytest_phase(artifacts, "checkpoints", env, *cases)
    report = artifacts / "checkpoints.xml"
    if not report.is_file():
        raise SystemExit(f"Checkpoint suite produced no JUnit report (pytest exit {status})")
    results = list(ET.parse(report).iter("testcase"))
    skipped = sum(case.find("skipped") is not None for case in results)
    if len(results) != expected or skipped:
        raise SystemExit(f"Expected {expected} cases with zero skips; got {len(results)}, {skipped} skipped")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
