"""Run functional regression on a Milvus instance already deployed by CI."""

import argparse
import importlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from running_requests_topology import ProxyAuthorizationTopology
from test_running_requests_t11_mixed_proxy import OLD_IMAGE

ROOT = Path(__file__).resolve().parent
T11 = ROOT / "test_running_requests_t11_mixed_proxy.py"
AUTH = ROOT / "test_running_requests_t08_rbac.py"


def invoke(*args, env):
    subprocess.run(args, cwd=ROOT, env=env, check=True)


def proxy_env(endpoints, env):
    (a_uri, a_id), (b_uri, b_id) = endpoints
    return dict(env, PROXY_A_URI=a_uri, PROXY_A_ID=str(a_id), PROXY_B_URI=b_uri, PROXY_B_ID=str(b_id))


def main(*, upgrade=False):
    description = "Run the T11 forward rolling upgrade." if upgrade else __doc__
    parser = argparse.ArgumentParser(description=description)
    if upgrade:
        parser.add_argument("--provision", type=Path, required=True,
                            help="Deploy and wait for the old dual-Proxy instance")
    args = parser.parse_args()
    env = dict(os.environ)
    required = (
        "MILVUS_L1_KUBECONFIG", "MILVUS_L1_NAMESPACE", "MILVUS_L1_RELEASE",
        "MILVUS_TOKEN", "MILVUS_TEST_USER", "MILVUS_L0_COLLECTION",
    )
    if upgrade:
        required += ("MILVUS_L1_NEW_IMAGE", "MILVUS_T11_CORE_DEPLOYMENTS", "MILVUS_L1_CHECKPOINT_PROVIDER")
    missing = [name for name in required if not env.get(name)]
    if missing:
        parser.error(f"Missing CI settings: {', '.join(missing)}")
    if upgrade:
        provider = importlib.import_module(env["MILVUS_L1_CHECKPOINT_PROVIDER"])
        for method in ("fail_management_rpc", "hold_proxy_search"):
            if not callable(getattr(provider, method, None)):
                parser.error(f"The configured checkpoint provider must implement {method}")
        env.update(MILVUS_L1_SHARED_PROXY_DEPLOYMENT="0", MILVUS_T11_FORWARD="1", MILVUS_L1_OLD_IMAGE=OLD_IMAGE, MILVUS_CI_IMAGE=OLD_IMAGE)
    else:
        env.pop("MILVUS_T11_FORWARD", None)
        env.pop("MILVUS_L1_OLD_IMAGE", None)
        env["MILVUS_L1_SHARED_PROXY_DEPLOYMENT"] = "1"

    topology = ProxyAuthorizationTopology(shared=not upgrade)
    with tempfile.TemporaryDirectory(prefix="milvus-requests-ci-") as work:
        env["MILVUS_L0_BASELINE"] = str(Path(work) / "baseline.npz")
        pytest = (sys.executable, "-m", "pytest", "-o", "addopts=")
        if upgrade:
            invoke(str(args.provision.resolve()), env=env)
            with topology.endpoints() as endpoints:
                invoke(sys.executable, str(ROOT / "setup_running_requests_l0.py"), env=proxy_env(endpoints, env))
            invoke(*pytest, str(T11) + "::test_t11_forward_rolling_upgrade", env=env)
            return

        with topology.two_replicas():
            with topology.endpoints(require_auth_config=False) as endpoints:
                active = proxy_env(endpoints, env)
                invoke(sys.executable, str(ROOT / "setup_running_requests_l0.py"), env=active)
                run_regression(pytest, active)
            with topology.endpoints() as endpoints:
                invoke(*pytest, str(AUTH) + "::test_t08_auth_disabled_smoke", env=proxy_env(endpoints, env))


def run_regression(pytest, active):
    l0 = ROOT / "test_running_requests_l0.py"
    invoke(*pytest, str(l0), env=active)
    invoke(*pytest, str(ROOT), "--ignore", str(T11), "--ignore", str(l0),
           "--ignore", str(ROOT / "fouram"), "--ignore", str(ROOT / "standalone"),
           "-k", "not test_t08_auth_disabled_smoke", env=active)


if __name__ == "__main__":
    main()
