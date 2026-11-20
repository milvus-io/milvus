"""Provision isolated Helm topologies for tests that need specific Proxy settings."""

import json
import os
import re
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path
from uuid import uuid4

import pytest


def run(command, *, stdin=None, timeout=30):
    result = subprocess.run(command, input=stdin, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        # Helm and Kubernetes errors can include rendered values with storage credentials.
        raise RuntimeError(f"{Path(command[0]).name} {command[1]} failed (exit {result.returncode})")
    return result.stdout


@contextmanager
def forward(kube, pod, port=19530):
    with tempfile.TemporaryDirectory(prefix="rr-forward-") as folder:
        log = Path(folder) / "forward.log"
        with log.open("w") as output:
            process = subprocess.Popen(
                [*kube, "port-forward", "--address", "127.0.0.1", pod if "/" in pod else f"pod/{pod}", f":{port}"],
                stdout=output,
                stderr=output,
                text=True,
            )
        try:
            deadline = time.monotonic() + 30
            while process.poll() is None and time.monotonic() < deadline:
                match = re.search(rf"Forwarding from 127\.0\.0\.1:(\d+) -> {port}", log.read_text())
                if match:
                    yield f"http://127.0.0.1:{match[1]}"
                    return
                time.sleep(0.2)
            raise RuntimeError(f"Port forwarding did not become ready for {pod}")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def ready_proxy(kube, selector):
    run([*kube, "wait", "--for=condition=Ready", "pod", "-l", selector, "--timeout=300s"], timeout=315)
    pods = json.loads(run([*kube, "get", "pods", "-l", selector, "-o", "json"]))["items"]
    return sorted(pod["metadata"]["name"] for pod in pods if not pod["metadata"].get("deletionTimestamp"))


def node_id(kube, pod):
    # Startup logs may have rotated on a long-running test instance.
    metrics = run(
        [*kube, "exec", pod, "-c", "proxy", "--", "curl", "-fsS", "--max-time", "5", "http://127.0.0.1:9091/metrics"]
    )
    ids = set(re.findall(r'^milvus_proxy_[^\n]*?\bnode_id="(\d+)"', metrics, re.MULTILINE))
    ids.discard("0")  # Metrics emitted before session registration use node ID zero.
    if len(ids) == 1:
        return int(ids.pop())
    if len(ids) > 1:
        raise RuntimeError(f"Ambiguous Proxy node IDs from {pod}: {sorted(ids)}")
    logs = run([*kube, "logs", pod, "-c", "proxy"])
    matches = re.findall(r"init proxy done[^\n]*?\bnodeID[=:]\s*(\d+)", logs)
    if not matches:
        raise RuntimeError(f"Could not discover Proxy node ID from {pod}")
    return int(matches[-1])


def create_old_proxy(kube, release, old_image):
    selector = f"app.kubernetes.io/instance={release},component=proxy"
    deployments = json.loads(run([*kube, "get", "deployment", "-l", selector, "-o", "json"]))["items"]
    if len(deployments) != 1:
        raise RuntimeError("Expected exactly one Helm-managed Proxy Deployment")
    spec = deployments[0]["spec"]
    labels = spec["template"]["metadata"]["labels"]
    labels["component"] = "proxy-old"
    spec["selector"] = {"matchLabels": {"app.kubernetes.io/instance": release, "component": "proxy-old"}}
    for container in spec["template"]["spec"]["containers"]:
        if container["name"] == "proxy":
            container["image"] = old_image
            break
    else:
        raise RuntimeError("Could not find the Proxy container")
    name = f"{release}-proxy-old"
    manifest = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": name}, "spec": spec}
    run([*kube, "create", "-f", "-"], stdin=json.dumps(manifest))
    run([*kube, "rollout", "status", f"deployment/{name}", "--timeout=300s"], timeout=315)


@contextmanager
def proxy_topology(*, mixed):
    new_image = os.environ.get("MILVUS_L1_NEW_IMAGE")
    old_image = os.environ.get("MILVUS_L1_OLD_IMAGE")
    if not new_image or (mixed and not old_image):
        pytest.fail("Set MILVUS_L1_NEW_IMAGE and, for T11, MILVUS_L1_OLD_IMAGE to compatible image tags")
    if mixed and old_image == new_image:
        pytest.fail("T11 needs two distinct Milvus images")
    kubeconfig = os.environ.get("MILVUS_L1_KUBECONFIG", os.environ.get("KUBECONFIG"))
    connection = ["--kubeconfig", kubeconfig] if kubeconfig else []
    kubectl = ["kubectl", *connection]
    helm = ["helm", *connection]
    chart = os.environ.get("MILVUS_L1_HELM_CHART", "milvus/milvus")
    chart_args = ["--version", "5.0.22"] if chart == "milvus/milvus" else []
    renderer = os.environ.get("MILVUS_L1_VALUES_RENDERER")
    if not renderer or not Path(renderer).is_file():
        pytest.fail("Set MILVUS_L1_VALUES_RENDERER to the vcluster Helm values renderer")
    for name in ("ZILLIZ_MINIO_LOCAL_ACCESS_KEY", "ZILLIZ_MINIO_LOCAL_SECRET_KEY"):
        if not os.environ.get(name, "").strip():
            pytest.fail(f"Load {name} before provisioning the test topology")
    release = f"rr-{'mixed' if mixed else 'noauth'}-{uuid4().hex[:8]}"
    kube = [*kubectl, "-n", release]
    instance = f"app.kubernetes.io/instance={release}"
    with ExitStack() as stack:
        folder = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="rr-helm-")))
        values = folder / "values.yaml"
        render = [
            sys.executable,
            renderer,
            "--name",
            release,
            "--namespace",
            release,
            "--image",
            new_image,
            "--output",
            str(values),
        ]
        for secret in os.environ.get("MILVUS_L1_IMAGE_PULL_SECRETS", "").split():
            render.extend(["--pull-secret", secret])
        run(render)
        # User configuration takes precedence over the chart defaults.
        overlay = folder / "topology.json"
        auth = "true" if mixed else "false"
        overlay.write_text(
            json.dumps(
                {
                    "proxy": {"replicas": 1 if mixed else 2},
                    "extraConfigFiles": {
                        "user.yaml": f"common:\n  storage:\n    useLoonFFI: true\n  security:\n    authorizationEnabled: {auth}\n"
                    },
                }
            ),
            encoding="utf-8",
        )
        run([*kubectl, "create", "namespace", release])
        stack.callback(run, [*kubectl, "delete", "namespace", release, "--wait=true", "--timeout=180s"], timeout=195)
        run(
            [
                *helm,
                "install",
                release,
                chart,
                "-n",
                release,
                *chart_args,
                "-f",
                str(values),
                "-f",
                str(overlay),
                "--wait",
                "--timeout",
                "20m",
            ],
            timeout=1220,
        )
        pods = ready_proxy(kube, f"{instance},component=proxy")
        if mixed:
            create_old_proxy(kube, release, old_image)
            pods += ready_proxy(kube, f"{instance},component=proxy-old")
        if len(pods) != 2:
            raise RuntimeError(f"Expected exactly two Ready Proxy Pods, found {pods}")
        endpoints = [(stack.enter_context(forward(kube, pod)), node_id(kube, pod)) for pod in pods]
        if endpoints[0][1] == endpoints[1][1]:
            raise RuntimeError("The two Proxy Pods reported the same node ID")
        yield endpoints


class ProxyAuthorizationTopology:
    """Restart the two existing Proxy Deployments around an authorization change."""

    def __init__(self, *, shared=None):
        if shared is None:
            shared = os.environ.get("MILVUS_L1_SHARED_PROXY_DEPLOYMENT") == "1"
        kubeconfig = os.environ.get("MILVUS_L1_KUBECONFIG") or os.environ.get("KUBECONFIG")
        if not kubeconfig and not os.environ.get("KUBERNETES_SERVICE_HOST"):
            kubeconfig = str(Path.home() / ".kube/config")
        namespace = os.environ.get("MILVUS_L1_NAMESPACE", "requests-l0-1112")
        release = os.environ.get("MILVUS_L1_RELEASE", "requests-l0")
        self.kube = ["kubectl", *(["--kubeconfig", kubeconfig] if kubeconfig else []), "-n", namespace]
        self.release = release
        self.shared = shared
        self.configmap = os.environ.get("MILVUS_L1_AUTH_CONFIGMAP", release)
        self.config_key = os.environ.get("MILVUS_L1_AUTH_CONFIG_KEY", "user.yaml")
        primary = os.environ.get("MILVUS_L1_PRIMARY_PROXY_DEPLOYMENT", f"{release}-milvus-proxy")
        self.deployments = (
            (primary,)
            if shared
            else (
                primary,
                f"{release}-milvus-proxy-secondary",
            )
        )

    def configuration(self):
        raw = run([*self.kube, "get", "configmap", self.configmap, "-o", "json"])
        return json.loads(raw)["data"][self.config_key]

    def replace_configuration(self, expected, value):
        key = self.config_key.replace("~", "~0").replace("/", "~1")
        patch = [
            {"op": "test", "path": f"/data/{key}", "value": expected},
            {"op": "replace", "path": f"/data/{key}", "value": value},
        ]
        run(
            [
                *self.kube,
                "patch",
                "configmap",
                self.configmap,
                "--type=json",
                "--patch-file=/dev/stdin",
            ],
            stdin=json.dumps(patch),
        )
        if self.configuration() != value:
            raise RuntimeError("The Proxy ConfigMap did not retain the requested configuration")

    def restart(self):
        run([*self.kube, "rollout", "restart", "deployment", *self.deployments])
        for name in self.deployments:
            run([*self.kube, "rollout", "status", f"deployment/{name}", "--timeout=300s"], timeout=315)

    @contextmanager
    def two_replicas(self):
        """Temporarily scale the existing CI Proxy Deployment."""
        (primary,) = self.deployments
        resource = f"deployment/{primary}"
        deployment = json.loads(run([*self.kube, "get", resource, "-o", "json"]))
        original = deployment["spec"].get("replicas", 1)
        if original not in (1, 2):
            raise RuntimeError(f"Expected one or two Proxy replicas, found {original}")
        scale = [*self.kube, "scale", resource]
        rollout = [*self.kube, "rollout", "status", resource, "--timeout=300s"]
        if original == 2:
            yield
            return
        run([*scale, "--current-replicas=1", "--replicas=2"])
        try:
            run(rollout, timeout=315)
            yield
        finally:
            run([*scale, "--current-replicas=2", "--replicas=1"])
            run(rollout, timeout=315)

    def proxy_pods(self, *, require_auth_config=False):
        """Return the Ready Pods and their Milvus node IDs in endpoint order."""
        endpoints = {}
        expected = 2 if len(self.deployments) == 1 else 1
        for name in self.deployments:
            raw = run([*self.kube, "get", "deployment", name, "-o", "json"])
            spec = json.loads(raw)["spec"]
            if require_auth_config and not any(
                volume.get("configMap", {}).get("name") == self.configmap
                for volume in spec["template"]["spec"].get("volumes", [])
            ):
                raise RuntimeError(f"{name} does not mount the authorization ConfigMap")
            selector = ",".join(f"{key}={value}" for key, value in spec["selector"]["matchLabels"].items())
            pods = ready_proxy(self.kube, selector)
            if len(pods) != expected:
                raise RuntimeError(f"Expected {expected} Ready Proxy Pods for {name}, got {pods}")
            for pod in pods:
                proxy_id = node_id(self.kube, pod)
                if proxy_id in endpoints:
                    raise RuntimeError(f"Duplicate Proxy node ID {proxy_id}")
                endpoints[proxy_id] = f"pod/{pod}"
        return endpoints

    @contextmanager
    def endpoints(self, *, require_auth_config=True):
        with ExitStack() as stack:
            yield [
                (stack.enter_context(forward(self.kube, pod)), proxy_id)
                for proxy_id, pod in self.proxy_pods(require_auth_config=require_auth_config).items()
            ]

    def set_operator_authorization(self, enabled):
        resource = ["milvus", self.release]
        value = json.dumps(enabled)
        patch = {"spec": {"config": {"common": {"security": {"authorizationEnabled": enabled}}}}}
        run([*self.kube, "patch", *resource, "--type=merge", "--patch-file=/dev/stdin"], stdin=json.dumps(patch))
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if re.search(rf"(?m)^\s*authorizationEnabled:\s*{value}\s*$", self.configuration()):
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"Operator did not render authorizationEnabled={value}")
        self.restart()

    @contextmanager
    def disabled(self, original, verify_restored):
        if self.shared:
            try:
                self.set_operator_authorization(False)
                yield
            finally:
                self.set_operator_authorization(True)
                verify_restored()
            return
        disabled, count = re.subn(
            r"(?m)^([ \t]+authorizationEnabled:[ \t]*)true([ \t]*(?:#.*)?)$",
            r"\g<1>false\2",
            original,
        )
        if count != 1 or original != self.configuration():
            raise RuntimeError(f"Expected one unchanged authorizationEnabled: true entry in {self.config_key}")
        try:
            self.replace_configuration(original, disabled)
            self.restart()
            yield
        finally:
            current = self.configuration()
            if current != original:
                self.replace_configuration(disabled, original)
            self.restart()
            verify_restored()
