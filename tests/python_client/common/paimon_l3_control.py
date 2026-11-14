"""Explicit, dedicated-deployment control boundary for Paimon L3 phase hooks.

The driver is supplied by the deployment owner. There is deliberately no default
kubectl/etcd target, global configuration mutation or simulated phase evidence.
"""

import json
import subprocess
from pathlib import Path
from uuid import uuid4


class PaimonL3Control:
    def __init__(self, path, uri):
        self.profile = json.loads(Path(path).read_text())
        if self.profile.get("protocol") != 1 or self.profile.get("dedicated") is not True:
            raise ValueError("L3 requires protocol=1 and dedicated=true")
        if self.profile.get("milvus_uri", "").rstrip("/") != uri.rstrip("/"):
            raise ValueError("L3 profile URI must match pytest's selected Milvus URI")
        self.uri = uri
        self.target = self.profile.get("target")
        if not isinstance(self.target, dict) or not all(
            self.target.get(key) for key in ["cluster", "namespace", "deployment"]
        ):
            raise ValueError("L3 target must identify cluster, namespace and deployment")
        self.command = self.profile.get("control_command")
        if (
            not isinstance(self.command, list)
            or not self.command
            or not all(isinstance(arg, str) and arg for arg in self.command)
        ):
            raise ValueError("L3 control_command must be a nonempty argv array")
        self.lease = uuid4().hex
        self.events = []

    def call(self, action, **payload):
        reserved = {"protocol", "lease", "action", "milvus_uri", "target"}
        if reserved & payload.keys():
            raise ValueError("L3 payload cannot override the selected target or protocol")
        request = {
            "protocol": 1,
            "lease": self.lease,
            "action": action,
            "milvus_uri": self.uri,
            "target": self.target,
            **payload,
        }
        result = subprocess.run(
            self.command, input=json.dumps(request), text=True, capture_output=True, timeout=60, check=False
        )
        if result.returncode:
            raise AssertionError(f"L3 control {action} failed (exit={result.returncode}): {result.stderr[:2000]}")
        response = json.loads(result.stdout)
        assert response.get("ok") is True, f"L3 control {action} did not acknowledge success"
        evidence = response["evidence"]
        assert isinstance(evidence, dict), "L3 evidence must be an object"
        self.events.append({"action": action, "evidence": evidence})
        return evidence
