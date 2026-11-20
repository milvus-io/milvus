"""Control the T04 Running test gate on the owning Proxy Pod."""

import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from time import monotonic, sleep

from running_requests_topology import ProxyAuthorizationTopology, run


@dataclass(frozen=True)
class Evidence:
    request_id: str
    state: str


@dataclass(frozen=True)
class Checkpoint:
    command: tuple
    gate: str
    state: str = "Running"

    def wait(self, timeout):
        deadline = monotonic() + timeout
        while monotonic() < deadline:
            request_id = run(
                [*self.command, "sh", "-c", 'if [ -f "$1" ]; then cat "$1"; fi', "t04", self.gate + ".reached"],
                timeout=5,
            ).strip()
            if request_id:
                return Evidence(request_id, self.state)
            sleep(0.1)
        raise TimeoutError("T04 Running did not reach the Proxy checkpoint")


@contextmanager
def hold_request_exit(collection, state):
    if state != "Running" or not re.fullmatch(r"running_requests_l1_[0-9a-f]{32}", collection):
        raise ValueError("T04 pilot requires Running and an isolated workload collection")
    topology = ProxyAuthorizationTopology(shared=True)
    proxy = topology.proxy_pods()[int(os.environ["PROXY_A_ID"])]
    command = (*topology.kube, "exec", proxy, "-c", "proxy", "--")
    gate = "/tmp/milvus-rr-t04-" + collection
    run([*command, "touch", gate + ".armed"], timeout=10)
    try:
        yield Checkpoint(command, gate)
    finally:
        run([*command, "rm", "-f", gate + ".armed"], timeout=10)
