"""Exercise real Cancel through both Proxies while one QueryNode task is waiting."""
from concurrent.futures import ThreadPoolExecutor
import os
import pytest
import re
import subprocess
from time import monotonic, sleep
from uuid import uuid4

from running_requests_l1_support import (
    assert_canceled, assert_search_baseline, assert_unregistered,
    cancel_exact, deployment, isolated_workload, list_complete, search, search_collection, search_hits,
    shared_dataset, workload,
)
from running_requests_topology import run, ProxyAuthorizationTopology
import json
from setup_running_requests_l0 import TOPK

@pytest.mark.skipif(os.getenv("RR_CI_EXTENDED_CHECKPOINTS") != "1",
                    reason="Requires the dedicated checkpoint image and explicit opt-in")
def test_t14_controlled_segment_wait(deployment, workload):
    uris, _, user, connect = deployment
    a, b = uris
    app, control, collection, vectors = workload
    operator = connect(b)
    trace_id = uuid4().hex
    reason = "T14-controlled-" + trace_id
    node = os.environ.get("RR_LOCAL_QUERYNODE")
    if node:
        execute = ["docker", "exec", node]
        log_command = ["docker", "logs", node]
    else:
        topology = ProxyAuthorizationTopology(shared=True)
        pods = json.loads(run([*topology.kube, "get", "pods", "-o", "json"]))["items"]
        release = os.environ["MILVUS_L1_RELEASE"]
        nodes = [p["metadata"]["name"] for p in pods
                 if p["metadata"]["name"].startswith(release+"-milvus-querynode-")
                 and not p["metadata"].get("deletionTimestamp")
                 and any(c["type"] == "Ready" and c["status"] == "True"
                         for c in p.get("status", {}).get("conditions", []))]
        assert len(nodes) == 1, "Controlled wait requires exactly one Ready QueryNode"
        node = nodes[0]
        execute = [*topology.kube, "exec", node, "-c", "querynode", "--"]
        log_command = [*topology.kube, "logs", node, "-c", "querynode", "--tail=10000"]
    marker = "/tmp/milvus-rr-t14-" + trace_id + ".armed"
    before = search_hits(search(control, collection, vectors[:1]), 1, TOPK)

    def events():
        output = subprocess.check_output(log_command, stderr=subprocess.STDOUT, text=True, timeout=5)
        return [line for line in output.splitlines() if trace_id in line]

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    run([*execute, "touch", marker])
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(app.search, collection, vectors[:1], anns_field="vector",
                                 limit=TOPK, search_params={"metric_type":"L2"}, timeout=30,
                                 client_request_id=trace_id)
            deadline = monotonic() + 10
            while monotonic() < deadline:
                if any("T14 segment wait entered" in l for l in events()):
                    break
                assert not future.done(), "Search exited before the wait point"
                sleep(0.05)
            else:
                raise AssertionError("QueryNode did not report the targeted wait")
            row, = [r for r in records() if r.trace_id == trace_id]
            assert row.proxy_id == a and not future.done()
            cancel_exact(operator, set(uris), [row.request_id], reason)
            assert_canceled(future, reason)
            deadline = monotonic() + 18
            while monotonic() < deadline:
                lines = events()
                if any("T14 segment wait exited on cancellation" in l for l in lines):
                    break
                sleep(0.05)
            else:
                raise AssertionError("QueryNode wait did not exit on cancellation")
            wait = next(l for l in lines if "T14 segment wait entered" in l)
            stopped = next(l for l in lines if "T14 segment wait exited on cancellation" in l)
            key = r"\[segmentID=(\d+)\].*\[spanID=([0-9a-f]+)\]"
            assert re.search(key, wait).groups() == re.search(key, stopped).groups()
            assert "context canceled" in stopped
            assert not any('["search segment..."]' in l or "PhyVectorSearchNode" in l for l in lines)
            print(row.request_id, trace_id, wait, stopped, sep="\n")
            assert_unregistered(records, {row.request_id})
    finally:
        run([*execute, "rm", "-f", marker])
    assert_search_baseline(search(control, collection, vectors[:1]), before, 1)
