"""Repeat real cancellations while both Proxies serve management and search traffic."""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from time import monotonic
from uuid import uuid4

import numpy as np
import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import (
    wait_for_records,
)
from test_running_requests_native_sdk import Native, complete, start
from test_running_requests_rest_cross_proxy import entity_payload, entity_request

pytestmark = [
    pytest.mark.tags("L1"),
    pytest.mark.skipif(
        os.getenv("MILVUS_FOURAM_FAULT_CI") != "1", reason="Requires dedicated management load deployment"
    ),
]


class ManagementClient:
    def __init__(self, language, uri, token, nodes, collection, user, log, request):
        self.uri, self.token, self.nodes = uri, token, nodes
        self.collection, self.user = collection, user
        self.worker = None
        if language != "rest":
            self.worker = Native(language, log)
            request.addfinalizer(self.worker.close)
            self.worker.ok("connect", "b", URI=uri, Token=token, DB="default")
            self.worker.nodes = nodes

    def records(self):
        if self.worker:
            result = complete(
                self.worker,
                self.worker.call(
                    "list",
                    Collection=self.collection,
                    User=self.user,
                ),
            )
            return [
                dict(requestId=row["request_id"], proxyId=row["proxy_id"], expr=row["expr"], type=row["type"])
                for row in result["requests"]
            ]
        return post_rest(
            self.uri,
            self.token,
            self.nodes,
            "list",
            {
                "collectionName": self.collection,
                "user": self.user,
            },
        )["requests"]

    def cancel(self, request_id, reason, *, active):
        if self.worker:
            result = complete(self.worker, self.worker.call("cancel", IDs=[request_id], Reason=reason))
            ids = [row["request_id"] for row in result["canceled"]]
            missing = result["not_found"] or []
        else:
            result = post_rest(
                self.uri,
                self.token,
                self.nodes,
                "cancel",
                {
                    "requestIds": [request_id],
                    "reason": reason,
                },
            )
            ids = [row["requestId"] for row in result["canceled"]]
            missing = result["notFound"]
        assert not result["undetermined"], result
        assert ids == ([request_id] if active else []), result
        assert missing == ([] if active else [request_id]), result

    def search(self, data, vectors, expr):
        if self.worker:
            call, _ = start(self.worker, data, client="b")
            return self.worker.call("await", Call=call)
        return entity_request(self.uri, self.token, "Search", entity_payload("Search", self.collection, expr, vectors))


@pytest.mark.parametrize("language", ["java", "go", "rest"])
def test_management_burst_preserves_search(language, deployment, workload, request, tmp_path):
    uris, token, user, _ = deployment
    a, b = uris
    _, _, collection, vectors = workload
    rounds = int(os.getenv("MILVUS_RR_STABILITY_ROUNDS", "5"))
    probes = int(os.getenv("MILVUS_RR_STABILITY_PROBES", "12"))
    assert 1 <= rounds <= 100 and 1 <= probes <= 100
    array = np.asarray(vectors[:8192], dtype="<f4")
    binary = tmp_path / "vectors.f32"
    array.tofile(binary)
    rest_vectors = array.astype("float64").round(6).tolist()
    target_data = dict(Collection=collection, Vectors=str(binary), NQ=len(array), Limit=10)
    control_data = dict(target_data, NQ=1)

    def client(name, proxy):
        return ManagementClient(
            language, uris[proxy], token, set(uris), collection, user, tmp_path / f"{name}.log", request
        )

    target = client("target", a)
    control = client("control", b)
    operator = client("operator", b)
    managers = {proxy: client(f"probes-{proxy}", proxy) for proxy in uris}

    def control_hits():
        body = control.search(control_data, rest_vectors[:1], "id >= 0")
        if control.worker:
            assert body["ok"], body
            assert len(body["hits"]) == 1 and len(body["hits"][0]) == 10, body
            return body["hits"]
        assert body["code"] == 0 and len(body["data"]) == 10, body
        return body["data"]

    expected = control_hits()
    stop = Event()
    progress = []

    def check_controls():
        while not stop.is_set():
            assert control_hits() == expected
            progress.append(monotonic())
            stop.wait(0.1)

    def run_probes(manager):
        for _ in range(probes):
            manager.records()
            manager.cancel(f"rr-unknown-{uuid4().hex}", "stability-probe", active=False)

    with ThreadPoolExecutor(max_workers=5) as pool:
        controls = pool.submit(check_controls)
        try:
            for _ in range(rounds):
                started = monotonic()
                expr = f"id >= -{int(uuid4().hex[:7], 16) + 1}"
                if target.worker:
                    call = uuid4().hex
                    target.worker.ok("start", "b", **dict(target_data, Kind="Search", Call=call, Expr=expr))
                    future = pool.submit(target.worker.call, "await", Call=call)
                else:
                    future = pool.submit(target.search, target_data, rest_vectors, expr)
                traffic = [pool.submit(run_probes, manager) for manager in managers.values()]
                row = wait_for_records(operator.records, {expr}, lambda item: item["expr"], [future], timeout=30)[expr]
                assert row["type"] == "Search" and row["proxyId"] == a, row
                reason = f"rr-stability-{uuid4().hex}"
                operator.cancel(row["requestId"], reason, active=True)
                outcome = future.result(timeout=95)
                assert outcome["code"] == 3002 and reason in outcome["message"], outcome
                for probe in traffic:
                    probe.result(timeout=150)
                assert row["requestId"] not in {item["requestId"] for item in operator.records()}
                assert any(sample >= started for sample in progress), "No control Search completed during round"
                print(
                    json.dumps(
                        {
                            "language": language,
                            "round_seconds": monotonic() - started,
                            "list_probes": probes * len(managers),
                            "cancel_probes": probes * len(managers),
                        }
                    ),
                    flush=True,
                )
        finally:
            stop.set()
            controls.result(timeout=95)
    assert control_hits() == expected
