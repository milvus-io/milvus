"""Exercise original DQL responses and List/Cancel through separate REST endpoints."""

import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from urllib.request import Request, urlopen
from uuid import uuid4

import numpy as np
import pytest
from running_requests_l1_rest_support import post_rest
from running_requests_l1_support import (
    TOPK,
    wait_for_records,
)

pytestmark = pytest.mark.tags("L1")


def entity_request(uri, token, kind, payload):
    endpoint = {"Search": "search", "Query": "query", "HybridSearch": "hybrid_search"}[kind]
    request = Request(
        f"{uri}/v2/vectordb/entities/{endpoint}",
        data=json.dumps(payload, separators=(",", ":")).encode(),
        headers={"Content-Type": "application/json", **({"Authorization": f"Bearer {token}"} if token else {})},
        method="POST",
    )
    with urlopen(request, timeout=90) as response:
        assert response.status == 200
        return json.load(response)


def entity_payload(kind, collection, expr, vectors, *, control=False):
    common = {"dbName": "default", "collectionName": collection}
    if kind == "Query":
        return dict(common, filter=expr, outputFields=["id", "vector"], limit=1 if control else 16384)
    ann = {"annsField": "vector", "data": vectors, "filter": expr, "limit": TOPK}
    if kind == "Search":
        return dict(common, **ann)
    return dict(common, search=[ann, dict(ann)], rerank={"strategy": "rrf", "params": {"k": 60}}, limit=TOPK)


@pytest.mark.parametrize(
    ("kind", "batch"),
    [
        ("Search", False),
        ("Query", False),
        ("HybridSearch", False),
        ("Search", True),
    ],
    ids=["search", "query", "hybrid", "search-batch"],
)
def test_rest_cross_proxy_cancel(kind, batch, deployment, workload):
    uris, token, user, _ = deployment
    proxy_a, proxy_b = uris
    _, _, collection, vectors = workload
    # Keep REST uploads within the forwarder's practical payload size.
    vectors = np.asarray(vectors[:8192], dtype="float64").round(6).tolist()
    target_proxies = (proxy_a, proxy_b) if batch else (proxy_a,)
    expressions = {proxy: f"id >= -{int(uuid4().hex[:7], 16)}" for proxy in target_proxies}
    control_expressions = {proxy: f"id >= -{int(uuid4().hex[:7], 16) + 1}" for proxy in target_proxies} if batch else {}
    jobs = {expr: (proxy, vectors[:-1] if batch else vectors) for proxy, expr in expressions.items()}
    jobs.update({expr: (proxy, vectors) for proxy, expr in control_expressions.items()})
    assert len(jobs) == len(expressions) + len(control_expressions)
    reason = "rest-cross-proxy-" + uuid4().hex

    if batch:
        assert len(vectors) > 1
        baseline = entity_request(
            uris[proxy_a],
            token,
            kind,
            entity_payload(kind, collection, "id >= 0", vectors),
        )
        assert baseline["code"] == 0 and len(baseline["data"]) == len(vectors) * TOPK
        assert baseline["topks"] == [TOPK] * len(vectors)

    def records():
        return post_rest(
            uris[proxy_b],
            token,
            set(uris),
            "list",
            {
                "dbName": "default",
                "collectionName": collection,
                "user": user,
            },
        )["requests"]

    ready = Barrier(len(jobs), timeout=10)

    def invoke(proxy, expr, query_vectors):
        ready.wait()
        return entity_request(uris[proxy], token, kind, entity_payload(kind, collection, expr, query_vectors))

    with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        futures = {
            expr: pool.submit(invoke, proxy, expr, query_vectors) for expr, (proxy, query_vectors) in jobs.items()
        }
        rows = wait_for_records(
            records,
            set(jobs),
            lambda row: row["expr"],
            futures.values(),
            timeout=30,
        )
        for expr, (proxy, _) in jobs.items():
            row = rows[expr]
            assert row["proxyId"] == proxy and row["type"] == kind and row["cancellable"], row
        assert len({row["requestId"] for row in rows.values()}) == len(jobs)
        ids = [rows[expr]["requestId"] for expr in expressions.values()]
        control_ids = {rows[expr]["requestId"] for expr in control_expressions.values()}
        result = post_rest(
            uris[proxy_b],
            token,
            set(uris),
            "cancel",
            {
                "requestIds": ids,
                "reason": reason,
                "user": user,
            },
        )
        assert sorted(row["requestId"] for row in result["canceled"]) == sorted(ids), result
        assert result["notFound"] == result["undetermined"] == [], result
        active = {row["requestId"] for row in records()} if batch else set()
        for expr in expressions.values():
            outcome = futures[expr].result(timeout=90)
            assert outcome["code"] == 3002 and reason in outcome["message"], outcome
        for expr in control_expressions.values():
            outcome = futures[expr].result(timeout=90)
            assert outcome["code"] == 0 and outcome["topks"] == baseline["topks"]
            assert [hit["id"] for hit in outcome["data"]] == [hit["id"] for hit in baseline["data"]]
            assert [hit["distance"] for hit in outcome["data"]] == pytest.approx(
                [hit["distance"] for hit in baseline["data"]]
            )
    assert not (set(ids) | control_ids) & {row["requestId"] for row in records()}
    assert control_ids <= active, "OBSERVATION_WINDOW: controls were no longer visible after Cancel"
    for proxy in target_proxies:
        response = entity_request(
            uris[proxy],
            token,
            kind,
            entity_payload(kind, collection, expressions[proxy], vectors[:1], control=True),
        )
        assert response["code"] == 0, response
        assert len(response["data"]) == (1 if kind == "Query" else TOPK), response
