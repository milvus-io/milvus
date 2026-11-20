"""T11 forward rolling upgrade on one CI-provisioned Milvus instance."""

import importlib
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing, contextmanager, nullcontext
from time import monotonic, sleep
from uuid import uuid4

import pytest
import requests
from pymilvus import MilvusClient
from running_requests_l1_support import (
    assert_canceled,
    assert_search_baseline,
    assert_unregistered,
    cancel_exact,
    check_record,
    list_complete,
    search,
    wait_for_records,
)
from running_requests_topology import ProxyAuthorizationTopology, run
from setup_running_requests_l0 import TOPK, load_baseline

pytestmark = pytest.mark.tags("L1")


@contextmanager
def clients(topology, token):
    with topology.endpoints() as endpoints, ExitStack() as stack:
        connections = tuple(
            stack.enter_context(closing(MilvusClient(uri=uri, token=token, timeout=10, dedicated=True)))
            for uri, _ in endpoints
        )
        yield endpoints, connections


def rollout(topology, name, image=None, *, container="proxy"):
    deployment = f"deployment/{name}"
    if image is None:
        run([*topology.kube, "rollout", "restart", deployment])
    else:
        run([*topology.kube, "set", "image", deployment, f"{container}={image}"])
    run([*topology.kube, "rollout", "status", deployment, "--timeout=300s"], timeout=310)


def rest_baseline(uri, token, collection, vector):
    response = requests.post(
        f"{uri}/v2/vectordb/entities/search",
        headers={"Authorization": f"Bearer {token}", "Accept-Type-Allow-Int64": "true"},
        json={
            "collectionName": collection,
            "data": [vector.tolist()],
            "annsField": "vector",
            "limit": TOPK,
            "searchParams": {"metricType": "L2"},
        },
        timeout=20,
    )
    response.raise_for_status()
    body = response.json()
    assert body["code"] == 0 and len(body["data"]) == TOPK, body
    return sorted((int(hit["id"]), hit["distance"]) for hit in body["data"])


def assert_rest_baseline(actual, expected):
    assert [pk for pk, _ in actual] == [pk for pk, _ in expected]
    assert [dist for _, dist in actual] == pytest.approx([dist for _, dist in expected])


def wait_nodes(client, expected):
    deadline = monotonic() + 120
    while monotonic() < deadline:
        result = client.list_running_requests(timeout=10)
        nodes = {node.node_id: (node.answered, node.unimplemented) for node in result.node_results}
        if nodes == expected:
            assert result.complete == all(answered for answered, _ in expected.values())
            return
        sleep(1)
    pytest.fail(f"Proxy membership did not converge: expected={expected}, observed={nodes}")


def check_write_and_ddl(client, vector):
    name = f"t11_smoke_{uuid4().hex}"
    try:
        client.create_collection(name, dimension=len(vector), metric_type="L2", timeout=60)
        assert client.has_collection(name, timeout=10)
        written = client.insert(name, [{"id": 1, "vector": vector.tolist()}], timeout=30)
        assert written["insert_count"] == 1
        client.flush(name, timeout=60)
        rows = client.query(name, filter="id == 1", output_fields=["id"], consistency_level="Strong", timeout=30)
        assert rows == [{"id": 1}]
    finally:
        if client.has_collection(name, timeout=10):
            client.drop_collection(name, timeout=30)
    assert not client.has_collection(name, timeout=10)


def cancel_with_control(
    operator, target, control, target_id, expected_nodes, collection, user, vectors, baseline, *, control_gate=None
):
    mixed = control_gate is not None
    reason = f"T11-{uuid4().hex}"
    unknown = f"999-{uuid4().hex[:8]}-9"

    def records():
        result = operator.list_running_requests(collection_name=collection, user=user, timeout=10)
        assert {n.node_id: (n.answered, n.unimplemented) for n in result.node_results} == expected_nodes
        assert result.complete == (not mixed)
        return result.requests

    with ThreadPoolExecutor(max_workers=2) as pool, control_gate or nullcontext() as gate:
        target_future = pool.submit(search, target, collection, vectors[:-1])
        control_future = pool.submit(search, control, collection, vectors)
        if mixed:
            gate.wait_started(timeout=20)
            expected_records = {target_id}
        else:
            expected_records = set(expected_nodes)
        snapshot = wait_for_records(
            records,
            expected_records,
            lambda item: item.proxy_id,
            [target_future, control_future],
        )
        row = snapshot[target_id]
        if not mixed:
            (control_id,) = expected_records - {target_id}
            check_record(snapshot[control_id], control_id, user, collection)
            assert snapshot[control_id].request_id != row.request_id
        check_record(row, target_id, user, collection)
        assert not target_future.done() and not control_future.done()
        if mixed:
            result = operator.cancel_requests([row.request_id, unknown], reason=reason, timeout=10)
            assert {n.node_id: (n.answered, n.unimplemented) for n in result.node_results} == expected_nodes
            assert [item.request_id for item in result.canceled] == [row.request_id]
            assert result.not_found == [unknown] and not result.undetermined
        else:
            cancel_exact(operator, set(expected_nodes), [row.request_id], reason)
            control_request_id = snapshot[control_id].request_id
            assert any(
                item.request_id == control_request_id and item.state in {"Queued", "Running"} for item in records()
            ), f"Control request {control_request_id} was not in flight after Cancel"
        if mixed:
            gate.assert_held()
            gate.release()
        assert_canceled(target_future, reason)
        assert_search_baseline(control_future.result(timeout=90), baseline, len(vectors))
    assert_unregistered(records, {row.request_id})


def check_restart(topology, token, collection, user, vectors, baseline, *, observe_restart=False):
    with clients(topology, token) as (((_, a_id), (_, old_b)), (a, b)):
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(search, b, collection, vectors)

            def records():
                return list_complete(a, {a_id, old_b}, collection_name=collection, user=user)

            old_id = wait_for_records(records, {old_b}, lambda row: row.proxy_id, [future])[old_b].request_id
            if not observe_restart:
                rollout(topology, topology.deployments[1])
            else:
                marker = f"restart-probe-{uuid4().hex}"

                def restart_b():
                    if len(topology.deployments) == 2:
                        rollout(topology, topology.deployments[1])
                        return
                    pod = topology.proxy_pods()[old_b]
                    metadata = json.loads(run([*topology.kube, "get", pod, "-o", "json"]))["metadata"]
                    path = f"/api/v1/namespaces/{metadata['namespace']}/pods/{metadata['name']}"
                    options = {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": metadata["uid"]}}
                    run([*topology.kube, "delete", "--raw", path, "-f", "-"], stdin=json.dumps(options))
                    deadline = monotonic() + 180
                    while monotonic() < deadline:
                        try:
                            current = topology.proxy_pods()
                        except (RuntimeError, subprocess.CalledProcessError):
                            sleep(1)
                            continue
                        if a_id in current and old_b not in current:
                            return
                        sleep(1)
                    raise AssertionError("Proxy B replacement did not become Ready")

                with ThreadPoolExecutor(max_workers=1) as restart_pool:
                    restart = restart_pool.submit(restart_b)
                    while not restart.done():
                        listed = a.list_running_requests(collection_name=collection, user=user, timeout=20)
                        canceled = a.cancel_requests([marker], timeout=20)
                        assert any(node.node_id == a_id and node.answered for node in listed.node_results)
                        assert any(node.node_id == a_id and node.answered for node in canceled.node_results)
                        assert not canceled.canceled
                        assert (marker in canceled.not_found) != (marker in canceled.undetermined)
                        print(
                            "restart window",
                            listed.node_results,
                            canceled.node_results,
                            canceled.not_found,
                            canceled.undetermined,
                        )
                        sleep(0.2)
                    restart.result()

    with clients(topology, token) as (((_, first_id), (_, second_id)), (a, b)):
        if first_id != a_id:
            a, b = b, a
            first_id, second_id = second_id, first_id
        assert first_id == a_id
        new_b = second_id
        assert new_b != old_b
        nodes = {a_id: (True, False), new_b: (True, False)}
        wait_nodes(a, nodes)

        def records():
            return list_complete(a, set(nodes), collection_name=collection, user=user)

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(search, b, collection, vectors)
            row = wait_for_records(records, {new_b}, lambda item: item.proxy_id, [future])[new_b]
            assert row.request_id != old_id and not future.done()
            cancel_exact(a, set(nodes), [], "T11 stale ID", not_found=[old_id])
            if future.done():
                future.result()
            assert_unregistered(records, {old_id})
            assert_search_baseline(future.result(timeout=90), baseline, len(vectors))


def test_t11_forward_rolling_upgrade():
    if os.environ.get("MILVUS_T11_FORWARD") != "1":
        pytest.skip("Run through the forward-upgrade CI entry point")
    token = os.environ["MILVUS_TOKEN"]
    user = os.environ["MILVUS_TEST_USER"]
    collection = os.environ["MILVUS_L0_COLLECTION"]
    old_image = os.environ["MILVUS_L1_OLD_IMAGE"]
    new_image = os.environ["MILVUS_L1_NEW_IMAGE"]
    provider = importlib.import_module(os.environ["MILVUS_L1_CHECKPOINT_PROVIDER"])
    assert callable(provider.hold_proxy_search)
    topology = ProxyAuthorizationTopology()
    core = os.environ["MILVUS_T11_CORE_DEPLOYMENTS"].split()
    assert core and not set(core).intersection(topology.deployments)
    assert len(core) == len(set(core)) and new_image != old_image
    containers = {}
    for name in (*core, *topology.deployments):
        deployment = json.loads(run([*topology.kube, "get", "deployment", name, "-o", "json"]))
        matches = [
            item["name"] for item in deployment["spec"]["template"]["spec"]["containers"] if item["image"] == old_image
        ]
        assert len(matches) == 1, (name, matches)
        containers[name] = matches[0]

    with clients(topology, token) as (((a_uri, _), (b_uri, _)), (a, b)):
        vectors, baselines = load_baseline(a, collection)
        assert len(vectors) >= 3
        expected_rest = rest_baseline(a_uri, token, collection, vectors[0])
        assert_rest_baseline(rest_baseline(b_uri, token, collection, vectors[0]), expected_rest)
        check_write_and_ddl(a, vectors[0])
    baseline = baselines[len(vectors)]

    for name in core:
        rollout(topology, name, new_image, container=containers[name])
    rollout(topology, topology.deployments[0], new_image, container=containers[topology.deployments[0]])

    with clients(topology, token) as (((_, a_id), (_, b_id)), (a, b)):
        mixed_nodes = {a_id: (True, False), b_id: (False, True)}
        wait_nodes(a, mixed_nodes)
        control_gate = provider.hold_proxy_search(proxy_id=b_id, collection=collection, nq=len(vectors))
        cancel_with_control(a, a, b, a_id, mixed_nodes, collection, user, vectors, baseline, control_gate=control_gate)
        check_write_and_ddl(b, vectors[0])

    rollout(topology, topology.deployments[1], new_image, container=containers[topology.deployments[1]])
    with clients(topology, token) as (((a_uri, a_id), (b_uri, b_id)), (a, b)):
        nodes = {a_id: (True, False), b_id: (True, False)}
        for uri, operator, target, target_id in ((a_uri, a, b, b_id), (b_uri, b, a, a_id)):
            wait_nodes(operator, nodes)
            assert_rest_baseline(rest_baseline(uri, token, collection, vectors[0]), expected_rest)
            cancel_with_control(operator, target, operator, target_id, nodes, collection, user, vectors, baseline)
        check_write_and_ddl(a, vectors[0])

    check_restart(topology, token, collection, user, vectors, baseline)
