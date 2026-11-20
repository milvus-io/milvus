"""TC-L0-01 for a dedicated Milvus test cluster with two proxies.

Set PROXY_A_URI, PROXY_B_URI, PROXY_A_ID, PROXY_B_ID, MILVUS_TOKEN, and
MILVUS_TEST_USER, and MILVUS_L0_COLLECTION. Run setup_running_requests_l0.py
once to prepare the collection and its Search baseline. Both URIs must
address individual proxies directly. Run serially on the dedicated
collection with the same MILVUS_L0_NQ and optional MILVUS_L0_BASELINE as setup.
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from setup_running_requests_l0 import TOPK, VECTOR_DIMENSION, load_baseline, search_hits
from setup_running_requests_l0 import search as sdk_search

LIST_WAIT_SECONDS = 60
SEARCH_TIMEOUT_SECONDS = 90


def assert_all_proxies_answered(result, proxy_ids):
    nodes = result.node_results
    assert len(nodes) == len(proxy_ids), nodes
    assert {node.node_id for node in nodes} == proxy_ids, nodes
    assert all(node.answered and not node.unimplemented for node in nodes), nodes


def search_clients(request, uri, token):
    from pymilvus import MilvusClient

    clients = []
    for _ in range(2):
        client = MilvusClient(
            uri=uri,
            token=token,
            dedicated=True,
            grpc_options={"grpc.use_local_subchannel_pool": 1},
        )
        request.addfinalizer(client.close)
        clients.append(client)
    return tuple(clients)


def search_vectors(default_nq=8192):
    import numpy as np

    nq = int(os.environ.get("MILVUS_L0_NQ", default_nq))
    return np.random.default_rng().random((nq, VECTOR_DIMENSION), dtype=np.float32)


@pytest.mark.tags("L0")
def test_list_and_cancel_search_across_proxies(request):
    """List two in-flight searches on B, cancel the target, and verify both outcomes."""
    names = (
        "PROXY_A_URI",
        "PROXY_B_URI",
        "PROXY_A_ID",
        "PROXY_B_ID",
        "MILVUS_TOKEN",
        "MILVUS_TEST_USER",
        "MILVUS_L0_COLLECTION",
    )
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        pytest.skip(f"Set the required L0 inputs: {', '.join(missing)}")
    proxy_a, proxy_b = (os.environ[name].rstrip("/") for name in names[:2])
    proxy_a_id, proxy_b_id = (int(os.environ[name]) for name in names[2:4])
    token, user, collection = (os.environ[name] for name in names[4:])
    assert proxy_a != proxy_b and proxy_a_id != proxy_b_id
    proxy_ids = {proxy_a_id, proxy_b_id}

    from pymilvus import MilvusClient
    from pymilvus.exceptions import MilvusException

    assert hasattr(MilvusClient, "list_running_requests")
    assert hasattr(MilvusClient, "cancel_requests")
    operator = MilvusClient(uri=proxy_b, token=token)
    request.addfinalizer(operator.close)
    target_client, control_client = search_clients(request, proxy_a, token)
    load_state = target_client.get_load_state(collection, timeout=10)
    assert load_state["state"].name == "Loaded", (
        f"Run setup_running_requests_l0.py first for {collection!r}: {load_state}"
    )
    control, baselines = load_baseline(target_client, collection)
    control_nq = len(control)
    baseline = baselines[control_nq]
    assert control_nq >= 2
    # Distinct nq values identify the target and control in one List snapshot.
    target = control[:-1]
    target_nq = control_nq - 1
    topk = TOPK
    database = "default"
    filters = {"collection_name": collection, "user": user}
    reason = f"TC-L0-01 {uuid4().hex}"

    def search(client, vectors):
        return sdk_search(client, collection, vectors, timeout=SEARCH_TIMEOUT_SECONDS)

    def list_requests():
        result = operator.list_running_requests(timeout=10, **filters)
        assert_all_proxies_answered(result, proxy_ids)
        assert result.complete, result
        return result.requests

    start = Barrier(2)

    def concurrent_search(client, vectors):
        start.wait(timeout=5)
        return search(client, vectors)

    with ThreadPoolExecutor(max_workers=2) as executor:
        target_call = executor.submit(concurrent_search, target_client, target)
        control_call = executor.submit(concurrent_search, control_client, control)
        deadline = time.monotonic() + LIST_WAIT_SECONDS
        observed = {}
        while time.monotonic() < deadline:
            records = list_requests()
            observed.update((record.request_id, record) for record in records)
            matches = [record for record in records if record.type == "Search" and record.nq in {target_nq, control_nq}]
            assert len(matches) <= 2, matches
            by_nq = {record.nq: record for record in matches}
            if by_nq.keys() == {target_nq, control_nq}:
                break
            if target_call.done() or control_call.done():
                for call in (target_call, control_call):
                    if call.done():
                        call.result()
                pytest.fail(
                    "A search completed before both appeared in one List response. "
                    f"Adjust MILVUS_L0_NQ for this cluster. Observed: {observed}"
                )
            time.sleep(0.02)
        else:
            pytest.fail(f"Both searches were not present in one complete List response. Observed: {observed}")

        for record in by_nq.values():
            assert record.proxy_id == proxy_a_id, record
            assert record.collection_name == collection, record
            assert record.db_name == database, record
            assert record.user == user and record.topk == topk, record
            assert record.cancellable is True, record
            assert isinstance(record.request_id, str) and record.request_id, record
        target_id = by_nq[target_nq].request_id
        assert target_id != by_nq[control_nq].request_id

        for label, call in (("Target", target_call), ("Control", control_call)):
            if call.done():
                call.result()
                pytest.fail(
                    f"{label} Search completed before Cancel was sent. Increase the Search workload for this cluster."
                )
        result = operator.cancel_requests([target_id], reason=reason, user=user, timeout=10)
        assert_all_proxies_answered(result, proxy_ids)
        if target_id in result.not_found and target_call.done():
            try:
                target_call.result()
            except MilvusException as error:
                pytest.fail(
                    f"Cancel reported target {target_id} as not found. "
                    f"The original Search raised code {error.code}: {error.message}"
                )
            pytest.fail(
                f"Target Search {target_id} returned normally and Cancel reported not found. "
                "The target may have finished between List and Cancel. "
                "Check the workload and timing on this cluster."
            )
        assert [item.request_id for item in result.canceled] == [target_id], result
        assert not result.not_found and not result.undetermined, result
        with pytest.raises(MilvusException) as canceled:
            target_call.result(timeout=60)
        assert canceled.value.code == 3002, canceled.value
        assert reason in canceled.value.message, canceled.value
        actual = search_hits(control_call.result(timeout=60), control_nq, topk)
        for actual_hits, baseline_hits in zip(actual, baseline):
            actual_ids, actual_distances = zip(*actual_hits)
            baseline_ids, baseline_distances = zip(*baseline_hits)
            assert actual_ids == baseline_ids
            assert actual_distances == pytest.approx(baseline_distances)

    records = list_requests()
    assert all(record.request_id != target_id for record in records), records
