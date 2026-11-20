"""Shared connections, workload fixtures, and assertions for running request L1."""

import os
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from setup_running_requests_l0 import DEFAULT_ROWS, TOPK, create_collection, load_baseline, search
from test_running_requests_l0 import (
    assert_all_proxies_answered,
    search_clients,
    search_hits,
    search_vectors,
)

WAIT_SECONDS = 30
QUERY_TIMEOUT = 90


def required_env(*names):
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        pytest.skip(f"Set the test inputs: {', '.join(missing)}")
    return tuple(os.environ[name] for name in names)


@pytest.fixture
def deployment(request):
    uri_a, uri_b, id_a, id_b, token, user = required_env(
        "PROXY_A_URI",
        "PROXY_B_URI",
        "PROXY_A_ID",
        "PROXY_B_ID",
        "MILVUS_TOKEN",
        "MILVUS_TEST_USER",
    )
    uris = {int(id_a): uri_a.rstrip("/"), int(id_b): uri_b.rstrip("/")}
    assert len(uris) == len(set(uris.values())) == 2
    from pymilvus import MilvusClient

    def connect(proxy_id, credential=token):
        client = MilvusClient(
            uri=uris[proxy_id],
            token=credential,
            timeout=10,
            dedicated=True,
            grpc_options={"grpc.use_local_subchannel_pool": 1},
        )
        request.addfinalizer(client.close)
        return client

    return uris, token, user, connect


@pytest.fixture
def search_collection(request):
    """Prepare a disposable collection for L1 cases that need isolated data."""

    def prepare(uri, token):
        client, control_client = search_clients(request, uri, token)
        collection = f"running_requests_l1_{uuid4().hex}"
        rows = int(os.environ.get("MILVUS_L0_ROWS", DEFAULT_ROWS))
        create_collection(
            client,
            collection,
            rows,
            on_created=lambda: request.addfinalizer(lambda: control_client.drop_collection(collection, timeout=30)),
        )
        return client, control_client, collection, search_vectors()

    return prepare


@pytest.fixture
def isolated_workload(deployment, search_collection):
    uris, token, _, _ = deployment
    owner, control, collection, vectors = search_collection(next(iter(uris.values())), token)
    assert len(vectors) >= 2
    return owner, control, collection, vectors


@pytest.fixture(scope="session")
def shared_dataset():
    from pymilvus import MilvusClient

    uri, token, collection = required_env("PROXY_A_URI", "MILVUS_TOKEN", "MILVUS_L0_COLLECTION")
    client = MilvusClient(uri=uri, token=token, timeout=10)
    try:
        assert client.get_load_state(collection, timeout=10)["state"].name == "Loaded"
        return load_baseline(client, collection)
    finally:
        client.close()


@pytest.fixture
def workload(request, deployment, search_collection):
    collection = os.environ.get("MILVUS_L0_COLLECTION")
    if not collection:
        return request.getfixturevalue("isolated_workload")
    uris, token, _, _ = deployment
    vectors, _ = request.getfixturevalue("shared_dataset")
    owner, control = search_clients(request, next(iter(uris.values())), token)
    return owner, control, collection, vectors


@pytest.fixture
def workload_baseline(request, workload):
    def get(nq=None):
        _, client, collection, vectors = workload
        nq = len(vectors) if nq is None else nq
        if os.environ.get("MILVUS_L0_COLLECTION"):
            _, baselines = request.getfixturevalue("shared_dataset")
            return baselines[nq]
        return search_hits(search(client, collection, vectors[:nq]), nq, TOPK)

    return get


def list_complete(client, proxy_ids, **filters):
    result = client.list_running_requests(timeout=10, **filters)
    assert_all_proxies_answered(result, proxy_ids)
    assert result.complete, result
    return result.requests


class ObservationWindowError(AssertionError):
    """The workload did not produce the required overlap."""


def wait_for_records(list_requests, keys, key_for, futures=(), *, exact=False, timeout=WAIT_SECONDS):
    """Require all expected records to coexist in the same complete snapshot."""
    deadline = time.monotonic() + timeout
    observed = set()
    while time.monotonic() < deadline:
        records = list_requests()
        matches = [record for record in records if key_for(record) in keys]
        counts = Counter(key_for(record) for record in matches)
        observed.update(counts)
        assert all(count == 1 for count in counts.values()), matches
        if counts.keys() == keys:
            if exact:
                assert len(records) == len(matches), f"Unexpected registry entries: {records}"
            return {key_for(record): record for record in matches}
        for future in futures:
            if future.done():
                future.result()
                raise ObservationWindowError(
                    f"Query finished before the required List snapshot: expected={keys}, observed={observed}"
                )
        time.sleep(0.02)
    raise ObservationWindowError(f"List window expired: expected={keys}, observed={observed}")


def check_record(record, proxy_id, user, collection):
    assert record.proxy_id == proxy_id, record
    assert record.user == user and record.collection_name == collection, record
    assert record.db_name == "default", record
    assert record.cancellable is True, record
    assert isinstance(record.request_id, str) and record.request_id, record


def cancel_exact(client, proxy_ids, ids, reason, *, not_found=()):
    result = client.cancel_requests([*ids, *not_found], reason=reason, timeout=10)
    assert_all_proxies_answered(result, proxy_ids)
    assert Counter(row.request_id for row in result.canceled) == Counter(ids), result
    assert Counter(result.not_found) == Counter(not_found), result
    assert not result.undetermined, result


def assert_canceled(future, reason):
    from pymilvus.exceptions import MilvusException

    with pytest.raises(MilvusException) as caught:
        future.result(timeout=QUERY_TIMEOUT)
    assert caught.value.code == 3002, caught.value
    assert reason in caught.value.message, caught.value


def assert_search_baseline(result, baseline, nq):
    actual = search_hits(result, nq, TOPK)
    for hits, expected in zip(actual, baseline):
        assert [item[0] for item in hits] == [item[0] for item in expected]
        assert [item[1] for item in hits] == pytest.approx([item[1] for item in expected])


def missing_active_requests(list_requests, ids):
    active = {row.request_id for row in list_requests() if row.state in {"Queued", "Running"}}
    return set(ids) - active


def assert_registered(list_requests, ids):
    """Require server-side evidence that the requests remain active."""
    missing = missing_active_requests(list_requests, ids)
    assert not missing, f"Missing active requests: {missing}"


def assert_unregistered(list_requests, ids):
    remaining = {record.request_id for record in list_requests()}
    assert not remaining.intersection(ids), remaining


def run_batch(
    case_id, deployment, workload, *, control_proxies, expected_missing, baseline, snapshot_timeout=WAIT_SECONDS
):
    """Cancel both targets and verify the original result of each control."""
    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    _, _, collection, vectors = workload
    operator = connect(proxy_b)
    nq = len(vectors)

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    tasks = {
        (proxy_a, nq - 1): vectors[:-1],
        (proxy_b, nq - 1): vectors[:-1],
        **{(proxy_id, nq): vectors for proxy_id in control_proxies},
    }
    clients = {key: connect(key[0]) for key in tasks}
    start = Barrier(len(tasks))

    def invoke(key, data):
        start.wait(timeout=5)
        return search(clients[key], collection, data)

    reason = f"{case_id}-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = {key: pool.submit(invoke, key, data) for key, data in tasks.items()}
        snapshot = wait_for_records(
            records,
            tasks.keys(),
            lambda row: (row.proxy_id, row.nq),
            futures.values(),
            timeout=snapshot_timeout,
        )
        for key, record in snapshot.items():
            check_record(record, key[0], user, collection)
            assert record.type == "Search" and record.topk == TOPK
        assert len({row.request_id for row in snapshot.values()}) == len(tasks)
        targets = [(proxy_a, nq - 1), (proxy_b, nq - 1)]
        target_ids = [snapshot[key].request_id for key in targets]
        cancel_exact(operator, set(uris), target_ids, reason, not_found=expected_missing)
        missing = missing_active_requests(
            records,
            {snapshot[proxy_id, nq].request_id for proxy_id in control_proxies},
        )
        for key in targets:
            assert_canceled(futures[key], reason)
        for proxy_id in control_proxies:
            result = futures[proxy_id, nq].result(timeout=QUERY_TIMEOUT)
            assert_search_baseline(result, baseline, nq)
    assert_unregistered(records, set(target_ids))
    if missing:
        pytest.fail(
            f"OBSERVATION_WINDOW: controls completed normally but their overlap with Cancel is unverified: {missing}"
        )
