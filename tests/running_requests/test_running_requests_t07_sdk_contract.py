"""TC-L1 functional scenario T07."""

import asyncio
import time
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    QUERY_TIMEOUT,
    TOPK,
    WAIT_SECONDS,
    assert_all_proxies_answered,
    assert_search_baseline,
    check_record,
    search,
)

pytestmark = pytest.mark.tags("L1")


def test_t07_async_sdk_cancel_contract(deployment, workload, workload_baseline, monkeypatch):
    """Validate async list/cancel objects and error mapping through the real SDK."""
    from pymilvus import AsyncMilvusClient
    from pymilvus.exceptions import MilvusException, ParamError

    uris, token, user, _ = deployment
    proxy_a, proxy_b = uris
    _, _, collection, vectors = workload
    baseline = workload_baseline()

    async def scenario():
        app = AsyncMilvusClient(uri=uris[proxy_a], token=token, timeout=10)
        try:
            operator = AsyncMilvusClient(uri=uris[proxy_b], token=token, timeout=10)
            try:
                await exercise(app, operator)
            finally:
                await operator.close()
        finally:
            await app.close()

    async def exercise(app, operator):
        operator_handler = await operator._get_connection()

        def unexpected_cancel_rpc(*args, **kwargs):
            pytest.fail("Async empty-ID validation issued a CancelRequests RPC")

        async def records():
            result = await operator.list_running_requests(
                collection_name=collection,
                user=user,
                timeout=10,
            )
            assert_all_proxies_answered(result, set(uris))
            assert result.complete
            return result.requests

        with monkeypatch.context() as patch:
            patch.setattr(operator_handler._async_stub, "CancelRequests", unexpected_cancel_rpc)
            with pytest.raises(ParamError) as invalid:
                await operator.cancel_requests([], timeout=10)
        assert invalid.value.code == 1

        app_handler = await app._get_connection()
        search_rpc = app_handler._async_stub.Search
        attempts = []

        def count_search(*args, **kwargs):
            attempts.append(1)
            return search_rpc(*args, **kwargs)

        monkeypatch.setattr(app_handler._async_stub, "Search", count_search)

        options = dict(anns_field="vector", limit=TOPK, search_params={"metric_type": "L2"})
        target = asyncio.create_task(app.search(collection, vectors, timeout=QUERY_TIMEOUT, **options))
        try:
            deadline = time.monotonic() + WAIT_SECONDS
            while time.monotonic() < deadline:
                snapshot = await records()
                if snapshot:
                    assert len(snapshot) == 1, snapshot
                    row = snapshot[0]
                    break
                if target.done():
                    await target
                    pytest.fail("Async Search finished before List captured it")
                await asyncio.sleep(0.02)
            else:
                pytest.fail("Async Search was not visible before the List deadline")
            check_record(row, proxy_a, user, collection)
            data = row.to_dict()
            assert data["request_id"] == row.request_id
            assert type(data["elapsed_ms"]) is int
            assert isinstance(data["task_ids"], list)
            assert all(type(task_id) is int for task_id in data["task_ids"])
            reason = f"T07-{uuid4().hex}"
            result = await operator.cancel_requests([row.request_id], reason=reason, timeout=10)
            assert_all_proxies_answered(result, set(uris))
            assert [item.request_id for item in result.canceled] == [row.request_id]
            assert not result.not_found and not result.undetermined
            with pytest.raises(MilvusException) as caught:
                await asyncio.wait_for(target, QUERY_TIMEOUT)
            assert caught.value.code == 3002 and reason in caught.value.message
            assert len(attempts) == 1, "Canceled async Search was retransmitted"
            assert all(item.request_id != row.request_id for item in await records())
            control = await app.search(collection, vectors, timeout=QUERY_TIMEOUT, **options)
            assert_search_baseline(control, baseline, len(vectors))
        finally:
            if not target.done():
                target.cancel()
                await asyncio.gather(target, return_exceptions=True)

    asyncio.run(scenario())


def test_t07_sync_sdk_contract(deployment, workload, workload_baseline, monkeypatch):
    """Verify local empty-ID rejection, populated objects, and canceled Search."""
    from concurrent.futures import ThreadPoolExecutor

    from pymilvus.exceptions import ParamError
    from running_requests_l1_support import assert_canceled, cancel_exact, list_complete, wait_for_records

    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    owner, control, collection, vectors = workload
    operator = connect(proxy_b)
    handler = operator._get_connection()

    def unexpected_rpc(*args, **kwargs):
        pytest.fail("Empty ID validation issued a CancelRequests RPC")

    with monkeypatch.context() as patch:
        patch.setattr(handler._stub, "CancelRequests", unexpected_rpc)
        with pytest.raises(ParamError) as invalid:
            operator.cancel_requests([], timeout=10)
        assert invalid.value.code == 1

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    search_rpc = owner._get_connection()._stub.Search
    attempts = []

    def count_search(*args, **kwargs):
        attempts.append(1)
        return search_rpc(*args, **kwargs)

    monkeypatch.setattr(owner._get_connection()._stub, "Search", count_search)
    nq = len(vectors)
    baseline = workload_baseline()
    reason = f"T07-sync-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, owner, collection, vectors)
        row = wait_for_records(records, {nq}, lambda item: item.nq, [future])[nq]
        check_record(row, proxy_a, user, collection)
        data = row.to_dict()
        assert data["request_id"] == row.request_id
        assert type(data["elapsed_ms"]) is int
        assert isinstance(data["task_ids"], list)
        assert all(type(task_id) is int for task_id in data["task_ids"])
        cancel_exact(operator, set(uris), [row.request_id], reason)
        assert_canceled(future, reason)
    assert len(attempts) == 1, attempts
    assert all(item.request_id != row.request_id for item in records())
    assert_search_baseline(search(control, collection, vectors), baseline, nq)


def test_t07_rest_empty_response_contract(deployment):
    """A complete response preserves every key for empty records and buckets."""
    from running_requests_l1_rest_support import post_rest

    uris, token, _, _ = deployment
    uri = list(uris.values())[1]
    missing = f"unknown_{uuid4().hex}"
    listed = post_rest(uri, token, set(uris), "list", {"collectionName": missing})
    assert listed["requests"] == [], listed
    canceled = post_rest(uri, token, set(uris), "cancel", {"requestIds": [missing]})
    assert canceled["notFound"] == [missing], canceled
    assert canceled["canceled"] == [], canceled
    assert canceled["undetermined"] == [], canceled


def test_t07_rest_int64_and_full_snapshot(deployment, workload):
    """Compare full REST snapshots with Python, including lossless int64 strings."""
    from concurrent.futures import ThreadPoolExecutor

    from running_requests_l1_rest_support import post_rest
    from running_requests_l1_support import assert_canceled, list_complete, wait_for_records

    uris, token, user, connect = deployment
    _, proxy_b = uris
    app, _, collection, vectors = workload
    operator = connect(proxy_b)

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    reason = f"T07-int64-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, app, collection, vectors)
        row = wait_for_records(records, {"Running"}, lambda item: item.state, [future], exact=True)["Running"]
        assert row.task_ids and any(value > 2**53 for value in row.task_ids)
        for allow in (False, True):
            listed = post_rest(
                uris[proxy_b],
                token,
                set(uris),
                "list",
                {
                    "dbName": "default",
                    "collectionName": collection,
                    "user": user,
                },
                headers={"Accept-Type-Allow-Int64": str(allow).lower()},
            )
            matches = [item for item in listed["requests"] if item["requestId"] == row.request_id]
            assert len(matches) == 1, "OBSERVATION_WINDOW: target exited before REST snapshot"
            item = matches[0]
            for name, expected in (
                ("proxyId", row.proxy_id),
                ("dbName", row.db_name),
                ("collectionName", row.collection_name),
                ("user", row.user),
                ("expr", row.expr),
                ("traceId", row.trace_id),
                ("startTimeMs", row.start_time_ms),
                ("nq", row.nq),
                ("topk", row.topk),
            ):
                assert item[name] == expected, (name, item)
            assert all(type(value) is (int if allow else str) for value in item["taskIds"])
            assert {int(value) for value in item["taskIds"]} == set(row.task_ids)
        canceled = post_rest(
            uris[proxy_b],
            token,
            set(uris),
            "cancel",
            {
                "requestIds": [row.request_id],
                "reason": reason,
            },
            headers={"Accept-Type-Allow-Int64": "false"},
        )
        assert [item["requestId"] for item in canceled["canceled"]] == [row.request_id]
        assert not canceled["notFound"] and not canceled["undetermined"]
        assert all(type(value) is str for value in canceled["canceled"][0]["taskIds"])
        assert_canceled(future, reason)


def test_t07_go_sdk_rejects_empty_ids(tmp_path):
    """Exercise empty-ID validation with the Go SDK revision used for manual testing."""
    import shutil
    import subprocess
    from pathlib import Path

    go = shutil.which("go")
    if go is None:
        pytest.skip("Go toolchain is required for the Go SDK contract")
    (tmp_path / "go.mod").write_text(
        "module running-requests-go-contract\n\ngo 1.27.3\n\n"
        "require github.com/milvus-io/milvus/client/v3 v3.0.0-20261112010801-61eef6cdbe40\n"
    )
    source = tmp_path / "manual_go_cancel_empty_ids.go"
    shutil.copyfile(Path(__file__).with_name("manual_go_cancel_empty_ids.go.in"), source)
    completed = subprocess.run(
        [go, "run", "-mod=mod", str(source)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, completed.stderr
    assert "code=1101" in completed.stdout, completed.stdout
