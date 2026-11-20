"""TC-L1 functional scenario T04."""

import os
import re
from concurrent.futures import ThreadPoolExecutor
from time import monotonic, sleep, time
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    TOPK,
    assert_all_proxies_answered,
    assert_canceled,
    assert_unregistered,
    cancel_exact,
    check_record,
    list_complete,
    search,
    wait_for_records,
)

pytestmark = pytest.mark.tags("L1")


def test_t04_running_snapshot_and_cleanup(deployment, workload):
    """Check two snapshots of a Running request, cancellation snapshot and cleanup."""
    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    client, _, collection, vectors = workload
    operator = connect(proxy_b)

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    with ThreadPoolExecutor(max_workers=1) as pool:
        submitted_ms = time() * 1000
        future = pool.submit(search, client, collection, vectors)
        sample_interval = None

        def timed_records():
            nonlocal sample_interval
            start = monotonic()
            rows = records()
            sample_interval = (start, monotonic())
            return rows

        first = wait_for_records(
            timed_records,
            {"Running"},
            lambda row: row.state,
            [future],
        )["Running"]
        check_record(first, proxy_a, user, collection)
        assert first.nq == len(vectors) and first.topk == TOPK
        assert first.client_addr
        assert first.expr == ""
        assert re.fullmatch(r"[0-9a-f]{32}", first.trace_id)
        assert first.task_ids and all(type(task_id) is int and task_id > 0 for task_id in first.task_ids)
        assert first.start_time_ms > 0 and first.elapsed_ms >= 0 and first.queued_ms >= 0
        # A bounded client/server clock skew is a precondition, not a precise
        # measurement of server registration or queue duration.
        skew_ms = float(os.getenv("MILVUS_CLOCK_SKEW_MAX_MS", "5000"))
        assert 0 <= skew_ms <= 60000
        assert submitted_ms - skew_ms <= first.start_time_ms <= time() * 1000 + skew_ms
        assert abs(time() * 1000 - first.start_time_ms - first.elapsed_ms) <= (
            skew_ms + (sample_interval[1] - sample_interval[0]) * 1000 + 100
        )
        first_start, first_end = sample_interval
        sleep(0.2)
        later = wait_for_records(
            timed_records,
            {first.request_id},
            lambda row: row.request_id if row.elapsed_ms > first.elapsed_ms else None,
            [future],
        )[first.request_id]
        assert later.start_time_ms == first.start_time_ms
        assert (later.client_addr, later.expr, later.trace_id) == (
            first.client_addr,
            first.expr,
            first.trace_id,
        )
        assert set(first.task_ids) <= set(later.task_ids)
        later_start, later_end = sample_interval
        delta_ms = later.elapsed_ms - first.elapsed_ms
        assert (later_start - first_end) * 1000 - 10 <= delta_ms <= (later_end - first_start) * 1000 + 10
        assert later.queued_ms == first.queued_ms
        reason = f"T04-{uuid4().hex}"
        result = operator.cancel_requests([first.request_id], reason=reason, timeout=10)
        assert_all_proxies_answered(result, set(uris))
        assert [row.request_id for row in result.canceled] == [first.request_id]
        assert result.canceled[0].state == "Running"
        assert not result.not_found and not result.undetermined
        assert_canceled(future, reason)
    assert_unregistered(records, {first.request_id})
    cancel_exact(operator, set(uris), [], reason, not_found=[first.request_id])


@pytest.mark.parametrize("state", ["Queued", "Running"])
def test_t04_cancel_and_repeat_while_exit_is_held(state, checkpoint_provider, deployment, workload):
    check_cancel_and_repeat_while_exit_is_held(state, checkpoint_provider, deployment, workload)


def check_cancel_and_repeat_while_exit_is_held(
    state,
    checkpoint_provider,
    deployment,
    workload,
    *,
    operator=None,
):
    """Observe the original state, then Canceling before the request is released."""
    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    app, _, collection, vectors = workload
    operator = connect(proxy_b) if operator is None else operator

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    reason = f"T04-{state}-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=1) as pool:
        with checkpoint_provider.hold_request_exit(collection=collection, state=state) as checkpoint:
            if state == "Queued":
                future = pool.submit(app.query, collection, filter="id == 0", timeout=90)
            else:
                future = pool.submit(search, app, collection, vectors)
            evidence = checkpoint.wait(timeout=30)
            request_id = evidence.request_id
            assert evidence.state == state
            snapshot = records()
            assert [row.request_id for row in snapshot] == [request_id]
            row = snapshot[0]
            check_record(row, proxy_a, user, collection)
            assert row.state == state
            if state == "Queued":
                assert row.queued_ms == 0 and not row.task_ids
            for expected in (state, "Canceling"):
                result = operator.cancel_requests([request_id], reason=reason, timeout=10)
                assert_all_proxies_answered(result, set(uris))
                assert not result.not_found and not result.undetermined
                assert [(item.request_id, item.state) for item in result.canceled] == [(request_id, expected)]
                assert [(item.request_id, item.state) for item in records()] == [(request_id, "Canceling")]
        assert_canceled(future, reason)
    assert_unregistered(records, {request_id})
    cancel_exact(operator, set(uris), [], reason, not_found=[request_id])
