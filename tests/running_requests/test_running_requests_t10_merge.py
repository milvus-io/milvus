"""T10. Cancel one member of a verified QueryNode search group."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    QUERY_TIMEOUT,
    assert_canceled,
    assert_search_baseline,
    assert_unregistered,
    cancel_exact,
    list_complete,
    search,
    wait_for_records,
)

pytestmark = pytest.mark.tags("L1")


@pytest.mark.parametrize(("member", "phase"), [("leader", "queued"), ("member", "executing")])
def test_t10_cancel_only_selected_group_member(
    member, phase, checkpoint_provider, deployment, workload, workload_baseline
):
    check_cancel_only_selected_group_member(member, phase, checkpoint_provider, deployment, workload, workload_baseline)


def check_cancel_only_selected_group_member(
    member, phase, checkpoint_provider, deployment, workload, workload_baseline, *, operator=None
):
    uris, _, user, connect = deployment
    proxy_a, proxy_b = uris
    first, second, collection, vectors = workload
    vectors = vectors[:2]
    assert len(vectors) == 2
    operator = connect(proxy_b) if operator is None else operator
    baseline = workload_baseline(1)

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    reason = f"T10-{uuid4().hex}"
    with ThreadPoolExecutor(max_workers=2) as pool:
        with checkpoint_provider.hold_search_group(collection=collection, phase=phase) as checkpoint:
            futures = {
                "first": pool.submit(search, first, collection, vectors[:1]),
                "second": pool.submit(search, second, collection, vectors[:1]),
            }
            group = checkpoint.wait(timeout=30)
            assert group.phase == phase
            assert group.group_id
            assert set(group.request_ids) == set(futures)
            ids = set(group.request_ids.values())
            assert len(ids) == 2 and group.leader_request_id in ids
            rows = wait_for_records(records, ids, lambda row: row.request_id, futures.values())
            assert all(row.proxy_id == proxy_a and row.type == "Search" for row in rows.values())
            target_id = (
                group.leader_request_id
                if member == "leader"
                else next(request_id for request_id in ids if request_id != group.leader_request_id)
            )
            cancel_exact(operator, set(uris), [target_id], reason)
            if phase == "queued":
                checkpoint.assert_member_cancel_arrived(group.group_id, target_id, timeout=30)
        for label, future in futures.items():
            if group.request_ids[label] == target_id:
                assert_canceled(future, reason)
            else:
                assert_search_baseline(future.result(timeout=QUERY_TIMEOUT), baseline, 1)
    checkpoint.assert_group_drained(group.group_id, timeout=30)
    if phase == "queued":
        checkpoint.assert_member_pruned_on_dequeue(group.group_id, target_id, timeout=30)
    assert_unregistered(records, ids)
