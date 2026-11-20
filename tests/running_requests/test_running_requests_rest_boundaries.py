"""REST boundary behavior on live requests; no invented maximum batch size."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from running_requests_l1_rest_support import post_rest, post_rest_response
from running_requests_l1_support import (
    assert_canceled,
    list_complete,
    search,
    wait_for_records,
)

pytestmark = pytest.mark.tags("L1")


def test_rest_empty_id_and_large_batch(deployment):
    uris, token, _, _ = deployment
    uri = list(uris.values())[1]
    # These are sampled supported sizes, not a claim of an API maximum.
    for ids in ([""], [f"missing_{uuid4().hex}_{i}" for i in range(1024)]):
        result = post_rest(uri, token, set(uris), "cancel", {"requestIds": ids})
        assert sorted(result["notFound"]) == sorted(ids)
        assert result["canceled"] == result["undetermined"] == []


@pytest.mark.parametrize(
    "milliseconds", [-1, 10**9, 2**63 - 1, 2**63], ids=["negative", "large-safe", "int64-max", "int64-overflow"]
)
def test_rest_elapsed_boundaries(deployment, workload, milliseconds):
    uris, token, user, connect = deployment
    a, b = uris
    owner, _, collection, vectors = workload
    operator = connect(b)

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, owner, collection, vectors)
        row = wait_for_records(records, {"Running"}, lambda r: r.state, [future], exact=True)["Running"]
        reason = f"boundary-{uuid4().hex}"
        try:
            body = post_rest_response(
                uris[b],
                token,
                "list",
                {
                    "collectionName": collection,
                    "user": user,
                    "minElapsedMs": milliseconds,
                },
            )
            if milliseconds == 2**63:
                assert body["code"] == 1801, body
            else:
                assert body["code"] == 0, body
                ids = [r["requestId"] for r in body["data"]["requests"]]
                if milliseconds < 0:
                    assert row.request_id in ids
                else:
                    assert row.request_id not in ids, "Positive threshold exceeds request age but matched"
            assert not future.done(), "OBSERVATION_WINDOW: target already completed"
        finally:
            operator.cancel_requests([row.request_id], reason=reason, timeout=10)
            assert_canceled(future, reason)


def test_rest_long_reason_round_trip(deployment, workload):
    uris, token, user, connect = deployment
    a, b = uris
    owner, _, collection, vectors = workload
    operator = connect(b)
    reason = "reason-start-" + "取消原因" * 1024 + "-reason-end"
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, owner, collection, vectors)
        row = wait_for_records(
            lambda: list_complete(operator, set(uris), collection_name=collection, user=user),
            {"Running"},
            lambda r: r.state,
            [future],
            exact=True,
        )["Running"]
        result = post_rest(uris[b], token, set(uris), "cancel", {"requestIds": [row.request_id], "reason": reason})
        assert [r["requestId"] for r in result["canceled"]] == [row.request_id]
        assert result["notFound"] == result["undetermined"] == []
        assert_canceled(future, reason)
