"""TC-L1 functional scenario T05."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from running_requests_l1_support import (
    QUERY_TIMEOUT,
    assert_search_baseline,
    assert_unregistered,
    list_complete,
    run_batch,
    search,
    wait_for_records,
)

pytestmark = pytest.mark.tags("L1")


def test_t05_batch_cancel_result_buckets(deployment, workload, workload_baseline):
    """Cancel two active IDs and classify a completed ID and an unknown ID."""
    uris, _, user, connect = deployment
    _, proxy_b = uris
    _, client, collection, vectors = workload
    operator = connect(proxy_b)
    nq = len(vectors)
    baseline = workload_baseline()

    def records():
        return list_complete(operator, set(uris), collection_name=collection, user=user)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(search, client, collection, vectors)
        row = wait_for_records(
            records,
            {nq},
            lambda item: item.nq,
            [future],
            timeout=QUERY_TIMEOUT,
        )[nq]
        assert_search_baseline(future.result(timeout=QUERY_TIMEOUT), baseline, nq)
    assert_unregistered(records, {row.request_id})
    run_batch(
        "T05",
        deployment,
        workload,
        control_proxies=(),
        expected_missing=[row.request_id, f"unknown_{uuid4().hex}"],
        baseline=baseline,
        snapshot_timeout=QUERY_TIMEOUT,
    )
