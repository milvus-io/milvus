"""Run the T10/T12 checkpoint scenarios through Java, Go and REST management APIs."""

import os

import pytest
from test_running_requests_t10_merge import check_cancel_only_selected_group_member
from test_running_requests_t12_internal_tasks import (
    check_cancel_across_internal_stages,
    check_cancel_hybrid_with_trace,
)

pytestmark = pytest.mark.tags("L1")


@pytest.mark.parametrize(("member", "phase"), [("leader", "queued"), ("member", "executing")])
def test_t10_transport_cancel_only_selected_group_member(
    member,
    phase,
    checkpoint_provider,
    deployment,
    workload,
    workload_baseline,
    t13_operator,
):
    check_cancel_only_selected_group_member(
        member,
        phase,
        checkpoint_provider,
        deployment,
        workload,
        workload_baseline,
        operator=t13_operator,
    )


@pytest.mark.parametrize("path", ["requery", "search_by_id", "retry", "hybrid"])
def test_t12_transport_cancel_across_internal_stages(
    path,
    checkpoint_provider,
    deployment,
    workload,
    workload_baseline,
    request,
    t13_operator,
):
    check_cancel_across_internal_stages(
        path,
        checkpoint_provider,
        deployment,
        workload,
        workload_baseline,
        request,
        operator=t13_operator,
    )


@pytest.mark.skipif(
    os.getenv("MILVUS_T12_HYBRID_TRACE") != "1",
    reason="Enable QueryNode DEBUG logs and fully sampled stdout trace for T12 HybridSearch",
)
def test_t12_transport_cancel_hybrid_with_trace(deployment, request, t13_operator):
    check_cancel_hybrid_with_trace(deployment, request, operator=t13_operator)
