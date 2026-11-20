"""T14 long execution evidence through Java, Go and REST List/Cancel."""

import pytest
from test_running_requests_t14_long_execution import (
    check_cancel_long_search,
    check_compute_cancellation_log_evidence,
)

pytestmark = pytest.mark.tags("L1")


def test_t14_transport_compute_log(deployment, workload, t13_operator):
    check_compute_cancellation_log_evidence(deployment, workload, operator=t13_operator)


@pytest.mark.parametrize("stage", ["waiting", "computing"])
def test_t14_transport_long_search(stage, deployment, stage_probe, t13_operator, request):
    check_cancel_long_search(stage, deployment, stage_probe, request, operator=t13_operator)
