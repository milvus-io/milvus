"""Cross-transport peer-address and Prometheus counter contracts."""

import pytest
from running_requests_l1_rest_support import post_rest_response
from test_running_requests_client_addr import check_client_addr_matches_proxy_socket
from test_running_requests_metric_branches import (
    check_batch_and_rejected_counter_branches,
    check_cancel_type_counter,
)

pytestmark = pytest.mark.tags("L1")


def test_t04_transport_client_addr(deployment, workload, t13_operator):
    check_client_addr_matches_proxy_socket(deployment, workload, operator=t13_operator)


@pytest.mark.parametrize("kind", ["HybridSearch", "Query"])
def test_t05_transport_cancel_type_counter(kind, deployment, workload, t13_operator):
    check_cancel_type_counter(kind, deployment, workload, operator=t13_operator)


def test_t05_transport_batch_and_rejected_counters(
    deployment,
    workload,
    create_rbac_identity,
    t13_operator,
):
    worker = t13_operator.worker

    def denied_cancel(ids, user, credential):
        if worker is None:
            result = post_rest_response(
                t13_operator.uri,
                credential,
                "cancel",
                {
                    "requestIds": ids,
                    "user": user,
                },
            )
        else:
            worker.ok("connect", "counter-restricted", URI=t13_operator.uri, Token=credential, DB="default")
            result = worker.call("cancel", "counter-restricted", IDs=ids, User=user)
            assert not result["ok"], result
        assert result["code"] == 1401, result

    check_batch_and_rejected_counter_branches(
        deployment,
        workload,
        create_rbac_identity,
        operator=t13_operator,
        denied_cancel=denied_cancel,
    )
