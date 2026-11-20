"""T08 cross-user Cancel must still respect the explicitly selected owner."""

import pytest
from test_running_requests_t08_rbac import check_authorized_cancel_with_wrong_user
from test_running_requests_t13_extended_management import ManagementOperator

pytestmark = pytest.mark.tags("L1")


def test_t08_transport_cancel_with_wrong_user(deployment, workload, workload_baseline, t13_operator):
    def connect_operator(uri, token):
        worker = t13_operator.worker
        client = "cancel-privilege"
        if worker is not None:
            worker.ok("connect", client, URI=uri, Token=token, DB="default")
        return ManagementOperator(uri, token, t13_operator.nodes, worker, client=client)

    check_authorized_cancel_with_wrong_user(
        deployment,
        workload,
        workload_baseline,
        connect_operator=connect_operator,
    )
