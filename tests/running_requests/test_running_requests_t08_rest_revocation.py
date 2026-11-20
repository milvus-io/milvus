"""T08. REST List survives revocation of cross-user Cancel."""

from types import SimpleNamespace

import pytest
from pymilvus.exceptions import MilvusException
from running_requests_l1_rest_support import checked_management_response, post_rest_response
from test_running_requests_t08_rbac import check_revoking_cancel_preserves_list
from test_running_requests_t13_extended_management import ManagementOperator

pytestmark = pytest.mark.tags("L1")


class RestrictedRestOperator(ManagementOperator):
    def cancel_requests(self, request_ids, *, user, timeout):
        body = post_rest_response(
            self.uri,
            self.token,
            "cancel",
            {
                "requestIds": request_ids,
                "user": user,
            },
        )
        if body["code"] == 1401:
            assert "PrivilegeCancelRequests" in body["message"], body
            raise MilvusException(code=body["code"], message=body["message"])
        data = checked_management_response(body, self.nodes, "cancel")
        # checked_management_response verifies complete fan-out before returning.
        return SimpleNamespace(
            node_results=[
                SimpleNamespace(node_id=node["nodeId"], answered=node["code"] == 0, unimplemented=node["unimplemented"])
                for node in data["nodeResults"]
            ],
            not_found=data["notFound"],
            canceled=data["canceled"],
            undetermined=data["undetermined"],
        )


def test_t08_rest_revoking_cancel_preserves_list(deployment, workload):
    def connect_operator(uri, token, nodes):
        return RestrictedRestOperator(uri, token, nodes, worker=None)

    check_revoking_cancel_preserves_list(deployment, workload, connect_operator=connect_operator)
