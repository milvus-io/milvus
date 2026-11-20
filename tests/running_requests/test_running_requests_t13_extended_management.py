"""Exercise T13 specialized workloads with three additional management transports."""

from types import SimpleNamespace

import pytest
from running_requests_l1_rest_support import post_rest
from test_running_requests_external import check_external_cancel
from test_running_requests_native_sdk import Native, complete
from test_running_requests_schema_regression import check_schema_cancel
from test_running_requests_t13_extended import check_iterator_cancel, check_struct_cancel

pytestmark = pytest.mark.tags("L1")


class ManagementOperator:
    """Project transport-specific management records into shared assertion fields."""

    def __init__(self, uri, token, nodes, worker, *, client="b"):
        self.uri = uri
        self.token = token
        self.nodes = nodes
        self.worker = worker
        self.client = client

    def call(self, action, **payload):
        listing = action == "list"
        if self.worker is None:
            result = post_rest(
                self.uri, self.token, self.nodes, action, payload, headers={"Accept-Type-Allow-Int64": "true"}
            )
            raw_nodes = result["nodeResults"]
            nodes = [
                SimpleNamespace(node_id=node["nodeId"], answered=node["code"] == 0, unimplemented=node["unimplemented"])
                for node in raw_nodes
            ]
            fields = {
                "request_id": "requestId",
                "proxy_id": "proxyId",
                "collection_name": "collectionName",
                "trace_id": "traceId",
                "db_name": "dbName",
                "client_addr": "clientAddr",
                "queued_ms": "queuedMs",
                "task_ids": "taskIds",
            }
            missing = result.get("notFound", [])
        else:
            keys = {"collectionName": "Collection", "user": "User", "reason": "Reason", "requestIds": "IDs"}
            result = complete(
                self.worker,
                self.worker.call(
                    action,
                    self.client,
                    **{keys[key]: value for key, value in payload.items()},
                ),
            )
            nodes = [SimpleNamespace(**node) for node in result["nodes"]]
            fields = {
                field: field
                for field in (
                    "request_id",
                    "proxy_id",
                    "collection_name",
                    "trace_id",
                    "db_name",
                    "client_addr",
                    "queued_ms",
                    "task_ids",
                )
            }
            missing = result.get("not_found") or []
        fields.update({field: field for field in ("type", "user", "expr", "state", "nq", "cancellable")})
        rows = [
            SimpleNamespace(**{key: row[value] for key, value in fields.items()})
            for row in result["requests" if listing else "canceled"]
        ]
        if listing:
            return SimpleNamespace(node_results=nodes, complete=True, requests=rows)
        return SimpleNamespace(
            node_results=nodes, canceled=rows, not_found=missing, undetermined=result.get("undetermined") or []
        )

    def list_running_requests(self, *, collection_name, user, timeout):
        return self.call("list", collectionName=collection_name, user=user)

    def cancel_requests(self, request_ids, *, timeout, reason="", user=None):
        payload = {"requestIds": request_ids, "reason": reason}
        if user is not None:
            payload["user"] = user
        return self.call("cancel", **payload)


@pytest.fixture(params=["java", "go", "rest"])
def t13_operator(request, deployment, tmp_path):
    uris, token, _, _ = deployment
    uri = uris[next(reversed(uris))]
    worker = None
    if request.param != "rest":
        worker = Native(request.param, tmp_path / "t13-native.stderr")
        request.addfinalizer(worker.close)
        worker.ok("connect", "b", URI=uri, Token=token, DB="default")
        worker.nodes = set(uris)
    return ManagementOperator(uri, token, set(uris), worker)


@pytest.mark.parametrize("kind", ["search", "query"])
def test_t13_manage_iterator(kind, deployment, workload, monkeypatch, t13_operator):
    check_iterator_cancel(kind, deployment, workload, monkeypatch, operator=t13_operator)


@pytest.mark.parametrize("kind", ["search", "query"])
def test_t13_manage_struct_array(kind, deployment, struct_data, t13_operator):
    check_struct_cancel(kind, deployment, struct_data, operator=t13_operator)


@pytest.mark.parametrize("kind", ["Search", "Query"])
def test_t13_manage_external_collection(kind, deployment, external_data, t13_operator):
    check_external_cancel(kind, deployment, external_data, operator=t13_operator)


@pytest.mark.parametrize("variant", ["mmap", "nullable", "add_field"])
def test_t13_manage_schema_regression(variant, deployment, request, t13_operator):
    check_schema_cancel(variant, deployment, request, operator=t13_operator)
