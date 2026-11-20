"""HTTP transport and complete-node assertions for REST L1 cases."""

import json
from urllib.request import Request, urlopen


def post_rest_response(uri, token, action, payload, *, headers=None):
    request = Request(
        f"{uri}/v2/vectordb/requests/running/{action}",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            **({"Authorization": f"Bearer {token}"} if token is not None else {}),
            **(headers or {}),
        },
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        assert response.status == 200
        return json.load(response)


def post_rest(uri, token, proxy_ids, action, payload, *, headers=None):
    body = post_rest_response(uri, token, action, payload, headers=headers)
    return checked_management_response(body, proxy_ids, action)


def checked_management_response(body, proxy_ids, action):
    assert body["code"] == 0, body
    data = body["data"]
    nodes = data["nodeResults"]
    assert len(nodes) == len(proxy_ids), nodes
    assert {node["nodeId"] for node in nodes} == proxy_ids, nodes
    for node in nodes:
        assert {"nodeId", "code", "message", "unimplemented"} <= node.keys(), node
        assert node["unimplemented"] is False, node
        assert node["code"] == 0 and node["message"] == "", node
    fields = (
        ("requests", "nodeResults") if action == "list" else ("canceled", "notFound", "undetermined", "nodeResults")
    )
    for field in fields:
        assert isinstance(data[field], list), (field, data)
    for row in data["requests" if action == "list" else "canceled"]:
        expected = {
            "requestId",
            "proxyId",
            "type",
            "dbName",
            "collectionName",
            "user",
            "clientAddr",
            "nq",
            "topk",
            "expr",
            "startTimeMs",
            "queuedMs",
            "elapsedMs",
            "state",
            "taskIds",
            "traceId",
            "cancellable",
        }
        assert expected <= row.keys(), (expected - row.keys(), row)
        assert isinstance(row["requestId"], str) and row["requestId"]
        assert isinstance(row["taskIds"], list)
    return data
