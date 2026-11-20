"""Milvus running request REST contracts from TC-L1-05 and TC-L1-06."""

import json
import os
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

import pytest

pytestmark = pytest.mark.tags("L1")


@pytest.fixture
def post():
    uri = os.environ.get("MILVUS_URI") or os.environ.get("PROXY_B_URI")
    if not uri:
        pytest.skip("Set MILVUS_URI to a Milvus test deployment with running requests enabled")
    token = os.environ.get("MILVUS_TOKEN")

    def send(action, payload):
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = Request(
            f"{uri.rstrip('/')}/v2/vectordb/requests/running/{action}",
            data=json.dumps(payload).encode(),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=15) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            with error:
                try:
                    return error.code, json.load(error)
                except (ValueError, UnicodeError):
                    pytest.fail(f"{action} returned HTTP {error.code} without a JSON response")

    return send


def assert_success(result, fields):
    status, body = result
    assert status == 200, result
    assert body["code"] == 0, result
    data = body["data"]
    assert fields <= data.keys(), result
    for field in fields:
        assert isinstance(data[field], list), result
    return data


@pytest.mark.parametrize(
    ("payload", "expected_code"),
    [
        pytest.param({}, 1802, id="missing-ids"),
        pytest.param({"requestIds": None}, 1802, id="null-ids"),
        pytest.param({"requestIds": []}, 1101, id="empty-ids"),
        pytest.param({"requestIds": "id"}, 1801, id="non-array-ids"),
        pytest.param({"requestIds": [123]}, 1801, id="numeric-id"),
        pytest.param({"requestIds": ["unknown_t06"], "reason": 123}, 1801, id="numeric-reason"),
        pytest.param({"requestIds": ["unknown_t06"], "user": {}}, 1801, id="object-user"),
    ],
)
def test_cancel_parameter_validation(post, payload, expected_code):
    """T06: Check business codes for missing and malformed cancellation input."""
    status, body = post("cancel", payload)
    assert 200 <= status < 500, (status, body)
    assert body["code"] == expected_code, (status, body)


@pytest.mark.parametrize(
    "request_id",
    [
        pytest.param("unknown_" + uuid4().hex, id="unknown-string"),
        pytest.param(str(uuid4().int), id="numeric-string"),
    ],
)
def test_cancel_unknown_id(post, request_id):
    """T06/T07: Verify the exact string is returned in the notFound bucket."""
    data = assert_success(
        post("cancel", {"requestIds": [request_id]}),
        {"canceled", "notFound", "undetermined", "nodeResults"},
    )
    assert data["notFound"] == [request_id], data
    assert data["canceled"] == [], data
    assert data["undetermined"] == [], data


def test_list_empty_result_contract(post):
    """T07: Verify an unmatched collection retains both response keys."""
    data = assert_success(
        post("list", {"collectionName": f"missing_{uuid4().hex}"}),
        {"requests", "nodeResults"},
    )
    assert data["requests"] == [], data
