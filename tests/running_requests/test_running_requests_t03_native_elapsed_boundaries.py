"""Verify the live List filter at each native SDK's elapsed-time boundaries."""

from uuid import uuid4

import pytest
from test_running_requests_native_sdk import canceled, capture, listed, start, terminal

pytestmark = pytest.mark.tags("L1")
INT64_MAX = 2**63 - 1
GO_DURATION_MAX_MS = INT64_MAX // 1_000_000


@pytest.mark.parametrize("boundary", ["negative", "large-safe", "sdk-max"])
def test_t03_native_min_elapsed_boundary(boundary, native, data):
    sdk_max = GO_DURATION_MAX_MS if native.language == "go" else INT64_MAX
    milliseconds = {"negative": -1, "large-safe": 10**9, "sdk-max": sdk_max}[boundary]
    call, expr = start(native, data)
    request_id = capture(native, data, call, expr)["request_id"]
    filters = {"Collection": data["Collection"], "User": native.user}

    def request_ids(**extra):
        return {row["request_id"] for row in listed(native, **filters, **extra)}

    matching = request_ids(MinMS=milliseconds)
    # The unfiltered observation rules out a passing negative assertion caused
    # by the request finishing before the filtered List was issued.
    assert request_id in request_ids(), "OBSERVATION_WINDOW: target completed during the filter check"
    reason = "T03-native-elapsed-" + uuid4().hex
    canceled(native, [request_id], reason)
    terminal(native, call, reason)
    assert request_id not in request_ids()
    assert (request_id in matching) == (milliseconds < 0), (native.language, milliseconds, matching)
