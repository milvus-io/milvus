import json
import time

import pytest
from chaos import chaos_commons as cc
from chaos import observability


class _CheckerState:
    def __init__(self, collection, success=0, failure=0):
        self.c_name = collection
        self._succ = success
        self._fail = failure
        self.average_time = 0.75
        self.current_operation = "search"
        self.current_operation_started_at = 10
        self.last_operation = "insert"
        self.last_operation_result = "success"
        self.last_operation_elapsed = 1.25
        self.last_operation_completed_at = 18
        self.last_success_at = 18
        self.last_failure_at = 12
        self.error_messages = {"one error"}


def _payload(message):
    return json.loads(message.removeprefix(f"{observability.LOG_PREFIX} "))


def test_format_event_includes_correlation_fields(monkeypatch):
    monkeypatch.setenv("CHAOS_CHECKER_RUN_ID", "run-123")
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw2")

    payload = _payload(observability.format_event("checker_test", collection="c1"))

    assert payload == {
        "collection": "c1",
        "event": "checker_test",
        "run_id": "run-123",
        "worker": "gw2",
    }


def test_checker_snapshot_reports_in_flight_operation_age():
    checker = _CheckerState("c1", success=7, failure=2)

    snapshot = observability.checker_snapshot("search", checker, now_monotonic=15.5, now_wall_time=20)

    assert snapshot["success"] == 7
    assert snapshot["failure"] == 2
    assert snapshot["success_rate"] == 0.7778
    assert snapshot["average_elapsed_seconds"] == 0.75
    assert snapshot["in_flight"] == "search"
    assert snapshot["in_flight_seconds"] == 5.5
    assert snapshot["error_types"] == 1
    assert snapshot["last_operation_completed_seconds_ago"] == 2
    assert snapshot["last_success_seconds_ago"] == 2
    assert snapshot["last_failure_seconds_ago"] == 8


def test_monitor_heartbeat_emits_one_aggregate_event(monkeypatch):
    messages = []
    monkeypatch.setattr(cc.log, "info", messages.append)
    checkers = {
        "insert": _CheckerState("c1", success=3, failure=1),
        "search": _CheckerState("c2", success=5, failure=2),
    }
    for checker in checkers.values():
        checker.current_operation_started_at = time.monotonic() - 5

    snapshots = cc.log_monitor_heartbeat(checkers, phase="workload_1_of_10")

    assert len(snapshots) == 2
    assert len(messages) == 1
    payload = _payload(messages[0])
    assert payload["event"] == "monitor_heartbeat"
    assert payload["phase"] == "workload_1_of_10"
    assert payload["total_success"] == 8
    assert payload["total_failure"] == 3
    assert payload["in_flight_count"] == 2


def test_monitor_heartbeat_fails_fast_for_stalled_operation(monkeypatch):
    messages = []
    monkeypatch.setattr(cc.log, "info", messages.append)
    monkeypatch.setattr(cc.log, "error", messages.append)
    checker = _CheckerState("c1")
    checker.current_operation_started_at = time.monotonic() - 181

    with pytest.raises(AssertionError, match="search=181"):
        cc.log_monitor_heartbeat({"search": checker})

    assert any('"event":"monitor_stalled"' in message for message in messages)
