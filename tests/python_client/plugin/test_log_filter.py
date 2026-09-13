import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest
from _pytest.reports import TestReport

from plugin import log_filter


def phase_report(phase, outcome):
    longrepr = ("case.py", 1, "skip reason") if outcome == "skipped" else f"{phase} error"
    return TestReport(
        nodeid="case.py::test_case",
        location=("case.py", 1, "test_case"),
        keywords={},
        outcome=outcome,
        longrepr=longrepr if outcome != "passed" else None,
        when=phase,
        duration=0.1,
    )


@pytest.mark.parametrize("marked", [False, True])
@pytest.mark.parametrize(
    "outcomes,expected",
    [
        (["passed", "passed", "passed"], "passed"),
        (["passed", "passed", "failed"], "failed"),
        (["passed", "failed", "passed"], "failed"),
        (["passed", "failed", "failed"], "failed"),
        (["failed", None, "passed"], "failed"),
        (["skipped", None, "passed"], "skipped"),
    ],
)
def test_integrity_report_waits_for_teardown_without_changing_ordinary_cases(
    monkeypatch, tmp_path, marked, outcomes, expected
):
    handler = log_filter.ConditionalLogHandler(SimpleNamespace(log_path=str(tmp_path)))
    item = SimpleNamespace(nodeid="case.py::test_case", get_closest_marker=lambda name: marked or None)
    handler.start_test(item)
    # Restore the live handler before pytest reports this outer test's call phase,
    # including when an assertion inside the simulated hook sequence fails.
    with monkeypatch.context() as patch:
        patch.setattr(log_filter, "_conditional_handler", handler)
        for phase, outcome in zip(("setup", "call", "teardown"), outcomes):
            if outcome is None:
                continue
            record = logging.LogRecord("audit", logging.INFO, "case.py", 1, phase, (), None)
            record.persist_on_pass = True
            handler.emit(record)
            report = phase_report(phase, outcome)
            hook = log_filter.pytest_runtest_makereport(item, None)
            next(hook)
            with pytest.raises(StopIteration):
                hook.send(SimpleNamespace(get_result=lambda: report))
            if marked and phase != "teardown":
                assert item.nodeid in handler.buffers
                assert all(not handler.test_stats[key] for key in ("passed", "failed", "skipped"))

    if not marked:
        # Preserve the pre-existing call-only reporting for ordinary tests.
        expected = outcomes[1]
        if expected is None:
            assert not any(handler.test_stats[key] for key in ("passed", "failed", "skipped"))
            return
    result = handler.test_stats[expected][0]
    assert sum(len(handler.test_stats[key]) for key in ("passed", "failed", "skipped")) == 1
    if marked:
        assert [phase["outcome"] for phase in result["phases"]] == [value for value in outcomes if value is not None]
        if expected != "skipped":
            assert result["logs"]["info"][-1]["message"] == "teardown"
        if outcomes[2] == "failed":
            assert "teardown error" in result["error"]["traceback"]
    else:
        assert "phases" not in result
        if expected == "passed":
            assert "logs" not in result
        else:
            assert result["logs"]["info"][-1]["message"] == "call"
    assert item.nodeid not in handler.buffers
    handler.generate_report()
    persisted = json.loads(Path(handler.report_json).read_text())["tests"][expected][0]
    if marked:
        assert persisted["phases"] == result["phases"]
        if expected != "skipped":
            assert persisted["logs"]["info"][-1]["message"] == "teardown"
        if expected == "failed":
            assert persisted["error"]["traceback"] == result["error"]["traceback"]
    else:
        assert "phases" not in persisted
