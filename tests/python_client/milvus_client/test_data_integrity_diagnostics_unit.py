import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from common.data_integrity_diagnostics import emit_data_integrity_event, exception_diagnostics

pytest_plugins = ["pytester"]


@pytest.mark.tags("CompactionIntegrityUnit")
def test_data_integrity_diagnostics_unit_default_entrypoint_emits_without_log_overrides(tmp_path):
    test_file = tmp_path / "test_entry_diagnostic_probe.py"
    test_file.write_text(
        "import pytest\n"
        "from common.data_integrity_diagnostics import emit_data_integrity_event\n"
        "@pytest.mark.tags('L3')\n"
        "@pytest.mark.compaction_data_integrity_serial\n"
        "def test_entry_diagnostic_probe():\n"
        "    emit_data_integrity_event({'event': 'continuous_heartbeat', 'lane': 'import'})\n"
        "    raise ValueError('safe entrypoint failure')\n"
    )
    tests_dir = Path(__file__).resolve().parents[2]
    report_dir = tmp_path / "report"
    env = dict(
        os.environ,
        PATH=f"{Path(sys.executable).parent}:{os.environ['PATH']}",
        CI_LOG_PATH=str(report_dir),
    )
    env.pop("DATA_INTEGRITY_DIAGNOSTICS_PATH", None)
    env.pop("PYTEST_ADDOPTS", None)
    env.pop("PYTHONPATH", None)
    result = subprocess.run(
        [
            "bash",
            str(tests_dir / "scripts/ci_compaction_integrity.sh"),
            # Load the real repository conftest before the external probe path.
            "milvus_client/test_milvus_client_continuous_data_integrity.py",
            "-c",
            str(tests_dir / "python_client/pytest.ini"),
            str(test_file),
            "-k",
            "test_entry_diagnostic_probe",
        ],
        env=env,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert '"event":"continuous_heartbeat"' in result.stdout
    events = [json.loads(line) for line in (report_dir / "case-events.jsonl").read_text().splitlines()]
    assert any(event["event"] == "continuous_heartbeat" for event in events)
    assert any(event.get("phase") == "call" and event.get("outcome") == "failed" for event in events)
    report = json.loads((report_dir / "test_report.json").read_text())
    assert "continuous_heartbeat" in json.dumps(report)


@pytest.mark.tags("CompactionIntegrityUnit")
def test_data_integrity_diagnostics_unit_persists_stack_without_payloads(tmp_path, monkeypatch):
    event_path = tmp_path / "events.jsonl"
    monkeypatch.setenv("DATA_INTEGRITY_DIAGNOSTICS_PATH", str(event_path))

    def append_row(depth):
        if depth:
            append_row(depth - 1)
        raise ValueError("password=credential-sentinel-value row-payload-sentinel")

    try:
        append_row(20)
    except ValueError as error:
        emit_data_integrity_event(
            {"event": "continuous_primary_failure", **exception_diagnostics(error)}, primary_failure=True
        )
    serialized = event_path.read_text()
    event = json.loads(serialized)
    assert event["error_type"] == "ValueError"
    assert event["error_reason"] == "invalid value"
    assert len(event["error_stack"]) == 16
    assert event["error_stack_truncated"] is True
    assert event["error_stack"][-1]["function"] == "append_row"
    assert event["error_stack"][-1]["line"] > 0
    assert "credential-sentinel-value" not in serialized
    assert "row-payload-sentinel" not in serialized
    assert all(set(frame) == {"file", "function", "line"} for frame in event["error_stack"])


def _run_inner(pytester, monkeypatch, source, *args):
    event_path = pytester.path / "case-events.jsonl"
    monkeypatch.setenv("DATA_INTEGRITY_DIAGNOSTICS_PATH", str(event_path))
    pytester.makepyfile(source)
    result = pytester.runpytest_inprocess(
        "-p",
        "common.data_integrity_diagnostics",
        "--junitxml=inner-junit.xml",
        *args,
    )
    events = [json.loads(line) for line in event_path.read_text().splitlines()] if event_path.exists() else []
    return result, events, pytester.path / "inner-junit.xml"


@pytest.mark.tags("CompactionIntegrityUnit")
def test_data_integrity_diagnostics_unit_records_each_pytest_phase(pytester, monkeypatch):
    result, events, junit_path = _run_inner(
        pytester,
        monkeypatch,
        "def test_regular_case():\n    assert True\n",
    )
    assert result.ret == 0
    phases = [event for event in events if event["event"] == "pytest_case_phase"]
    assert [(event["phase"], event["outcome"]) for event in phases] == [
        ("setup", "passed"),
        ("call", "passed"),
        ("teardown", "passed"),
    ]
    assert all(event["nodeid"].endswith("test_regular_case") for event in phases)
    assert junit_path.is_file()


@pytest.mark.tags("CompactionIntegrityUnit")
def test_data_integrity_diagnostics_unit_preserves_setup_call_and_teardown_failures(pytester, monkeypatch):
    source = """
import pytest

@pytest.fixture
def setup_failure():
    raise ValueError("safe setup failure")

@pytest.fixture
def teardown_failure():
    yield
    raise RuntimeError("safe teardown failure")

def test_setup_error(setup_failure):
    pass

def test_call_and_teardown_error(teardown_failure):
    raise AssertionError("safe call failure")
"""
    result, events, _ = _run_inner(pytester, monkeypatch, source)
    assert result.ret == 1
    failed = [event for event in events if event["event"] == "pytest_case_phase" and event["outcome"] == "failed"]
    setup = [event for event in failed if "test_setup_error" in event["nodeid"]]
    call_and_teardown = [event for event in failed if "test_call_and_teardown_error" in event["nodeid"]]
    assert [(event["phase"], event["failure_role"]) for event in setup] == [("setup", "primary")]
    assert {(event["phase"], event["failure_role"]) for event in call_and_teardown} == {
        ("call", "primary"),
        ("teardown", "secondary"),
    }


@pytest.mark.tags("CompactionIntegrityUnit")
def test_data_integrity_diagnostics_unit_preserves_skip_xfail_and_safe_outputs(pytester, monkeypatch):
    source = """
import pytest

def test_skip():
    pytest.skip("safe skip reason")

@pytest.mark.xfail(reason="safe expected failure")
def test_xfail():
    assert False

def test_safe_failure_without_serializing_locals():
    secret = "credential-" + "sentinel-value"
    raw_row = {"payload": "row-" + "sentinel-value"}
    assert secret and raw_row
    raise AssertionError("safe diagnostic failure")
"""
    result, events, junit_path = _run_inner(pytester, monkeypatch, source)
    assert result.ret == 1
    cases = [event for event in events if event["event"] == "pytest_case_phase" and event["phase"] == "call"]
    assert {event["nodeid"].split("::")[-1]: event["outcome"] for event in cases} == {
        "test_skip": "skipped",
        "test_xfail": "skipped",
        "test_safe_failure_without_serializing_locals": "failed",
    }
    output = result.stdout.str() + junit_path.read_text()
    assert "credential-sentinel-value" not in output
    assert "row-sentinel-value" not in output
    assert "safe diagnostic failure" in output
    serialized_events = json.dumps(events)
    assert "credential-sentinel-value" not in serialized_events
    assert "row-sentinel-value" not in serialized_events
    failed_event = next(event for event in events if event.get("error_summary"))
    assert failed_event["error_summary"] == "[see pytest output/JUnit]"


@pytest.mark.tags("CompactionIntegrityUnit")
def test_data_integrity_diagnostics_unit_records_collection_failure(pytester, monkeypatch):
    pytester.makefile(".py", test_broken="def test_broken(:\n    pass\n")
    event_path = pytester.path / "collection-events.jsonl"
    monkeypatch.setenv("DATA_INTEGRITY_DIAGNOSTICS_PATH", str(event_path))
    result = pytester.runpytest_inprocess("-p", "common.data_integrity_diagnostics", "--collect-only")
    assert result.ret != 0
    events = [json.loads(line) for line in event_path.read_text().splitlines()]
    assert any(event["event"] == "pytest_collection_failure" for event in events)


@pytest.mark.tags("CompactionIntegrityUnit")
def test_data_integrity_diagnostics_unit_bounds_events_and_tolerates_io_failure(tmp_path, monkeypatch):
    event_path = tmp_path / "events.jsonl"
    monkeypatch.setenv("DATA_INTEGRITY_DIAGNOSTICS_PATH", str(event_path))
    emit_data_integrity_event({"event": "large", "detail": "x" * 12000})
    event = json.loads(event_path.read_text().splitlines()[-1])
    assert event["diagnostics_truncated"] is True
    assert len(json.dumps(event).encode("utf-8")) < 8 * 1024

    blocked_parent = tmp_path / "not-a-directory"
    blocked_parent.write_text("file")
    monkeypatch.setenv("DATA_INTEGRITY_DIAGNOSTICS_PATH", str(blocked_parent / "events.jsonl"))
    emit_data_integrity_event({"event": "io-failure", "detail": "safe"})
