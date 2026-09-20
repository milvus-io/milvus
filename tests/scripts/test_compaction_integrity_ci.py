"""Server-independent regression checks for the dedicated integrity CI boundary."""

import ast
import importlib.util
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
MARKER = "compaction_data_integrity_serial"
NODEID = "test_example.py::TestExample::test_data"
FIXED_TIME = datetime(2026, 9, 15, 0, 0, 0)


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return FIXED_TIME


@pytest.fixture
def log_plugin(monkeypatch):
    path = ROOT / "tests/python_client/plugin/log_filter.py"
    spec = importlib.util.spec_from_file_location("integrity_log_filter_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "datetime", FixedDatetime)
    return module


def make_item(marked=False, opt_in=False, worker=False):
    config = SimpleNamespace(getoption=lambda name: opt_in)
    if worker:
        config.workerinput = {}
    return SimpleNamespace(
        nodeid=NODEID,
        config=config,
        get_closest_marker=lambda name: object() if marked and name == MARKER else None,
        iter_markers=lambda name: iter(()),
    )


def finish_test(handler, item, outcome="passed", audit_record=False):
    handler.start_test(item)
    for message, retained in [("ordinary log", False), ("audit log", audit_record)]:
        record = logging.LogRecord("ci_test", logging.INFO, "test_example.py", 7, message, (), None)
        record.created = FIXED_TIME.timestamp()
        if retained:
            record.persist_on_pass = True
        handler.emit(record)
    report = SimpleNamespace(
        passed=outcome in {"passed", "xpass"},
        failed=outcome == "failed",
        skipped=outcome in {"skipped", "xfail"},
        duration=1.25,
        longrepr=("test_example.py", 7, "reason"),
    )
    if outcome in {"xfail", "xpass"}:
        report.wasxfail = "expected reason"
    handler.end_test(item, report)


@pytest.mark.parametrize("worker_merge", [False, True])
@pytest.mark.parametrize("marked,audit_record", [(False, False), (False, True), (True, False), (True, True)])
def test_pass_report_retains_audit_only_for_marked_workloads(log_plugin, tmp_path, marked, audit_record, worker_merge):
    config = SimpleNamespace(log_path=str(tmp_path))
    handler = log_plugin.ConditionalLogHandler(config)
    finish_test(handler, make_item(marked=marked), audit_record=audit_record)
    if worker_merge:
        handler._save_worker_data(tmp_path / ".worker_gw0_data.json")
        handler = log_plugin.ConditionalLogHandler(config)
        handler._merge_worker_data()
    handler.generate_report()
    result = json.loads(Path(handler.report_json).read_text())
    expected = {
        "id": NODEID,
        "file": "test_example.py",
        "class": "TestExample",
        "function": "test_data",
        "duration": 1.25,
        "timestamp": FIXED_TIME.isoformat(),
    }
    if marked and audit_record:
        expected["logs"] = {
            "debug": [],
            "info": [
                {
                    "message": "audit log",
                    "timestamp": FIXED_TIME.isoformat(),
                    "location": "test_example.py:7",
                    "logger": "ci_test",
                }
            ],
            "warning": [],
            "error": [],
            "critical": [],
        }
    assert result["tests"]["passed"] == [expected]
    # The existing HTML success section still contains only test metadata.
    html = Path(handler.report_html).read_text()
    assert NODEID in html
    assert "ordinary log" not in html
    assert "audit log" not in html


@pytest.mark.parametrize("outcome", ["passed", "failed", "skipped", "xfail", "xpass"])
def test_unmarked_outcomes_ignore_audit_opt_in(log_plugin, tmp_path, outcome):
    artifacts = []
    for audit_record in [False, True]:
        handler = log_plugin.ConditionalLogHandler(SimpleNamespace(log_path=str(tmp_path)))
        finish_test(handler, make_item(), outcome=outcome, audit_record=audit_record)
        handler._save_worker_data(tmp_path / "worker.json")
        handler.generate_report()
        artifacts.append(
            tuple((tmp_path / name).read_bytes() for name in ["worker.json", "test_report.json", "test_report.html"])
        )
        entry = json.loads(Path(handler.report_json).read_text())["tests"][outcome][0]
        if outcome in {"failed", "xpass"}:
            assert [record["message"] for record in entry["logs"]["info"]] == ["ordinary log", "audit log"]
        else:
            assert "logs" not in entry
    assert artifacts[0] == artifacts[1]


@pytest.mark.parametrize(
    "marked,opt_in,worker,exception",
    [
        (False, False, False, None),
        (False, True, True, None),
        (True, False, False, pytest.skip.Exception),
        (True, False, True, pytest.skip.Exception),
        (True, True, True, pytest.fail.Exception),
        (True, True, False, None),
    ],
)
def test_execution_guard_is_scoped_to_integrity_workloads(marked, opt_in, worker, exception):
    # Load just the real hook, avoiding unrelated SDK and server fixtures.
    path = ROOT / "tests/python_client/conftest.py"
    tree = ast.parse(path.read_text())
    hook = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "pytest_runtest_setup")
    namespace = {"pytest": pytest}
    exec(compile(ast.Module(body=[hook], type_ignores=[]), str(path), "exec"), namespace)
    item = make_item(marked=marked, opt_in=opt_in, worker=worker)
    if exception is None:
        namespace["pytest_runtest_setup"](item)
    else:
        with pytest.raises(exception):
            namespace["pytest_runtest_setup"](item)


@pytest.mark.parametrize("exit_code", [0, 5, 7])
@pytest.mark.parametrize(
    "script,selection",
    [
        (
            "ci_compaction_integrity.sh",
            [
                "--tags",
                "L3",
                "--run-compaction-integrity-serial",
                "-m",
                MARKER,
                "milvus_client/test_milvus_client_data_integrity.py::TestMilvusClientCompactionDataIntegrity",
            ],
        ),
        (
            "ci_compaction_integrity_unit.sh",
            [
                "--tags",
                "CompactionIntegrityUnit",
                "--",
                "milvus_client/compaction_integrity_helper_tests.py",
                "milvus_client/test_milvus_client_data_integrity.py",
            ],
        ),
    ],
)
def test_dedicated_entry_preserves_arguments_and_exit_status(tmp_path, exit_code, script, selection):
    capture = tmp_path / "calls.jsonl"
    fake_pytest = tmp_path / "pytest"
    fake_pytest.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        f"with open({str(capture)!r}, 'a') as stream:\n"
        "    stream.write(json.dumps({'args': sys.argv[1:], 'cwd': os.getcwd(), "
        "'log_path': os.environ['CI_LOG_PATH']}) + '\\n')\n"
        f"sys.exit({exit_code})\n"
    )
    fake_pytest.chmod(0o755)
    caller_args = [
        "--uri",
        "http://custom-milvus:19530",
        "--etcd_host",
        "custom-etcd",
        "--etcd_root_path",
        "isolated-root",
        "--minio_bucket",
        "custom-bucket",
        "--collect-only",
        "-k",
        "storage_v2 or storage_v3",
        "-n",
        "6",
    ]
    env = dict(os.environ, PATH=f"{tmp_path}:{os.environ['PATH']}", CI_LOG_PATH=str(tmp_path / "report"))
    result = subprocess.run(
        ["bash", str(ROOT / "tests/scripts" / script), *caller_args],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == exit_code, result.stderr
    calls = [json.loads(line) for line in capture.read_text().splitlines()]
    assert calls == [
        {
            "args": [
                *caller_args,
                "-n",
                "0",
                *selection,
            ],
            "cwd": str(ROOT / "tests/python_client"),
            "log_path": str(tmp_path / "report"),
        }
    ]


def test_sdk_helpers_are_isolated_and_all_oracle_units_are_selectable():
    directory = ROOT / "tests/python_client/milvus_client"
    helper = directory / "compaction_integrity_helper_tests.py"
    assert helper.is_file()
    assert not (directory / "test_compaction_integrity_helper_tests.py").exists()
    assert not helper.match("test_*.py") and not helper.match("*_test.py")
    helper_tree = ast.parse(helper.read_text())
    marker = next(
        node.value
        for node in helper_tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets)
    )
    assert ast.unparse(marker) == "pytest.mark.tags('CompactionIntegrityUnit')"
    tree = ast.parse((directory / "test_milvus_client_data_integrity.py").read_text())
    unit_functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")]
    assert unit_functions
    for node in unit_functions:
        assert any(
            ast.unparse(decorator) == "pytest.mark.tags('CompactionIntegrityUnit')" for decorator in node.decorator_list
        ), node.name
