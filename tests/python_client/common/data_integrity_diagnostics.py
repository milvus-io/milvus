"""Opt-in pytest diagnostics for the Data Integrity L3 workflow."""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

_LOG = logging.getLogger("data_integrity_diagnostics")
_WRITE_LOCK = threading.Lock()
_CASE_FAILURES: dict[str, str] = {}
_FAILURE_LOCK = threading.Lock()
_SECRET_VALUE = re.compile(r"(?i)(\b(?:password|passwd|token|secret|access[_-]?key|api[_-]?key)\b\s*[:=]\s*)([^\s,;]+)")


def _safe_text(value: Any, limit: int = 2048) -> str:
    try:
        text = str(value)
    except BaseException:
        text = "<unavailable>"
    text = _SECRET_VALUE.sub(r"\1[REDACTED]", text)
    if len(text) > limit:
        return text[:limit] + "…[truncated]"
    return text


def exception_diagnostics(error: BaseException) -> dict[str, Any]:
    """Persist bounded stack locations without messages, source lines, or locals.

    These locations survive a stuck writer exit even when pytest never produces
    its terminal/JUnit traceback. Reasons are fixed categories, never server text.
    """
    try:
        reasons = {
            ValueError: "invalid value",
            TypeError: "invalid type",
            AssertionError: "assertion failed",
            TimeoutError: "operation timed out",
            ConnectionError: "connection failed",
        }
        frames = deque(maxlen=16)
        count = 0
        traceback = error.__traceback__
        while traceback is not None:
            code = traceback.tb_frame.f_code
            frames.append({"file": Path(code.co_filename).name, "function": code.co_name, "line": traceback.tb_lineno})
            count += 1
            traceback = traceback.tb_next
        return {
            "error_type": type(error).__name__,
            "error_reason": reasons.get(type(error), "exception raised; inspect error_stack origin"),
            "error_stack": list(frames),
            "error_stack_truncated": count > len(frames),
        }
    except BaseException:
        return {"error_type": type(error).__name__, "error_stack_status": "unavailable"}


def emit_data_integrity_event(event: dict[str, Any], *, primary_failure: bool = False) -> None:
    """Emit a bounded event to pytest logs and, when configured, the shared JSONL file."""
    try:
        truncated = False

        def sanitize(value: Any, key: str = "") -> Any:
            nonlocal truncated
            if key == "error_summary":
                # Exception strings can contain server responses or row values.
                # Pytest/JUnit retain the original traceback for detailed triage.
                return "[see pytest output/JUnit]"
            if re.search(r"(?i)(credential|password|passwd|token|secret|row_payload|traceback_locals)", key):
                truncated = True
                return "[REDACTED]"
            if isinstance(value, dict):
                return {str(name): sanitize(item, str(name)) for name, item in value.items()}
            if isinstance(value, (list, tuple)):
                return [sanitize(item, key) for item in value[:64]]
            if isinstance(value, str):
                safe = _safe_text(value, 4096)
                truncated = truncated or len(value) > 4096
                return safe
            if value is None or isinstance(value, (bool, int, float)):
                return value
            return _safe_text(value)

        safe_event = sanitize(event)
        if not isinstance(safe_event, dict):
            safe_event = {"event": "diagnostic", "detail": _safe_text(safe_event)}
        safe_event.setdefault("timestamp_utc", datetime.now(UTC).isoformat())
        limit = 64 * 1024 if primary_failure else 8 * 1024
        encoded = json.dumps(safe_event, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > limit:
            # Events are deliberately flat and bounded. Truncate text fields first, then
            # discard optional detail fields if a caller added unexpected metadata.
            for key, value in tuple(safe_event.items()):
                if isinstance(value, str) and len(value) > 256:
                    safe_event[key] = _safe_text(value, 256)
                    truncated = True
            encoded = json.dumps(safe_event, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if len(encoded.encode("utf-8")) > limit:
            safe_event = {
                "event": _safe_text(safe_event.get("event", "diagnostic"), 128),
                "diagnostics_truncated": True,
                "timestamp_utc": safe_event["timestamp_utc"],
            }
            encoded = json.dumps(safe_event, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            truncated = True
        if truncated:
            safe_event["diagnostics_truncated"] = True
            encoded = json.dumps(safe_event, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        _LOG.info("data_integrity_diagnostic %s", encoded)
        destination = os.getenv("DATA_INTEGRITY_DIAGNOSTICS_PATH")
        if destination:
            try:
                path = Path(destination)
                path.parent.mkdir(parents=True, exist_ok=True)
                payload = (encoded + "\n").encode("utf-8")
                with _WRITE_LOCK, path.open("ab") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
            except OSError as error:
                _LOG.warning("data_integrity_diagnostic_write_failed error=%s", _safe_text(error))
    except BaseException as error:
        # Diagnostics must never change pytest's result or mask the original exception.
        try:
            _LOG.warning("data_integrity_diagnostic_emit_failed error=%s", _safe_text(error, 512))
        except BaseException:
            pass


def _exception_summary(call: Any, report: Any) -> tuple[str | None, str | None]:
    excinfo = getattr(call, "excinfo", None)
    if excinfo is not None:
        error_type = getattr(getattr(excinfo, "type", None), "__name__", None)
        return error_type, _safe_text(getattr(excinfo, "value", ""))
    longrepr = getattr(report, "longrepr", None)
    reprcrash = getattr(longrepr, "reprcrash", None)
    if reprcrash is not None:
        message = getattr(reprcrash, "message", "")
        error_type, separator, error_message = str(message).partition(":")
        return _safe_text(error_type, 128), _safe_text(error_message if separator else message)
    return None, None


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: Any, call: Any):
    """Observe setup/call/teardown without changing the pytest report."""
    outcome = yield
    report = outcome.get_result()
    phase = str(report.when)
    nodeid = str(item.nodeid)
    error_type, error_message = _exception_summary(call, report) if report.failed else (None, None)
    failure_role = None
    if report.failed:
        with _FAILURE_LOCK:
            failure_role = "primary" if nodeid not in _CASE_FAILURES else "secondary"
            _CASE_FAILURES.setdefault(nodeid, phase)
    event = {
        "event": "pytest_case_phase",
        "nodeid": nodeid,
        "phase": phase,
        "outcome": str(report.outcome),
        "duration_seconds": round(float(report.duration), 6),
        "timestamp_utc": datetime.now(UTC).isoformat(),
    }
    if report.skipped and hasattr(report, "wasxfail"):
        event["wasxfail"] = _safe_text(report.wasxfail, 512)
    if report.failed:
        event.update(
            {
                "failure_role": failure_role,
                "error_type": error_type,
                "error_summary": error_message,
            }
        )
    emit_data_integrity_event(event, primary_failure=report.failed and failure_role == "primary")


def pytest_collectreport(report: Any) -> None:
    if report.failed:
        message = getattr(getattr(report.longrepr, "reprcrash", None), "message", "collection failed")
        emit_data_integrity_event(
            {
                "event": "pytest_collection_failure",
                "nodeid": _safe_text(getattr(report, "nodeid", ""), 512),
                "error_summary": _safe_text(message),
            },
            primary_failure=True,
        )


def pytest_runtest_logstart(nodeid: str, location: Any) -> None:
    with _FAILURE_LOCK:
        _CASE_FAILURES.pop(nodeid, None)
