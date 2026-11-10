"""Opt-in credential-safe logging and result evidence for PyUDF regression."""

import json
import logging

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--pyudf-mmap-observe-seconds",
        type=float,
        default=0,
        help="Hold each owned Q06 pre/post-query phase for bounded external mapping observation (0 to 10 seconds).",
    )


def sanitize(message):
    """Remove credential-bearing lines before any handler or pytest capture sees them."""
    sensitive = (
        "authorization",
        "password",
        "token",
        "credential",
        "access_key",
        "secret_key",
        "[connections.connect]",
        "[milvusclient]",
        "[milvusclient.create_user]",
        "[create_user]",
        "[create_credential]",
    )
    return "\n".join(
        "[REDACTED credential-bearing log line]" if any(key in line.lower() for key in sensitive) else line
        for line in str(message).splitlines()
    )


def pytest_configure(config):
    original = logging.getLogRecordFactory()

    def safe_record(*args, **kwargs):
        record = original(*args, **kwargs)
        record.msg = sanitize(record.getMessage())
        record.args = ()
        return record

    logging.setLogRecordFactory(safe_record)
    config.add_cleanup(lambda: logging.setLogRecordFactory(original))


@pytest.hookimpl(trylast=True)
def pytest_sessionstart(session):
    # The REST suite can persist request headers outside the logging system.
    # Only its optional raw request dump is disabled; API calls remain unchanged.
    import sys

    rest = sys.modules.get("api.milvus")
    if rest is not None:
        rest.ENABLE_LOG_SAVE = False


def pytest_runtest_logreport(report):
    # Dev CLI retains stdout but does not enable JUnit automatically. Persist
    # record_property evidence even when the ephemeral runner exits.
    if report.when == "call" and report.user_properties:
        print(
            sanitize(
                json.dumps(
                    {
                        "event": "PYUDF_R2_EVIDENCE",
                        "nodeid": report.nodeid,
                        "outcome": report.outcome,
                        "properties": report.user_properties,
                    },
                    default=str,
                )
            ),
            flush=True,
        )
