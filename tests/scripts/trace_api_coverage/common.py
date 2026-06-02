#!/usr/bin/env python3

import argparse
import gzip
import json
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable


DEFAULT_GRPC_URI = "http://127.0.0.1:19530"
DEFAULT_REST_URI = "http://127.0.0.1:19530"
DEFAULT_TOKEN = "root:Milvus"


def new_trace_id() -> str:
    return uuid.uuid4().hex


def new_span_id() -> str:
    return uuid.uuid4().hex[:16]


def trace_headers(trace_id: str) -> dict[str, str]:
    span_id = new_span_id()
    return {
        "traceparent": f"00-{trace_id}-{span_id}-01",
        "client_request_id": trace_id,
        "client-request-id": trace_id,
        "RequestId": trace_id,
    }


def grpc_trace_kwargs(trace_id: str) -> dict[str, str]:
    return {"client_request_id": trace_id}


@dataclass
class ApiResult:
    protocol: str
    name: str
    trace_id: str = ""
    status: str = "unknown"
    error: str = ""
    elapsed_ms: int = 0
    log_matched: bool = False
    log_files: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def run_case(protocol: str, name: str, fn: Callable[[str], Any]) -> ApiResult:
    trace_id = new_trace_id()
    result = ApiResult(protocol=protocol, name=name, trace_id=trace_id)
    start = time.monotonic()
    try:
        fn(trace_id)
        result.status = "ok"
    except SkipCase as exc:
        result.status = "skipped"
        result.error = str(exc)
    except Exception as exc:  # noqa: BLE001 - report every API failure.
        result.status = "failed"
        result.error = f"{type(exc).__name__}: {exc}"
    result.elapsed_ms = int((time.monotonic() - start) * 1000)
    return result


class SkipCase(RuntimeError):
    pass


class LogVerifier:
    def __init__(self, log_dir: str | None = None, timeout: float = 15, interval: float = 0.5):
        self.log_dir = Path(log_dir).expanduser() if log_dir else self._default_log_dir()
        self.timeout = timeout
        self.interval = interval

    def _default_log_dir(self) -> Path | None:
        explicit = os.getenv("MILVUS_LOG_DIR")
        if explicit:
            return Path(explicit).expanduser()

        volume = os.getenv("MILVUS_VOLUME_DIRECTORY")
        if not volume:
            return None

        root = Path(volume).expanduser()
        candidates = sorted(root.glob("*/milvus-logs"), key=lambda p: p.stat().st_mtime if p.exists() else 0)
        return candidates[-1] if candidates else None

    def annotate(self, results: list[ApiResult]) -> None:
        pending = [r for r in results if r.trace_id and r.status != "skipped"]
        if not pending or self.log_dir is None:
            return

        deadline = time.monotonic() + self.timeout
        while pending and time.monotonic() < deadline:
            self._scan_once(pending)
            pending = [r for r in pending if not r.log_matched]
            if pending:
                time.sleep(self.interval)

    def _scan_once(self, results: list[ApiResult]) -> None:
        files = list(self._log_files())
        if not files:
            return

        trace_to_result = {r.trace_id: r for r in results}
        for path in files:
            try:
                reader: Iterable[str]
                if path.suffix == ".gz":
                    reader = (line.decode("utf-8", "ignore") for line in gzip.open(path, "rb"))
                else:
                    reader = path.open("r", encoding="utf-8", errors="ignore")
                with reader if hasattr(reader, "__enter__") else _GeneratorContext(reader) as lines:
                    for line in lines:
                        for trace_id, result in trace_to_result.items():
                            if not result.log_matched and trace_id in line:
                                result.log_matched = True
                                result.log_files.append(str(path))
                if all(r.log_matched for r in results):
                    return
            except OSError:
                continue

    def _log_files(self) -> Iterable[Path]:
        if not self.log_dir or not self.log_dir.exists():
            return []
        patterns = ("*.log", "*.log.*", "*.out", "*.err", "*.gz")
        files: list[Path] = []
        for pattern in patterns:
            files.extend(self.log_dir.rglob(pattern))
        return sorted(set(files), key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)


class _GeneratorContext:
    def __init__(self, lines: Iterable[str]):
        self.lines = lines

    def __enter__(self) -> Iterable[str]:
        return self.lines

    def __exit__(self, *_args: Any) -> None:
        return None


def write_json(path: str | None, payload: Any) -> None:
    text = json.dumps(payload, indent=2, sort_keys=True)
    if path:
        Path(path).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--grpc-uri", default=os.getenv("MILVUS_GRPC_URI", DEFAULT_GRPC_URI))
    parser.add_argument("--rest-uri", default=os.getenv("MILVUS_REST_URI", DEFAULT_REST_URI))
    parser.add_argument("--token", default=os.getenv("MILVUS_TOKEN", DEFAULT_TOKEN))
    parser.add_argument("--log-dir", default=os.getenv("MILVUS_LOG_DIR"))
    parser.add_argument("--log-timeout", type=float, default=15)
    parser.add_argument("--skip-log-check", action="store_true")
    parser.add_argument("--output", default="")


def finalize_results(args: argparse.Namespace, results: list[ApiResult], declared: list[str]) -> dict[str, Any]:
    if not args.skip_log_check:
        LogVerifier(args.log_dir, timeout=args.log_timeout).annotate(results)

    executed = {r.name for r in results}
    return {
        "summary": {
            "declared": len(declared),
            "executed": len(results),
            "ok": sum(1 for r in results if r.status == "ok"),
            "failed": sum(1 for r in results if r.status == "failed"),
            "skipped": sum(1 for r in results if r.status == "skipped"),
            "log_matched": sum(1 for r in results if r.log_matched),
            "not_executed": len([name for name in declared if name not in executed]),
        },
        "not_executed": [name for name in declared if name not in executed],
        "results": [r.to_json() for r in results],
    }
