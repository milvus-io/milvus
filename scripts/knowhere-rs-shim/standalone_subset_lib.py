#!/usr/bin/env python3

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import re
import shlex
import subprocess
import sys
import time
from typing import Iterable
from urllib.parse import urlparse


DEFAULT_TIMEOUT = "20m"


def load_manifest(manifest_path: pathlib.Path) -> list[dict]:
    return json.loads(manifest_path.read_text())


def flatten_manifest(manifest_path: pathlib.Path) -> list[dict]:
    flattened: list[dict] = []
    for group in load_manifest(manifest_path):
        for case_name in group["cases"]:
            flattened.append(
                {
                    "suite": group["suite"],
                    "runner": group["runner"],
                    "working_dir": group["working_dir"],
                    "file": group["file"],
                    "case": case_name,
                    "path_kinds": list(group["path_kinds"]),
                    "index_families": list(group["index_families"]),
                    "selection_reason": group["selection_reason"],
                }
            )
    return flattened


def selector_for_case(case: dict) -> str:
    if case["runner"] == "go_test":
        return case["case"]
    relative_file = os.path.relpath(case["file"], start=case["working_dir"])
    relative_file = relative_file.replace(os.sep, "/")
    return f"{relative_file}::{case['case']}"


def slug_for_case(case: dict) -> str:
    raw = f"{case['suite']}__{pathlib.Path(case['file']).stem}__{case['case']}"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("_")


def endpoint_host_port(endpoint: str) -> tuple[str, int]:
    parsed = urlparse(endpoint)
    if parsed.scheme:
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 19530
        return host, port

    if ":" in endpoint:
        host, port_text = endpoint.rsplit(":", 1)
        return host, int(port_text)
    return endpoint, 19530


def build_case_command(
    case: dict,
    root_dir: pathlib.Path,
    endpoint: str,
    token: str,
    minio_host: str,
    timeout: str = DEFAULT_TIMEOUT,
) -> dict:
    cwd = root_dir / case["working_dir"]
    selector = selector_for_case(case)

    if case["suite"] == "go_client":
        argv = [
            "go",
            "test",
            "./testcases",
            "-run",
            f"^{case['case']}$",
            "-count=1",
            f"-timeout={timeout}",
            "-args",
            "-addr",
            endpoint,
        ]
    elif case["suite"] == "python_client":
        host, port = endpoint_host_port(endpoint)
        argv = [
            "python3",
            "-W",
            "ignore",
            "-m",
            "pytest",
            "-q",
            "-s",
            selector,
            "--host",
            host,
            "--port",
            str(port),
            "--uri",
            endpoint,
            "--token",
            token,
        ]
    elif case["suite"] == "restful_client":
        argv = [
            "python3",
            "-m",
            "pytest",
            "-q",
            "-s",
            selector,
            "--endpoint",
            endpoint,
            "--token",
            token,
        ]
    elif case["suite"] == "restful_client_v2":
        argv = [
            "python3",
            "-m",
            "pytest",
            "-q",
            "-s",
            selector,
            "--endpoint",
            endpoint,
            "--token",
            token,
            "--minio_host",
            minio_host,
        ]
    else:
        raise ValueError(f"unsupported suite: {case['suite']}")

    return {
        "suite": case["suite"],
        "case": case["case"],
        "file": case["file"],
        "runner": case["runner"],
        "selector": selector,
        "slug": slug_for_case(case),
        "cwd": str(cwd),
        "argv": argv,
    }


def filter_cases(
    cases: Iterable[dict], suites: set[str] | None = None, case_pattern: str | None = None
) -> list[dict]:
    compiled = re.compile(case_pattern) if case_pattern else None
    filtered: list[dict] = []
    for case in cases:
        if suites and case["suite"] not in suites:
            continue
        if compiled and not compiled.search(case["case"]):
            continue
        filtered.append(case)
    return filtered


def summarize_results(results: list[dict]) -> dict:
    summary: dict[str, dict] = {"totals": {"cases": 0}, "suites": {}}
    failures: list[dict] = []
    for result in results:
        status = result["status"]
        suite = result["suite"]
        summary["totals"]["cases"] += 1
        summary["totals"][status] = summary["totals"].get(status, 0) + 1

        suite_summary = summary["suites"].setdefault(suite, {"cases": 0})
        suite_summary["cases"] += 1
        suite_summary[status] = suite_summary.get(status, 0) + 1
        if status != "PASS":
            failures.append(
                {
                    "suite": result["suite"],
                    "case": result["case"],
                    "status": result["status"],
                    "exit_code": result["exit_code"],
                    "result_path": result["result_path"],
                }
            )

    summary["failures"] = failures
    return summary


def run_cases(
    manifest_path: pathlib.Path,
    root_dir: pathlib.Path,
    artifact_root: pathlib.Path,
    suites: set[str],
    endpoint: str,
    token: str,
    minio_host: str,
    timeout: str = DEFAULT_TIMEOUT,
    case_pattern: str | None = None,
) -> tuple[list[dict], dict]:
    cases = flatten_manifest(manifest_path)
    cases = filter_cases(cases, suites=suites, case_pattern=case_pattern)

    artifact_root.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []

    for case in cases:
        command = build_case_command(
            case,
            root_dir=root_dir,
            endpoint=endpoint,
            token=token,
            minio_host=minio_host,
            timeout=timeout,
        )

        case_dir = artifact_root / command["suite"] / command["slug"]
        case_dir.mkdir(parents=True, exist_ok=True)
        log_path = case_dir / "output.log"
        command_path = case_dir / "command.sh"
        result_path = case_dir / "result.json"

        command_path.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            f"cd {shlex.quote(command['cwd'])}\n"
            f"exec {shlex.join(command['argv'])}\n"
        )
        command_path.chmod(0o755)

        started_at = dt.datetime.now(dt.timezone.utc)
        start = time.perf_counter()
        with log_path.open("w", encoding="utf-8") as log_file:
            completed = subprocess.run(
                command["argv"],
                cwd=command["cwd"],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
        duration_seconds = round(time.perf_counter() - start, 3)
        finished_at = dt.datetime.now(dt.timezone.utc)

        result = {
            "suite": command["suite"],
            "case": command["case"],
            "file": command["file"],
            "runner": command["runner"],
            "selector": command["selector"],
            "slug": command["slug"],
            "cwd": command["cwd"],
            "argv": command["argv"],
            "command_shell": shlex.join(command["argv"]),
            "status": "PASS" if completed.returncode == 0 else "FAIL",
            "exit_code": completed.returncode,
            "duration_seconds": duration_seconds,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "log_path": str(log_path),
            "result_path": str(result_path),
        }
        result_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        results.append(result)

    summary = summarize_results(results)
    summary_path = artifact_root / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return results, summary


def _cmd_list(args: argparse.Namespace) -> int:
    cases = filter_cases(
        flatten_manifest(pathlib.Path(args.manifest)),
        suites=set(args.suite) if args.suite else None,
        case_pattern=args.case_pattern,
    )
    root_dir = pathlib.Path(args.root_dir)
    for case in cases:
        command = build_case_command(
            case,
            root_dir=root_dir,
            endpoint=args.endpoint,
            token=args.token,
            minio_host=args.minio_host,
            timeout=args.timeout,
        )
        print(json.dumps(command, sort_keys=True))
    return 0


def _cmd_run_suite(args: argparse.Namespace) -> int:
    suites = set(args.suite)
    _, summary = run_cases(
        manifest_path=pathlib.Path(args.manifest),
        root_dir=pathlib.Path(args.root_dir),
        artifact_root=pathlib.Path(args.artifact_root),
        suites=suites,
        endpoint=args.endpoint,
        token=args.token,
        minio_host=args.minio_host,
        timeout=args.timeout,
        case_pattern=args.case_pattern,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["totals"].get("FAIL", 0) == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common_arguments(cmd: argparse.ArgumentParser) -> None:
        cmd.add_argument("--manifest", required=True)
        cmd.add_argument("--root-dir", required=True)
        cmd.add_argument("--endpoint", required=True)
        cmd.add_argument("--token", required=True)
        cmd.add_argument("--minio-host", default="127.0.0.1")
        cmd.add_argument("--timeout", default=DEFAULT_TIMEOUT)
        cmd.add_argument("--case-pattern")
        cmd.add_argument("--suite", action="append")

    list_parser = subparsers.add_parser("list")
    add_common_arguments(list_parser)
    list_parser.set_defaults(func=_cmd_list)

    run_parser = subparsers.add_parser("run-suite")
    add_common_arguments(run_parser)
    run_parser.add_argument("--artifact-root", required=True)
    run_parser.set_defaults(func=_cmd_run_suite)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
