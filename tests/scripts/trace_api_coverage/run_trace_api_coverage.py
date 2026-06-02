#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import add_common_args


SCRIPT_DIR = Path(__file__).resolve().parent


def run_child(script: str, args: argparse.Namespace, output: Path) -> int:
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / script),
        "--grpc-uri",
        args.grpc_uri,
        "--rest-uri",
        args.rest_uri,
        "--token",
        args.token,
        "--log-timeout",
        str(args.log_timeout),
        "--output",
        str(output),
    ]
    if args.log_dir:
        cmd.extend(["--log-dir", args.log_dir])
    if args.skip_log_check:
        cmd.append("--skip-log-check")
    return subprocess.call(cmd)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run gRPC and REST trace API coverage.")
    add_common_args(parser)
    parser.add_argument("--protocol", choices=["grpc", "rest", "both"], default="both")
    args = parser.parse_args()

    output_dir = Path(args.output or "trace_api_coverage_reports")
    output_dir.mkdir(parents=True, exist_ok=True)

    exit_code = 0
    reports = {}
    if args.protocol in ("grpc", "both"):
        grpc_output = output_dir / "grpc_report.json"
        exit_code |= run_child("grpc_api_coverage.py", args, grpc_output)
        reports["grpc"] = json.loads(grpc_output.read_text(encoding="utf-8"))
    if args.protocol in ("rest", "both"):
        rest_output = output_dir / "rest_report.json"
        exit_code |= run_child("restful_api_coverage.py", args, rest_output)
        reports["rest"] = json.loads(rest_output.read_text(encoding="utf-8"))

    summary = {name: report["summary"] for name, report in reports.items()}
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
