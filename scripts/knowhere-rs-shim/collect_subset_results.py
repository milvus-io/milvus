#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import pathlib
import sys


def collect_results(artifact_root: pathlib.Path) -> dict:
    results = []
    candidate_paths = set(artifact_root.glob("*/*/result.json"))
    candidate_paths.update(
        path
        for path in artifact_root.glob("*/*.json")
        if path.name not in {"summary.json"} and path.parent.name != artifact_root.name
    )

    for result_path in sorted(candidate_paths):
        results.append(json.loads(result_path.read_text()))

    summary: dict[str, dict] = {"totals": {"cases": len(results)}, "suites": {}, "failures": []}
    for result in results:
        suite = result["suite"]
        status = result["status"]
        summary["totals"][status] = summary["totals"].get(status, 0) + 1

        suite_summary = summary["suites"].setdefault(suite, {"cases": 0})
        suite_summary["cases"] += 1
        suite_summary[status] = suite_summary.get(status, 0) + 1

        if status != "PASS":
            summary["failures"].append(
                {
                    "suite": suite,
                    "case": result["case"],
                    "status": status,
                    "exit_code": result["exit_code"],
                    "result_path": result.get("result_path", str(result_path)),
                    "log_path": result.get("log_path", ""),
                }
            )

    return summary


def markdown_summary(summary: dict) -> str:
    lines = [
        "# Standalone Subset Summary",
        "",
        f"- total cases: {summary['totals'].get('cases', 0)}",
        f"- passes: {summary['totals'].get('PASS', 0)}",
        f"- failures: {summary['totals'].get('FAIL', 0)}",
        "",
        "## Suites",
        "",
    ]

    for suite in sorted(summary["suites"]):
        suite_summary = summary["suites"][suite]
        lines.append(
            f"- {suite}: cases={suite_summary.get('cases', 0)} "
            f"pass={suite_summary.get('PASS', 0)} fail={suite_summary.get('FAIL', 0)}"
        )

    if summary["failures"]:
        lines.extend(["", "## Failures", ""])
        for failure in summary["failures"]:
            lines.append(
                f"- {failure['suite']} {failure['case']} "
                f"(exit={failure['exit_code']}, log={failure['log_path']})"
            )

    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact_root")
    parser.add_argument("--summary-json")
    parser.add_argument("--summary-md")
    args = parser.parse_args(argv)

    artifact_root = pathlib.Path(args.artifact_root)
    summary = collect_results(artifact_root)

    summary_json = pathlib.Path(args.summary_json) if args.summary_json else artifact_root / "summary.json"
    summary_md = pathlib.Path(args.summary_md) if args.summary_md else artifact_root / "summary.md"

    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    summary_md.write_text(markdown_summary(summary))

    print(summary_json)
    print(summary_md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
