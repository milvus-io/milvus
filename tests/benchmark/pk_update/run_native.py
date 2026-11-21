#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file in the project root for terms.

"""Run each production Retrieve configuration in a fresh native test process."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import xml.etree.ElementTree as ET


FIXTURES = {"1pct-1x": (1, 1), "5pct-1x": (5, 1), "5pct-3x": (5, 3)}
REPORT_PREFIX = "PK_UPDATE_NATIVE_BENCH "


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def verify_fixture(fixture, percent, rounds):
    manifest_path = fixture / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if (manifest["update_percent"], manifest["update_rounds"]) != (percent, rounds):
        raise ValueError(f"Wrong update schedule in {manifest_path}")
    hashes = manifest["sha256"]
    if set(hashes) != {"base.jsonl", "mirror.jsonl", "updates.jsonl", "oracle.json"}:
        raise ValueError(f"Incomplete fixture hashes in {manifest_path}")
    for name, expected in hashes.items():
        if digest(fixture / name) != expected:
            raise ValueError(f"Fixture digest mismatch: {fixture / name}")
    return digest(manifest_path)


def run(binary, root, test_name, benchmark_env=None):
    result_xml = root / "result.xml"
    root.mkdir()
    overrides = {"MILVUS_TEST_ROOT_DIR": str(root / "native-data"), **(benchmark_env or {})}
    # The runner calls the benchmark in its own ordinary GTest process.
    command = [
        str(binary),
        f"--gtest_filter=PkUpdateNativeBenchmark.{test_name}",
        f"--gtest_output=xml:{result_xml}",
    ]
    if benchmark_env:
        command.append("--gtest_also_run_disabled_tests")
    write_json(root / "command.json", {"argv": command, "environment": overrides})
    with (root / "stdout.log").open("x", encoding="utf-8") as log:
        completed = subprocess.run(
            command,
            env={**os.environ, **overrides},
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=1800,
            check=False,
        )
    write_json(root / "exit.json", {"exit_code": completed.returncode})
    completed.check_returncode()
    cases = ET.parse(result_xml).findall(".//testcase")
    if len(cases) != 1 or cases[0].get("result") != "completed" or list(cases[0]):
        raise RuntimeError(f"Expected one completed native test: {result_xml}")
    if not benchmark_env:
        return None
    with (root / "stdout.log").open(encoding="utf-8", errors="replace") as log:
        [report] = [json.loads(line[len(REPORT_PREFIX):]) for line in log if line.startswith(REPORT_PREFIX)]
    write_json(root / "timing.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--core-library", required=True, type=Path)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    if args.output_dir.exists():
        parser.error("use a new output directory to preserve existing results")
    fixtures = args.fixtures.resolve()
    binary = args.binary.resolve()
    core = args.core_library.resolve()
    manifest_hashes = {
        name: verify_fixture(fixtures / name, percent, rounds)
        for name, (percent, rounds) in FIXTURES.items()
    }
    output = args.output_dir.resolve()
    output.mkdir(parents=True)
    repo = Path(__file__).resolve().parents[3]
    write_json(
        output / "provenance.json",
        {
            "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
            "source_status": subprocess.check_output(["git", "status", "--porcelain"], cwd=repo, text=True).splitlines(),
            "native_binary": str(binary),
            "native_binary_sha256": digest(binary),
            "core_library": str(core),
            "core_library_sha256": digest(core),
            "fixture_root": str(fixtures),
            "fixture_manifest_sha256": manifest_hashes,
            "platform": platform.platform(),
            "machine": platform.machine(),
        },
    )
    run(binary, output / "snapshot-projection", "SnapshotProjection")
    results = {}
    for name in FIXTURES:
        for residency in ("raw", "stl_sort"):
            for role in ("candidate", "mirror"):
                config = f"{name}-{residency}-{role}"
                print(config, flush=True)
                results[config] = run(
                    binary,
                    output / config,
                    "DISABLED_ClickBench",
                    {
                        "PK_UPDATE_BENCH_FIXTURE": str(fixtures / name),
                        "PK_UPDATE_BENCH_RESIDENCY": residency,
                        "PK_UPDATE_BENCH_ROLE": role,
                    },
                )
    write_json(output / "matrix.json", results)


if __name__ == "__main__":
    main()
