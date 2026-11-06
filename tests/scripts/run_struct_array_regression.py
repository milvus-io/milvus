#!/usr/bin/env python3
# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run the retained Struct Array cases using each suite's existing pytest fixtures."""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("python", "rest"), required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("pytest_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    manifest = json.loads(Path(__file__).with_name("struct_array_regression.json").read_text())
    suite = manifest[args.suite]
    cwd = root / suite["cwd"]
    for name in suite["files"]:
        if not (cwd / name).is_file():
            parser.error(f"Missing retained testcase: {name}")
    output = args.artifacts.resolve()
    output.mkdir(parents=True, exist_ok=False)
    extra = args.pytest_args[1:] if args.pytest_args[:1] == ["--"] else args.pytest_args
    command = [
        sys.executable,
        "-m",
        "pytest",
        *suite["files"],
        f"--junitxml={output / 'junit.xml'}",
        f"--alluredir={output / 'allure-results'}",
        *extra,
    ]
    revision = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    # Arguments can contain authentication values; retain selection, not secrets.
    (output / "selection.json").write_text(
        json.dumps({"suite": args.suite, "test_commit": revision, "files": suite["files"]}, indent=2) + "\n"
    )
    result = subprocess.run(command, cwd=cwd, check=False)
    (output / "exit-code.txt").write_text(f"{result.returncode}\n")
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
