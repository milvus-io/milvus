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

"""Generate the standalone worker protocol into its wheel package."""

from __future__ import annotations

import argparse
import importlib.metadata
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from google.protobuf import descriptor_pb2

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "internal/util/function/pyudf/python/milvus_pyudf_runtime/proto"
PACKAGE = "milvus_pyudf_runtime.proto"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Check committed products without modifying them")
    args = parser.parse_args()
    # Pin the runtime and generators together in pyproject.toml. Do not silently
    # regenerate checked-in products with whichever compiler happens to be installed.
    for package, expected in (("grpcio-tools", "1.74.0"), ("grpcio", "1.74.0"), ("protobuf", "6.31.1")):
        actual = importlib.metadata.version(package)
        if actual != expected:
            raise SystemExit(f"{package}=={expected} required for codegen, found {actual}")
    command = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        "-I" + str(ROOT / "pkg/proto"),
    ]
    with tempfile.TemporaryDirectory(prefix="pyudf-proto-") as directory:
        temporary = Path(directory)
        descriptor = temporary / "protocol.pb"
        subprocess.run([*command, "--include_imports", f"--descriptor_set_out={descriptor}", "pyudf.proto"], check=True)
        files = descriptor_pb2.FileDescriptorSet.FromString(descriptor.read_bytes())
        sources = [f.name for f in files.file]
        if sources != ["pyudf.proto"] or files.file[0].dependency:
            raise SystemExit("pyudf.proto must remain standalone, without proto imports")
        subprocess.run([*command, f"--python_out={temporary}", *sources], check=True)
        subprocess.run([*command, f"--grpc_python_out={temporary}", "pyudf.proto"], check=True)
        modules = {Path(name).stem + "_pb2" for name in sources}
        if not args.check:
            OUTPUT.mkdir(parents=True, exist_ok=True)
        differences = []
        for generated in sorted(temporary.glob("*_pb2*.py")):
            source = generated.read_text()
            # protoc emits top-level imports. Make them package-relative without
            # changing file descriptors, registering sys.modules aliases, or
            # vendoring google.protobuf's well-known types.
            source = re.sub(
                r"^import (\w+_pb2) as (\w+)$",
                lambda m: f"from . import {m[1]} as {m[2]}" if m[1] in modules else m[0],
                source,
                flags=re.MULTILINE,
            )
            for module in modules:
                source = source.replace(f"'{module}', _globals)", f"'{PACKAGE}.{module}', _globals)")
            target = OUTPUT / generated.name
            if not target.exists() or target.read_bytes() != source.encode("utf-8"):
                differences.append(generated.name)
                if not args.check:
                    target.write_bytes(source.encode("utf-8"))
        # Remove obsolete generated dependencies from earlier runtime wheels.
        generated_names = {path.name for path in temporary.glob("*_pb2*.py")}
        for stale in OUTPUT.glob("*_pb2*.py"):
            if stale.name not in generated_names:
                differences.append(stale.name + " (obsolete)")
                if not args.check:
                    stale.unlink()
        if args.check:
            if differences:
                raise SystemExit(
                    "Python PyUDF protocol products are out of date: "
                    + ", ".join(differences)
                    + "\nRun make generate-pyudf-proto and commit the generated files."
                )
            print("Python PyUDF protocol products are up to date")
        else:
            print("Generated Python PyUDF protocol: " + ", ".join(sources))


if __name__ == "__main__":
    main()
