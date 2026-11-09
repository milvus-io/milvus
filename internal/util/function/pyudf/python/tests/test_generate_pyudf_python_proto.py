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

"""Exercise protocol drift detection against an isolated repository copy."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[6]
PRODUCTS = Path("internal/util/function/pyudf/python/milvus_pyudf_runtime/proto")
SCRIPT = Path("scripts/generate_pyudf_python_proto.py")
PROTO = Path("pkg/proto/pyudf.proto")


class ProtocolProductCheckTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="pyudf-codegen-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for relative in (SCRIPT, PROTO):
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / relative, target)
        shutil.copytree(ROOT / PRODUCTS, self.root / PRODUCTS, ignore=shutil.ignore_patterns("__pycache__"))

    def snapshot(self):
        return {
            str(path.relative_to(self.root)): (path.read_bytes(), path.stat().st_mtime_ns)
            for path in self.root.rglob("*")
            if path.is_file()
        }

    def run_generator(self, check=True):
        before = self.snapshot()
        result = subprocess.run(
            [sys.executable, str(self.root / SCRIPT), *(["--check"] if check else [])],
            capture_output=True,
            text=True,
            check=False,
        )
        if check:
            self.assertEqual(before, self.snapshot(), "check mode must not modify files")
        return result

    def assert_drift(self):
        result = self.run_generator()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("out of date", result.stderr)
        self.assertIn("make generate-pyudf-proto", result.stderr)

    def test_current_products(self):
        result = self.run_generator()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_changed_proto_and_regeneration(self):
        proto = self.root / PROTO
        with proto.open("a") as stream:
            stream.write("\nmessage CodegenDriftProbe { string value = 1; }\n")
        self.assert_drift()
        result = self.run_generator(check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.run_generator().returncode, 0)

    def test_modified_product(self):
        target = self.root / PRODUCTS / "pyudf_pb2_grpc.py"
        target.write_bytes(target.read_bytes() + b"\n# stale product\n")
        self.assert_drift()

    def test_missing_product(self):
        (self.root / PRODUCTS / "pyudf_pb2.py").unlink()
        self.assert_drift()

    def test_obsolete_product(self):
        (self.root / PRODUCTS / "obsolete_pb2.py").write_text("# obsolete\n")
        self.assert_drift()

    def test_missing_output_directory(self):
        shutil.rmtree(self.root / PRODUCTS)
        self.assert_drift()
        self.assertFalse((self.root / PRODUCTS).exists())


if __name__ == "__main__":
    unittest.main()
