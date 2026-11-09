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

"""Validate the installed wheel without allowing imports from the source tree."""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


@unittest.skipUnless(
    os.environ.get("PYUDF_TEST_INSTALLED_RUNTIME") == "1",
    "set PYUDF_TEST_INSTALLED_RUNTIME=1 after installing the runtime wheel",
)
@unittest.skipUnless(sys.platform in ("linux", "darwin"), "supervisor requires Linux or macOS")
class InstalledRuntimeTests(unittest.TestCase):
    def test_installed_wheel_starts_supervisor_and_executes_udf(self):
        probe = Path(__file__).with_name("installed_runtime_probe.py").resolve()
        source_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="pyudf-installed-") as directory:
            result = subprocess.run(
                [sys.executable, "-I", str(probe), str(source_root)],
                cwd=directory,
                capture_output=True,
                text=True,
                timeout=30,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
