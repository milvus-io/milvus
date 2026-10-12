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

"""Check stderr formatting without changing the test runner's root logger."""

import os
import subprocess
import sys
import unittest
from pathlib import Path


class LoggingTests(unittest.TestCase):
    def test_stderr_format_and_exception(self):
        root = Path(__file__).resolve().parents[1]
        code = """
import logging
import sys
import time
sys.path.insert(0, sys.argv[1])
time.tzset()
from milvus_pyudf_runtime.logging_config import configure_logging
configure_logging()
logger = logging.getLogger("milvus.pyudf")
logger.info('中文 "message"\\nnext')
logger.warning("warning %d", 42)
try:
    raise ValueError("example failure")
except ValueError:
    logger.exception("worker failed")
"""
        result = subprocess.run(
            [sys.executable, "-I", "-c", code, str(root)],
            env={**os.environ, "TZ": "Asia/Shanghai"},
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        )
        self.assertEqual(result.stdout, "")
        lines = result.stderr.splitlines()
        self.assertEqual(len(lines), 3)
        for line, level in zip(lines, ("INFO", "WARN", "ERROR")):
            self.assertRegex(
                line,
                rf"^\[\d{{4}}/\d{{2}}/\d{{2}} \d{{2}}:\d{{2}}:\d{{2}}\.\d{{3}} \+08:00\] "
                rf'\[{level}\] \[PyUDF\] \[<string>:\d+\] \[".*"\] \[pid=\d+\]',
            )
        self.assertIn(r"中文 \"message\"\nnext", lines[0])
        self.assertIn('["warning 42"]', lines[1])
        self.assertIn('[stack="Traceback', lines[2])
        self.assertIn("ValueError: example failure", lines[2])


if __name__ == "__main__":
    unittest.main()
