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

"""Fault injection for supervisor tests; all files here are test-only signals."""

import atexit
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
from milvus_pyudf_runtime.config import parse_startup_args
from milvus_pyudf_runtime.supervisor import Supervisor

mode, directory = sys.argv[1:3]
directory = Path(directory)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(process)d] %(message)s")
args = sys.argv[3:]
if mode == "parent":
    child = subprocess.Popen([sys.executable, "-I", __file__, "normal", str(directory), *args])
    (directory / "supervisor.pid").write_text(str(child.pid))
    time.sleep(60)
    os._exit(2)


class ProbeSupervisor(Supervisor):
    def _spawn(self, index, now):
        assert "grpc" not in sys.modules and "pyarrow" not in sys.modules
        assert threading.active_count() == 1
        if sys.platform == "linux":
            assert len(list(Path("/proc/self/task").iterdir())) == 1
        if mode == "partial_fork" and index == 1:
            raise OSError("injected fork failure")
        if (
            mode == "replacement_fork_failure"
            and any(slot.failures for slot in self.slots)
            and not getattr(self, "failed_fork", False)
        ):
            self.failed_fork = True
            (directory / "fork-failed").touch()
            raise OSError("injected replacement fork failure")
        super()._spawn(index, now)
        if self.slots[index].pid:
            with (directory / "workers.jsonl").open("a") as output:
                output.write(json.dumps({"index": index, "pid": self.slots[index].pid}) + "\n")

    def _run_worker(self, index):
        atexit.register(lambda: (directory / f"atexit-{os.getpid()}").touch())
        if mode == "fail_worker" and index == 1:
            os._exit(31)
        if mode == "delayed" and index == 1:
            while not (directory / "release").exists():
                time.sleep(0.01)
            logging.info("test released delayed worker")
        if mode == "replacement_hang" and index == 0 and self.slots[index].failures:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            time.sleep(60)
            os._exit(2)
        from milvus_pyudf_runtime.worker import WorkerServer

        original = WorkerServer.start

        def start(worker, address=None):
            if mode == "ignore_term":
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
            port = original(worker, address)
            logging.info("test worker listening slot=%d", index)
            return port

        WorkerServer.start = start
        super()._run_worker(index)


code = 0
try:
    ProbeSupervisor(parse_startup_args(args)).run()
except BaseException:
    import traceback

    traceback.print_exc()
    code = 1
os._exit(code)
