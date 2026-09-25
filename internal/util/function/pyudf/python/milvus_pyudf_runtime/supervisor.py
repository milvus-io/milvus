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

"""Linux/macOS process supervision. Never import gRPC/PyArrow or start threads here."""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from dataclasses import dataclass

from .config import ServerConfig, parse_startup_args
from .logging_config import configure_logging

_LOG = logging.getLogger("milvus.pyudf.supervisor")
_TICK = 0.05


@dataclass
class WorkerSlot:
    pid: int = 0
    started_at: float = 0
    restart_at: float = 0
    failures: int = 0


class Supervisor:
    def __init__(self, config: ServerConfig):
        config.validate()
        self.config = config
        self.slots = [WorkerSlot() for _ in range(self.config.worker_count)]
        self.stopping = False

    def _stop_signal(self, signum, frame):
        self.stopping = True

    def _run_worker(self, index: int):
        # Import worker dependencies only in the forked child.
        from .worker import WorkerServer

        WorkerServer(self.config).run()

    def _spawn(self, index: int, now: float):
        if self.stopping:
            return
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT})
        try:
            pid = os.fork()
            if pid == 0:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                signal.signal(signal.SIGINT, signal.SIG_DFL)
                signal.pthread_sigmask(signal.SIG_SETMASK, previous)
                try:
                    self._run_worker(index)
                except BaseException:
                    _LOG.exception("worker initialization failed")
                os._exit(1)
            slot = self.slots[index]
            slot.pid = pid
            slot.started_at = now
            _LOG.info("worker process started slot=%d pid=%d", index, pid)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)

    @staticmethod
    def _signal(pid: int, signum: int):
        try:
            os.kill(pid, signum)
        except ProcessLookupError:
            pass  # Still waitpid: sending a signal is not proof of reaping.

    def _reap(self, now: float):
        for index, slot in enumerate(self.slots):
            if not slot.pid:
                continue
            pid, status = os.waitpid(slot.pid, os.WNOHANG)
            if not pid:
                continue
            _LOG.info("worker reaped slot=%d pid=%d status=%d", index, pid, status)
            slot.pid = 0
            if self.stopping:
                continue
            self._schedule_restart(slot, now)

    @staticmethod
    def _schedule_restart(slot: WorkerSlot, now: float):
        slot.failures = min(slot.failures + 1, 6)
        slot.restart_at = now + min(0.1 * 2 ** (slot.failures - 1), 3.2)

    def _maintain(self, now: float):
        self._reap(now)
        for index, slot in enumerate(self.slots):
            if self.stopping:
                return
            if not slot.pid:
                if now >= slot.restart_at:
                    try:
                        self._spawn(index, now)
                    except OSError:
                        self._schedule_restart(slot, now)
                        _LOG.exception("worker fork failed slot=%d; will retry", index)
            elif now - slot.started_at >= 10:
                slot.failures = 0

    def _shutdown(self):
        self.stopping = True
        for slot in self.slots:
            if slot.pid:
                self._signal(slot.pid, signal.SIGTERM)
        deadline = time.monotonic() + self.config.shutdown_timeout_ms / 1000
        while any(slot.pid for slot in self.slots):
            self._reap(time.monotonic())
            if time.monotonic() >= deadline:
                for slot in self.slots:
                    if slot.pid:
                        self._signal(slot.pid, signal.SIGKILL)
            if any(slot.pid for slot in self.slots):
                time.sleep(_TICK)

    def run(self):
        previous = {sig: signal.signal(sig, self._stop_signal) for sig in (signal.SIGTERM, signal.SIGINT)}
        previous[signal.SIGCHLD] = signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        try:
            while not self.stopping:
                now = time.monotonic()
                self._maintain(now)
                time.sleep(_TICK)
        finally:
            try:
                self._shutdown()
            finally:
                for sig, handler in previous.items():
                    signal.signal(sig, handler)


def main():
    configure_logging()
    config = parse_startup_args(sys.argv[1:])
    code = 0
    try:
        Supervisor(config).run()
    except BaseException:
        _LOG.exception("PyUDF supervisor failed")
        code = 1
    # Worker shutdown already used waitpid; do not execute inherited user hooks.
    os._exit(code)


if __name__ == "__main__":
    main()
