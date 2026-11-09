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

"""Exercise concurrent loading, cancellation and retry in bounded subprocesses."""

import faulthandler
import subprocess
import sys
import tempfile
import threading
import types
import unittest
import zipfile
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
from milvus_pyudf_runtime import PyUDFLoader, PyUDFLoadError  # noqa: E402
from milvus_pyudf_runtime.errors import RequestInactive  # noqa: E402


class LoaderConcurrencyTests(unittest.TestCase):
    def probe(self, case):
        with tempfile.TemporaryDirectory(prefix="pyudf-load-") as directory:
            result = subprocess.run(
                [sys.executable, "-I", __file__, case, directory],
                capture_output=True,
                text=True,
                timeout=10,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_same_module_wait_is_cancellable_and_other_wheels_progress(self):
        self.probe("cancelled_module_wait")

    def test_cancelled_initializer_does_not_cancel_active_waiter(self):
        self.probe("cancelled_initializer")

    def test_failed_import_releases_locks_and_can_retry(self):
        self.probe("failed_import")

    def test_concurrent_package_claims_cannot_load_the_wrong_wheel(self):
        self.probe("package_collision")

    def test_failed_assertion_releases_blocked_import(self):
        self.probe("assertion_failure")


class Probe:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.number = 0
        self.loader = PyUDFLoader()
        self.support = types.ModuleType("load_probe_support")
        sys.modules[self.support.__name__] = self.support
        self.support.entered = threading.Event()
        self.support.release = threading.Event()
        self.support.imports = 0
        self.support.factories = []
        self.support.failure = ValueError

    @staticmethod
    def active():
        pass

    def wheel(self, source=None):
        self.number += 1
        package = f"load_probe_{self.number}"
        path = self.directory / f"{package}.whl"
        source = source or "class UDF:\n def transform_query(self,p,c): return c\ndef factory(ctx): return UDF()\n"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr(package + "/__init__.py", source)
            archive.writestr("probe.dist-info/entry_points.txt", f"[milvus.pyudf]\nmain = {package}:factory\n")
        return path

    def load(self, path, stage="stage", check=None, loader=None):
        return (loader or self.loader).load("test", path, stage, check or self.active)

    @staticmethod
    def assert_waiting(call):
        try:
            call.result(timeout=0.1)
        except FutureTimeoutError:
            return
        raise AssertionError("load finished while initialization was blocked")

    @contextmanager
    def blocked_threads(self):
        with ThreadPoolExecutor(2) as threads:
            try:
                yield threads
            finally:
                # Unblock imports before ThreadPoolExecutor waits for workers,
                # including when an assertion or unexpected exception escapes.
                self.support.release.set()

    @staticmethod
    def cancelled(event):
        def check():
            if event.is_set():
                raise RequestInactive()

        return check

    @staticmethod
    def expect_error(call, kind, message=None):
        try:
            call()
        except kind as exc:
            if message is not None:
                causes = []
                while exc is not None:
                    causes.append(str(exc))
                    exc = exc.__cause__
                assert any(message in item for item in causes), causes
        else:
            raise AssertionError(f"expected {kind.__name__}")

    def blocked_module(self, fail=False):
        self.support.fail = fail
        return self.wheel("""
import load_probe_support as s
s.imports += 1
s.entered.set()
s.release.wait()
if s.fail: raise s.failure('module failed')
class UDF:
 def transform_query(self,p,c): return c
def factory(ctx):
 s.factories.append(ctx.stage)
 return UDF()
""")

    def cancelled_module_wait(self):
        path = self.blocked_module()
        other_loader = PyUDFLoader()
        cancel = threading.Event()
        with self.blocked_threads() as threads:
            first = threads.submit(self.load, path, "first")
            assert self.support.entered.wait(2)
            second = threads.submit(self.load, path, "second", self.cancelled(cancel), other_loader)
            self.assert_waiting(second)
            self.load(self.wheel())  # An unrelated wheel imports while this one is blocked.
            cancel.set()
            self.expect_error(lambda: second.result(timeout=1), RequestInactive)
            assert all(entry.instance is None for entry in other_loader._entries.values())
            assert not self.support.factories
            self.support.release.set()
            first.result(timeout=2)
        self.load(path, "second", loader=other_loader)
        assert self.support.imports == 1
        assert self.support.factories == ["first", "second"]

    def cancelled_initializer(self):
        path = self.blocked_module()
        cancel = threading.Event()
        with self.blocked_threads() as threads:
            first = threads.submit(self.load, path, "same", self.cancelled(cancel))
            assert self.support.entered.wait(2)
            second = threads.submit(self.load, path, "same")
            self.assert_waiting(second)
            cancel.set()
            self.support.release.set()
            self.expect_error(lambda: first.result(timeout=2), RequestInactive)
            instance = second.result(timeout=2)
        assert self.support.imports == 1 and self.support.factories == ["same"]
        assert self.load(path, "same") is instance

    def failed_import(self):
        for failure in (ValueError, MemoryError, SystemExit):
            self.loader = PyUDFLoader()
            self.support.entered.clear()
            self.support.release.clear()
            self.support.imports = 0
            self.support.factories = []
            self.support.failure = failure
            path = self.blocked_module(fail=True)
            expected = PyUDFLoadError if failure is ValueError else failure
            with self.blocked_threads() as threads:
                first = threads.submit(self.load, path, "first")
                assert self.support.entered.wait(2)
                second = threads.submit(self.load, path, "second")
                self.assert_waiting(second)
                self.support.release.set()
                for call in (first, second):
                    self.expect_error(lambda: call.result(timeout=2), expected, "module failed")
            assert self.support.imports == 2 and not self.support.factories
            assert all(entry.instance is None and not entry.lock.locked() for entry in self.loader._entries.values())
            self.support.fail = False
            self.load(path)
            assert self.support.imports == 3 and self.support.factories == ["stage"]

    def assertion_failure(self):
        path = self.blocked_module()

        def fail_while_importing():
            with self.blocked_threads() as threads:
                threads.submit(self.load, path)
                assert self.support.entered.wait(2)
                raise AssertionError("injected assertion failure")

        self.expect_error(fail_while_importing, AssertionError, "injected assertion failure")
        assert self.support.release.is_set()
        assert self.support.factories == ["stage"]

    def package_collision(self):
        first = self.wheel("""
import load_probe_support as s
class UDF:
 def __init__(self, ctx): self.context=ctx
 def transform_query(self,p,c): return c
def factory(ctx):
 s.factories.append(ctx.wheel_path)
 return UDF(ctx)
""")
        second = self.directory / "conflicting.whl"
        second.write_bytes(first.read_bytes())
        barrier = threading.Barrier(2)

        def load(path):
            barrier.wait()
            return self.load(path, loader=PyUDFLoader())

        success = []
        failures = []
        with ThreadPoolExecutor(2) as threads:
            calls = [threads.submit(load, path) for path in (first, second)]
            for call in calls:
                try:
                    success.append(call.result(timeout=2))
                except PyUDFLoadError as exc:
                    failures.append(exc)
        assert len(success) == len(failures) == 1
        assert "already claimed" in str(failures[0])
        winner = success[0].instance.context.wheel_path
        assert self.support.factories == [winner]
        assert sys.modules[first.stem].__file__.startswith(winner + "/")


if __name__ == "__main__":
    faulthandler.dump_traceback_later(6)
    getattr(Probe(sys.argv[2]), sys.argv[1])()
    faulthandler.cancel_dump_traceback_later()
