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

"""Standard-library tests for the trusted PyUDF wheel loader."""

from __future__ import annotations

import importlib
import sys
import tempfile
import unittest
import zipfile
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import MappingProxyType

import pyarrow as pa

_RUNTIME_ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(_RUNTIME_ROOT))

from milvus_pyudf_runtime import (  # noqa: E402
    RUNTIME_API_VERSION,
    PyUDFContext,
    PyUDFExecutionError,
    PyUDFInstance,
    PyUDFLoader,
    PyUDFLoadError,
)


class WheelFixture:
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.number = 0

    def make(
        self,
        *,
        package: str | None = None,
        entry_points: str | None = None,
        module: str | None = None,
    ) -> Path:
        self.number += 1
        package = package or f"udf_package_{self.number}"
        entry_points = entry_points or f"[milvus.pyudf]\nmain = {package}:factory\n"
        path = self.directory / f"fixture-{self.number}.whl"
        source = module or (
            "class UDF:\n"
            "    def __init__(self, context):\n"
            "        self.context = context\n"
            "    def transform(self):\n"
            "        return None\n"
            "def factory(context):\n"
            "    return UDF(context)\n"
        )
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as wheel:
            wheel.writestr(f"{package}/__init__.py", source)
            wheel.writestr(
                "fixture-1.0.dist-info/METADATA",
                "Metadata-Version: 2.1\nName: fixture\nVersion: 1.0\n",
            )
            wheel.writestr("fixture-1.0.dist-info/entry_points.txt", entry_points)
        return path


class PyUDFRuntimeTest(unittest.TestCase):
    def test_runtime_api_version(self) -> None:
        self.assertEqual(RUNTIME_API_VERSION, 1)

    def test_loader_cache_and_process_import_ownership(self) -> None:
        wheel = self.fixtures.make()
        initial = self.load(wheel)
        self.assertIs(initial, self.load(wheel))
        self.assertIs(initial, self.udf_loader.load("alias", wheel.parent / "." / wheel.name, "RERANK", lambda: None))
        other_stage = self.udf_loader.load("fixture", wheel, "OTHER", lambda: None)
        self.assertIsNot(initial, other_stage)
        self.assertEqual(other_stage.instance.context.stage, "OTHER")
        other_loader = PyUDFLoader()
        other_instance = other_loader.load("fixture", wheel, "RERANK", lambda: None)
        self.assertIsNot(initial, other_instance)
        self.assertIs(type(initial.instance), type(other_instance.instance))
        self.assertEqual(1, sys.path.count(str(wheel.resolve())))

    def test_instance_execute_query_validates_outputs_and_preserves_cause(self) -> None:
        class UDF:
            def transform_query(self, params, columns):
                return [pa.array([columns[0][0].as_py() + params["delta"]])]

        loaded = PyUDFInstance(UDF())
        outputs = loaded.execute_query(MappingProxyType({"delta": 2}), [pa.array([3])])
        self.assertEqual(outputs[0].to_pylist(), [5])

        class FailingUDF:
            def transform_query(self, params, columns):
                raise ValueError("boom")

        failing = PyUDFInstance(FailingUDF())
        with self.assertRaisesRegex(PyUDFExecutionError, "raised") as captured:
            failing.execute_query(MappingProxyType({}), [pa.array([1])])
        self.assertIsInstance(captured.exception.__cause__, ValueError)

    def test_instance_execute_query_rejects_invalid_contracts(self) -> None:
        class UDF:
            def __init__(self, result):
                self.result = result

            def transform_query(self, params, columns):
                return self.result

        def loaded(result):
            return PyUDFInstance(UDF(result))

        class TransformUDF:
            def transform(self):
                pass

        params = MappingProxyType({})
        columns = [pa.array([1])]
        invalid = [
            (loaded(pa.array([1])), "return a sequence"),
            (loaded([[1]]), "must be pyarrow.Array"),
            (loaded([pa.chunked_array([[1]])]), "must be pyarrow.Array"),
            (loaded([pa.array([1]), pa.array([1, 2])]), "equal lengths"),
            (PyUDFInstance(TransformUDF()), "does not implement"),
        ]
        for value, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(PyUDFExecutionError, message):
                    value.execute_query(params, columns)

    def setUp(self) -> None:
        self.udf_loader = PyUDFLoader()
        self.tempdir = tempfile.TemporaryDirectory()
        self.fixtures = WheelFixture(Path(self.tempdir.name))
        self._modules: set[str] = set()
        self._paths: set[str] = set()
        self._module_locks = dict(PyUDFLoader._module_locks)
        self._claims = dict(PyUDFLoader._package_claims)
        self._wheel_paths = set(PyUDFLoader._wheel_paths)

    def tearDown(self) -> None:
        for name in self._modules:
            sys.modules.pop(name, None)
        for path in self._paths:
            while path in sys.path:
                sys.path.remove(path)
        PyUDFLoader._module_locks.clear()
        PyUDFLoader._module_locks.update(self._module_locks)
        PyUDFLoader._package_claims.clear()
        PyUDFLoader._package_claims.update(self._claims)
        PyUDFLoader._wheel_paths.clear()
        PyUDFLoader._wheel_paths.update(self._wheel_paths)
        self.tempdir.cleanup()

    def load(self, wheel: Path, **kwargs: object):
        package = kwargs.pop("package", None)
        if package is None:
            with zipfile.ZipFile(wheel) as archive:
                entry_points = archive.read("fixture-1.0.dist-info/entry_points.txt").decode()
            package = entry_points.split("=", 1)[1].strip().split(":", 1)[0]
        self._modules.add(str(package).split(".", 1)[0])
        self._paths.add(str(wheel.resolve()))
        return self.udf_loader.load(
            resource_name="fixture",
            local_path=wheel,
            stage="RERANK",
            check_active=lambda: None,
            **kwargs,
        )

    def test_context_is_immutable_and_only_contains_runtime_values(self) -> None:
        context = PyUDFContext(
            resource_name="resource",
            wheel_path="/tmp/resource.whl",
            stage="RERANK",
            logger=__import__("logging").getLogger("test"),
        )
        self.assertEqual(context.resource_name, "resource")
        self.assertIn("python_version", context.runtime_info)
        with self.assertRaises(FrozenInstanceError):
            context.stage = "SEARCH"  # type: ignore[misc]
        with self.assertRaises(TypeError):
            context.runtime_info["node"] = "x"  # type: ignore[index]
        self.assertNotIn("params", context.__dataclass_fields__)
        self.assertNotIn("udf_name", context.__dataclass_fields__)

    def test_loads_one_entry_point_and_creates_one_instance(self) -> None:
        wheel = self.fixtures.make()
        loaded = self.load(wheel)
        self.assertEqual("transform", loaded.callable_name)
        self.assertEqual(str(wheel.resolve()), loaded.instance.context.wheel_path)
        self.assertEqual(1, sys.path.count(str(wheel.resolve())))

    def test_load_does_not_scan_unused_wheel_members(self) -> None:
        wheel = self.fixtures.make()
        marker = b"unused-corruption-probe"
        with zipfile.ZipFile(wheel, "a", compression=zipfile.ZIP_STORED) as archive:
            archive.writestr("unused/model.bin", marker)

        contents = wheel.read_bytes()
        marker_offset = contents.index(marker)
        wheel.write_bytes(contents[:marker_offset] + b"X" + contents[marker_offset + 1 :])
        with zipfile.ZipFile(wheel) as archive:
            self.assertEqual("unused/model.bin", archive.testzip())

        loaded = self.load(wheel)
        self.assertEqual("transform", loaded.callable_name)

    def test_load_rejects_an_invalid_zip(self) -> None:
        wheel = Path(self.tempdir.name) / "invalid.whl"
        wheel.write_bytes(b"not a zip archive")
        with self.assertRaisesRegex(PyUDFLoadError, "cannot open local wheel"):
            self.load(wheel, package="invalid_package")

    def test_requires_exactly_one_entry_point(self) -> None:
        missing = self.fixtures.make(entry_points="[console_scripts]\nx = absent_package:factory\n")
        with self.assertRaisesRegex(PyUDFLoadError, "exactly one"):
            self.load(missing)
        multiple = self.fixtures.make(
            entry_points="[milvus.pyudf]\na = first_package:factory\nb = second_package:factory\n"
        )
        with self.assertRaisesRegex(PyUDFLoadError, "exactly one"):
            self.load(multiple)
        malformed = self.fixtures.make(entry_points="[milvus.pyudf]\na = malformed_package.factory\n")
        with self.assertRaisesRegex(PyUDFLoadError, "module:factory"):
            self.load(malformed)

    def test_factory_errors_and_non_callable_factories_are_reported(self) -> None:
        failing = self.fixtures.make(module="def factory(context):\n    raise ValueError('boom')\n")
        with self.assertRaisesRegex(PyUDFLoadError, "factory raised") as captured:
            self.load(failing)
        self.assertIsInstance(captured.exception.__cause__, ValueError)
        non_callable = self.fixtures.make(module="factory = 42\n")
        with self.assertRaisesRegex(PyUDFLoadError, "not callable"):
            self.load(non_callable)

    def test_exactly_one_callable_transform_interface_is_required(self) -> None:
        neither = self.fixtures.make(module="class UDF: pass\ndef factory(context): return UDF()\n")
        with self.assertRaisesRegex(PyUDFLoadError, "exactly one"):
            self.load(neither)
        both = self.fixtures.make(
            module=(
                "class UDF:\n"
                "    def transform(self): pass\n"
                "    def transform_query(self): pass\n"
                "def factory(context): return UDF()\n"
            )
        )
        with self.assertRaisesRegex(PyUDFLoadError, "exactly one"):
            self.load(both)
        query = self.fixtures.make(
            module=("class UDF:\n    def transform_query(self): pass\ndef factory(context): return UDF()\n")
        )
        self.assertEqual("transform_query", self.load(query).callable_name)

    def test_same_path_reuses_instance_and_different_path_is_rejected(self) -> None:
        first = self.fixtures.make(
            package="collision_pkg",
            entry_points="[milvus.pyudf]\nmain = collision_pkg:factory\n",
        )
        self._modules.add("collision_pkg")
        self._paths.add(str(first.resolve()))
        initial = self.load(first, package="collision_pkg")
        again = self.load(first, package="collision_pkg")
        self.assertIs(initial, again)
        second = self.fixtures.make(
            package="collision_pkg",
            entry_points="[milvus.pyudf]\nmain = collision_pkg:factory\n",
        )
        self._paths.add(str(second.resolve()))
        self.udf_loader = PyUDFLoader()  # Claims must survive across loader instances.
        with self.assertRaisesRegex(PyUDFLoadError, "already claimed"):
            self.load(second, package="collision_pkg")

    def test_requires_one_top_level_package_matching_the_entry_point(self) -> None:
        multiple = self.fixtures.make(package="udf_package")
        with zipfile.ZipFile(multiple, "a", compression=zipfile.ZIP_STORED) as wheel:
            wheel.writestr("vendored_dependency/__init__.py", "VALUE = 1\n")
        with self.assertRaisesRegex(PyUDFLoadError, "exactly one top-level Python import root"):
            self.load(multiple, package="udf_package")

        mismatch = self.fixtures.make(
            package="wheel_package",
            entry_points="[milvus.pyudf]\nmain = other_package:factory\n",
        )
        with self.assertRaisesRegex(PyUDFLoadError, "matching the milvus.pyudf entry point"):
            self.load(mismatch, package="other_package")

    def test_dependencies_from_site_packages_are_not_claimed(self) -> None:
        wheel = self.fixtures.make(
            package="dependency_consumer",
            entry_points="[milvus.pyudf]\nmain = dependency_consumer:factory\n",
            module=("import pyarrow\nclass UDF:\n    def transform(self): pass\ndef factory(context): return UDF()\n"),
        )
        self.load(wheel, package="dependency_consumer")
        self.assertIn("dependency_consumer", PyUDFLoader._package_claims)
        self.assertNotIn("pyarrow", PyUDFLoader._package_claims)

    def test_close_is_optional_validated_and_propagates_failure(self) -> None:
        optional = self.fixtures.make()
        self.load(optional).close()
        bad = self.fixtures.make(
            module=("class UDF:\n    close = 1\n    def transform(self): pass\ndef factory(context): return UDF()\n")
        )
        with self.assertRaisesRegex(PyUDFLoadError, "close"):
            self.load(bad)
        closing = self.fixtures.make(
            module=(
                "calls = []\n"
                "class UDF:\n"
                "    def transform(self): pass\n"
                "    def close(self):\n"
                "        calls.append('closed')\n"
                "        raise RuntimeError('close failed')\n"
                "def factory(context): return UDF()\n"
            )
        )
        loaded = self.load(closing)
        with self.assertRaisesRegex(PyUDFLoadError, "close"):
            loaded.close()
        with zipfile.ZipFile(closing) as archive:
            package = (
                archive.read("fixture-1.0.dist-info/entry_points.txt")
                .decode()
                .split("=", 1)[1]
                .strip()
                .split(":", 1)[0]
            )
        module = importlib.import_module(package)
        self.assertEqual(["closed"], module.calls)

    def test_wrapper_validation_failure_closes_created_instance(self) -> None:
        wheel = self.fixtures.make(
            module=(
                "created = []\n"
                "class UDF:\n"
                "    def transform(self): pass\n"
                "    def transform_query(self): pass\n"
                "    def close(self): created.append('closed')\n"
                "def factory(context): return UDF()\n"
            )
        )
        with self.assertRaisesRegex(PyUDFLoadError, "exactly one"):
            self.load(wheel)
        with zipfile.ZipFile(wheel) as archive:
            package = (
                archive.read("fixture-1.0.dist-info/entry_points.txt")
                .decode()
                .split("=", 1)[1]
                .strip()
                .split(":", 1)[0]
            )
        module = importlib.import_module(package)
        self.assertEqual(["closed"], module.created)


if __name__ == "__main__":
    unittest.main()
