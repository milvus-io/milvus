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

from dataclasses import FrozenInstanceError
import importlib
from types import MappingProxyType
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile

import pyarrow as pa
from pyarrow.cffi import ffi

_RUNTIME_ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(_RUNTIME_ROOT))

from milvus_pyudf_runtime import (  # noqa: E402
    RUNTIME_API_VERSION,
    PyUDFArrowError,
    PyUDFContext,
    PyUDFExecutionError,
    PyUDFLoadError,
    close_instances,
    export_array,
    freeze_params,
    import_array,
    load_instances,
    make_chunked_array,
    run_transform_query,
)
from milvus_pyudf_runtime import loader  # noqa: E402


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
        entry_points = entry_points or f"[milvus.pudf]\nmain = {package}:factory\n"
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

    def test_freeze_params_is_recursively_immutable(self) -> None:
        params = freeze_params(
            {
                "bool": True,
                "int": 42,
                "double": 1.25,
                "string": "value",
                "bytes": b"\x00\xff",
                "array": [1, {"nested": "value"}],
            }
        )
        self.assertIsInstance(params, MappingProxyType)
        self.assertEqual(params["array"][0], 1)
        self.assertEqual(params["array"][1]["nested"], "value")
        self.assertIsInstance(params["array"][1], MappingProxyType)
        with self.assertRaises(TypeError):
            params["new"] = 1  # type: ignore[index]
        with self.assertRaises(TypeError):
            params["array"][1]["nested"] = "changed"  # type: ignore[index]

    def test_run_transform_query_validates_outputs_and_preserves_cause(self) -> None:
        class UDF:
            def transform_query(self, params, columns):
                return [pa.array([columns[0][0].as_py() + params["delta"]])]

        loaded = __import__("milvus_pyudf_runtime").LoadedPyUDFInstance(
            instance=UDF(),
            callable_name="transform_query",
            concurrency_mode="serialized",
            close=None,
        )
        outputs = run_transform_query(
            loaded,
            freeze_params({"delta": 2}),
            [pa.array([3])],
            1,
        )
        self.assertEqual(outputs[0].to_pylist(), [5])

        class FailingUDF:
            def transform_query(self, params, columns):
                raise ValueError("boom")

        failing = __import__("milvus_pyudf_runtime").LoadedPyUDFInstance(
            instance=FailingUDF(),
            callable_name="transform_query",
            concurrency_mode="serialized",
            close=None,
        )
        with self.assertRaisesRegex(PyUDFExecutionError, "raised") as captured:
            run_transform_query(failing, freeze_params({}), [pa.array([1])], 1)
        self.assertIsInstance(captured.exception.__cause__, ValueError)

    def test_run_transform_query_rejects_invalid_contracts(self) -> None:
        class UDF:
            def __init__(self, result):
                self.result = result

            def transform_query(self, params, columns):
                return self.result

        def loaded(result, callable_name="transform_query"):
            return __import__("milvus_pyudf_runtime").LoadedPyUDFInstance(
                instance=UDF(result),
                callable_name=callable_name,
                concurrency_mode="serialized",
                close=None,
            )

        params = freeze_params({})
        columns = [pa.array([1])]
        invalid = [
            (loaded(pa.array([1])), "return a sequence"),
            (loaded([[1]]), "must be pyarrow.Array"),
            (loaded([pa.chunked_array([[1]])]), "must be pyarrow.Array"),
            (loaded([pa.array([1, 2])]), "2 rows, expected 1"),
            (loaded([pa.array([1])], "transform"), "does not implement"),
        ]
        for value, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(PyUDFExecutionError, message):
                    run_transform_query(value, params, columns, 1)

    def test_arrow_c_data_round_trip_preserves_views_and_nulls(self) -> None:
        base = pa.array(["alpha", "beta", None])
        value = base.slice(1, 2)
        c_array = ffi.new("struct ArrowArray*")
        c_schema = ffi.new("struct ArrowSchema*")
        array_address = int(ffi.cast("uintptr_t", c_array))
        schema_address = int(ffi.cast("uintptr_t", c_schema))

        export_array(value, array_address, schema_address)
        imported = import_array(array_address, schema_address)
        self.assertEqual(imported.to_pylist(), ["beta", None])
        self.assertEqual(imported.offset, 1)
        self.assertEqual(c_array.release, ffi.NULL)
        self.assertEqual(c_schema.release, ffi.NULL)

    def test_arrow_chunked_builder_requires_arrow_arrays_of_one_type(self) -> None:
        chunked = make_chunked_array([pa.array([1, None]), pa.array([], type=pa.int64())])
        self.assertEqual(chunked.to_pylist(), [1, None])
        self.assertEqual(chunked.num_chunks, 2)
        with self.assertRaisesRegex(PyUDFArrowError, "only pyarrow.Array"):
            make_chunked_array([pa.array([1]), [2]])  # type: ignore[list-item]
        with self.assertRaisesRegex(PyUDFArrowError, "cannot build"):
            make_chunked_array([pa.array([1]), pa.array(["x"])])

    def test_arrow_helpers_reject_invalid_addresses_and_objects(self) -> None:
        with self.assertRaisesRegex(PyUDFArrowError, "positive integer"):
            import_array(0, 1)
        with self.assertRaisesRegex(PyUDFArrowError, "positive integer"):
            export_array(pa.array([1]), 1, False)  # type: ignore[arg-type]
        with self.assertRaisesRegex(PyUDFArrowError, "pyarrow.Array"):
            export_array([1], 1, 1)  # type: ignore[arg-type]

    def test_arrow_import_failure_consumes_descriptors(self) -> None:
        value = pa.array([1, 2])
        c_array = ffi.new("struct ArrowArray*")
        c_schema = ffi.new("struct ArrowSchema*")
        array_address = int(ffi.cast("uintptr_t", c_array))
        schema_address = int(ffi.cast("uintptr_t", c_schema))
        value._export_to_c(array_address, schema_address)
        invalid_format = ffi.new("char[]", b"invalid")
        c_schema.format = invalid_format

        with self.assertRaises(pa.ArrowInvalid):
            import_array(array_address, schema_address)
        self.assertEqual(c_array.release, ffi.NULL)
        self.assertEqual(c_schema.release, ffi.NULL)

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.fixtures = WheelFixture(Path(self.tempdir.name))
        self._modules: set[str] = set()
        self._paths: set[str] = set()
        self._claims = dict(loader._PACKAGE_CLAIMS)
        self._wheel_paths = set(loader._WHEEL_PATHS)

    def tearDown(self) -> None:
        for name in self._modules:
            sys.modules.pop(name, None)
        for path in self._paths:
            while path in sys.path:
                sys.path.remove(path)
        loader._PACKAGE_CLAIMS.clear()
        loader._PACKAGE_CLAIMS.update(self._claims)
        loader._WHEEL_PATHS.clear()
        loader._WHEEL_PATHS.update(self._wheel_paths)
        self.tempdir.cleanup()

    def load(self, wheel: Path, **kwargs: object):
        package = kwargs.pop("package", None)
        if package is None:
            with zipfile.ZipFile(wheel) as archive:
                entry_points = archive.read("fixture-1.0.dist-info/entry_points.txt").decode()
            package = entry_points.split("=", 1)[1].strip().split(":", 1)[0]
        self._modules.add(str(package).split(".", 1)[0])
        self._paths.add(str(wheel.resolve()))
        return load_instances(
            resource_name="fixture",
            local_path=wheel,
            stage="RERANK",
            instance_count=kwargs.pop("instance_count", 1),
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

    def test_loads_one_entry_point_and_creates_requested_instances(self) -> None:
        wheel = self.fixtures.make()
        instances = self.load(wheel, instance_count=3)
        self.assertEqual(3, len(instances))
        self.assertTrue(all(item.callable_name == "transform" for item in instances))
        self.assertTrue(all(item.concurrency_mode == "serialized" for item in instances))
        self.assertEqual(str(wheel.resolve()), instances[0].instance.context.wheel_path)
        self.assertEqual(1, sys.path.count(str(wheel.resolve())))

    def test_requires_exactly_one_entry_point(self) -> None:
        missing = self.fixtures.make(entry_points="[console_scripts]\nx = absent_package:factory\n")
        with self.assertRaisesRegex(PyUDFLoadError, "exactly one"):
            self.load(missing)
        multiple = self.fixtures.make(
            entry_points="[milvus.pudf]\na = first_package:factory\nb = second_package:factory\n"
        )
        with self.assertRaisesRegex(PyUDFLoadError, "exactly one"):
            self.load(multiple)
        malformed = self.fixtures.make(entry_points="[milvus.pudf]\na = malformed_package.factory\n")
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
            module=(
                "class UDF:\n"
                "    def transform_query(self): pass\n"
                "def factory(context): return UDF()\n"
            )
        )
        self.assertEqual("transform_query", self.load(query)[0].callable_name)

    def test_serialized_capability_is_accepted_and_other_declarations_rejected(self) -> None:
        serialized = self.fixtures.make(
            module=(
                "class UDF:\n"
                "    milvus_udf_capabilities = {'concurrency_mode': 'serialized'}\n"
                "    def transform(self): pass\n"
                "def factory(context): return UDF()\n"
            )
        )
        self.assertEqual("serialized", self.load(serialized)[0].concurrency_mode)
        malformed = self.fixtures.make(
            module=(
                "class UDF:\n"
                "    milvus_udf_capabilities = 'serialized'\n"
                "    def transform(self): pass\n"
                "def factory(context): return UDF()\n"
            )
        )
        with self.assertRaisesRegex(PyUDFLoadError, "mapping"):
            self.load(malformed)
        reentrant = self.fixtures.make(
            module=(
                "class UDF:\n"
                "    milvus_udf_capabilities = {'concurrency_mode': 'reentrant'}\n"
                "    def transform(self): pass\n"
                "def factory(context): return UDF()\n"
            )
        )
        with self.assertRaisesRegex(PyUDFLoadError, "reentrant"):
            self.load(reentrant)

    def test_same_identity_reuses_package_and_different_identity_is_rejected(self) -> None:
        first = self.fixtures.make(
            package="collision_pkg",
            entry_points="[milvus.pudf]\nmain = collision_pkg:factory\n",
        )
        self._modules.add("collision_pkg")
        self._paths.add(str(first.resolve()))
        initial = self.load(first, resource_identity=(7, "first"), package="collision_pkg")
        again = self.load(first, resource_identity=(7, "first"), package="collision_pkg")
        self.assertIs(initial[0].instance.__class__, again[0].instance.__class__)
        second = self.fixtures.make(
            package="collision_pkg",
            entry_points="[milvus.pudf]\nmain = collision_pkg:factory\n",
        )
        self._paths.add(str(second.resolve()))
        with self.assertRaisesRegex(PyUDFLoadError, "already claimed"):
            self.load(second, resource_identity=(8, "second"), package="collision_pkg")

    def test_imported_dependency_package_is_claimed_by_its_first_wheel(self) -> None:
        first = self.fixtures.make(
            package="dependency_owner",
            entry_points="[milvus.pudf]\nmain = dependency_owner:factory\n",
            module=(
                "from shared_dependency import SharedUDF\n"
                "def factory(context): return SharedUDF()\n"
            ),
        )
        with zipfile.ZipFile(first, "a", compression=zipfile.ZIP_STORED) as wheel:
            wheel.writestr(
                "shared_dependency/__init__.py",
                "class SharedUDF:\n    def transform(self): pass\n",
            )
        self._modules.update({"dependency_owner", "shared_dependency"})
        self._paths.add(str(first.resolve()))
        self.load(
            first,
            resource_identity=(9, "dependency-owner"),
            package="dependency_owner",
        )

        second = self.fixtures.make(
            package="shared_dependency",
            entry_points="[milvus.pudf]\nmain = shared_dependency:factory\n",
        )
        self._paths.add(str(second.resolve()))
        with self.assertRaisesRegex(PyUDFLoadError, "already claimed"):
            self.load(
                second,
                resource_identity=(10, "dependency-consumer"),
                package="shared_dependency",
            )

    def test_close_is_optional_validated_and_attempts_all_instances(self) -> None:
        optional = self.fixtures.make()
        self.assertIsNone(self.load(optional)[0].close)
        bad = self.fixtures.make(
            module=(
                "class UDF:\n"
                "    close = 1\n"
                "    def transform(self): pass\n"
                "def factory(context): return UDF()\n"
            )
        )
        with self.assertRaisesRegex(PyUDFLoadError, "close"):
            self.load(bad)
        closing = self.fixtures.make(
            module=(
                "created = 0\n"
                "calls = []\n"
                "class UDF:\n"
                "    def __init__(self, context):\n"
                "        global created\n"
                "        self.index = created\n"
                "        created += 1\n"
                "    def transform(self): pass\n"
                "    def close(self):\n"
                "        calls.append(self.index)\n"
                "        if self.index == 0: raise RuntimeError('first')\n"
                "def factory(context): return UDF(context)\n"
            )
        )
        instances = self.load(closing, instance_count=2)
        with self.assertRaisesRegex(PyUDFLoadError, "close"):
            close_instances(instances)
        with zipfile.ZipFile(closing) as archive:
            package = archive.read("fixture-1.0.dist-info/entry_points.txt").decode().split("=", 1)[1].strip().split(":", 1)[0]
        module = importlib.import_module(package)
        self.assertEqual([0, 1], module.calls)

    def test_partial_factory_failure_closes_previously_created_instances(self) -> None:
        wheel = self.fixtures.make(
            module=(
                "created = []\n"
                "class UDF:\n"
                "    def transform(self): pass\n"
                "    def close(self): created.append('closed')\n"
                "def factory(context):\n"
                "    if created: raise RuntimeError('second')\n"
                "    created.append('created')\n"
                "    return UDF()\n"
            )
        )
        with self.assertRaisesRegex(PyUDFLoadError, "factory raised"):
            self.load(wheel, instance_count=2)
        with zipfile.ZipFile(wheel) as archive:
            package = archive.read("fixture-1.0.dist-info/entry_points.txt").decode().split("=", 1)[1].strip().split(":", 1)[0]
        module = importlib.import_module(package)
        self.assertEqual(["created", "closed"], module.created)


if __name__ == "__main__":
    unittest.main()
