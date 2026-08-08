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

"""Load a PyUDF factory directly from a local wheel without pip."""

from __future__ import annotations

import configparser
import importlib
import logging
import os
import sys
from threading import RLock
from typing import Any, Mapping, Sequence
import zipfile

from .context import PyUDFContext
from .wrapper import LoadedPyUDFInstance

_ENTRY_POINT_GROUP = "milvus.pudf"
_SERIALIZED = "serialized"
_REENTRANT = "reentrant"

# sys.modules cannot safely unload a wheel's modules. These process-lifetime
# claims prevent a later wheel from silently using previously imported modules.
_PACKAGE_CLAIMS: dict[str, object] = {}
_WHEEL_PATHS: set[str] = set()
_LOCK = RLock()


def _module_origins() -> dict[str, str]:
    origins: dict[str, str] = {}
    for name, module in tuple(sys.modules.items()):
        if module is None:
            continue
        origin = getattr(module, "__file__", None)
        if isinstance(origin, str):
            origins[name] = os.path.realpath(origin)
    return origins


def _claim_wheel_modules(
    identity: object,
    wheel_path: str,
    before: Mapping[str, str],
) -> None:
    wheel_prefix = wheel_path + os.sep
    for name, origin in _module_origins().items():
        if before.get(name) == origin:
            continue
        if origin == wheel_path or origin.startswith(wheel_prefix):
            _claim_package(name.split(".", 1)[0], identity)


def _reject_cached_module(module_name: str, wheel_path: str, identity: object) -> None:
    top_level_package = module_name.split(".", 1)[0]
    _claim_package(top_level_package, identity)
    cached = sys.modules.get(top_level_package)
    if cached is None:
        return
    origin = getattr(cached, "__file__", None)
    real_origin = os.path.realpath(origin) if isinstance(origin, str) else ""
    if real_origin != wheel_path and not real_origin.startswith(wheel_path + os.sep):
        raise PyUDFLoadError(
            f"top-level package {top_level_package!r} is already imported "
            "from outside this PyUDF wheel"
        )


class PyUDFLoadError(RuntimeError):
    """Raised when a wheel does not satisfy the PyUDF load contract."""


def _identity_key(resource_identity: object | None, wheel_path: str) -> object:
    return os.path.realpath(wheel_path) if resource_identity is None else resource_identity


def _validate_wheel_path(wheel_path: str | os.PathLike[str]) -> str:
    path = os.path.realpath(os.fspath(wheel_path))
    if not path.lower().endswith(".whl"):
        raise PyUDFLoadError("local wheel path must end with .whl")
    try:
        with zipfile.ZipFile(path) as wheel:
            if wheel.testzip() is not None:
                raise PyUDFLoadError("local wheel contains corrupt data")
    except (OSError, zipfile.BadZipFile) as exc:
        raise PyUDFLoadError(f"cannot open local wheel {path!r}") from exc
    return path


def _entry_point_from_wheel(wheel_path: str) -> tuple[str, str]:
    try:
        with zipfile.ZipFile(wheel_path) as wheel:
            metadata_names = [
                name
                for name in wheel.namelist()
                if name.endswith(".dist-info/entry_points.txt")
            ]
            if len(metadata_names) != 1:
                raise PyUDFLoadError(
                    "wheel must contain exactly one entry_points.txt metadata file"
                )
            entry_points_text = wheel.read(metadata_names[0]).decode("utf-8")
        parser = configparser.ConfigParser(interpolation=None)
        parser.optionxform = str
        parser.read_string(entry_points_text)
        values = (
            list(parser[_ENTRY_POINT_GROUP].values())
            if parser.has_section(_ENTRY_POINT_GROUP)
            else []
        )
    except PyUDFLoadError:
        raise
    except (OSError, UnicodeDecodeError, configparser.Error) as exc:
        raise PyUDFLoadError(f"cannot read wheel metadata from {wheel_path!r}") from exc

    if len(values) != 1:
        raise PyUDFLoadError(
            "wheel must provide exactly one milvus.pudf entry point "
            f"(found {len(values)})"
        )

    value = values[0]
    if value.count(":") != 1:
        raise PyUDFLoadError("milvus.pudf entry point must be module:factory")
    module_name, factory_name = (part.strip() for part in value.split(":"))
    if not module_name or not factory_name or "." in factory_name:
        raise PyUDFLoadError("milvus.pudf entry point must be module:factory")
    if not all(piece.isidentifier() for piece in module_name.split(".")):
        raise PyUDFLoadError("entry point module is not a normal Python module name")
    if not factory_name.isidentifier():
        raise PyUDFLoadError("entry point factory is not a normal Python identifier")
    return module_name, factory_name


def _claim_package(top_level_package: str, identity: object) -> None:
    if top_level_package not in _PACKAGE_CLAIMS:
        _PACKAGE_CLAIMS[top_level_package] = identity
    elif _PACKAGE_CLAIMS[top_level_package] != identity:
        raise PyUDFLoadError(
            f"top-level package {top_level_package!r} is already claimed "
            "by a different PyUDF resource"
        )


def _controlled_sys_path_add(wheel_path: str) -> None:
    if wheel_path not in _WHEEL_PATHS:
        # Put the trusted local artifact ahead of ambient imports and retain it
        # process-wide so lazy imports and package resources keep working.
        sys.path.insert(0, wheel_path)
        _WHEEL_PATHS.add(wheel_path)


def _load_factory(module_name: str, factory_name: str) -> Any:
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise PyUDFLoadError(f"cannot import PyUDF entry module {module_name!r}") from exc
    try:
        factory = getattr(module, factory_name)
    except AttributeError as exc:
        raise PyUDFLoadError(
            f"PyUDF entry factory {module_name}:{factory_name} does not exist"
        ) from exc
    if not callable(factory):
        raise PyUDFLoadError(f"PyUDF entry factory {module_name}:{factory_name} is not callable")
    return factory


def _capability(instance: Any) -> str:
    try:
        declared = instance.milvus_udf_capabilities
    except AttributeError:
        return _SERIALIZED
    if not isinstance(declared, Mapping):
        raise PyUDFLoadError("milvus_udf_capabilities must be a mapping")
    if set(declared) != {"concurrency_mode"}:
        raise PyUDFLoadError(
            "milvus_udf_capabilities must contain only concurrency_mode"
        )
    mode = declared["concurrency_mode"]
    if mode not in (_SERIALIZED, _REENTRANT):
        raise PyUDFLoadError("milvus_udf_capabilities has an invalid concurrency_mode")
    if mode == _REENTRANT:
        raise PyUDFLoadError("reentrant PyUDF capability is not supported")
    return mode


def _wrap_instance(instance: Any) -> LoadedPyUDFInstance:
    transform = getattr(instance, "transform", None)
    transform_query = getattr(instance, "transform_query", None)
    if callable(transform) == callable(transform_query):
        raise PyUDFLoadError(
            "PyUDF instance must implement exactly one callable transform or transform_query"
        )
    close = getattr(instance, "close", None)
    if close is not None and not callable(close):
        raise PyUDFLoadError("PyUDF close attribute must be callable when present")
    return LoadedPyUDFInstance(
        instance=instance,
        callable_name="transform" if callable(transform) else "transform_query",
        concurrency_mode=_capability(instance),
        close=close,
    )


def load_instances(
    resource_name: str,
    local_path: str | os.PathLike[str],
    stage: str,
    instance_count: int,
    resource_identity: object | None = None,
) -> Sequence[LoadedPyUDFInstance]:
    """Create serialized PyUDF instances from one exact local wheel.

    Resource identity is intentionally opaque to make this callable suitable for
    the C++ bridge, which owns the request identity.  The registry and sys.path
    additions deliberately outlive individual resources.
    """
    if not isinstance(instance_count, int) or isinstance(instance_count, bool) or instance_count <= 0:
        raise PyUDFLoadError("instance_count must be a positive integer")
    if not isinstance(resource_name, str) or not resource_name.strip():
        raise PyUDFLoadError("resource_name must be nonblank")
    if not isinstance(stage, str) or not stage.strip():
        raise PyUDFLoadError("stage must be nonblank")

    wheel_path = _validate_wheel_path(local_path)
    identity = _identity_key(resource_identity, wheel_path)
    module_name, factory_name = _entry_point_from_wheel(wheel_path)
    top_level_package = module_name.split(".", 1)[0]
    context = PyUDFContext(
        resource_name=resource_name,
        wheel_path=wheel_path,
        stage=stage,
        logger=logging.getLogger("milvus.pyudf"),
    )

    with _LOCK:
        _reject_cached_module(module_name, wheel_path, identity)
        before_modules = _module_origins()
        _controlled_sys_path_add(wheel_path)
        try:
            factory = _load_factory(module_name, factory_name)
        finally:
            _claim_wheel_modules(identity, wheel_path, before_modules)
        loaded: list[LoadedPyUDFInstance] = []
        try:
            for _ in range(instance_count):
                try:
                    instance = factory(context)
                except Exception as exc:
                    raise PyUDFLoadError("PyUDF factory raised an exception") from exc
                loaded.append(_wrap_instance(instance))
        except Exception:
            _cleanup_instances(loaded)
            raise

        modes = {item.concurrency_mode for item in loaded}
        if len(modes) != 1:
            _cleanup_instances(loaded)
            raise PyUDFLoadError("all PyUDF instances must declare the same capability")
        return tuple(loaded)


def _cleanup_instances(instances: Sequence[LoadedPyUDFInstance]) -> None:
    """Best-effort rollback for a partial load; never mask the load error."""
    for loaded in reversed(instances):
        if loaded.close is not None:
            try:
                loaded.close()
            except Exception:
                pass


def close_instances(instances: Sequence[LoadedPyUDFInstance]) -> None:
    """Close every instance, then raise the first close exception if any.

    This is idempotent only at the resource-handle layer.  The C++ owner calls
    it once when deleting a resource; this helper ensures one bad close never
    prevents remaining instances from being released.
    """
    first_error: BaseException | None = None
    for loaded in instances:
        if loaded.close is not None:
            try:
                loaded.close()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
    if first_error is not None:
        raise PyUDFLoadError("PyUDF close raised an exception") from first_error
