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
import zipfile
from threading import RLock
from typing import Any

from .context import PyUDFContext
from .wrapper import LoadedPyUDFInstance

_ENTRY_POINT_GROUP = "milvus.pyudf"
# sys.modules cannot safely unload a wheel's entry package. These process-lifetime
# claims prevent another PyUDF resource from silently reusing the same package.
_PACKAGE_CLAIMS: dict[str, object] = {}
_WHEEL_PATHS: set[str] = set()
_LOCK = RLock()


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
            f"top-level package {top_level_package!r} is already imported from outside this PyUDF wheel"
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
        # Validate the ZIP directory without scanning every member. Required
        # metadata, modules, and resources are decompressed and checked when
        # they are read, avoiding a full pass over large model wheels here.
        with zipfile.ZipFile(path):
            pass
    except (OSError, zipfile.BadZipFile) as exc:
        raise PyUDFLoadError(f"cannot open local wheel {path!r}") from exc
    return path


def _wheel_python_roots(member_names: list[str]) -> set[str]:
    roots: set[str] = set()
    for member_name in member_names:
        normalized = member_name.rstrip("/")
        if not normalized:
            continue
        first, separator, _ = normalized.partition("/")
        if first.endswith((".dist-info", ".data")):
            continue
        if not separator and first.endswith(".py"):
            module_name = first.removesuffix(".py")
            if module_name.isidentifier():
                roots.add(module_name)
            continue
        if separator and first.isidentifier() and normalized.endswith(".py"):
            roots.add(first)
    return roots


def _entry_point_from_wheel(wheel_path: str) -> tuple[str, str]:
    try:
        with zipfile.ZipFile(wheel_path) as wheel:
            member_names = wheel.namelist()
            metadata_names = [name for name in member_names if name.endswith(".dist-info/entry_points.txt")]
            if len(metadata_names) != 1:
                raise PyUDFLoadError("wheel must contain exactly one entry_points.txt metadata file")
            entry_points_text = wheel.read(metadata_names[0]).decode("utf-8")
        parser = configparser.ConfigParser(interpolation=None)
        parser.optionxform = str
        parser.read_string(entry_points_text)
        values = list(parser[_ENTRY_POINT_GROUP].values()) if parser.has_section(_ENTRY_POINT_GROUP) else []
    except PyUDFLoadError:
        raise
    except (OSError, UnicodeDecodeError, configparser.Error) as exc:
        raise PyUDFLoadError(f"cannot read wheel metadata from {wheel_path!r}") from exc

    if len(values) != 1:
        raise PyUDFLoadError(f"wheel must provide exactly one milvus.pyudf entry point (found {len(values)})")

    value = values[0]
    if value.count(":") != 1:
        raise PyUDFLoadError("milvus.pyudf entry point must be module:factory")
    module_name, factory_name = (part.strip() for part in value.split(":"))
    if not module_name or not factory_name or "." in factory_name:
        raise PyUDFLoadError("milvus.pyudf entry point must be module:factory")
    if not all(piece.isidentifier() for piece in module_name.split(".")):
        raise PyUDFLoadError("entry point module is not a normal Python module name")
    if not factory_name.isidentifier():
        raise PyUDFLoadError("entry point factory is not a normal Python identifier")

    entry_root = module_name.split(".", 1)[0]
    python_roots = _wheel_python_roots(member_names)
    if python_roots != {entry_root}:
        found = ", ".join(sorted(python_roots)) or "none"
        raise PyUDFLoadError(
            "wheel must provide exactly one top-level Python import root matching "
            f"the milvus.pyudf entry point {entry_root!r} (found: {found})"
        )
    return module_name, factory_name


def _claim_package(top_level_package: str, identity: object) -> None:
    if top_level_package not in _PACKAGE_CLAIMS:
        _PACKAGE_CLAIMS[top_level_package] = identity
    elif _PACKAGE_CLAIMS[top_level_package] != identity:
        raise PyUDFLoadError(
            f"top-level package {top_level_package!r} is already claimed by a different PyUDF resource"
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
        raise PyUDFLoadError(f"PyUDF entry factory {module_name}:{factory_name} does not exist") from exc
    if not callable(factory):
        raise PyUDFLoadError(f"PyUDF entry factory {module_name}:{factory_name} is not callable")
    return factory


def _wrap_instance(instance: Any) -> LoadedPyUDFInstance:
    transform = getattr(instance, "transform", None)
    transform_query = getattr(instance, "transform_query", None)
    if callable(transform) == callable(transform_query):
        raise PyUDFLoadError("PyUDF instance must implement exactly one callable transform or transform_query")
    close = getattr(instance, "close", None)
    if close is not None and not callable(close):
        raise PyUDFLoadError("PyUDF close attribute must be callable when present")
    return LoadedPyUDFInstance(
        instance=instance,
        callable_name="transform" if callable(transform) else "transform_query",
        close=close,
    )


def load_instance(
    resource_name: str,
    local_path: str | os.PathLike[str],
    stage: str,
    resource_identity: object | None = None,
) -> LoadedPyUDFInstance:
    """Create one PyUDF instance from one exact local wheel.

    Resource identity is intentionally opaque to make this callable suitable for
    the C++ bridge, which owns the request identity.  The registry and sys.path
    additions deliberately outlive individual resources.
    """
    if not isinstance(resource_name, str) or not resource_name.strip():
        raise PyUDFLoadError("resource_name must be nonblank")
    if not isinstance(stage, str) or not stage.strip():
        raise PyUDFLoadError("stage must be nonblank")

    wheel_path = _validate_wheel_path(local_path)
    identity = _identity_key(resource_identity, wheel_path)
    module_name, factory_name = _entry_point_from_wheel(wheel_path)
    context = PyUDFContext(
        resource_name=resource_name,
        wheel_path=wheel_path,
        stage=stage,
        logger=logging.getLogger("milvus.pyudf"),
    )

    with _LOCK:
        _reject_cached_module(module_name, wheel_path, identity)
        _controlled_sys_path_add(wheel_path)
        factory = _load_factory(module_name, factory_name)
        try:
            instance = factory(context)
        except Exception as exc:
            raise PyUDFLoadError("PyUDF factory raised an exception") from exc
        try:
            return _wrap_instance(instance)
        except BaseException:
            _cleanup_instance(instance)
            raise


def _cleanup_instance(instance: Any) -> None:
    """Best-effort rollback for an instance that failed wrapper validation."""
    try:
        close = getattr(instance, "close", None)
    except BaseException:
        return
    if callable(close):
        try:
            close()
        except BaseException:
            pass


def close_instance(loaded: LoadedPyUDFInstance) -> None:
    """Close one loaded instance."""
    if not isinstance(loaded, LoadedPyUDFInstance):
        raise PyUDFLoadError("loaded value must be a LoadedPyUDFInstance")
    if loaded.close is not None:
        try:
            loaded.close()
        except BaseException as exc:
            raise PyUDFLoadError("PyUDF close raised an exception") from exc
