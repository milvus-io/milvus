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

"""Load and cache UDF objects from complete local wheel paths."""

from __future__ import annotations

import configparser
import importlib
import logging
import os
import sys
import zipfile
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import Lock
from typing import Any

from .context import PyUDFContext
from .errors import PyUDFLoadError, resource_io_code
from .instance import PyUDFInstance

_ENTRY_POINT_GROUP = "milvus.pyudf"
_LOG = logging.getLogger("milvus.pyudf.loader")


@dataclass
class _CacheEntry:
    lock: Lock = field(default_factory=Lock)
    instance: PyUDFInstance | None = None


class PyUDFLoader:
    """Cache one instance per path/stage; user code must not reenter the loader.

    Registry access takes a short process-wide lock. Creation takes only the
    entry lock, so unrelated UDFs can load concurrently. Python modules belong
    to the process and share import locks across stages and loader instances.
    """

    _package_claims: dict[str, str] = {}
    _wheel_paths: set[str] = set()
    _module_locks: dict[tuple[str, str], Lock] = {}
    _state_lock = Lock()

    def __init__(self):
        self._entries: dict[tuple[str, str], _CacheEntry] = {}

    @staticmethod
    @contextmanager
    def _acquire(lock: Lock, check_active: Callable[[], None]):
        while True:
            check_active()
            if lock.acquire(timeout=0.05):
                break
        try:
            check_active()
            yield
        finally:
            lock.release()

    def load(
        self,
        resource_name: str,
        local_path: str | os.PathLike[str],
        stage: str,
        check_active: Callable[[], None],
    ) -> PyUDFInstance:
        check_active()
        path = os.path.realpath(os.fspath(local_path))
        key = (path, stage)
        with self._state_lock:
            entry = self._entries.get(key)
            if entry is None:
                entry = self._entries[key] = _CacheEntry()
        with self._acquire(entry.lock, check_active):
            if entry.instance is None:
                entry.instance = self._create_instance(resource_name, path, stage, check_active)
            return entry.instance

    def _import_factory(self, module_name: str, factory_name: str, wheel_path: str, check_active: Callable[[], None]):
        package = module_name.split(".", 1)[0]
        key = (wheel_path, module_name)
        check_active()
        with self._state_lock:
            claimed_path = self._package_claims.setdefault(package, wheel_path)
            if claimed_path != wheel_path:
                raise PyUDFLoadError(f"top-level package {package!r} is already claimed by a different PyUDF resource")
            if wheel_path not in self._wheel_paths:
                # Retain the path for lazy imports and package resource access.
                sys.path.insert(0, wheel_path)
                self._wheel_paths.add(wheel_path)
            module_lock = self._module_locks.get(key)
            if module_lock is None:
                module_lock = self._module_locks[key] = Lock()
        # Keep same-module waits cancellable instead of waiting inside Python's
        # import lock. Completed modules are cached by Python itself.
        with self._acquire(module_lock, check_active):
            module = _import_module(module_name, wheel_path)
        check_active()
        return _load_factory(module, module_name, factory_name)

    def _create_instance(
        self, resource_name: str, local_path: str, stage: str, check_active: Callable[[], None]
    ) -> PyUDFInstance:
        if not isinstance(resource_name, str) or not resource_name.strip():
            raise PyUDFLoadError("resource_name must be nonblank")

        check_active()
        wheel_path = _validate_wheel_path(local_path)
        check_active()
        module_name, factory_name = _entry_point_from_wheel(wheel_path)
        check_active()
        context = PyUDFContext(
            resource_name=resource_name,
            wheel_path=wheel_path,
            stage=stage,
            logger=logging.getLogger("milvus.pyudf"),
        )

        factory = self._import_factory(module_name, factory_name, wheel_path, check_active)
        check_active()
        try:
            instance = factory(context)
        except MemoryError:
            raise
        except Exception as exc:
            raise PyUDFLoadError("PyUDF factory raised an exception") from exc
        try:
            return PyUDFInstance(instance)
        except BaseException:
            _cleanup_instance(instance)
            raise


def _validate_wheel_path(wheel_path: str | os.PathLike[str]) -> str:
    path = os.path.realpath(os.fspath(wheel_path))
    if not path.lower().endswith(".whl"):
        raise PyUDFLoadError("local wheel path must end with .whl")
    try:
        # Check only the ZIP directory, avoiding a full scan of large model
        # wheels. ZipFile.read() checks metadata CRCs when read below, but
        # zipimport does not check module/resource CRCs; this is not a wheel
        # integrity check.
        with zipfile.ZipFile(path):
            pass
    except OSError as exc:
        raise PyUDFLoadError(f"cannot open local wheel {path!r}", resource_io_code(exc)) from exc
    except zipfile.BadZipFile as exc:
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
    except MemoryError:
        raise
    except OSError as exc:
        raise PyUDFLoadError(f"cannot read wheel metadata from {wheel_path!r}", resource_io_code(exc)) from exc
    except Exception as exc:
        raise PyUDFLoadError(f"invalid wheel metadata in {wheel_path!r}") from exc

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


def _check_module_origin(module_name: str, wheel_path: str) -> None:
    package = module_name.split(".", 1)[0]
    cached = sys.modules.get(package)
    if cached is None:
        return
    origin = getattr(cached, "__file__", None)
    real_origin = os.path.realpath(origin) if isinstance(origin, str) else ""
    if real_origin != wheel_path and not real_origin.startswith(wheel_path + os.sep):
        raise PyUDFLoadError(f"top-level package {package!r} is already imported from outside this PyUDF wheel")


def _import_module(module_name: str, wheel_path: str):
    try:
        # User module attributes/import hooks may run here, outside state locks.
        _check_module_origin(module_name, wheel_path)
        module = importlib.import_module(module_name)
        _check_module_origin(module_name, wheel_path)
        return module
    except PyUDFLoadError:
        raise
    except MemoryError:
        raise
    except Exception as exc:
        raise PyUDFLoadError(f"cannot import PyUDF entry module {module_name!r}") from exc


def _load_factory(module, module_name: str, factory_name: str) -> Any:
    try:
        factory = getattr(module, factory_name)
    except MemoryError:
        raise
    except Exception as exc:
        raise PyUDFLoadError(f"cannot resolve PyUDF entry factory {module_name}:{factory_name}") from exc
    if not callable(factory):
        raise PyUDFLoadError(f"PyUDF entry factory {module_name}:{factory_name} is not callable")
    return factory


def _cleanup_instance(instance: Any) -> None:
    """Best-effort rollback for an instance that failed wrapper validation."""
    try:
        close = getattr(instance, "close", None)
    except BaseException:
        _LOG.warning("Cannot access close while cleaning up a failed PyUDF instance", exc_info=True)
        return
    if callable(close):
        try:
            close()
        except BaseException:
            _LOG.warning("Failed to close a failed PyUDF instance", exc_info=True)
