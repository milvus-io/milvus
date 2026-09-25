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

"""PyUDF server objects, imported lazily so supervisor startup stays fork-safe."""

from importlib import import_module

RUNTIME_API_VERSION = 1

_EXPORTS = {
    "PyUDFInstance": "instance",
    "PyUDFLoader": "loader",
    "WorkerServer": "worker",
    "PyUDFContext": "context",
    "PyUDFExecutionError": "errors",
    "PyUDFLoadError": "errors",
}
__all__ = ["RUNTIME_API_VERSION", *_EXPORTS]


def __getattr__(name):
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f".{module}", __name__), name)
    globals()[name] = value
    return value
