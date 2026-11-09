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

"""Single Python worker. Import only after fork; no supervisor or task queue."""

from __future__ import annotations

import logging
import os
import signal
import sys
from concurrent import futures
from types import MappingProxyType

import grpc

from .config import MAX_PARAM_NESTING_DEPTH, ServerConfig, parse_startup_args
from .errors import RequestInactive, WorkerError, error_response
from .health import register_health_service
from .ipc import OutputIPC, decode_inputs
from .loader import PyUDFLoader
from .logging_config import configure_logging
from .proto import pyudf_pb2 as wire
from .proto import pyudf_pb2_grpc

_LOG = logging.getLogger("milvus.pyudf.worker")


def _log_error_response(request, error, code):
    _LOG.error(
        "PyUDF Execute failed resource=%r stage=%r code=%d",
        request.resource_name,
        request.stage,
        code,
        exc_info=(type(error), error, error.__traceback__),
    )
    return error_response(error, code)


def _params_object(value, depth=0):
    if depth > MAX_PARAM_NESTING_DEPTH:
        raise WorkerError("parameter nesting depth exceeds the protocol limit", wire.INVALID_ARGUMENT)
    return MappingProxyType({key: _param(item, depth + 1) for key, item in value.fields.items()})


def _param(value, depth):
    if depth > MAX_PARAM_NESTING_DEPTH:
        raise WorkerError("parameter nesting depth exceeds the protocol limit", wire.INVALID_ARGUMENT)
    kind = value.WhichOneof("value")
    if kind == "object_value":
        return _params_object(value.object_value, depth)
    if kind == "array_value":
        return tuple(_param(item, depth + 1) for item in value.array_value.values)
    if kind in ("bool_value", "int64_value", "double_value", "string_value", "bytes_value"):
        return getattr(value, kind)
    raise WorkerError("unset function parameter", wire.INVALID_ARGUMENT)


class PyUDFWorkerService(pyudf_pb2_grpc.PyUDFWorkerServicer):
    def __init__(self):
        self.loader = PyUDFLoader()

    def Execute(self, request, context):
        def check_active():
            if not context.is_active():
                raise RequestInactive()

        try:
            check_active()
            if not request.resource_name.strip():
                raise WorkerError("resource_name must be nonblank")
            if (
                not request.udf_path
                or "\x00" in request.udf_path
                or not os.path.isabs(request.udf_path)
                or not request.udf_path.lower().endswith(".whl")
            ):
                raise WorkerError("udf_path must be an absolute wheel path")
            params = _params_object(request.params)
            batches = decode_inputs(request.inputs, check_active)
            indices = tuple(request.input_column_indices)
            if any(index >= batches[0].num_columns for index in indices):
                raise WorkerError("input column reference is out of range")
            instance = self.loader.load(request.resource_name, request.udf_path, request.stage, check_active)
            output = OutputIPC()
            for query, batch in enumerate(batches):
                check_active()
                columns = batch.columns
                if indices:
                    columns = [columns[index] for index in indices]
                try:
                    values = instance.execute_query(params, columns)
                except WorkerError as exc:
                    raise WorkerError(f"query={query}: UDF execution failed", exc.code) from exc
                check_active()
                output.append(values)
            payload = output.finish(check_active)
            response = wire.ExecuteResponse(outputs=payload)
            check_active()
            return response
        except RequestInactive:
            _LOG.warning(
                "PyUDF Execute cancelled or expired resource=%r stage=%r",
                request.resource_name,
                request.stage,
            )
            context.abort(grpc.StatusCode.CANCELLED, "PyUDF RPC is no longer active")
        except MemoryError as exc:
            return _log_error_response(request, exc, wire.OUT_OF_MEMORY)
        except WorkerError as exc:
            return _log_error_response(request, exc, exc.code)
        except BaseException as exc:
            return _log_error_response(request, exc, wire.INTERNAL)


class WorkerServer:
    """Own the gRPC service, its sole executor and worker process lifecycle.

    Construct only after fork. Tests may bind an ephemeral port with start();
    production enters run() and terminates the process without executor draining.
    """

    def __init__(self, config: ServerConfig):
        config.validate()
        self.config = config
        self._started = False
        self.executor = futures.ThreadPoolExecutor(max_workers=config.grpc_concurrency)
        self.server = grpc.server(
            self.executor,
            maximum_concurrent_rpcs=config.max_concurrent_rpcs,
            options=(
                ("grpc.so_reuseport", 1),
                ("grpc.max_send_message_length", config.max_message_bytes),
                ("grpc.max_receive_message_length", config.max_message_bytes),
            ),
        )
        self.service = PyUDFWorkerService()
        pyudf_pb2_grpc.add_PyUDFWorkerServicer_to_server(self.service, self.server)
        register_health_service(self.server, self._is_ready)

    def _is_ready(self) -> bool:
        return self._started

    def start(self, address: str | None = None) -> int:
        """Bind/start once; address override is for isolated test listeners."""
        port = self.server.add_insecure_port(self.config.address if address is None else address)
        if not port:
            raise RuntimeError("failed to bind worker address")
        self.server.start()
        self._started = True
        return port

    def run(self) -> None:
        """Run until signalled; termination does not drain the executor."""
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        try:
            self.start()
            self.server.wait_for_termination()
        except BaseException:
            _LOG.exception("PyUDF worker failed")
            os._exit(1)
        # Never run executor/atexit shutdown or close cached user instances.
        os._exit(0)


if __name__ == "__main__":
    configure_logging()
    WorkerServer(parse_startup_args(sys.argv[1:])).run()
