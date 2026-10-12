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
from dataclasses import replace
from types import MappingProxyType

import grpc
import pyarrow as pa

from .config import MAX_PARAM_NESTING_DEPTH, ServerConfig, parse_startup_args
from .context import PyUDFCallContext, PyUDFQueryInput
from .errors import RequestInactive, WorkerError, error_response
from .health import register_health_service
from .ipc import OutputIPC, decode_inputs
from .loader import PyUDFLoader
from .logging_config import configure_logging
from .proto import pyudf_pb2 as wire
from .proto import pyudf_pb2_grpc

_LOG = logging.getLogger("milvus.pyudf.worker")


def _request_scope(request):
    scope = request.request_context
    return {
        "database": scope.database,
        "collection": scope.collection,
        "user": scope.user,
        "resource_name": scope.resource_name or request.resource_name,
    }


def _request_logger(request):
    # Each call owns its adapter and immutable extras. Never mutate the shared
    # logger, the cached UDF factory context, or another invocation's identity.
    return logging.LoggerAdapter(_LOG, MappingProxyType(_request_scope(request)))


def _log_error_response(request, error, code, logger):
    logger.error(
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


def _query_inputs(inputs, nq=None):
    result = []
    for item in inputs:
        if not item.data_type or item.data_type == "None" or nq is not None and len(item.values) != nq:
            raise WorkerError("query metadata type/count does not match NQ")
        try:
            values = (
                tuple(value.decode("utf-8") for value in item.values)
                if item.data_type == "VarChar"
                else tuple(item.values)
            )
        except UnicodeDecodeError as exc:
            raise WorkerError("query metadata contains invalid UTF-8") from exc
        result.append(PyUDFQueryInput(item.data_type, values, item.tag))
    return tuple(result)


def _query_origins(values, rows, original_count):
    origins = tuple(values)
    if len(origins) != rows:
        raise WorkerError("query mapping count differs from prepared rows")
    previous = -1
    for origin in origins:
        if origin < 0 or origin >= original_count or origin < previous:
            raise WorkerError("query mapping has invalid bounds or parent order")
        previous = origin
    return origins


def _argument_schema(request, batch):
    indices = tuple(request.input_column_indices)
    return (
        pa.schema([batch.schema.field(index) for index in indices], metadata=batch.schema.metadata)
        if indices
        else batch.schema
    )


def _call_context(request, batches, logger=None):
    scope = _request_scope(request)
    logger = logger if logger is not None else _request_logger(request)
    stage = request.stage.lower()
    if stage not in ("ingestion", "pre_process", "l0_rerank", "l1_rerank", "l2_rerank", "post_process"):
        raise WorkerError("unsupported query-context execution stage", wire.UNSUPPORTED)
    if any(batch.num_columns == 0 for batch in batches):
        raise WorkerError("query-context execution requires input columns")
    if stage == "ingestion":
        if request.HasField("query_context"):
            raise WorkerError("ingestion must not contain query metadata")
        return PyUDFCallContext(
            request.stage, 0, input_schema=_argument_schema(request, batches[0]), logger=logger, **scope
        )
    if stage == "pre_process":
        if len(batches) != 1:
            raise WorkerError("PRE_PROCESS requires one query-row batch")
        nq = batches[0].num_rows
        offsets = ()
    else:
        nq = len(batches)
        sizes = [0]
        for batch in batches:
            sizes.append(sizes[-1] + batch.num_rows)
        offsets = tuple(sizes)
    expanded = request.execution_version >= 5
    if not expanded and (
        request.query_context.row_query_origins
        or any(item.original_query_indices for item in request.query_context.queries)
    ):
        raise WorkerError("expanded query metadata requires execution version 5", wire.UNSUPPORTED)
    if request.table_result or expanded:
        original_inputs = request.query_context.original_queries
        nq = len(original_inputs[0].values) if original_inputs else 0
    original = _query_inputs(request.query_context.original_queries, nq)
    queries = _query_inputs(request.query_context.queries, None if expanded else nq)
    if len(original) != len(queries) or not queries and not request.table_result:
        raise WorkerError("query-context execution requires aligned original and prepared ANN inputs")
    if any(item.original_query_indices for item in request.query_context.original_queries):
        raise WorkerError("original query inputs must not contain prepared mappings")
    origins = []
    for prepared in request.query_context.queries:
        rows = len(prepared.values)
        if prepared.original_query_indices:
            origins.append(_query_origins(prepared.original_query_indices, rows, nq))
        elif rows == nq:
            origins.append(tuple(range(nq)))
        elif rows == 0 and expanded:
            origins.append(())
        else:
            raise WorkerError("prepared query count changed without a mapping")
    row_origins = ()
    if stage == "pre_process":
        rows = batches[0].num_rows
        if request.query_context.row_query_origins:
            row_origins = _query_origins(request.query_context.row_query_origins, rows, nq)
        elif rows == nq:
            row_origins = tuple(range(nq))
        elif rows != 0 or not expanded:
            raise WorkerError("PRE row count changed without row query origins")
    elif request.query_context.row_query_origins:
        raise WorkerError("current row query origins require PRE")
    elif not request.table_result and any(len(query.values) != len(batches) for query in queries):
        raise WorkerError("prepared query count differs from candidate groups")
    return PyUDFCallContext(
        stage=request.stage,
        query_count=nq,
        queries=queries,
        original_queries=original,
        prepared_query_counts=tuple(len(query.values) for query in queries),
        query_origins=tuple(origins),
        row_query_origins=row_origins,
        result_offsets=offsets,
        table_result=request.table_result,
        reduction=request.reduction,
        input_schema=_argument_schema(request, batches[0]),
        logger=logger,
        **scope,
    )


class PyUDFWorkerService(pyudf_pb2_grpc.PyUDFWorkerServicer):
    def __init__(self):
        self.loader = PyUDFLoader()

    def Execute(self, request, context):
        logger = _LOG

        def check_active():
            if not context.is_active():
                raise RequestInactive()

        try:
            logger = _request_logger(request)
            check_active()
            if request.execution_version not in (0, 2, 3, 4, 5, 6):
                raise WorkerError("unsupported execution version", wire.UNSUPPORTED)
            if request.HasField("request_context") and request.execution_version != 6:
                raise WorkerError("request scope requires execution version 6", wire.UNSUPPORTED)
            if request.execution_version == 6 and not request.HasField("request_context"):
                raise WorkerError("scope execution requires request context")
            if request.execution_version == 3:
                if not request.table_result or request.stage.lower() != "post_process":
                    raise WorkerError("execution version 3 requires POST TABLE")
            elif request.execution_version in (4, 5, 6):
                if request.table_result and request.stage.lower() != "post_process":
                    raise WorkerError("schema execution TABLE requires POST_PROCESS")
                if request.reduction and not request.table_result:
                    raise WorkerError("schema execution reduction requires TABLE")
            elif request.table_result or request.reduction:
                raise WorkerError("table and reduction flags require execution version 3")
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
            call_context = _call_context(request, batches, logger) if request.execution_version >= 2 else None
            instance = self.loader.load(request.resource_name, request.udf_path, request.stage, check_active)
            output = OutputIPC()

            def arguments(batch):
                columns = batch.columns
                return tuple(columns[index] for index in indices) if indices else tuple(columns)

            if request.execution_version >= 2 and request.stage.lower() == "post_process":
                check_active()
                groups = tuple(arguments(batch) for batch in batches)
                results = instance.execute_batch(params, groups, call_context)
                check_active()
                for index, values in enumerate(results):
                    check_active()
                    output.append(values, inputs=groups[index], input_schema=_argument_schema(request, batches[index]))
            else:
                for query, batch in enumerate(batches):
                    check_active()
                    group_context = call_context
                    if call_context is not None and request.stage.lower() in ("l0_rerank", "l1_rerank", "l2_rerank"):
                        parents = {origins[query] for origins in call_context.query_origins}
                        group_context = replace(
                            call_context,
                            query_index=query,
                            original_query_index=next(iter(parents)) if len(parents) == 1 else None,
                        )
                    try:
                        selected = arguments(batch)
                        if group_context is None:
                            values = instance.execute(params, selected, request.stage)
                        else:
                            values = instance.execute(params, selected, request.stage, context=group_context)
                    except WorkerError as exc:
                        raise WorkerError(f"query={query}: UDF execution failed", exc.code) from exc
                    check_active()
                    output.append(values, inputs=selected, input_schema=_argument_schema(request, batch))
            payload = output.finish(check_active)
            response = wire.ExecuteResponse(outputs=payload, execution_version=request.execution_version)
            check_active()
            return response
        except RequestInactive:
            logger.warning(
                "PyUDF Execute cancelled or expired resource=%r stage=%r",
                request.resource_name,
                request.stage,
            )
            context.abort(grpc.StatusCode.CANCELLED, "PyUDF RPC is no longer active")
        except MemoryError as exc:
            return _log_error_response(request, exc, wire.OUT_OF_MEMORY, logger)
        except WorkerError as exc:
            return _log_error_response(request, exc, exc.code, logger)
        except BaseException as exc:
            return _log_error_response(request, exc, wire.INTERNAL, logger)


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
