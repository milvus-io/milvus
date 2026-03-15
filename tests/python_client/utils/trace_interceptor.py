import hashlib
import os
from collections import namedtuple

import grpc

from utils.util_log import test_log as log


class _ClientCallDetails(
    namedtuple("_ClientCallDetails", ["method", "timeout", "metadata", "credentials"]),
    grpc.ClientCallDetails,
):
    pass


class TraceInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
):
    """gRPC interceptor: inject W3C traceparent into every call.

    - trace_id prefix (8 hex): derived from test name hash, same test always gets the same prefix
    - trace_id suffix (24 hex): random per gRPC call, so each call has a unique trace_id
    - On the server side, grep by prefix to find all requests from the same test case
    """

    def __init__(self, test_name: str):
        super().__init__()
        self.trace_prefix = hashlib.md5(test_name.encode(), usedforsecurity=False).hexdigest()[:8]
        log.info(f"[TRACE] test={test_name}  trace_id_prefix={self.trace_prefix}")

    def _inject(self, client_call_details):
        trace_id = self.trace_prefix + os.urandom(12).hex()
        span_id = os.urandom(8).hex()
        traceparent = f"00-{trace_id}-{span_id}-01"

        metadata = list(client_call_details.metadata or [])
        metadata.append(("traceparent", traceparent))

        new_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
        )

        method = client_call_details.method
        if isinstance(method, bytes):
            method = method.decode("utf-8", errors="replace")
        short = method.rsplit("/", 1)[-1] if "/" in method else method

        log.info(f"[TRACE] trace_id={trace_id}  method={short}")
        return new_details

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return continuation(self._inject(client_call_details), request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return continuation(self._inject(client_call_details), request)
