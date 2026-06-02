# Milvus Trace API Coverage Scripts

These scripts exercise Milvus gRPC and REST APIs with an injected trace context, then scan local Milvus logs for the same trace ID.

## Prerequisites

1. Build and run a local Milvus cluster from this worktree.
2. Install Python dependencies from `tests/python_client/requirements.txt` and `tests/restful_client_v2/requirements.txt`.
3. Keep `MILVUS_VOLUME_DIRECTORY` set so the scripts can auto-discover `<workspace>/milvus-logs`.

## Run

```bash
python3 tests/scripts/trace_api_coverage/run_trace_api_coverage.py \
  --grpc-uri http://127.0.0.1:19530 \
  --rest-uri http://127.0.0.1:19530 \
  --token root:Milvus \
  --output /tmp/milvus-trace-api-coverage
```

Useful options:

- `--protocol grpc|rest|both`
- `--log-dir <dir>` when auto-discovery is not enough
- `--skip-log-check` to only validate API execution
- `--log-timeout 30` to wait longer for async log flushing

## Trace Injection

- gRPC uses PyMilvus `client_request_id=<32 hex trace id>`. Milvus server fallback propagator maps that ID into an OpenTelemetry trace context when no `traceparent` is present.
- REST uses `traceparent`, `client_request_id`, `client-request-id`, and `RequestId` headers.

## Reports

The runner writes:

- `grpc_report.json`
- `rest_report.json`
- `summary.json`

Each API case records API status, injected trace ID, and whether that trace ID appeared in local Milvus logs.
