# gRPC Observability

> **How to use this guide**: This page is the gRPC observability index for
> Milvus agents. Use it to find the relevant config, code paths, and debug
> entry points. Read the linked source before changing behavior.

Milvus gRPC observability covers:

- server/client gRPC Prometheus metrics
- optional server/client gRPC access logs
- method filters for temporary debugging
- trace/span correlation when the RPC context carries tracing data

Access log messages:

- `grpc.server.call`
- `grpc.client.call`

## Key Code Paths

- [gRPC observability interceptors](../../../pkg/util/interceptor/observability_interceptor.go)
- [gRPC metrics collectors](../../../pkg/metrics/grpc.go)
- [gRPC access-log config keys](../../../pkg/util/paramtable/component_param.go)

The metrics implementation depends on
`github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus`.

## Configuration

Access logs are disabled by default. Enable them with method filters.

| Key | Default | Purpose |
|---|---:|---|
| `grpc.serverLog.level` | `info` | Level for `grpc.server.call`. |
| `grpc.clientLog.level` | `info` | Level for `grpc.client.call`. |
| `grpc.serverLog.methods` | empty | Server full-method filter. |
| `grpc.clientLog.methods` | empty | Client full-method filter. |

Valid levels: `debug`, `info`, `warn`, `error`.

Method filters use comma-separated gRPC full methods. Use `re:` for Go regexp
filters. Exact methods and regex filters can be mixed:

```text
grpc.serverLog.methods=/milvus.proto.milvus.MilvusService/Search
grpc.clientLog.methods=/milvus.proto.query.QueryNode/Search
grpc.serverLog.methods=/milvus.proto.milvus.MilvusService/Search,re:^/milvus\.proto\.query\.QueryNode/.+$
```

Prefer exact method filters first. Use anchored regexes unless substring
matching is intentional.

## Access Log Fields

Use the source links above for the exact field list and emission points.
Important debug fields are:

| Field | Use |
|---|---|
| `method` | Confirms the gRPC full method. |
| `code` | Separates cancellation, deadline, unavailable, and internal errors. |
| `duration` | Compares client-side and server-side timing. |
| `traceID`, `spanID` | Correlates RPC logs when tracing context exists. |
| `dstServerID` | Identifies the target server when server-id metadata is present. |

## Debug Workflow

1. Identify the side to inspect.
   - Incoming request: start with `grpc.serverLog.methods`.
   - Outgoing internal call: start with `grpc.clientLog.methods`.
2. Enable one exact full method first.
3. Use `info` for normal access logs; use `debug` only when global log level
   allows debug logs.
4. Reproduce once and capture the time window, role, method, status code,
   duration, and trace IDs.
5. Compare access logs with `milvus_grpc_*` metrics.
6. Clear `grpc.serverLog.methods` and `grpc.clientLog.methods` after debugging.

## Debug Cases

### Access Logs Missing Or Too Broad

- Method filter is empty or does not match the full method.
- Plain filter entries are exact matches; regex entries require `re:`.
- Configured access-log level is below the active global log level.
- The relevant gRPC chain is missing the observability interceptor.
- Replace broad regexes with exact method filters when logs are too noisy.
- Anchor regex filters with `^` and `$`.

### Missing Correlation Fields

Access logs only report trace/span fields already present in the RPC context.
`dstServerID` depends on server-id metadata. Inspect the client chain linked
above before using this field for routing conclusions.

### Metrics Errors Without Access Logs

Metrics are always recorded; access logs are filtered. Enable a narrow method
filter for the metric method or service with elevated errors.

## Do Not

- Do not log raw gRPC request or response payloads.
- Do not enable broad regex filters for long-running environments.
- Do not change gRPC metric label sets without checking compatibility.
