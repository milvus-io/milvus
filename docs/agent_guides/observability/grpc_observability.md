# gRPC Observability

> **How to use this guide**: This page is the gRPC observability index for
> Milvus agents. Use it to find the relevant config, code paths, and debug
> entry points. Read the linked source before changing behavior.

Milvus gRPC observability covers:

- server/client gRPC Prometheus metrics
- optional server/client gRPC middleware logs
- method filters for temporary debugging
- trace/span correlation when the RPC context carries tracing data

Log events come from `github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging`:

- `started call`
- `finished call`
- `request received`
- `request sent`
- `response received`
- `response sent`

## Key Code Paths

- [gRPC observability interceptors](../../../pkg/util/interceptor/observability_interceptor.go)
- [gRPC metrics collectors](../../../pkg/metrics/grpc.go)
- [gRPC logging config keys](../../../pkg/util/paramtable/component_param.go)

The metrics implementation depends on
`github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus`.
The logging implementation depends on
`github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging`.

## Configuration

gRPC middleware logs are disabled by default. Enable them with method filters.

| Key | Default | Purpose |
|---|---:|---|
| `grpc.log.server.level` | `info` | Server gRPC log level. |
| `grpc.log.client.level` | `info` | Client gRPC log level. |
| `grpc.log.server.methods` | empty | Server full-method filter. |
| `grpc.log.client.methods` | empty | Client full-method filter. |
| `grpc.log.server.events` | `finish_call` | Server logging events. |
| `grpc.log.client.events` | `finish_call` | Client logging events. |
| `grpc.log.server.fields` | allowlist | Server log field allowlist. |
| `grpc.log.client.fields` | allowlist | Client log field allowlist. |

Valid levels: `debug`, `info`, `warn`, `error`.
Valid events: `start_call`, `finish_call`, `payload_received`, `payload_sent`.

Method filters use comma-separated gRPC full methods. Use `re:` for Go regexp
filters. Exact methods and regex filters can be mixed:

```text
grpc.log.server.methods=/milvus.proto.milvus.MilvusService/Search
grpc.log.client.methods=/milvus.proto.query.QueryNode/Search
grpc.log.server.methods=/milvus.proto.milvus.MilvusService/Search,re:^/milvus\.proto\.query\.QueryNode/.+$
grpc.log.server.events=start_call,finish_call
grpc.log.server.fields=method,grpc.code,grpc.duration,grpc.request.deadline
```

Prefer exact method filters first. Use anchored regexes unless substring
matching is intentional.
Payload fields such as `grpc.request.content` and `grpc.response.content` are
not in the default field allowlist; add them explicitly only for narrow debug
windows.
`traceID` and `spanID` are mlog context fields, not middleware fields; they
appear only when the log context carries a valid span.

## Access Log Fields

Use the source links above for the exact field list and emission points.
Important debug fields are:

| Field | Use |
|---|---|
| `method` | Confirms the gRPC full method. |
| `grpc.code` | Separates cancellation, deadline, unavailable, and internal errors. |
| `grpc.duration` | Compares client-side and server-side timing. |
| `traceID`, `spanID` | Correlates RPC logs when tracing context exists. |
| `dstServerID` | Identifies the target server when server-id metadata is present. |

## Debug Workflow

1. Identify the side to inspect.
   - Incoming request: start with `grpc.log.server.methods`.
   - Outgoing internal call: start with `grpc.log.client.methods`.
2. Enable one exact full method first.
3. Keep events at `finish_call` first; add `start_call` or payload events only
   for a narrow method and a short debug window.
4. Use `info` for normal gRPC logs; use `debug` only when global log level
   allows debug logs.
5. Reproduce once and capture the time window, role, method, status code,
   duration, and trace IDs.
6. Compare gRPC logs with `milvus_grpc_*` metrics.
7. Clear `grpc.log.server.methods` and `grpc.log.client.methods` after debugging.

## Debug Cases

### gRPC Logs Missing Or Too Broad

- Method filter is empty or does not match the full method.
- Plain filter entries are exact matches; regex entries require `re:`.
- Configured gRPC log level is below the active global log level.
- The relevant gRPC chain is missing the observability interceptor.
- Replace broad regexes with exact method filters when logs are too noisy.
- Anchor regex filters with `^` and `$`.
- Add payload content fields explicitly when payload events are enabled and
  response/request bodies are needed.

### Missing Correlation Fields

gRPC logs only report trace/span fields already present in the RPC context.
`dstServerID` depends on server-id metadata. Inspect the client chain linked
above before using this field for routing conclusions.

### Metrics Errors Without Access Logs

Metrics are always recorded; gRPC logs are filtered. Enable a narrow method
filter for the metric method or service with elevated errors.

## Do Not

- Do not enable payload events outside a short, narrow debug window.
- Do not enable broad regex filters for long-running environments.
- Do not change gRPC metric label sets without checking compatibility.
