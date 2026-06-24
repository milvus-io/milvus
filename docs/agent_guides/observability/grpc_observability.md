# gRPC Observability - AI Agent Guide

This guide explains Milvus gRPC metrics and access logs for AI agents working on
observability, debugging, or configuration changes. Use it as both a feature
guide and a debugging checklist when investigating RPC latency, failures, missing
trace context, or unexpected component-to-component traffic.

## What This Feature Does

Milvus installs unified gRPC observability interceptors for client and server
RPCs. The interceptors always record Prometheus gRPC metrics, and optionally
emit structured access logs when a method filter allows the RPC.

Access log messages are:

- `grpc.server.call` for server-side RPC handling
- `grpc.client.call` for client-side outgoing RPC calls

Metrics are always enabled once `metrics.RegisterGRPCMetrics` runs during
process startup. Access logs are disabled by default because the method filters
are empty by default.

## Key Code Paths

- Interceptor implementation:
  `pkg/util/interceptor/observability_interceptor.go`
- gRPC metrics registration:
  `pkg/metrics/grpc.go`
- Startup registration:
  `cmd/roles/roles.go`
- Log config definitions:
  `pkg/util/paramtable/component_param.go`

## Configuration

The access log configs are hot-reloadable and were introduced in `3.0.0`.

| Key | Default | Meaning |
|---|---:|---|
| `grpc.serverLog.level` | `info` | Log level used by `grpc.server.call`. |
| `grpc.clientLog.level` | `info` | Log level used by `grpc.client.call`. |
| `grpc.serverLog.methods` | empty | Server-side method filter. Empty disables server access logs. |
| `grpc.clientLog.methods` | empty | Client-side method filter. Empty disables client access logs. |

Valid level values are `debug`, `info`, `warn`, and `error`. Unknown or empty
level strings fall back to `info`.

## Method Filter Syntax

The method filter is a comma-separated list. Each entry is trimmed.

Plain entries are exact full-method matches:

```text
grpc.serverLog.methods=/milvus.proto.milvus.MilvusService/Search
```

Entries prefixed with `re:` are Go regular expressions:

```text
grpc.serverLog.methods=re:^/milvus\.proto\.milvus\.MilvusService/(Search|Query)$
```

Exact and regex entries can be mixed:

```text
grpc.clientLog.methods=/svc/Exact,re:^/svc/Debug.+$
```

The match target is the gRPC full method string, normally:

```text
/package.Service/Method
```

Prefer anchored regexes (`^...$`) unless a prefix or substring match is
intentional.

## Logging Decision Flow

For each RPC:

1. Load the current method filter with an atomic pointer read.
2. If the filter is empty, do not print an access log.
3. Check the exact-match map first.
4. If exact matching misses, check each compiled regex in order.
5. If no entry matches, do not print an access log.
6. If an entry matches, check whether the current global `mlog` level enables
   the configured gRPC access-log level.
7. Only then emit `grpc.server.call` or `grpc.client.call`.

Metrics are still recorded even when the access log is skipped.

## Hot Reload Behavior

The interceptor watches both the level key and method-filter key.

- Level updates are parsed immediately. Unknown values become `info`.
- Method filter updates are parsed and regex entries are compiled on update.
- If a method update contains any invalid regex, the update is rejected and the
  previous filter remains active.
- Invalid regex entries are logged with `mlog.Warn`.

This avoids a bad hot-reload value unexpectedly disabling or changing existing
access-log behavior.

## Performance Rules

Keep the default path cheap:

- Do not compile regexes in the RPC hot path.
- Preserve exact map lookup before regex matching.
- Keep filters immutable after parsing and swap them with `atomic.Pointer`.
- Do not add high-cardinality metric labels.
- Do not add request or response payload fields to access logs.

The current hot path is:

```text
atomic pointer load -> exact map lookup -> optional regex loop
```

With the default empty method filter, access logging exits before field
construction.

## Log Fields

Server access logs include:

- `method`
- `code`
- `duration`
- `error`
- trace/span fields automatically added by `mlog` from `ctx` when present

Client access logs include the same core fields plus:

- `dstServerID`

The client-side `dstServerID` is read from outgoing gRPC metadata populated by
the server ID injection client interceptor.

## Common Agent Tasks

When adding a new server or client gRPC chain, include the observability
interceptor in the chain so metrics remain complete:

```go
interceptor.NewObservabilityServerUnaryInterceptor()
interceptor.NewObservabilityServerStreamInterceptor()
interceptor.NewObservabilityClientUnaryInterceptor()
interceptor.NewObservabilityClientStreamInterceptor()
```

For server chains, keep Milvus context-aware `mlog` interceptors in the chain as
well. The observability interceptor emits access logs; `mlog` interceptors
handle context-aware logging behavior.

When changing filter behavior, update tests in:

```text
pkg/util/interceptor/observability_interceptor_test.go
```

At minimum, cover:

- empty filter disables access logs
- exact matching still works
- regex matching works only with `re:` entries
- invalid regex updates do not replace the previous filter

## Example Troubleshooting Configs

Log one method exactly:

```text
grpc.serverLog.level=info
grpc.serverLog.methods=/milvus.proto.milvus.MilvusService/Search
```

Log all methods in one service:

```text
grpc.serverLog.level=debug
grpc.serverLog.methods=re:^/milvus\.proto\.milvus\.MilvusService/.+$
```

Log selected outgoing client calls:

```text
grpc.clientLog.level=debug
grpc.clientLog.methods=/milvus.proto.query.QueryNode/Search,re:^/milvus\.proto\.rootcoord\..+/.+$
```

Disable access logs again:

```text
grpc.serverLog.methods=
grpc.clientLog.methods=
```

## Debugging Workflow

Use this flow when the user reports gRPC latency, failures, missing trace IDs, or
unexpected calls between Milvus components.

1. Identify the RPC side.
   - User-facing request entering a Milvus service: start with
     `grpc.serverLog.methods`.
   - Internal call from one Milvus component to another: start with
     `grpc.clientLog.methods` on the caller and `grpc.serverLog.methods` on the
     callee if both sides are available.

2. Start narrow.
   - Prefer one exact full method first.
   - Use `re:` only after confirming the method namespace.
   - Avoid enabling broad regexes such as `re:.*` except for very short local
     reproductions.

3. Set the access-log level.
   - Use `info` when you only need request completion, status code, and
     duration.
   - Use `debug` when the global `mlog` level is also debug and you want lower
     level access logs.
   - Remember: the configured gRPC access-log level must also pass the global
     `mlog` level check.

4. Reproduce the issue once.
   - Capture the exact time window.
   - Record the component role, method, status code, duration, `traceID`,
     `spanID`, and `dstServerID` when present.

5. Correlate logs and metrics.
   - Use access logs to inspect individual calls.
   - Use Prometheus gRPC metrics to verify whether the problem is isolated or
     systemic.
   - Metrics continue to work even when access logs are disabled.

6. Reduce the filter after debugging.
   - Clear `grpc.serverLog.methods` and `grpc.clientLog.methods` after the
     investigation unless the user explicitly wants ongoing logging.

## What To Look For

Use these fields first:

| Field | Meaning | Debug Use |
|---|---|---|
| `method` | gRPC full method | Confirms whether the filter matched the intended RPC. |
| `code` | gRPC status code | Separates success, cancellation, deadline, unavailable, and internal errors. |
| `duration` | Interceptor-measured call duration | Finds slow calls and compares caller/callee timing. |
| `traceID` | Trace ID from `ctx`, added by `mlog` | Correlates logs in the same trace when tracing context exists. |
| `spanID` | Span ID from `ctx`, added by `mlog` | Helps distinguish individual spans in the same trace. |
| `dstServerID` | Client-side target server ID metadata | Identifies the destination Milvus server for client calls. |
| `error` | Error returned by the RPC handler/invoker | Captures the terminal RPC error. |

## Common Debug Cases

### Access logs do not appear

Check these in order:

1. The method filter is empty by default. Set the correct
   `grpc.serverLog.methods` or `grpc.clientLog.methods`.
2. The filter must match the gRPC full method, including the leading `/`.
3. Plain entries are exact matches. Use `re:` only for regex matching.
4. The access-log level must pass the current global `mlog` level.
5. Invalid regex hot updates are rejected; look for
   `ignore invalid gRPC log method regex`.
6. Confirm the relevant interceptor is in the server/client chain.

### Too many logs appear

The filter is likely too broad.

- Replace broad regexes with exact methods.
- Anchor regexes with `^` and `$`.
- Split server-side and client-side filters; do not enable both unless needed.
- Clear the method config after the reproduction.

### `traceID` or `spanID` is missing

Access logs do not create trace context. They only log trace/span fields already
present in `ctx` through `mlog`.

Common causes:

- The caller did not start or propagate a trace.
- The access log is emitted before tracing metadata is attached.
- The RPC is an internal call path that does not carry OpenTelemetry context.
- The server and client interceptors are ordered such that tracing context is not
  available to this log call.

When this happens, inspect the interceptor order around `otelgrpc` and `mlog`
interceptors for the role being debugged, then compare client and server logs for
the same method and time window.

### Client calls show empty `dstServerID`

`dstServerID` is read from outgoing metadata populated by the server ID injection
client interceptor. If it is empty:

- The connection path may not use the server ID injection interceptor.
- The target may not have a known Milvus server ID at call time.
- The call may be using a generic gRPC client path instead of Milvus component
  client helpers.

Start from the caller's client construction path and confirm the interceptor
chain includes server ID injection before relying on `dstServerID`.

### Metrics show errors but logs do not

Metrics are unconditional; access logs are filtered. If metrics show gRPC errors
but no access logs appear, enable a narrow method filter for the method or service
with elevated error counts.

### Client duration and server duration differ

This is expected. Client duration includes client-side waiting and transport
cost. Server duration measures handler execution through the server interceptor.
Use both sides together to distinguish network/client-side queueing from server
handler latency.

## Safe Debug Config Examples

Prefer exact matching first:

```text
grpc.serverLog.level=info
grpc.serverLog.methods=/milvus.proto.milvus.MilvusService/Search
```

Enable both sides for one known method:

```text
grpc.clientLog.level=info
grpc.clientLog.methods=/milvus.proto.query.QueryNode/Search
grpc.serverLog.level=info
grpc.serverLog.methods=/milvus.proto.query.QueryNode/Search
```

Temporarily match a service namespace:

```text
grpc.serverLog.level=info
grpc.serverLog.methods=re:^/milvus\.proto\.query\.QueryNode/.+$
```

Disable after collecting evidence:

```text
grpc.clientLog.methods=
grpc.serverLog.methods=
```

## Do Not

- Do not replace exact matching with regex-only matching.
- Do not use unanchored regexes in examples unless substring matching is the
  point.
- Do not use `zap` or the old `pkg/log` package in this code path.
- Do not log raw request or response payloads.
- Do not change metric label sets without considering cardinality and dashboard
  compatibility.
