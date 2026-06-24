# Observability - AI Agent Guides

This directory contains observability guides for AI agents working on Milvus.
Use these guides before changing logging, metrics, tracing, or related
configuration.

## Guides

| Guide | Use When |
|---|---|
| [mlog - AI Agent Logging Guide](logging.md) | Adding or changing application logs. Covers `mlog` usage, context requirements, fields, levels, and logging rules. |
| [WAL Tracing](../streaming-system/wal/tracing.md) | Understanding or changing WAL trace span semantics across append, consume, transaction, broadcast, and replication paths. |
| [gRPC Observability - AI Agent Guide](grpc_observability.md) | Debugging gRPC latency/failures/trace context, or working on gRPC metrics, access logs, method filters, and related hot-reloadable configs. |

## Rules of Thumb

- Use `mlog` for all Milvus logs. Do not use `zap`, the old `pkg/log` package,
  the standard `log` package, or `fmt.Println` for runtime logging.
- Keep observability hot paths cheap. Avoid per-request regex compilation,
  payload logging, and high-cardinality metric labels.
- When debugging, start from the narrowest available evidence such as trace ID,
  request time window, node, collection, channel, method, or error message.
- Preserve compatibility for metric names, label sets, config keys, and log
  field names unless the task explicitly requires a breaking change.
- Add or update focused tests when changing observability behavior.
