# mlog — AI Agent Logging Guide

- ALWAYS USE `github.com/milvus-io/milvus/pkg/v2/mlog` PACKAGE TO LOG.
- NEVER USE `zap` OR `log` PACKAGE DIRECTLY.

## Rules

1. Every log call must receive a `ctx context.Context`. Never pass `nil`. Choose ctx by priority: function parameter ctx > struct-level ctx (e.g. `s.ctx`) > `context.TODO()`.
2. If the current struct has a `*mlog.Logger` field, use it. Otherwise use package-level functions like `mlog.Info(ctx, ...)`.
3. When a predefined `FieldXxx` exists for a key, always use `FieldXxx(val)`. Never write `mlog.Int64("segment_id", v)`.
4. In loops or hot paths, use `Rated` variants: `mlog.RatedInfo(ctx, limit, msg, fields...)`.
5. For Debug logs on hot paths where field construction is expensive (`fmt.Sprintf`, serialization, iteration), guard with `LevelEnabled`.
6. `mlog.Any` has poor performance. Use only when the type is unknown.

## Logging

```go
// Package-level
mlog.Info(ctx, "segment loaded", mlog.FieldSegmentID(id), mlog.Duration("cost", d))
mlog.Error(ctx, "flush failed", mlog.Err(err))

// Logger method (when struct has *mlog.Logger)
l.Info(ctx, "search started", mlog.Int64("nq", nq))

// Rate-limited (loops / hot paths). limit = events per second; rate.Inf = unlimited
mlog.RatedWarn(ctx, 1.0, "lagging", mlog.Int64("gap", gap))

// LevelEnabled guard (hot path + expensive field construction)
if mlog.LevelEnabled(mlog.DebugLevel) {
    mlog.Debug(ctx, "detail", mlog.String("dump", strings.Join(paths, ",")))
}
```

**Choosing log level:**

| Level | When to use |
|---|---|
| `Debug` | Internal state details useful only during development or troubleshooting. Disabled in production by default. |
| `Info` | Normal operational events: startup, shutdown, configuration loaded, request completed, task finished. |
| `Warn` | Unexpected but recoverable situations: timeout retry, transient RPC failure with retry, fallback path taken, deprecated API called. |
| `Error` | Operation failed and cannot be completed: unrecoverable RPC failure, data corruption, invariant broken. Always attach `mlog.Err(err)`. |
| `Fatal` | Process cannot continue. Calls `os.Exit(1)`. Use only during initialization for unrecoverable setup failures. |
| `DPanic` / `Panic` | Reserved for "should never happen" invariant violations. Rarely used. |
Each level has a corresponding `Rated` variant. Logger methods have the same signature as package-level functions.

## Constructing Fields

Priority: `FieldXxx(val)` > typed constructor like `mlog.String(key, val)` > `mlog.Any(key, val)`.

**Predefined FieldXxx** (key is built-in; never write the key string manually):

| Function | Type | Built-in Key |
|---|---|---|
| `FieldNodeID(v)` | int64 | `node_id` |
| `FieldModule(v)` | string | `module` |
| `FieldTraceID(v)` | string | `trace_id` |
| `FieldSpanID(v)` | string | `span_id` |
| `FieldDbID(v)` | int64 | `db_id` |
| `FieldDbName(v)` | string | `db_name` |
| `FieldCollectionID(v)` | int64 | `collection_id` |
| `FieldCollectionName(v)` | string | `collection_name` |
| `FieldPartitionID(v)` | int64 | `partition_id` |
| `FieldPartitionName(v)` | string | `partition_name` |
| `FieldSegmentID(v)` | int64 | `segment_id` |
| `FieldIndexID(v)` | int64 | `index_id` |
| `FieldFieldID(v)` | int64 | `field_id` |
| `FieldTaskID(v)` | int64 | `task_id` |
| `FieldBroadcastID(v)` | int64 | `broadcast_id` |
| `FieldJobID(v)` | int64 | `job_id` |
| `FieldBuildID(v)` | int64 | `build_id` |
| `FieldVChannel(v)` | string | `vchannel` |
| `FieldPChannel(v)` | string | `pchannel` |
| `FieldMessageID(v)` | ObjectMarshaler | `message_id` |
| `FieldMessage(v)` | ObjectMarshaler | `message` |

**Generic typed constructors** (use when no predefined FieldXxx exists; function names match Go types):
`String` / `Int64` / `Int` / `Float64` / `Bool` / `Duration` / `Time` / `Stringer` / `Binary` / `Err` (key fixed to `"error"`), etc.
Each type has pointer variant `Xxxp` and slice variant `Xxxs`. See `field.go` for the full list.

## Binding Fields

```
Should the field follow the request chain (bind to ctx)?
├─ Yes → ctx = mlog.WithFields(ctx, fields...)
│        Lazily encoded; duplicate keys are overridden by later values.
│        To propagate across gRPC, add OptPropagated():
│          mlog.WithFields(ctx, mlog.FieldCollectionID(id, mlog.OptPropagated()))
│
└─ No  → Bind to a Logger
          ├─ Component-level (struct lifetime) → mlog.With(fields...) stored as a field
          ├─ Function-level (shared across multiple log calls in scope) → l := mlog.With(fields...) as local var
          └─ Fields may be filtered by level → mlog.WithLazy(fields...) — lazily encoded
```

```go
// Bind to ctx at request entry point
ctx = mlog.WithFields(ctx, mlog.FieldCollectionID(collID), mlog.String("request_id", reqID))

// Bind to Logger at component construction
l := mlog.With(mlog.FieldModule("querynode"), mlog.FieldNodeID(nodeID))

// Local Logger to eliminate repeated fields within a function
func (s *compactor) compact(ctx context.Context, segID int64, plan *Plan) error {
    l := mlog.With(mlog.FieldSegmentID(segID), mlog.Int64("planID", plan.ID))
    l.Info(ctx, "compact start")
    // ...
    l.Info(ctx, "compact done", mlog.Duration("cost", elapsed))
    return nil
}
```
