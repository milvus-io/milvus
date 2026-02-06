# mlog - Context-Aware Logging Library

mlog is a context-aware logging library built on [zap](https://github.com/uber-go/zap), designed specifically for Milvus distributed systems.

## Design Goals

1. **Mandatory Context Passing** - All logging operations require a context, ensuring request traceability
2. **Zero-Overhead Abstraction** - Uses type aliases to avoid wrapper overhead, performance comparable to direct zap usage
3. **Automatic Field Accumulation** - Context fields automatically accumulate through the call chain, child contexts inherit parent fields
4. **Cross-Service Propagation** - Supports propagating key fields via gRPC metadata for distributed tracing
5. **Lazy Encoding** - Uses `WithLazy` for deferred field encoding, avoiding encoding overhead when log level is disabled

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        mlog Package                          │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  logger.go  │  │  context.go │  │     field.go        │  │
│  │             │  │             │  │                     │  │
│  │ - Init()    │  │ - WithFields│  │ - Field constructors│  │
│  │ - InitNode()│  │ - GetProp.. │  │ - PropagatedString  │  │
│  │ - Debug()   │  │ - logContext│  │ - PropagatedInt64   │  │
│  │ - Info()    │  │             │  │                     │  │
│  │ - Warn()    │  │             │  │                     │  │
│  │ - Error()   │  └─────────────┘  └─────────────────────┘  │
│  │ - Logger    │                                            │
│  │   .With()   │                                            │
│  │   .WithLazy │                                            │
│  └─────────────┘                                            │
│                                                              │
│  ┌─────────────┐  ┌─────────────────────────────────────┐  │
│  │  level.go   │  │         field_enum.go               │  │
│  │             │  │                                     │  │
│  │ - SetLevel  │  │ - Well-known keys (KeyXXX)         │  │
│  │ - GetLevel  │  │ - Field helpers (FieldXXX)         │  │
│  └─────────────┘  └─────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                      mlog/grpc Package                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────┐    │
│  │                  interceptor.go                      │    │
│  │                                                      │    │
│  │  - UnaryServerInterceptor(module)                   │    │
│  │  - StreamServerInterceptor(module)                  │    │
│  │  - UnaryClientInterceptor()                         │    │
│  │  - StreamClientInterceptor()                        │    │
│  │  - extractPropagated() / injectPropagated()         │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

## Core Concepts

### 1. logContext

The logging context stored in context.Context:

```go
type logContext struct {
    fieldKeys map[string]*Field  // Field deduplication map
    logger    *zap.Logger        // Cached logger with accumulated fields
}
```

- **fieldKeys**: Used for field deduplication, later values override earlier ones for the same key
- **logger**: Cached logger with fields already added, avoiding repeated construction

### 2. Field Types

| Type | Description | Propagated |
|------|-------------|------------|
| `String`, `Int64`, ... | Regular fields, local logging only | No |
| `PropagatedString` | Propagatable string field | Yes |
| `PropagatedInt64` | Propagatable int64 field (serialized as string) | Yes |

### 3. Well-Known Keys

Predefined standard field names (lowercase with underscores, gRPC metadata compatible):

```go
const (
    KeyNodeID         = "node_id"
    KeyModule         = "module"
    KeyTraceID        = "trace_id"
    KeySpanID         = "span_id"
    KeyDbID           = "db_id"
    KeyDbName         = "db_name"
    KeyCollectionID   = "collection_id"
    KeyCollectionName = "collection_name"
    KeyPartitionID    = "partition_id"
    KeyPartitionName  = "partition_name"
    KeySegmentID      = "segment_id"
    KeyVChannel       = "vchannel"
    KeyPChannel       = "pchannel"
    KeyMessageID      = "message_id"
    KeyMessage        = "message"
)
```

## Usage Guide

### Initialization

```go
// Option 1: Use custom logger
logger, _ := zap.NewProduction()
mlog.Init(logger)

// Option 2: Initialize with node ID (recommended, nodeId included in all logs)
logger, _ := zap.NewProduction()
mlog.InitNode(logger, nodeId)
```

### Basic Logging

```go
ctx := context.Background()

mlog.Debug(ctx, "debug message", mlog.String("key", "value"))
mlog.Info(ctx, "info message", mlog.Int64("count", 42))
mlog.Warn(ctx, "warning message", mlog.Duration("latency", time.Second))
mlog.Error(ctx, "error message", mlog.Err(err))
```

### Component-Level Logger

Components can create their own Logger with preset fields. Fields are automatically merged with ctx fields when logging:

```go
// Create component Logger (fields pre-encoded for best performance)
type QueryNode struct {
    logger *mlog.Logger
}

func NewQueryNode(nodeID int64) *QueryNode {
    return &QueryNode{
        logger: mlog.With(
            mlog.FieldModule("querynode"),
            mlog.FieldNodeID(nodeID),
        ),
    }
}

func (qn *QueryNode) Search(ctx context.Context, req *SearchRequest) {
    // ctx carries request-level fields (traceID, collectionID, etc.)
    // Automatically merges component fields + ctx fields when logging
    qn.logger.Info(ctx, "search started", mlog.Int64("nq", req.NQ))
    // Output: {"module":"querynode", "node_id":123, "trace_id":"xxx", "nq":10, ...}
}
```

**Logger Methods:**

| Method | Description |
|--------|-------------|
| `mlog.With(fields...)` | Create new Logger with immediately encoded fields |
| `mlog.WithLazy(fields...)` | Create new Logger with lazily encoded fields |
| `(*Logger) With(fields...)` | Add fields (immediately encoded), returns new Logger |
| `(*Logger) WithLazy(fields...)` | Add fields (lazily encoded), returns new Logger |
| `Level()` | Get current log level |
| `Debug/Info/Warn/Error(ctx, msg, fields...)` | Log a message |

**Performance Optimization:**

When logging, compares the pre-encoded field count between ctx and component Logger, selects the one with more fields as base logger:

```
Component Logger: 2 fields (module, nodeID) - pre-encoded
ctx logger:       5 fields (traceID, spanID, ...) - pre-encoded

→ Select ctx logger as base, only need to encode component's 2 fields
→ Faster than using component logger and encoding 5 ctx fields
```

### Context Fields

```go
// Add fields to context (fields accumulate)
ctx = mlog.WithFields(ctx, mlog.String("request_id", "abc123"))
ctx = mlog.WithFields(ctx, mlog.Int64("user_id", 42))

// Subsequent logs automatically include these fields
mlog.Info(ctx, "processing request")
// Output: {"msg":"processing request", "request_id":"abc123", "user_id":42, ...}
```

### Field Deduplication

```go
ctx = mlog.WithFields(ctx, mlog.String("status", "pending"))
ctx = mlog.WithFields(ctx, mlog.String("status", "completed"))  // Overrides previous value

mlog.Info(ctx, "task done")
// Output: {"msg":"task done", "status":"completed", ...}
```

### Cross-Service Field Propagation

```go
// Client: Mark fields for propagation
ctx = mlog.WithFields(ctx,
    mlog.PropagatedString(mlog.KeyCollectionName, "my_collection"),
    mlog.PropagatedInt64(mlog.KeyCollectionID, 12345),
)

// Get propagated fields (for manual propagation scenarios)
props := mlog.GetPropagated(ctx)
// props = map[string]string{"collection_name": "my_collection", "collection_id": "12345"}
```

### gRPC Interceptors

```go
import mloggrpc "github.com/milvus-io/milvus/pkg/v2/mlog/grpc"

// Server configuration
server := grpc.NewServer(
    grpc.UnaryInterceptor(mloggrpc.UnaryServerInterceptor("querynode")),
    grpc.StreamInterceptor(mloggrpc.StreamServerInterceptor("querynode")),
)

// Client configuration
conn, _ := grpc.Dial(addr,
    grpc.WithUnaryInterceptor(mloggrpc.UnaryClientInterceptor()),
    grpc.WithStreamInterceptor(mloggrpc.StreamClientInterceptor()),
)
```

**Interceptor Functions:**

| Interceptor | Function |
|-------------|----------|
| `UnaryServerInterceptor(module)` | Extract TraceID/SpanID, metadata fields, auto-add module |
| `StreamServerInterceptor(module)` | Same as above, for streaming RPC |
| `UnaryClientInterceptor()` | Inject propagated fields into outgoing metadata |
| `StreamClientInterceptor()` | Same as above, for streaming RPC |

### Dynamic Log Level

```go
// Change log level at runtime
mlog.SetLevel(mlog.DebugLevel)
mlog.SetLevel(mlog.WarnLevel)

// Get current level
level := mlog.GetLevel()

// Get AtomicLevel (for custom configuration integration)
atomicLevel := mlog.GetAtomicLevel()
```

## Performance Optimizations

### 1. Early Return

Returns immediately when log level is disabled, avoiding field processing:

```go
func log(ctx context.Context, level Level, msg string, fields ...Field) {
    if !globalLevel.Enabled(level) {
        return  // Fast return, zero overhead
    }
    // ...
}
```

### 2. Lazy Field Encoding

Context fields use `zap.WithLazy` for deferred encoding, only encoded when log is actually written:

```go
// WithFields uses WithLazy internally
ctx = mlog.WithFields(ctx, mlog.String("key", "value"))

// Fields are only encoded when log is written
mlog.Info(ctx, "message")  // Fields encoded here

// If log level is disabled, fields are never encoded
mlog.SetLevel(mlog.ErrorLevel)
mlog.Debug(ctx, "message")  // Fields not encoded, zero overhead
```

Note: Global fields (like nodeId) use `.With()` for immediate encoding since they always need to be output.

### 3. Logger Caching

Each `logContext` caches the constructed logger, avoiding repeated construction:

```go
type logContext struct {
    fieldKeys map[string]*Field
    logger    *zap.Logger  // Cached logger
}
```

### 4. Single-Pass Deduplication

Field deduplication is detected while building the map, avoiding extra traversals:

```go
for i := range fields {
    if _, exists := newFieldKeys[fields[i].Key]; exists {
        hasDuplicate = true
    }
    newFieldKeys[fields[i].Key] = &fields[i]
}
```

## Best Practices

### 1. Always Pass Valid Context

```go
// Recommended
mlog.Info(ctx, "message")

// Not recommended (adds _ctx_nil warning field)
mlog.Info(nil, "message")
```

### 2. Add Request-Level Fields at Entry Points

```go
func HandleRequest(ctx context.Context, req *Request) {
    ctx = mlog.WithFields(ctx,
        mlog.String("request_id", req.ID),
        mlog.String("method", req.Method),
    )
    // All subsequent logs automatically include these fields
    processRequest(ctx, req)
}
```

### 3. Use PropagatedString/PropagatedInt64 for Cross-Service Fields

```go
// Use Propagated version for fields that need cross-service tracing
ctx = mlog.WithFields(ctx,
    mlog.PropagatedString(mlog.KeyCollectionName, collectionName),
    mlog.PropagatedInt64(mlog.KeyCollectionID, collectionId),
)
```

### 4. Use Predefined Well-Known Keys

```go
// Recommended: Use predefined constants
mlog.String(mlog.KeyCollectionName, name)

// Not recommended: Hard-coded strings
mlog.String("collection_name", name)
```

### 5. Specify Module Name in Server Interceptors

```go
// Each service uses its corresponding module name
mloggrpc.UnaryServerInterceptor("proxy")
mloggrpc.UnaryServerInterceptor("querynode")
mloggrpc.UnaryServerInterceptor("datanode")
```

## API Reference

### mlog Package

**Global Functions:**

| Function | Description |
|----------|-------------|
| `Init(logger)` | Initialize global logger |
| `InitNode(logger, nodeId)` | Initialize with node ID |
| `Debug(ctx, msg, fields...)` | Log at Debug level |
| `Info(ctx, msg, fields...)` | Log at Info level |
| `Warn(ctx, msg, fields...)` | Log at Warn level |
| `Error(ctx, msg, fields...)` | Log at Error level |
| `Log(ctx, level, msg, fields...)` | Log at specified level |
| `WithFields(ctx, fields...)` | Add fields to context |
| `FieldsFromContext(ctx)` | Extract fields from context |
| `GetPropagated(ctx)` | Get propagated fields |
| `SetLevel(level)` | Set log level |
| `GetLevel()` | Get current log level |

**Logger Type:**

| Method | Description |
|--------|-------------|
| `With(fields...)` | Create component-level Logger with immediately encoded fields |
| `WithLazy(fields...)` | Create component-level Logger with lazily encoded fields |
| `(*Logger) With(fields...)` | Add fields (immediately encoded), returns new Logger |
| `(*Logger) WithLazy(fields...)` | Add fields (lazily encoded), returns new Logger |
| `(*Logger) Level()` | Get current log level |
| `(*Logger) Debug(ctx, msg, fields...)` | Log at Debug level |
| `(*Logger) Info(ctx, msg, fields...)` | Log at Info level |
| `(*Logger) Warn(ctx, msg, fields...)` | Log at Warn level |
| `(*Logger) Error(ctx, msg, fields...)` | Log at Error level |

### mlog/grpc Package

| Function | Description |
|----------|-------------|
| `UnaryServerInterceptor(module)` | Unary server interceptor |
| `StreamServerInterceptor(module)` | Stream server interceptor |
| `UnaryClientInterceptor()` | Unary client interceptor |
| `StreamClientInterceptor()` | Stream client interceptor |

### Field Constructors

Complete list of field constructors (corresponding to zap):

| Category | Functions |
|----------|-----------|
| **String** | `String`, `Stringp`, `Strings`, `ByteString`, `ByteStrings`, `Stringer` |
| **Bool** | `Bool`, `Boolp`, `Bools` |
| **Int** | `Int`, `Intp`, `Ints`, `Int8/16/32/64` and their `p`/`s` variants |
| **Uint** | `Uint`, `Uintp`, `Uints`, `Uint8/16/32/64` and their `p`/`s` variants, `Uintptr` |
| **Float** | `Float32`, `Float32p`, `Float32s`, `Float64`, `Float64p`, `Float64s` |
| **Complex** | `Complex64`, `Complex64p`, `Complex64s`, `Complex128`, `Complex128p`, `Complex128s` |
| **Time** | `Time`, `Timep`, `Times`, `Duration`, `Durationp`, `Durations` |
| **Error** | `Err`, `NamedError`, `Errors` |
| **Special** | `Any`, `Binary`, `Reflect` |
| **Structured** | `Object`, `Array`, `Inline`, `Namespace` |
| **Debug** | `Stack`, `StackSkip`, `Skip` |
| **Propagation** | `PropagatedString`, `PropagatedInt64` |

### Well-Known Field Functions

Predefined field constructors providing type-safe field creation:

| Function | Type | Description |
|----------|------|-------------|
| `FieldNodeID(val)` | int64 | Node ID |
| `FieldModule(val)` | string | Module name |
| `FieldTraceID(val)` | string | Trace ID |
| `FieldSpanID(val)` | string | Span ID |
| `FieldDbID(val)` | int64 | Database ID |
| `FieldDbName(val)` | string | Database name |
| `FieldCollectionID(val)` | int64 | Collection ID |
| `FieldCollectionName(val)` | string | Collection name |
| `FieldPartitionID(val)` | int64 | Partition ID |
| `FieldPartitionName(val)` | string | Partition name |
| `FieldSegmentID(val)` | int64 | Segment ID |
| `FieldVChannel(val)` | string | Virtual channel name |
| `FieldPChannel(val)` | string | Physical channel name |
| `FieldMessageID(val)` | ObjectMarshaler | Message ID |
| `FieldMessage(val)` | ObjectMarshaler | Message content |

**Usage Example:**

```go
// Using FieldXXX functions (recommended)
mlog.Info(ctx, "segment loaded",
    mlog.FieldCollectionID(12345),
    mlog.FieldSegmentID(67890),
)

// Equivalent to using Key constants
mlog.Info(ctx, "segment loaded",
    mlog.Int64(mlog.KeyCollectionID, 12345),
    mlog.Int64(mlog.KeySegmentID, 67890),
)
```
