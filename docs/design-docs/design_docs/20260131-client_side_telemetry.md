# MEP: Client-Side Telemetry with Heartbeat and Server Command Support

- **Created:** 2026-01-31
- **Author(s):** @xiaofanluan
- **Status:** Implemented
- **Component:** SDK | Proxy | Coordinator
- **Related Issues:** #46934, #47281
- **Implemented by:** #47523, #47542

> This document was refreshed to match the implementation. Where the original draft
> described interfaces that were never built (`set_sampling_rate`, `enable_collections`,
> `update_config`, `/api/v1/telemetry/*`, `NewConfigHash`), those sections have been
> replaced with what the code actually does. Source of truth is
> `internal/rootcoord/telemetry/`, `internal/proxy/telemetry_*.go` and
> `client/milvusclient/telemetry.go`.

## Summary

This MEP introduces a client-side telemetry system for the Go SDK that collects operational metrics, sends periodic heartbeats to the server, and supports bidirectional communication through server-pushed commands. The system provides visibility into client behavior, enables real-time monitoring through a WebUI dashboard, and allows server-initiated configuration changes.

## Motivation

Currently, Milvus lacks visibility into client-side operations and behavior. Operators cannot:
- Monitor which clients are connected to the cluster
- Understand client-side performance characteristics (latency, error rates)
- Identify problematic clients or usage patterns
- Push configuration changes to connected clients dynamically

This feature addresses these gaps by implementing a comprehensive client telemetry system that:
1. Collects client-side metrics (request counts, latencies, errors)
2. Reports telemetry data to the server via heartbeats
3. Enables server-to-client command channels for dynamic configuration
4. Provides a WebUI for operators to monitor and manage clients

## Public Interfaces

### Go SDK APIs

```go
// TelemetryConfig holds configurable settings for client telemetry
type TelemetryConfig struct {
    Enabled           bool          // Enable/disable telemetry collection (default: true)
    HeartbeatInterval time.Duration // Heartbeat frequency (default: 30s)
    SamplingRate      float64       // Sampling rate 0.0-1.0 (default: 1.0)
    ErrorMaxCount     int           // Max errors to track (default: 100)
}

// ClientConfig gains a new field
type ClientConfig struct {
    // ... existing fields ...
    TelemetryConfig *TelemetryConfig
}
```

`DefaultTelemetryConfig()` returns `Enabled: true`. Telemetry is opt-in at the level of
whether a caller constructs a `TelemetryConfig` at all, not via this flag.

### HTTP REST APIs (Proxy)

Served on the internal HTTP port (`9091` by default), not on the gRPC port.

```
GET    /_telemetry/clients                      - List connected clients
GET    /_telemetry/clients/{clientId}           - Metrics for one client
GET    /_telemetry/clients/{clientId}/config    - Ask a client for its config (async)
GET    /_telemetry/clients/{clientId}/history   - Ask a client for latency history (async)
POST   /_telemetry/commands                     - Push a command to clients
GET    /_telemetry/commands/{commandId}/reply   - Fetch a client's reply to a command
DELETE /_telemetry/commands/{commandId}         - Delete a command
GET    /telemetry                               - WebUI dashboard
```

Path constants live in `internal/http/router.go`; registration is in
`internal/proxy/impl.go` (`registerHTTPHandlers`).

### gRPC APIs

The RPCs live in their own service, `ClientTelemetryService`, registered on the Proxy's
**external** gRPC server. Proxy forwards to MixCoord, which forwards to RootCoord, where
`TelemetryManager` holds the state.

```protobuf
service ClientTelemetryService {
    // Client heartbeat with metrics; response carries pending commands
    rpc ClientHeartbeat(ClientHeartbeatRequest) returns (ClientHeartbeatResponse);

    // Query connected clients
    rpc GetClientTelemetry(GetClientTelemetryRequest) returns (GetClientTelemetryResponse);

    // Push commands to clients
    rpc PushClientCommand(PushClientCommandRequest) returns (PushClientCommandResponse);

    // Delete commands
    rpc DeleteClientCommand(DeleteClientCommandRequest) returns (DeleteClientCommandResponse);
}
```

`ClientHeartbeat` carries no `privilege_ext_obj` annotation, so the privilege interceptor
short-circuits for it and only normal authentication applies. The REST endpoints are
guarded by `TelemetryAuthMiddleware`, which is Basic Auth only -- there is no RBAC
privilege check on this surface.

## Design Details

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client (Go SDK)                                 │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    ClientTelemetryManager                              │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │  │
│  │  │ Operation       │  │ Error           │  │ Command         │       │  │
│  │  │ Metrics         │  │ Collector       │  │ Handler         │       │  │
│  │  │ Collector       │  │ (Ring Buffer)   │  │ Registry        │       │  │
│  │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘       │  │
│  │           │                    │                    │                 │  │
│  │           └────────────────────┼────────────────────┘                 │  │
│  │                                ▼                                       │  │
│  │                    ┌───────────────────────┐                          │  │
│  │                    │   Heartbeat Loop      │───────── 30s interval    │  │
│  │                    │   (Background)        │                          │  │
│  │                    └───────────┬───────────┘                          │  │
│  └────────────────────────────────┼──────────────────────────────────────┘  │
└───────────────────────────────────┼─────────────────────────────────────────┘
                                    │
                          ClientHeartbeat RPC
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Server (Milvus)                                 │
│  ┌────────────────┐          ┌───────────────────────────────────────────┐  │
│  │     Proxy      │◄────────►│              RootCoord                     │  │
│  │                │          │  ┌─────────────────────────────────────┐  │  │
│  │  HTTP API      │          │  │      Telemetry Manager              │  │  │
│  │  /_telemetry/* │          │  │  ┌───────────┐  ┌───────────────┐   │  │  │
│  │                │          │  │  │ Client    │  │ Command       │   │  │  │
│  │  WebUI         │          │  │  │ Cache     │  │ Store         │   │  │  │
│  │  telemetry.html│          │  │  └───────────┘  └──────┬────────┘   │  │  │
│  └────────────────┘          │  └────────────────────────┼────────────┘  │  │
│                              └───────────────────────────┼───────────────┘  │
│                                                          ▼                   │
│                                            etcd: /client-telemetry/configs/  │
│                                            (persistent configs only)         │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Client-Side Components

#### 1. ClientTelemetryManager

The central component managing telemetry collection and heartbeat communication.

```go
type ClientTelemetryManager struct {
    config          *TelemetryConfig
    client          *Client
    clientID        string                              // UUID, see below
    collectors      map[string]*OperationMetricsCollector
    errorCollector  *ErrorCollectorImpl
    commandHandlers map[string]CommandHandler

    // Deduplication and change detection
    configHash           string
    lastCommandTimestamp atomic.Int64
    executedCommands     map[string]int64

    // Heartbeat management
    stopCh chan struct{}
    wg     sync.WaitGroup
}
```

**Key behaviors:**
- Generates a client UUID on creation. It is stable for the lifetime of the `Client`, and
  therefore across gRPC reconnects, but **not across process restarts** -- each `New()`
  produces a fresh UUID. Servers see a restarted process as a new client.
- Starts background heartbeat loop on `Start()`
- Sends first heartbeat immediately, then every `HeartbeatInterval`
- Uses `time.After` per iteration rather than a `time.Ticker`, so a server-pushed interval
  change takes effect on the next cycle
- Collects and resets metrics atomically during snapshot creation

**Instrumented operations (7):** `Search`, `Query`, `HybridSearch`, `RunAnalyzer`,
`Insert`, `Delete`, `Upsert`. DDL, index and partition operations are not instrumented.

#### 2. OperationMetricsCollector

Per-operation metrics collection with global and per-collection breakdown.

```go
type OperationMetricsCollector struct {
    // Global metrics
    requestCount int64
    successCount int64
    errorCount   int64
    totalLatency int64  // microseconds
    maxLatency   int64

    // P99 calculation (ring buffer of 1000 samples)
    latencySamples []int64
    totalSamples   int64

    // Per-collection metrics
    collectionMetrics map[string]*CollectionMetrics
}
```

**Metrics tracked:** request count, success/error counts, average latency (ms), P99 latency
(ms, from the 1000-sample buffer), max latency (ms).

P99 is computed inside the locked snapshot, *before* the sample buffer is reset, so a
concurrent heartbeat cannot read a cleared buffer. `totalSamples` is tracked separately
from the buffer index so a genuine 0µs latency is distinguishable from an unwritten slot.

Per-collection metrics are **off by default** and enabled by the `collection_metrics`
command.

#### 3. ErrorCollectorImpl

Ring buffer implementation for tracking recent errors.

```go
type ErrorCollectorImpl struct {
    errors   []*ErrorInfo
    maxCount int
    index    int  // Ring buffer index
}

type ErrorInfo struct {
    Timestamp  int64  `json:"timestamp"`             // Unix ms
    Operation  string `json:"operation"`
    ErrorMsg   string `json:"error_msg"`
    Collection string `json:"collection,omitempty"`
    RequestID  string `json:"request_id,omitempty"`
}
```

#### 4. Command Handler System

Supports server-pushed commands with extensible handler registration.

```go
type CommandHandler func(cmd *ClientCommand) *CommandReply
```

**Command types.** These five strings are the complete set. `command_type` is a free-form
string on the wire and the server does not validate it on push, so an unknown type reaches
the client and is answered with `"unknown command type: <type>"`.

| Type | Purpose | May be persistent |
|------|---------|-------------------|
| `push_config` | Change client telemetry settings | **Yes (the only one)** |
| `collection_metrics` | Enable/disable per-collection metrics | No |
| `show_errors` | Return the last N client-side errors | No |
| `show_latency_history` | Return client metric snapshots for a time window | No |
| `get_config` | Return the client's current effective config | No |

`command_store.go` rejects `persistent=true` for anything other than `push_config`.

**Payload schemas.** `ClientCommand.payload` is raw JSON (not protobuf).

```go
// push_config
type PushConfigPayload struct {
    Enabled             *bool    `json:"enabled,omitempty"`
    HeartbeatIntervalMs *int64   `json:"heartbeat_interval_ms,omitempty"`
    SamplingRate        *float64 `json:"sampling_rate,omitempty"`   // 0.0-1.0
    TTLSeconds          int64    `json:"ttl_seconds,omitempty"`
}

// collection_metrics -- "*" in Collections is the all-collections wildcard
type CollectionMetricsPayload struct {
    Collections  []string `json:"collections"`
    Enabled      bool     `json:"enabled"`
    MetricsTypes []string `json:"metrics_types,omitempty"`
}

// show_errors
type ErrorMessagesPayload struct {
    MaxCount int `json:"max_count,omitempty"`   // default 100
}

// show_latency_history -- RFC3339 timestamps, window must be <= 1 hour
type LatencyHistoryPayload struct {
    StartTime string `json:"start_time"`
    EndTime   string `json:"end_time"`
    Detail    bool   `json:"detail"`
}

// get_config -- no payload
```

Reply payloads are capped at 1 MB client-side; `show_errors` halves the returned count
until it fits. `get_config` deliberately omits `Password` and `APIKey`.

### Server-Side Components

#### 1. Telemetry Manager (RootCoord)

Central server-side storage for client telemetry data. Client state is held in a
`sync.Map` keyed by client ID.

```go
type ClientMetricsCache struct {
    ClientInfo        *commonpb.ClientInfo
    LastHeartbeatTime int64
    Status            string       // "active" or "inactive"
    AccessedDatabases sync.Map     // accumulative, never pruned
    LatestMetrics     []*commonpb.OperationMetrics
    CommandReplies    []*StoredCommandReply   // last 50
    LastCommandTS     int64
}
```

**Client identity.** The manager reads `ClientInfo.Reserved["client_id"]`. When absent it
falls back to `legacy:<host>:<hash(sdkType|sdkVersion|host|user)>`, in which case two
processes on the same host with the same SDK and user **collide into one entry** and
`client:` scoping becomes unusable. Database association comes from
`Reserved["db_name"]` (or `Reserved["database"]`).

**Hardcoded limits.** There are no `milvus.yaml` or paramtable keys for this feature;
`DefaultTelemetryConfig()` in `manager.go` is never overridden in production:

| Setting | Value | Effect |
|---------|-------|--------|
| `ClientStatusThreshold` | 1 min | No heartbeat for this long → `Status: "inactive"` |
| `InactiveClientThreshold` | 10 min | No heartbeat for this long → evicted from memory |
| `CleanupInterval` | 1 min | Sweep cadence for expired commands and dead clients |
| `MaxClientsInMemory` | 100000 | Then LRU eviction by last heartbeat |
| `MaxMetricsPerClient` | 1 MB | Larger payloads are truncated (see below) |
| `MaxOperationTypesPerClient` | 100 | Operation list truncated to this many |

**Metrics ingest guards.** On each heartbeat the server drops every `CollectionMetrics`
entry whose `RequestCount == 0`, truncates the operation list to 100, and if the message
still exceeds 1 MB, first nulls all `CollectionMetrics`, then truncates further.

#### 2. Command Store

Manages pending commands, with different storage for the two kinds:

- **Persistent (`push_config` only)** → written to etcd under `/client-telemetry/configs/`.
  Survives RootCoord restart. Deduplicated by `(ConfigType, TargetScope)`: pushing a second
  config for the same scope **JSON-merges** the new payload over the old one and deletes the
  superseded entry, so partial updates accumulate rather than replace.
- **One-time** → in-memory on the RootCoord that received the push. Lost on restart and
  **not shared across coordinator replicas**.

**Command targeting** (`TargetScope`, built server-side from the push request):

- `global` — all clients
- `client:<clientID>` — one client, exact match
- `database:<dbName>` — clients that have accessed that database

**TTL.** `ttl_seconds` is a field of `PushClientCommandRequest`, not of `ClientCommand`, so
clients never see it. It is resolved once at push time:

| Requested | Stored | Meaning |
|-----------|--------|---------|
| unset (`0`) | `300` | Default: ten heartbeat cycles at the nominal 30s interval |
| `> 0` | as given | Honoured verbatim |
| `< 0` | as given | Never expires (every expiry check treats `<= 0` as immortal) |

A one-time command that has not been collected within ten heartbeat cycles is not going to
be, so it is dropped. Without a default, a client that restarted, crashed, or simply never
answered would leave the command in RootCoord memory indefinitely — and clients are
expected to be ephemeral, since the SDK generates a fresh client ID per process.

Replying still deletes a command immediately; the TTL is the backstop for when no reply
ever arrives. Persistent configs ignore TTL entirely.

#### 3. HTTP Handlers (Proxy)

REST API endpoints for WebUI and external integrations.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/_telemetry/clients` | GET | List clients; `?database=`, `?client_id=`, `?include_metrics=` |
| `/_telemetry/clients/{clientId}` | GET | Metrics for one client |
| `/_telemetry/clients/{clientId}/config` | GET | Push `get_config`; returns a command ID |
| `/_telemetry/clients/{clientId}/history` | GET | Push `show_latency_history`; `?start_time=`, `?end_time=`, `?detail=` |
| `/_telemetry/commands` | POST | Push an arbitrary command |
| `/_telemetry/commands/{commandId}/reply` | GET | Fetch a client's reply; `?client_id=`, `?wait=` |
| `/_telemetry/commands/{commandId}` | DELETE | Remove a command |

**Authentication:** Basic Auth via `TelemetryAuthMiddleware`, active only when
`common.security.authorizationEnabled` is set. No RBAC.

**Asynchrony.** Commands are answered on the client's next heartbeat, so any endpoint that
pulls data from a client is inherently asynchronous. `/config`, `/history` and the
command-reply endpoint share one response shape:

```json
{"command_id": "...", "client_id": "...", "status": "pending" | "done", "reply": { ... }}
```

Callers branch on `status` and read `reply` when it is `done`. `pending` is returned with
HTTP 200, not an error: a reply that has not arrived is indistinguishable from one that
never will, since replies are also evicted once a client accumulates more than 50.

Two ways to collect a result:

- **Synchronous** — pass `?wait=30s` to `/config`, `/history`, or the reply endpoint. The
  proxy polls until the reply lands or the budget expires. Waits are clamped to 90s and
  bounded by the request context; expiry is reported as `pending`.
- **Deferred** — push without `wait`, keep the returned `command_id`, and fetch it later
  from `/_telemetry/commands/{commandId}/reply`. Passing `?client_id=` makes that a
  targeted lookup instead of a scan of all clients.

Replies are also visible in the `command_replies` array of `GET /_telemetry/clients`,
which is how the WebUI polls; on the wire they are JSON-encoded into
`ClientInfo.Reserved["command_replies"]` rather than carried in a dedicated proto field.

### Heartbeat Protocol

```
Client                                      Server
   │                                           │
   │──── ClientHeartbeatRequest ──────────────►│
   │     - ClientInfo (ID, SDK version, host)  │
   │     - Metrics (per-operation, per-coll)   │
   │     - CommandReplies                      │
   │     - ConfigHash                          │
   │     - LastCommandTimestamp                │
   │                                           │
   │◄─── ClientHeartbeatResponse ─────────────│
   │     - ServerTimestamp                     │
   │     - Commands (pending for this client)  │
   │                                           │
```

**Heartbeat interval:** 30 seconds (client default; the server can change it via
`push_config`).

**`report_timestamp`** is accepted but ignored — the server uses its own clock for
`LastHeartbeat` and `server_timestamp`.

#### Config hash

Used to avoid re-sending persistent configs on every heartbeat. Both sides must compute it
identically or configs are re-pushed forever.

```
if no configs:            hash = ""
else:                     sort configs by ID ascending
                          h = sha256()
                          for each: h.write(ID); h.write(Type); h.write(Payload)
                          hash = hex(h.sum())[:16]      // first 16 hex chars
```

The server computes it over the configs **already filtered to this client's scope**, and
sends persistent configs only when `request.ConfigHash != serverHash`. The response has no
"new hash" field — the client recomputes it locally after processing commands.

#### Command delivery and deduplication

One-time commands are returned only when `command.CreateTime > request.LastCommandTimestamp`
(strict). The client advances `LastCommandTimestamp` to the maximum `CreateTime` it has
seen, **after** processing the whole batch, so a mid-batch crash re-fetches. Because the
comparison is strict, clients must additionally deduplicate by command ID to handle two
commands created in the same millisecond.

Consequence: a one-time command is delivered **once**. It is not redelivered if the client
fails to execute it, because the client has already advanced its watermark past it.

#### Replies

Any reply with a non-empty `command_id` — **whether or not `success` is true** — deletes the
corresponding non-persistent command server-side. Replies are the fast path for reclaiming
a command; the TTL above is the backstop for clients that never answer.

The client queues replies and clears them **only after a successful heartbeat**, so replies
survive a failed heartbeat and are retried on the next one. Commands already executed still
receive an idempotent success ACK.

Persistent configs are never deleted by a reply; they are removed only via
`DeleteClientCommand`, and are suppressed on the wire whenever `config_hash` matches.

### Data Flow

1. **Metrics Collection:**
   - Each instrumented SDK operation records into a per-operation collector
   - Sampling rate determines whether an operation is tracked (default 100%)

2. **Heartbeat Cycle:**
   - Background goroutine sends immediately on start, then every `HeartbeatInterval`
   - Creates an atomic snapshot of all metrics (resetting counters, computing P99)
   - Sends the snapshot, pending replies, config hash and watermark
   - Receives and processes any pending commands

3. **Command Processing:**
   - Server returns commands in the heartbeat response
   - Client dispatches to the registered handler for that type
   - Reply is queued and sent with the next heartbeat
   - Persistent configs are re-sent only when the client's `config_hash` disagrees

### WebUI Dashboard

A telemetry dashboard served at `/telemetry` (source: `internal/http/webui/telemetry.html`)
provides:

- **Client List:** Active/inactive clients with connection details
- **Metrics View:** Per-client operation metrics with latency charts
- **Command Interface:** Send commands to specific clients or broadcast
- **Filtering:** By database, client ID, or status

## Compatibility, Deprecation, and Migration Plan

- **Backward Compatible:** No wire or API breaking changes.
- **No Breaking Changes:** Existing SDK usage remains unchanged.
- **Server Compatibility:** Old clients simply never heartbeat; they appear only through
  their `Connect` call, not in telemetry.
- **Client Compatibility:** Against a server without `ClientTelemetryService`, the heartbeat
  fails with `codes.Unimplemented`. The client latches telemetry off and stops the heartbeat
  loop rather than retrying a call that can never succeed. Because telemetry is best-effort
  and the `client/` module carries no logger, the failure is not raised through the normal
  API; inspect it with `ClientTelemetryManager.IsSupported()` and `LastHeartbeatError()`.

## Implementation Status

Client-side telemetry is implemented in the **Go SDK only**. pymilvus, the Java, Node.js and
Rust SDKs do not implement the client half; the generated protobuf stubs exist for some of
them but are unused. Any operator-facing claim about client coverage should be read as
"Go SDK clients only".

Server-side components are complete. Two pieces of the original design are present but not
wired into the live path: `CommandRouter` validates payload shapes but is never invoked from
`HandleHeartbeat`, and `PushCommand` does not validate `command_type` against it.

## Test Plan

### Unit Tests
- `client/milvusclient/telemetry_test.go`: metrics collection, P99, ring buffer,
  command dispatch, config hash
- `internal/proxy/telemetry_http_handler_test.go`: HTTP API handlers
- `internal/rootcoord/telemetry/*_test.go`: manager, command store, scope matching

### Integration Tests
- `client/milvusclient/telemetry_integration_test.go`: end-to-end heartbeat flow
- Multi-client scenarios with different configurations
- Command push and execution verification

### Manual Testing
- WebUI functionality verification
- Performance impact measurement under load
- Network failure and recovery scenarios

## Rejected Alternatives

### 1. Streaming Telemetry (Rejected)
Using streaming RPC instead of periodic heartbeats.
- **Rejected because:** Higher resource usage, more complex failure handling

### 2. External Metrics System (Rejected)
Push metrics to external systems (Prometheus, etc.) from client.
- **Rejected because:** Adds external dependencies, complicates deployment

### 3. Pull-Based Model (Rejected)
Server polls clients for metrics.
- **Rejected because:** Doesn't scale, requires client to expose endpoints

## References

- Server implementation: `internal/rootcoord/telemetry/`
- Proxy HTTP layer: `internal/proxy/telemetry_http_handler.go`, `telemetry_models.go`
- Go SDK client: `client/milvusclient/telemetry.go`
- gRPC health checking: https://github.com/grpc/grpc/blob/master/doc/health-checking.md
- OpenTelemetry SDK patterns: https://opentelemetry.io/docs/instrumentation/go/
