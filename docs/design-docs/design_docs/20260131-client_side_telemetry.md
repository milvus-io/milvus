# MEP: Client-Side Telemetry with Heartbeat and Server Command Support

- **Created:** 2026-01-31
- **Author(s):** @xiaofanluan
- **Status:** Implemented
- **Component:** SDK | Proxy | Coordinator
- **Related Issues:** #46934, #47281
- **Implemented by:** #47523, #47542

> This document was refreshed to match the implementation. Where the original draft
> described interfaces that were never built (`set_sampling_rate`, `enable_collections`,
> `update_config`, `NewConfigHash`), those sections have been
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
    HeartbeatInterval time.Duration // Heartbeat frequency (default: 10s)
    SamplingRate      float64       // Sampling rate 0.0-1.0 (default: 1.0)
    ErrorMaxCount     int           // Max errors to track (default: 100)
    ClientID          string        // Optional stable identity across process restarts
}

// ClientConfig gains a new field
type ClientConfig struct {
    // ... existing fields ...
    TelemetryConfig *TelemetryConfig
}
```

**Telemetry is on by default and must be turned off explicitly.** `New()` always constructs
and starts the manager; a nil `TelemetryConfig` is replaced by `DefaultTelemetryConfig()`,
which returns `Enabled: true` with a 10s heartbeat and 100% sampling. A caller who never
mentions `TelemetryConfig` therefore still reports heartbeats and metrics to the server. To
disable it:

```go
client.New(ctx, &client.ClientConfig{
    Address:         "localhost:19530",
    TelemetryConfig: &milvusclient.TelemetryConfig{Enabled: false},
})
```

An initial `Enabled: false` is an explicit opt-out and starts no heartbeat worker. If an
already-running client is disabled later by a server `push_config`, operation collection and
metric payloads stop, but the lightweight command heartbeat remains active so the disable
reply is acknowledged and a later re-enable can be received.

### HTTP REST APIs (Proxy)

Served on the internal HTTP port (`9091` by default), not on the gRPC port.

```
GET    /api/v1/_telemetry/clients                    - List connected clients
GET    /api/v1/_telemetry/clients/{clientId}         - Metrics for one client
GET    /api/v1/_telemetry/clients/{clientId}/config  - Ask a client for its config
GET    /api/v1/_telemetry/clients/{clientId}/history - Ask a client for latency history
POST   /api/v1/_telemetry/commands                   - Push a command to clients
GET    /api/v1/_telemetry/commands/{commandId}/reply - Fetch a client's reply to a command
DELETE /api/v1/_telemetry/commands/{commandId}       - Delete a command
GET    /webui/telemetry.html                         - WebUI dashboard
```

The path constants in `internal/http/router.go` are relative: `RegisterRestRouter` is
mounted on the `/api/v1` group in `internal/distributed/proxy/service.go`, so the served
paths carry that prefix. Verified against a running standalone.

`internal/http/router.go` also declares `TelemetryUIPath = "/telemetry"` and registers a
handler for it, but that route returns 404 on a running server; the dashboard is reachable
only as `/webui/telemetry.html`.

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
│  │                    │   Heartbeat Loop      │───────── 10s default     │  │
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
│  │ /api/v1/_tel..│          │  │  ┌───────────┐  ┌───────────────┐   │  │  │
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
- Generates a client UUID on creation unless `TelemetryConfig.ClientID` pins a stable value.
  The resolved ID is stable for the lifetime of the `Client` and across gRPC reconnects. It
  survives a process restart only when the caller supplies `ClientID`.
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
Custom handlers execute on the heartbeat goroutine. A handler panic is recovered and turned
into a failed command reply so it cannot terminate the process or stop later heartbeats.

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

Latency snapshots are retained by timestamp for one hour, independent of the dynamically
configured heartbeat interval. A 4096-snapshot hard cap is the final memory bound for
sub-second intervals. The previous fixed 120-snapshot cap represented one hour only at the
obsolete 30-second default and retained just 20 minutes at the current 10-second default.
Each operation/window also retains an internal 128-point, evenly spaced quantile sketch
from the collector's recent sample ring. Aggregated history merges those weighted samples
and computes P99 from the combined distribution; averaging the P99 values of individual
windows is mathematically invalid. The sketch is internal and is not added to heartbeat or
detail-mode response payloads.

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

**Persistent configs and client scope.** A persistent config is keyed by target scope, so
a `client:` config keeps applying only for as long as the target keeps its ID. By default
the SDK generates a fresh UUID per process, so such a config would silently stop applying
after a restart while remaining in etcd. It is therefore rejected unless the target client
is currently connected *and* reports `Reserved["client_id_stable"] = "true"`, which the Go
SDK sets when the caller pinned `TelemetryConfig.ClientID`.

The decision is made on the identity the client declares, not on the scope: a pinned ID
does survive restarts, and a config aimed at one is legitimate. For the same reason,
existing client-scoped configs are loaded normally at startup and never deleted
automatically — retire them with `DeleteClientCommand`.

**TTL.** `ttl_seconds` is a field of `PushClientCommandRequest`, not of `ClientCommand`, so
clients never see it.

| `ttl_seconds` | Meaning at the RPC / store |
|---------------|---------------------------|
| absent | Same as `0` — no expiry. The store applies no default. |
| `0` | Never expires (every expiry check treats `<= 0` as immortal) |
| `> 0` | Expires that many seconds after the push |
| `< 0` | Never expires |

**The one-hour default is applied by the HTTP layer, not the store**, and marking the field
`optional` does not change that. Presence only helps senders that know about it: proto3
implicit presence means a client built against the older definition emits *nothing* for an
explicit `0`, so the server receives it as absent and cannot tell that deliberate "never
expire" apart from "unspecified". Defaulting on absence would silently give every such
client a one-hour expiry with no way to ask for the old behavior back.

`POST /_telemetry/commands` decodes `ttl_seconds` into a pointer, so it genuinely can see
the difference: omit the field and you get 3600, send `0` and you get no expiry. It then
sends an explicit value onward, so the store never has to guess.

That default is a **bound on how long an unanswered command occupies memory, not a delivery
window**. It deliberately does not encode "N heartbeat cycles": `HeartbeatInterval` is
client-side config with no upper bound, the server is never told what it is, and clients
matched by one scope may use different values. A default expressed in cycles would expire
before a client on a long interval ever got a chance to read the command.

Without any default, a command that no client ever collects stays in RootCoord memory for
the life of the process.

Persistent configs ignore TTL entirely.

**Do not combine a broadcast scope with `ttl_seconds: 0`.** A `global` or `database:` command
is removed only by its TTL, and until then it is still handed to clients that connect later.
With no expiry it never goes away and every new client keeps executing it.

A reply reclaims a command only when the command named a single client — see
[Replies](#replies). A broadcast command is answered by many clients, so for it the TTL is
the only thing that ever removes it.

#### 3. HTTP Handlers (Proxy)

REST API endpoints for WebUI and external integrations.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/_telemetry/clients` | GET | List clients; `?database=`, `?client_id=`, `?include_metrics=` |
| `/api/v1/_telemetry/clients/{clientId}` | GET | Metrics for one client |
| `/api/v1/_telemetry/clients/{clientId}/config` | GET | Push `get_config`; returns a command ID |
| `/api/v1/_telemetry/clients/{clientId}/history` | GET | Push `show_latency_history`; `?start_time=`, `?end_time=`, `?detail=` |
| `/api/v1/_telemetry/commands` | POST | Push an arbitrary command |
| `/api/v1/_telemetry/commands/{commandId}/reply` | GET | Fetch a client's reply; `?client_id=` |
| `/api/v1/_telemetry/commands/{commandId}` | DELETE | Remove a command |

**Authentication:** Basic Auth via `TelemetryAuthMiddleware`, active only when
`common.security.authorizationEnabled` is set. No RBAC.

**Asynchrony.** Commands are answered on the client's next heartbeat, so any endpoint that
pulls data from a client is inherently asynchronous. `/config`, `/history` and the
command-reply endpoint share one response shape:

```json
{
  "command_id": "...",
  "status": "pending" | "done",
  "responded": 2,
  "observed_clients": 3,
  "replies": [ {"client_id": "...", "reply": { ... }} ],
  "client_id": "...",
  "reply": { ... }
}
```

Callers branch on `status` and read `replies` when it is `done`. `pending` is returned with
HTTP 200, not an error: a reply that has not arrived is indistinguishable from one that
never will, since replies are also evicted once a client accumulates more than 50.

**One command can have many answers.** A command with neither `target_client_id` nor
`target_database` is stored with scope `global` and delivered to *every* connected client,
each of which answers under the same command ID. So `replies` is always an array, one entry
per answering client, and each entry carries the `client_id` it came from — otherwise an
operator reads one arbitrary client's data as the cluster's. Entries are ordered by client
ID so a repeated request is stable; the underlying iteration is over a `sync.Map`, whose
order is unspecified.

A broadcast command is **not** deleted when the first client answers. It is delivered to
every matching client, each replying on its own heartbeat, so retiring it on the first reply
would let whichever client heartbeats soonest cancel delivery to the rest — with clients on
a 30s and a 5min interval, the slow one would never see the command at all. Only
`client:`-scoped commands, which have exactly one recipient, are removed on reply; `global`
and `database:` ones live until their TTL. Clients skip commands older than their
`last_command_timestamp` watermark, so retention does not cause re-execution — but a client
that connects during the TTL window does execute the command, which is what you want for a
fleet-wide state change and merely noisy for a one-off query.

`responded` and `observed_clients` are **observations, not a progress bar**.
`observed_clients` counts what the lookup scanned, not what the command targeted: the scan
covers every cached client regardless of the command's scope, includes clients that have
gone inactive or connected after the push, and its membership changes between polls. The
server does not record who a broadcast command was delivered to, so neither number
establishes completeness. Re-querying later returns everything accumulated so far.

`reply` and `client_id` repeat the first entry. They are the whole answer for a
client-scoped command — which `/config` and `/history` always are — and anything reading a
broadcast command must use `replies`.

**Collecting a result is always deferred.** Push the command, keep the returned
`command_id`, and read it later from `/api/v1/_telemetry/commands/{commandId}/reply`.
Passing `?client_id=` makes that a lookup of one client instead of a scan of every cached
one, and is strongly preferred.

There is deliberately **no server-side blocking mode**. An earlier revision of this work
offered `?wait=`, which polled RootCoord on the caller's behalf; it was removed because the
cost multiplies in a way the caller cannot see:

- Every lookup carries each matching client's **entire** stored reply history — up to 50
  replies — because `command_replies` is encoded into `ClientInfo.Reserved` regardless of
  `IncludeMetrics`, and the proxy filters by command ID only after decoding.
- Reply payloads are bounded at 1MiB only for the **built-in** handlers. A reply from a
  handler registered through `RegisterCommandHandler` has no size limit at all.
- Without `client_id` the scan covers the whole fleet, so the per-lookup cost scales with
  the number of connected clients.
- `common.security.authorizationEnabled` defaults to **false**, and
  `TelemetryAuthMiddleware` passes every request straight through when it is off — so the
  endpoint is unauthenticated by default.

A blocking mode turns one unauthenticated HTTP request into dozens of full-history
transfers inside the cluster. A caller that wants to wait polls the endpoint on its own
schedule instead, which keeps one request to one internal query and puts the cost where it
is visible.

`GetClientTelemetryRequest.command_id`
([milvus-io/milvus-proto#647](https://github.com/milvus-io/milvus-proto/pull/647)) would let
the server return just the requested reply and remove the per-lookup amplification
entirely. This repo pins a milvus-proto that predates it; using it is a follow-up gated on a
proto bump, and a blocking mode should only be reconsidered once it is in place.

Replies are also visible in the `command_replies` array of `GET /api/v1/_telemetry/clients`,
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

**Heartbeat interval:** 10 seconds (client default; the server can change it via
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
seen, **after** processing the whole batch, so a mid-batch crash re-fetches.

Consequence: a one-time command is delivered **once**. It is not redelivered if the client
fails to execute it, because the client has already advanced its watermark past it.

> **Known defect: a millisecond-granularity watermark loses commands.**
>
> The cursor is a millisecond timestamp and the comparison is strict, so a command created
> in the *same millisecond* as one the client has already processed is filtered out of every
> subsequent heartbeat and is never delivered. It survives only until its TTL and then
> disappears, with no error to the pusher and no record on the client.
>
> Relaxing the comparison to `>=` does not fix it: the client prunes its executed-ID set at
> the watermark, so commands at exactly that timestamp would eventually be re-executed
> instead. A correct fix needs a cursor that is unique per command — a strictly monotonic
> sequence number, or a `(timestamp, command_id)` pair compared lexicographically — which is
> a protocol change on both sides.
>
> This is pre-existing, not introduced by the client-telemetry audit, and is tracked in
> [#51963](https://github.com/milvus-io/milvus/issues/51963). It is recorded here because an
> earlier revision of this document described the watermark as already correct; it is not.

#### Replies

A reply with a non-empty `command_id` — **whether or not `success` is true** — reclaims the
corresponding non-persistent command server-side, but **only when that command was scoped to
a single client** (`client:<id>`). There the reply is the whole answer, so the command is
finished.

A `global` or `database:` command is delivered to every matching client and answered by each
on its own heartbeat. Deleting it on the first reply would let whichever client heartbeats
soonest cancel delivery to all the others — with clients on a 30s and a 5min interval, the
slow one would never receive it at all. Those commands are removed only by their TTL.

So: replies are the fast path for reclaiming a **client-scoped** command; the TTL is the
backstop for client-scoped commands nobody answers, and the *only* mechanism for broadcast
ones.

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
   - A custom-handler panic becomes a failed reply; later commands and heartbeats continue
   - Reply is queued and sent with the next heartbeat
   - Persistent configs are re-sent only when the client's `config_hash` disagrees

### WebUI Dashboard

A telemetry dashboard served at `/webui/telemetry.html` provides:

- **Client List:** Active/inactive clients with connection details
- **Metrics View:** Per-client operation metrics with latency charts
- **Command Interface:** Send commands to specific clients or broadcast
- **Filtering:** By database, client ID, or status

## Compatibility, Deprecation, and Migration Plan

- **Backward Compatible:** No wire or API breaking changes, and no proto change at all —
  this work runs entirely on the existing RPC surface.
- **No Breaking Changes:** Existing SDK usage remains unchanged.
- **Server Compatibility:** Old clients simply never heartbeat; they appear only through
  their `Connect` call, not in telemetry.
- **Client Compatibility:** Against a server without `ClientTelemetryService`, the heartbeat
  fails with `codes.Unimplemented`. The client does **not** switch telemetry off. It backs
  off exponentially — doubling the heartbeat interval per consecutive rejection, capped at
  30 minutes, never shortening below the configured interval — and keeps probing. The
  streak resets on the first reply, so the client recovers on its own once the cluster is
  upgraded.

  Latching off would be wrong here: a client load-balances across proxies, so during a
  rolling upgrade one heartbeat can land on an old proxy while the rest of the cluster
  already supports the service, and a client that gave up would stay dark until restarted.
  Backing off makes talking to a genuinely old cluster cost roughly nothing while keeping
  recovery automatic.

  Because telemetry is best-effort and the `client/` module carries no logger, the failure
  is not raised through the normal API. `ClientTelemetryManager.IsSupported()` reports
  whether the server is currently known *not* to implement the service — it is optimistic,
  returning true before the first heartbeat, so pair it with `LastHeartbeatError()` to tell
  "no evidence of an old server" from "confirmed working".

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
