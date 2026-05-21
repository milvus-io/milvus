# MEP: Client-Side Telemetry with Heartbeat and Server Command Support

- **Created:** 2026-01-31
- **Author(s):** @xiaofanluan
- **Status:** Draft
- **Component:** SDK | Proxy | Coordinator
- **Related Issues:** #46934
- **Released:** [TBD]

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
    Enabled           bool          // Enable/disable telemetry collection
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

### HTTP REST APIs (Proxy)

```
GET  /api/v1/telemetry/clients          - List connected clients with optional filtering
POST /api/v1/telemetry/commands         - Push commands to clients
DELETE /api/v1/telemetry/commands/{id}  - Delete a command
```

### gRPC APIs (RootCoord)

```protobuf
service RootCoord {
    // Client heartbeat with metrics
    rpc ClientHeartbeat(ClientHeartbeatRequest) returns (ClientHeartbeatResponse);

    // Query connected clients
    rpc GetClientTelemetry(GetClientTelemetryRequest) returns (GetClientTelemetryResponse);

    // Push commands to clients
    rpc PushClientCommand(PushClientCommandRequest) returns (PushClientCommandResponse);

    // Delete commands
    rpc DeleteClientCommand(DeleteClientCommandRequest) returns (DeleteClientCommandResponse);
}
```

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
│  │  │ Collector       │  │ (Ring Buffer)   │  │ Router          │       │  │
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
│  │  /telemetry/*  │          │  │  ┌───────────┐  ┌───────────────┐   │  │  │
│  │                │          │  │  │ Client    │  │ Command       │   │  │  │
│  │  WebUI         │          │  │  │ Store     │  │ Store         │   │  │  │
│  │  telemetry.html│          │  │  └───────────┘  └───────────────┘   │  │  │
│  └────────────────┘          │  └─────────────────────────────────────┘  │  │
│                              └───────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Client-Side Components

#### 1. ClientTelemetryManager

The central component managing telemetry collection and heartbeat communication.

```go
type ClientTelemetryManager struct {
    config         *TelemetryConfig
    client         *Client
    clientID       string                              // Stable UUID
    collectors     map[string]*OperationMetricsCollector
    errorCollector *ErrorCollectorImpl
    commandHandlers map[string]CommandHandler

    // Heartbeat management
    stopCh         chan struct{}
    wg             sync.WaitGroup
}
```

**Key behaviors:**
- Generates stable client UUID on creation
- Starts background heartbeat loop on `Start()`
- Sends first heartbeat immediately, then every `HeartbeatInterval`
- Collects and resets metrics atomically during snapshot creation

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

**Metrics tracked:**
- Request count
- Success/error counts
- Average latency (ms)
- P99 latency (ms) - calculated from 1000 sample buffer
- Max latency (ms)

#### 3. ErrorCollectorImpl

Ring buffer implementation for tracking recent errors.

```go
type ErrorCollectorImpl struct {
    errors   []*ErrorInfo
    maxCount int
    index    int  // Ring buffer index
}

type ErrorInfo struct {
    Timestamp  int64
    Operation  string
    ErrorMsg   string
    Collection string
    RequestID  string
}
```

#### 4. Command Handler System

Supports server-pushed commands with extensible handler registration.

```go
type CommandHandler func(cmd *ClientCommand) string  // Returns error message or ""

// Built-in commands:
const (
    CmdSetSamplingRate   = "set_sampling_rate"        // Adjust sampling rate
    CmdEnableCollections = "enable_collections"       // Enable specific collections
    CmdUpdateConfig      = "update_config"            // Update telemetry config
)
```

### Server-Side Components

#### 1. Telemetry Manager (RootCoord)

Central server-side storage for client telemetry data.

```go
type TelemetryManager struct {
    clients      map[string]*ClientTelemetry  // clientID -> telemetry
    commandStore *CommandStore
}

type ClientTelemetry struct {
    ClientInfo        *commonpb.ClientInfo
    LastHeartbeatTime int64
    Status            string  // "active" or "inactive"
    Databases         []string
    Metrics           []*commonpb.OperationMetrics
}
```

#### 2. Command Store

Manages pending commands for clients with support for persistent and one-time commands.

```go
type CommandStore struct {
    commands     []*commonpb.ClientCommand  // One-time commands
    persistent   []*commonpb.ClientCommand  // Persistent commands
}
```

**Command targeting:**
- `TargetScope = "*"` - All clients
- `TargetScope = "client_id:xxx"` - Specific client
- `TargetScope = "db:database_name"` - Clients using specific database

#### 3. HTTP Handlers (Proxy)

REST API endpoints for WebUI and external integrations.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/telemetry/clients` | GET | List connected clients |
| `/api/v1/telemetry/clients?database=X` | GET | Filter by database |
| `/api/v1/telemetry/clients?include_metrics=true` | GET | Include operation metrics |
| `/api/v1/telemetry/commands` | POST | Push command to clients |
| `/api/v1/telemetry/commands/{id}` | DELETE | Remove a command |

**Authentication:** Uses existing Milvus Basic Auth when authorization is enabled.

### Heartbeat Protocol

```
Client                                      Server
   │                                           │
   │──── ClientHeartbeatRequest ──────────────►│
   │     - ClientInfo (ID, SDK version, host)  │
   │     - Metrics (per-operation, per-coll)   │
   │     - CommandReplies                      │
   │     - ConfigHash (for change detection)   │
   │                                           │
   │◄─── ClientHeartbeatResponse ─────────────│
   │     - Commands (pending for this client)  │
   │     - NewConfigHash (if config changed)   │
   │                                           │
```

**Heartbeat interval:** 30 seconds (configurable, server can override)

**Config hash:** SHA-256 of client configuration, used to detect when server pushes new config.

### Data Flow

1. **Metrics Collection:**
   - Each SDK operation (Search, Insert, Query, etc.) records metrics
   - Sampling rate determines if operation is tracked (default: 100%)
   - Metrics stored in per-operation collectors

2. **Heartbeat Cycle:**
   - Background goroutine wakes every 30 seconds
   - Creates atomic snapshot of all metrics (resets counters)
   - Sends snapshot to RootCoord via ClientHeartbeat RPC
   - Receives and processes any pending commands

3. **Command Processing:**
   - Server pushes commands via heartbeat response
   - Client executes command handler
   - Reply sent in next heartbeat request
   - Persistent commands re-sent until explicitly deleted

### WebUI Dashboard

A new telemetry dashboard (`/webui/telemetry.html`) provides:

- **Client List:** Active/inactive clients with connection details
- **Metrics View:** Per-client operation metrics with latency charts
- **Command Interface:** Send commands to specific clients or broadcast
- **Filtering:** By database, client ID, or status

## Compatibility, Deprecation, and Migration Plan

- **Backward Compatible:** Telemetry is opt-in and disabled by default can be enabled
- **No Breaking Changes:** Existing SDK usage remains unchanged
- **Server Compatibility:** Old clients work with new servers (no heartbeats sent)
- **Client Compatibility:** New clients work with old servers (heartbeats fail silently)

## Test Plan

### Unit Tests
- `telemetry_test.go`: Metrics collection, P99 calculation, ring buffer
- `telemetry_http_handler_test.go`: HTTP API handlers
- `command_router_test.go`: Command routing and handling

### Integration Tests
- `telemetry_integration_test.go`: End-to-end heartbeat flow
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

- Milvus existing telemetry: `internal/proxy/impl.go`
- gRPC health checking: https://github.com/grpc/grpc/blob/master/doc/health-checking.md
- OpenTelemetry SDK patterns: https://opentelemetry.io/docs/instrumentation/go/
