# GetReplicateConfiguration API Design

**Date:** 2026-01-28

## Overview

Add a new public API `GetReplicateConfiguration` that allows cluster administrators to view the current cross-cluster replication topology. The API returns configured replication relationships without exposing sensitive connection parameters like tokens.

## Motivation

Operators need visibility into the current replication setup to:
- Verify replication topology is configured correctly
- Troubleshoot replication issues
- Audit cluster configuration

Currently, `UpdateReplicateConfiguration` exists but there's no corresponding read API to inspect the current state.

## API Definition

Add to `milvus.proto` on the `MilvusService`:

```protobuf
rpc GetReplicateConfiguration(GetReplicateConfigurationRequest)
    returns (GetReplicateConfigurationResponse) {}

message GetReplicateConfigurationRequest {
  option (common.privilege_ext_obj) = {
    object_type: Global
    object_privilege: PrivilegeGetReplicateConfiguration
    object_name_index: -1
  };
}

message GetReplicateConfigurationResponse {
  common.Status status = 1;
  common.ReplicateConfiguration configuration = 2;
}
```

### Response Behavior

- Returns the existing `common.ReplicateConfiguration` structure
- `ConnectionParam.token` fields are cleared/redacted before returning
- If no replication is configured, returns empty configuration with success status

## Security

### Authorization

- Requires **ClusterAdmin** privilege
- Unauthenticated or unauthorized requests return permission denied error

### Data Sanitization

The implementation must sanitize sensitive fields before returning:
- `MilvusCluster.connection_param.token` → empty string

## Implementation Flow

```
Client SDK
    │
    ▼
Proxy (MilvusService)
    │  - Check ClusterAdmin permission
    │  - Forward to StreamingCoord
    ▼
StreamingCoord
    │  - Retrieve current ReplicateConfiguration
    │  - Sanitize: clear token fields
    │  - Return response
    ▼
Proxy
    │  - Return to client
    ▼
Client SDK
```

## Files to Modify

### Proto Changes (milvus-proto repo)
- `proto/milvus.proto` - Add RPC definition and request/response messages

### Proxy Layer
- `internal/proxy/impl.go` - Add `GetReplicateConfiguration` method
- `internal/proxy/proxy.go` - Wire up the new API

### StreamingCoord Layer
- `internal/streamingcoord/server/service/` - Add handler to fetch and sanitize config

### Permission
- Register the API with ClusterAdmin privilege check

### Tests
- Unit tests for sanitization logic
- Integration test for the full API flow

## Alternatives Considered

### 1. Expose via StreamingCoordStateService only (internal)
Rejected: Requires direct access to internal services; not accessible via standard SDKs.

### 2. Return full configuration including tokens
Rejected: Security risk; tokens should not be readable via API.

### 3. Include health/status information
Deferred: Keeping initial implementation simple to match the Update API symmetry. Health info can be added later if needed.

## Open Questions

None.
