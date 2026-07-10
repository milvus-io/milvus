# QueryCoord Autoscale Load Resource Precheck

## Overview

This document describes the design and implementation of a resource precheck mechanism for QueryCoord autoscaling. The feature validates that sufficient memory capacity exists across QueryNodes before attempting to load segments, enabling safe and informed autoscaling decisions.

## Motivation

When autoscaling systems attempt to load segments on QueryNodes, insufficient resource capacity can lead to:
- Out-of-Memory (OOM) errors
- Load request rejections
- Cascading failures in dependent operations
- Poor user experience with unpredictable performance

By introducing a precheck validation layer in QueryCoord, we can:
1. Fail-fast before initiating expensive load operations
2. Provide autoscalers with clear scaling recommendations
3. Prevent resource exhaustion scenarios
4. Enable more intelligent autoscaling decisions

## Architecture

### Components

```
AutoscalePreChecker (querycoordv2/autoscale_precheck.go)
├── PrecheckLoadResource()
│   ├── Aggregate segment resource estimations
│   ├── Query available QueryNode capacity
│   ├── Calculate total available memory
│   └── Return error with scale-out suggestion if insufficient
├── GetNodeResourceMetrics()
│   └── Expose in-flight and available resource metrics per node
└── getNodeInflightResources()
    └── Retrieve scheduled-but-not-yet-committed resources
```

### Integration Points

1. **LoadCollectionJob.Execute()** — Call precheck before spawning replicas
2. **ResourceManager** — Query resource group node capacity
3. **NodeManager** — Retrieve QueryNode memory limits and current stats
4. **Broker/TargetObserver** — Track in-flight segment assignments

## Implementation Details

### PrecheckLoadResource()

**Purpose:** Validates memory capacity before loading segments.

**Inputs:**
- `collectionID` — Target collection
- `segmentEstimations` — Map of segmentID → estimated resource usage (memory and disk)

**Process:**
1. Aggregate total estimated memory from all segments
2. Retrieve all online QueryNodes
3. Calculate per-node estimated usage (simplified: even distribution)
4. Accumulate total available memory across all nodes
5. Compare: if estimated > available, return error with scale-out percentage

**Error Handling:**
- Returns `merr.ErrInsufficientMemory` with scale-out percentage suggestion
- Suggests: `(shortfall / available) * 100%` additional nodes needed
- Example: Need 2000 MB, have 1000 MB available → suggest 100% scale-out

**Limitations:**
- Assumes uniform distribution of load across nodes (no affinity-aware balancing)
- Does not account for rack/zone constraints
- Disk capacity check deferred to per-node QueryNode load handler (no distributed disk check)

### GetNodeResourceMetrics()

**Purpose:** Expose comprehensive resource metrics for external autoscalers.

**Returns:**
- `MemoryLimit` — Total memory capacity (MB × 1024 × 1024)
- `MemoryInflight` — Bytes scheduled but not yet committed
- `SegmentCount` — Number of loaded segments
- `ChannelCount` — Number of assigned channels
- `LastUpdateTime` — Timestamp of last heartbeat

**Use Cases:**
- External autoscalers query metrics to make scaling decisions
- Monitoring dashboards track resource utilization
- Alerting systems detect resource exhaustion patterns

### getNodeInflightResources()

**Purpose:** Calculate in-flight (scheduled-but-not-committed) resources per node.

**Current Implementation:** Returns zeros (placeholder).

**Future Enhancement:** Scan pending target assignments in TargetObserver/DistributionManager to compute:
- Segments assigned but not yet loaded
- Memory/disk footprint of pending segments

## Data Structures

### SegmentResourceEstimate
```go
type SegmentResourceEstimate struct {
    SegmentID   int64
    MemoryBytes uint64
    DiskBytes   uint64
}
```

### NodeResourceMetrics
```go
type NodeResourceMetrics struct {
    NodeID         int64
    MemoryLimit    uint64
    MemoryInflight uint64
    SegmentCount   int64
    ChannelCount   int64
    LastUpdateTime int64
}
```

## Testing

Unit tests cover:
1. Empty segment estimations (no-op)
2. No available nodes (error)
3. Sufficient capacity (pass)
4. Insufficient capacity (error with scale-out suggestion)
5. Multi-node distribution
6. Edge cases (zero available, exact match)

Run tests:
```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/querycoordv2 -run TestAutoscale -v
```

## Usage Example

```go
// Create precheck validator
checker := NewAutoscalePreChecker(dist, meta, nodeMgr)

// Prepare segment estimations (from query execution or metadata analysis)
estimations := map[int64]*SegmentResourceEstimate{
    segmentID1: {MemoryBytes: 100*1024*1024, DiskBytes: 500*1024*1024},
    segmentID2: {MemoryBytes: 150*1024*1024, DiskBytes: 750*1024*1024},
}

// Run precheck
if err := checker.PrecheckLoadResource(ctx, collectionID, estimations); err != nil {
    // Log error message with scale-out suggestion
    // Trigger autoscaling logic to add more QueryNodes
    log.Warn("Load rejected, autoscaling required: %v", err)
    return err
}

// Safe to proceed with load
log.Info("Precheck passed, proceeding with load")
```

## Future Enhancements

1. **In-flight Resource Tracking:** Implement `getNodeInflightResources()` to track pending loads
2. **Affinity-Aware Balancing:** Account for replica placement constraints and data locality
3. **Disk Capacity Aggregation:** Implement distributed disk precheck (currently per-node only)
4. **Predictive Scaling:** Use historical metrics to forecast future capacity needs
5. **Multi-Resource-Group Support:** Handle non-default resource group constraints
6. **Autoscaler Integration:** Expose metrics via gRPC for external autoscaling systems

## Error Codes

- `ErrInsufficientMemory` — Insufficient memory capacity across all nodes
- `ErrServiceInternal` — Internal service error (e.g., resource group not found)
- `ErrNodeNotFound` — Query node does not exist

## Observability

### Logging
- INFO: Precheck passed with resource summary
- WARN: Insufficient capacity with shortfall and suggestion
- DEBUG: Per-node resource status

### Metrics
- `querycoord_autoscale_precheck_memory_shortfall_bytes` (future)
- `querycoord_autoscale_scale_out_percentage` (future)

## References

- GitHub Issue: [milvus-io/milvus#51217](https://github.com/milvus-io/milvus/issues/51217)
- QueryCoord Architecture: `internal/querycoordv2/`
- QueryNode Resource Estimation: `internal/querynodev2/segments/segment_loader.go`
