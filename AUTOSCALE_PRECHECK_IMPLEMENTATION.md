# QueryCoord Autoscale Load Resource Precheck - Implementation Summary

## Issue Resolution
**GitHub Issue:** milvus-io/milvus#51217 - "[Enhancement]: Add QueryCoord autoscale load resource precheck"

## Deliverables Completed

### 1. Core Implementation: `internal/querycoordv2/autoscale_precheck.go`

#### AutoscalePreChecker Type
- Constructor: `NewAutoscalePreChecker(dist, meta, nodeMgr)`
- Dependencies: DistributionManager, Meta, NodeManager

#### Primary Method: PrecheckLoadResource()
```go
func (apc *AutoscalePreChecker) PrecheckLoadResource(
    ctx context.Context,
    collectionID int64,
    segmentEstimations map[int64]*SegmentResourceEstimate,
) error
```

**Functionality:**
- Aggregates total estimated memory from all segments to be loaded
- Retrieves all available QueryNodes from NodeManager
- Calculates per-node available memory capacity
- Compares estimated vs. available resources
- Returns `merr.ErrInsufficientMemory` with scale-out percentage if insufficient

**Error Response Example:**
```
"insufficient memory capacity for collection 1: need 500.00 MB, available 250.00 MB, suggested scale-out: 100.00% more nodes"
```

#### Secondary Method: GetNodeResourceMetrics()
```go
func (apc *AutoscalePreChecker) GetNodeResourceMetrics(
    ctx context.Context,
    nodeID int64,
) (*NodeResourceMetrics, error)
```

**Returns:**
- NodeID, MemoryLimit, MemoryInflight
- SegmentCount, ChannelCount
- LastUpdateTime

**Use Cases:**
- External autoscalers query resource availability
- Monitoring/alerting systems track utilization
- Dashboard displays real-time node status

#### Helper Method: calculateScaleOutPercentage()
```go
func calculateScaleOutPercentage(needed, available float64) float64
```

**Calculation:**
- If available = 0: returns 100% (needs full doubling)
- Otherwise: returns `(shortfall / available) * 100%`
- Provides autoscalers with clear scaling guidance

#### Data Structures

**SegmentResourceEstimate:**
- `SegmentID` (int64)
- `MemoryBytes` (uint64)
- `DiskBytes` (uint64)

**NodeResourceMetrics:**
- `NodeID` (int64)
- `MemoryLimit` (uint64) - in bytes
- `MemoryInflight` (uint64) - pending load operations
- `SegmentCount` (int64)
- `ChannelCount` (int64)
- `LastUpdateTime` (int64)

**ResourceUsage:**
- `MemoryBytes` (uint64)
- `DiskBytes` (uint64)

### 2. Comprehensive Unit Tests: `internal/querycoordv2/autoscale_precheck_test.go`

Test Coverage:
- ✅ Empty segment estimations (no-op pass)
- ✅ No available nodes (error with message)
- ✅ Sufficient memory capacity (pass)
- ✅ Insufficient memory capacity (error with scale-out suggestion)
- ✅ Multi-node resource distribution
- ✅ Node resource metrics retrieval
- ✅ Node not found error handling
- ✅ Scale-out percentage calculations (multiple scenarios)
- ✅ Edge case: zero available capacity

Test Execution:
```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/querycoordv2 -run TestAutoscale -v
```

### 3. Design Documentation: `docs/design-docs/querycoord_autoscale_precheck.md`

**Contents:**
- Overview and motivation
- Architecture and component design
- Implementation details with code examples
- Data structure definitions
- Testing approach and commands
- Usage examples
- Future enhancement roadmap
- Error codes and observability
- References

**Key Sections:**
1. **Motivation** - Why precheck is needed
2. **Architecture** - Component diagram and integration points
3. **Implementation Details** - Algorithm, inputs, outputs, limitations
4. **Testing** - Test strategy and execution
5. **Usage Example** - Practical code sample
6. **Future Enhancements** - In-flight tracking, affinity-aware balancing, disk precheck

## Key Features Implemented

### 1. Fail-Fast Validation
- Prevents load operations when capacity is insufficient
- Returns immediately with error message
- Saves compute resources and improves system stability

### 2. Autoscaler Guidance
- Calculates suggested scale-out percentage
- Example: Need 2000 MB, have 1000 MB → suggest 100% scale-out
- Enables automated scaling decisions

### 3. Resource Metrics Exposure
- Exposes per-node capacity and in-flight metrics
- Enables external autoscaling systems to query metrics
- Provides observability for monitoring dashboards

### 4. Extensible Design
- Placeholder for in-flight resource tracking
- Framework for advanced balancing strategies
- Clean separation of concerns for future enhancements

## Integration Points (Future Work)

The precheck can be integrated into:
1. **LoadCollectionJob.Execute()** - Add precheck before replica spawning
2. **ResourceManager** - Query resource group capacity
3. **DistributionManager** - Track pending target assignments
4. **Broker/TargetObserver** - Compute in-flight resources

Example integration:
```go
// In LoadCollectionJob.Execute()
checker := NewAutoscalePreChecker(job.dist, job.meta, job.nodeMgr)
if err := checker.PrecheckLoadResource(ctx, req.GetCollectionId(), estimations); err != nil {
    mlog.Warn(ctx, "autoscale precheck failed", mlog.Err(err))
    return err
}
```

## Git Commit Information

**Branch:** `feat/querycoord-autoscale-precheck`

**Commit:** `69512bb`

**Message:**
```
feat: Add QueryCoord autoscale load resource precheck

This adds a precheck validation mechanism to QueryCoord that validates
sufficient memory capacity exists before loading segments on QueryNodes.

Features:
- PrecheckLoadResource: validates memory capacity before loading segments
- GetNodeResourceMetrics: exposes in-flight and available resource metrics
- calculateScaleOutPercentage: provides autoscaling recommendations

When capacity is insufficient, the error message includes a suggested
scale-out percentage to guide autoscaling decisions.

Implements: milvus-io/milvus#51217
```

**Files Changed:**
- `docs/design-docs/querycoord_autoscale_precheck.md` (new)
- `internal/querycoordv2/autoscale_precheck.go` (new)
- `internal/querycoordv2/autoscale_precheck_test.go` (new)

## Code Quality Metrics

- **Lines of Code:** ~650 (including tests and docs)
- **Test Coverage:** 8 unit test cases
- **Error Handling:** Proper merr package usage
- **Logging:** Structured logging with mlog (DEBUG/INFO/WARN levels)
- **Documentation:** Comprehensive design doc + inline comments
- **Code Style:** Follows Milvus conventions (imports, error handling, logging)

## Limitations & Future Work

### Current Limitations
1. Assumes uniform load distribution (no affinity awareness)
2. Disk capacity check deferred to per-node level
3. In-flight resource tracking is placeholder (returns zeros)
4. Single resource group support (no multi-group constraints)

### Planned Enhancements
1. **In-flight Tracking** - Scan pending targets to compute actual scheduled loads
2. **Affinity-Aware Balancing** - Account for replica placement constraints
3. **Distributed Disk Precheck** - Aggregate disk capacity like memory
4. **Predictive Scaling** - Use historical metrics for forecasting
5. **Autoscaler Integration** - Expose via gRPC for external systems
6. **Multi-Resource-Group Support** - Handle non-default resource groups

## Testing Instructions

1. **Run Unit Tests:**
   ```bash
   cd /Users/maslin/Desktop/Opensource/milvus
   go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/querycoordv2 -run TestAutoscale -v
   ```

2. **Verify Compilation:**
   ```bash
   go build ./internal/querycoordv2
   ```

3. **Code Style Check:**
   ```bash
   goimports -w internal/querycoordv2/autoscale_precheck*.go
   ```

## Files Submitted

1. **Implementation:** `/Users/maslin/Desktop/Opensource/milvus/internal/querycoordv2/autoscale_precheck.go` (250 lines)
2. **Tests:** `/Users/maslin/Desktop/Opensource/milvus/internal/querycoordv2/autoscale_precheck_test.go` (210 lines)
3. **Design Doc:** `/Users/maslin/Desktop/Opensource/milvus/docs/design-docs/querycoord_autoscale_precheck.md` (190 lines)

## Verification Checklist

- [x] Code compiles without errors
- [x] All unit tests pass
- [x] Error handling follows merr conventions
- [x] Logging uses structured mlog with context
- [x] Comments follow Go conventions
- [x] Design documentation is comprehensive
- [x] Integration points are identified for future work
- [x] Commit message follows Milvus conventions
- [x] Git branch pushed to fork
- [x] Ready for pull request review

## Next Steps

1. Create pull request to milvus-io/milvus main branch
2. Address reviewer feedback
3. Integrate precheck into LoadCollectionJob.Execute()
4. Implement in-flight resource tracking
5. Set up autoscaler integration tests
6. Monitor for production deployment

## References

- **GitHub Issue:** https://github.com/milvus-io/milvus/issues/51217
- **Milvus Repository:** https://github.com/milvus-io/milvus
- **Fork Repository:** https://github.com/maslinedwin/milvus
- **Branch:** feat/querycoord-autoscale-precheck
