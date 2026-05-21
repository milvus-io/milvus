# MEP: Scalar Index Version Management

- **Created:** 2026-03-13
- **Author(s):** @zhengbuqian
- **Status:** Proposal
- **Component:** Coordinator, QueryNode, DataNode
- **Released:** TBD

## Summary

Introduce scalar index version management capabilities on par with vector indexes, including target version override, force rebuild, and MaximumVersion propagation and clamping for both vector and scalar indexes.

## Motivation

Milvus already has a complete **vector index version management system**:

- Each QueryNode registers its `[MinimalIndexVersion, CurrentIndexVersion]` from knowhere at startup via etcd Session.
- DataCoord's `IndexEngineVersionManager` aggregates all QN versions to determine cluster-wide safe build versions.
- `autoUpgradeSegmentIndex` triggers compaction to rebuild indexes when versions lag behind.
- `targetVecIndexVersion` provides temporary flexibility to override the default build version.
- `forceRebuildSegmentIndex` forces all vector indexes to rebuild to a specified version.

However, **scalar indexes** lack equivalent version management. Currently, scalar index versions only have hardcoded `MinimalScalarIndexEngineVersion` and `CurrentScalarIndexEngineVersion` constants, with no target override, force rebuild, or MaxVersion validation.

Additionally, the **vector index** side's `MaximumIndexVersion` (the upper bound knowhere supports) exists in C++ but was never propagated to the Go layer, and `targetVecIndexVersion` was not validated against the upper bound.

### Knowhere Version Semantics

| Version | Meaning |
|---------|---------|
| `MinimalIndexVersion` | Lowest version knowhere can **build and search** |
| `CurrentIndexVersion` | Hardcoded default build version |
| `MaximumIndexVersion` | Highest version knowhere can **build and search** (non-beta) |

Knowhere guarantees it can build and search any index version in `[MinimalIndexVersion, MaximumIndexVersion]`.

### Configuration Parameter Design Intent

| Parameter | Intent |
|-----------|--------|
| `autoUpgradeSegmentIndex` | After rolling upgrade, automatically upgrade old indexes to the cluster's current version |
| `targetVecIndexVersion` | Temporary flexibility: override default build version without code changes (default -1, inactive) |
| `forceRebuildSegmentIndex` | Emergency measure: force all indexes to rebuild to the target version (rebuilds already-built indexes too) |

## Public Interfaces

### New Configuration Parameters

| Parameter | Key | Default | Description |
|-----------|-----|---------|-------------|
| `TargetScalarIndexVersion` | `dataCoord.targetScalarIndexVersion` | `-1` | Target scalar index version, -1 means unspecified |
| `ForceRebuildScalarSegmentIndex` | `dataCoord.forceRebuildScalarSegmentIndex` | `false` | Force rebuild scalar index toggle |

Semantics symmetric with the vector side:
- `forceRebuildScalarSegmentIndex=true` + `targetScalarIndexVersion=N`: Force all scalar indexes to rebuild to version N
- `forceRebuildScalarSegmentIndex=false` + `targetScalarIndexVersion=N`: New scalar indexes use `max(cluster aggregated value, N)`
- `targetScalarIndexVersion=-1`: Inactive, follows cluster aggregated value

### New Constants

```go
const MaximumScalarIndexEngineVersion = int32(2)
```

Currently Maximum equals Current for scalar indexes (unlike knowhere vector side, scalar has no reserved beta versions). In the future, Maximum will be bumped first, then Current once stable.

### IndexEngineVersionManager Interface Additions

```go
GetMaximumIndexEngineVersion() int32
GetMaximumScalarIndexEngineVersion() int32
ResolveVecIndexVersion() int32
ResolveScalarIndexVersion() int32
```

### Session IndexEngineVersion Struct

```go
type IndexEngineVersion struct {
    MinimalIndexVersion int32 `json:"MinimalIndexVersion,omitempty"`
    CurrentIndexVersion int32 `json:"CurrentIndexVersion,omitempty"`
    MaximumIndexVersion int32 `json:"MaximumIndexVersion,omitempty"`  // new
}
```

## Design Details

### Overall Architecture

Data flow after changes:

```
┌──────────────────────────────────────────────────────┐
│                   QueryNode Startup                   │
│                                                       │
│  Vector versions: knowhere C++ → [Min, Current, Max]  │
│  Scalar versions: Go constants → [Min, Current, Max]  │
│                                                       │
│  Written to etcd Session                              │
└────────────────────────┬──────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────┐
│          DataCoord IndexEngineVersionManager          │
│                                                       │
│  Vector aggregation:                                  │
│    GetCurrentIndexEngineVersion()  = MIN(all QN.Cur)  │
│    GetMinimalIndexEngineVersion()  = MAX(all QN.Min)  │
│    GetMaximumIndexEngineVersion()  = MIN(all QN.Max)  │
│                                                       │
│  Scalar aggregation:                                  │
│    GetCurrentScalarIndexEngineVersion()  = MIN(...)   │
│    GetMinimalScalarIndexEngineVersion()  = MAX(...)   │
│    GetMaximumScalarIndexEngineVersion()  = MIN(...)   │
│                                                       │
│  Resolve methods (target override + max clamp):       │
│    ResolveVecIndexVersion()                           │
│    ResolveScalarIndexVersion()                        │
└────────────┬─────────────────────┬────────────────────┘
             │                     │
             ▼                     ▼
┌────────────────────────┐  ┌───────────────────────────┐
│  Build New Index        │  │  Compaction Trigger        │
│  (prepareJobRequest)    │  │  (ShouldRebuildSegIndex)   │
│                         │  │                            │
│  Vec: ResolveVec()      │  │  Path 1: autoUpgrade       │
│  Scalar: ResolveScalar()│  │    vec+scalar, version <   │
│                         │  │                            │
│  Stats task:            │  │  Path 2: forceRebuild vec  │
│    ResolveScalar()      │  │    resolved target !=      │
│                         │  │                            │
│  Mix compaction:        │  │  Path 3: forceRebuild      │
│    ResolveScalar()      │  │    scalar (NEW)            │
└────────────────────────┘  │    resolved target !=       │
                            └───────────────────────────┘
```

### Maximum Version Aggregation

Takes MIN across all QNs — Maximum represents "highest version a node can handle", so the cluster-safe upper bound is the lowest across all nodes.

```go
func (m *versionManagerImpl) GetMaximumIndexEngineVersion() int32 {
    maximum := int32(math.MaxInt32)
    for _, version := range m.versions {
        if version.MaximumIndexVersion == 0 {
            continue // skip old QNs that don't report Maximum
        }
        if version.MaximumIndexVersion < maximum {
            maximum = version.MaximumIndexVersion
        }
    }
    if maximum == math.MaxInt32 {
        return math.MaxInt32 // all QNs are old, no upper bound check
    }
    return maximum
}
```

### Resolve Methods

Centralize the version resolution logic (target override + MaxVersion clamp) to avoid duplication across task_index, task_stats, and compaction_task_mix:

```go
func (m *versionManagerImpl) ResolveScalarIndexVersion() int32 {
    version := m.GetCurrentScalarIndexEngineVersion()
    if Params.DataCoordCfg.TargetScalarIndexVersion.GetAsInt64() != -1 {
        if Params.DataCoordCfg.ForceRebuildScalarSegmentIndex.GetAsBool() {
            version = Params.DataCoordCfg.TargetScalarIndexVersion.GetAsInt32()
        } else {
            version = max(version, Params.DataCoordCfg.TargetScalarIndexVersion.GetAsInt32())
        }
    }
    if maxVersion := m.GetMaximumScalarIndexEngineVersion(); version > maxVersion {
        version = maxVersion
    }
    return version
}
```

### Compaction Trigger: Force Rebuild

The compaction trigger's force-rebuild paths also use the resolved (clamped) target to compare against, ensuring that when target > cluster maximum, the trigger converges after a single rebuild (since the built index version matches the resolved target).

### DataNode Scalar Version Clamp

The DataNode's `getCurrentScalarIndexVersion` is fixed to clamp against `MaximumScalarIndexEngineVersion` instead of `CurrentScalarIndexEngineVersion`, aligning with how the vector side clamps against `C.GetMaximumIndexVersion()`.

DataNode text index paths (stats task and sort compaction) are fixed to use the request-carried scalar version (from DataCoord's Resolve method) instead of hardcoding `common.CurrentScalarIndexEngineVersion`.

## Compatibility, Deprecation, and Migration Plan

### Rolling Upgrade Compatibility

The only cross-node persistent data format change is the **etcd Session JSON structure** — adding `MaximumIndexVersion` field.

| Scenario | Behavior |
|----------|----------|
| **Old QN + New DataCoord** | Old QN's Session JSON lacks `MaximumIndexVersion`, Go deserializes it as zero. `GetMaximumIndexEngineVersion()` skips zero-valued entries. If all QNs are old, returns `math.MaxInt32` (no upper bound check). |
| **New QN + Old DataCoord** | New QN's `MaximumIndexVersion` field is silently ignored by old DataCoord (Go `encoding/json` ignores unknown fields). No impact. |
| **New QN + New DataCoord** | All QNs report `MaximumIndexVersion`, aggregation works normally, upper bound check fully effective. |

New config parameters (`TargetScalarIndexVersion` default -1, `ForceRebuildScalarSegmentIndex` default false) are only read by DataCoord. Default behavior is identical to the old version.

## Test Plan

- Unit tests for `GetMaximumIndexEngineVersion()` and `GetMaximumScalarIndexEngineVersion()` aggregation, including mixed old/new QN scenarios
- Unit tests for `ResolveVecIndexVersion()` and `ResolveScalarIndexVersion()` with various target/force/max combinations
- Unit tests for scalar force-rebuild compaction trigger path
- Integration test: set `targetScalarIndexVersion` and verify scalar indexes are built with correct version
- Integration test: set `forceRebuildScalarSegmentIndex=true` and verify all scalar indexes are rebuilt
- Verify rolling upgrade compatibility: old QN (no MaximumIndexVersion) + new DataCoord works correctly

