package balancer

import (
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// actionKind is the Phase 1 classification for a dirty shard.
// It decides whether the Policy should produce a Prepare / Release / no-op
// and whether the decision is mandatory or a steady-state optimization.
type actionKind int

const (
	// actionNone: nothing to do for this shard in this batch.
	actionNone actionKind = iota

	// actionRelease: desired state is absent but current views exist.
	// The Policy emits a Release in the plan; no allocation needed.
	actionRelease

	// actionMust: a new view must be generated (e.g., initial load,
	// data version changed, node unavailable, settings changed).
	// The Policy unconditionally adds the candidate to the plan.
	actionMust

	// actionMayOptimize: the current placement is valid; the Policy may
	// emit a migration if steady-state balance scoring finds a clearly
	// better assignment (Phase 3 gated).
	actionMayOptimize
)

// classifyShard inspects the snapshot and returns the action kind for shardID.
//
// The decision rules, in order:
//
//  1. Desired absent + current exists     → Release
//  2. Desired present + only Preparing    → None (avoid stacking)
//  3. Desired present + no current view   → Must (initial load / post-Unrecoverable)
//  4. Both absent                         → None
//  5. Current DataVersion < DataView DV   → Must (data changed)
//  6. Current view references an unavailable node → Must (node lost)
//  7. Settings differ (partitions/fields) → Must
//  8. Already has a Preparing view        → None (avoid stacking)
//  9. Otherwise                           → MayOptimize
func classifyShard(snap *BalancerSnapshot, shardID qviews.ShardID) actionKind {
	desired := snap.ConfigForShard(shardID)
	stats := snap.ShardStatsMap()[shardID]
	hasUpView := stats != nil && stats.UpVersion != nil
	hasPreparing := stats != nil && stats.PreparingVersion != nil
	hasAnyView := hasUpView || hasPreparing || (stats != nil && len(stats.Segments) > 0)

	// 1. Desired absent → release any residual views.
	if desired == nil {
		if hasAnyView {
			return actionRelease
		}
		return actionNone
	}

	// 2. Desired present but no Up view → must create.
	if !hasUpView {
		if hasPreparing {
			return actionNone
		}
		return actionMust
	}

	// 3. DataVersion advanced?
	if dataViewVersionAdvanced(snap, desired, stats) {
		return actionMust
	}

	// 4. Any node in the current Up view is unavailable?
	if hasUnavailableNode(stats, snap.Nodes) {
		return actionMust
	}

	// 5. Settings differ between desired and current?
	if settingsDiffer(desired, stats.UpSettings) {
		return actionMust
	}

	// 6. Already have a Preparing view? skip this cycle.
	if hasPreparing {
		return actionNone
	}

	// 7. Steady-state — candidate for balance optimization.
	return actionMayOptimize
}

// dataViewVersionAdvanced returns true if the shard's current Up view was
// built on an older DataVersion than the collection's current DataView.
// Returns false when the snapshot has no DataVersion for the collection
// (DataView Manager hasn't reported one yet); the next reconcile cycle will
// pick up the change.
func dataViewVersionAdvanced(snap *BalancerSnapshot, desired *loadmgr.LoadConfig, stats *coordview.ShardStats) bool {
	if stats.UpVersion == nil {
		return false
	}
	latest, ok := snap.DataVersionForCollection(desired.CollectionID)
	if !ok {
		return false
	}
	return latest.GT(stats.UpVersion.DataVersion)
}

// hasUnavailableNode returns true if any node in the Up view is missing
// from the snapshot's Nodes map or marked not-alive / stopping.
func hasUnavailableNode(stats *coordview.ShardStats, nodes map[int64]*BalanceNode) bool {
	for _, segment := range stats.Segments {
		for nodeID, state := range segment.Nodes {
			if state != coordview.SegmentStateUp {
				continue
			}
			n, ok := nodes[nodeID]
			if !ok {
				return true
			}
			if !n.Alive || n.Stopping {
				return true
			}
		}
	}
	return false
}

// settingsDiffer returns true if the desired config's partitions/fields
// differ from the Up view's settings. Not all LoadConfig fields map to
// QueryViewSettings — only the ones that affect view composition matter.
func settingsDiffer(desired *loadmgr.LoadConfig, current *viewpb.QueryViewSettings) bool {
	if current == nil {
		return true
	}
	if !int64SetEq(desired.PartitionIDs, current.GetRequiredPartitions()) {
		return true
	}
	if !loadFieldsEq(desired.LoadFields, current.GetRequiredFields()) {
		return true
	}
	return false
}

// int64SetEq compares two int64 slices as sets (order-insensitive).
func int64SetEq(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[int64]int, len(a))
	for _, v := range a {
		m[v]++
	}
	for _, v := range b {
		m[v]--
		if m[v] < 0 {
			return false
		}
	}
	return true
}

// loadFieldsEq compares the desired LoadField configs (carrying field IDs)
// against the current view's required field IDs as a set.
func loadFieldsEq(desired []*messagespb.LoadFieldConfig, currentFields []int64) bool {
	if len(desired) != len(currentFields) {
		return false
	}
	desiredIDs := make([]int64, len(desired))
	for i, f := range desired {
		desiredIDs[i] = f.GetFieldId()
	}
	return int64SetEq(desiredIDs, currentFields)
}
