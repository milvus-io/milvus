package balancer

import (
	"slices"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
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
	// data version changed, node unavailable, load info changed).
	// The Policy unconditionally adds the candidate to the plan.
	actionMust

	// actionMayOptimize: the current placement is valid; the Policy may
	// emit a migration when the complete steady-state candidate differs
	// from the current assignment.
	actionMayOptimize
)

// classifyShard inspects the snapshot and returns the action kind for shardID.
//
// The decision rules, in order:
//
//  1. Desired absent + current exists     → Release
//  2. Desired present + no Up view        → Must unless Preparing exists
//  3. Already has a Preparing view        → None (avoid stacking)
//  4. Current DataVersion < DataView DV   → Must (data changed)
//  5. Current view references an unavailable node → Must (node lost)
//  6. LoadInfoVersion differs            → Must
//  7. Otherwise                           → MayOptimize
func classifyShard(snap balanceInput, shardID qviews.ShardID) actionKind {
	desired := snap.ConfigForShard(shardID)
	stats := snap.GetShardStats(shardID)
	hasUpView := stats != nil && stats.UpVersion != nil
	hasPreparing := stats != nil && stats.PreparingVersion != nil
	hasAnyView := hasUpView || hasPreparing || (stats != nil && len(stats.Segments) > 0)
	if entry := snap.ShardEntry(shardID); entry != nil && len(entry.ResidentNodes()) > 0 {
		hasAnyView = true
	}

	// 1. Desired absent → release any residual views.
	if desired == nil {
		if hasAnyView {
			return actionRelease
		}
		return actionNone
	}
	if target, managed := snap.TargetNodes(shardID); managed && len(target) == 0 {
		if !hasAnyView {
			return actionNone
		}
		if !healthyUp(snap, shardID) {
			return actionRelease
		}
		// Never retire the last healthy serving cover merely to reduce replicas.
		for _, replica := range desired.Replicas {
			sibling := qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: shardID.VChannel}
			if nodes, ok := snap.TargetNodes(sibling); ok && len(nodes) > 0 && healthyUp(snap, sibling) {
				return actionRelease
			}
		}
		return actionNone
	}
	if hasPreparing {
		if entry := snap.ShardEntry(shardID); entry != nil {
			for _, node := range entry.PreparingNodes() {
				if !eligibleShardNode(snap, desired, shardID, node) {
					return actionMust
				}
			}
		}
	}

	// 2. Desired present but no Up view → must create.
	if !hasUpView {
		if hasPreparing {
			return actionNone
		}
		return actionMust
	}

	// 3. Already have a Preparing view? Skip this cycle even if a newer
	// DataVersion or load config has arrived. The next reconcile after the
	// in-flight view reaches Up will pick up the latest snapshot.
	if hasPreparing {
		return actionNone
	}

	// 4. DataVersion advanced?
	if dataViewVersionAdvanced(snap, desired, stats) {
		return actionMust
	}

	// 5. Any node in the current Up view is unavailable?
	if hasUnavailableNode(stats, snap.NodesMap()) {
		return actionMust
	}

	// 6. LoadInfo differs between desired and current?
	if loadInfoDiffer(snap, desired, stats) {
		return actionMust
	}
	if target, managed := snap.TargetNodes(shardID); managed {
		if entry := snap.ShardEntry(shardID); entry != nil {
			for _, node := range entry.UpNodes() {
				if _, found := slices.BinarySearch(target, node); !found {
					return actionMust
				}
			}
		}
	}

	// 7. Steady-state — candidate for balance optimization.
	return actionMayOptimize
}

func eligibleShardNode(snap balanceInput, cfg *loadmgr.LoadConfig, id qviews.ShardID, node int64) bool {
	n := snap.NodesMap()[node]
	r := findReplica(cfg, id.ReplicaID)
	return n != nil && n.Alive && !n.Stopping && r != nil && n.ResourceGroup == r.ResourceGroup
}

func healthyUp(snap balanceInput, id qviews.ShardID) bool {
	stats := snap.GetShardStats(id)
	if stats == nil || stats.UpVersion == nil {
		return false
	}
	entry := snap.ShardEntry(id)
	if entry != nil {
		for _, node := range entry.UpNodes() {
			n := snap.NodesMap()[node]
			if n == nil || !n.Alive || n.Stopping {
				return false
			}
		}
	}
	return true
}

// dataViewVersionAdvanced returns true if the shard's current Up view was
// built on an older DataVersion than the collection's current DataView.
// Returns false when the snapshot has no DataVersion for the collection
// (DataView Manager hasn't reported one yet); the next reconcile cycle will
// pick up the change.
func dataViewVersionAdvanced(snap balanceInput, desired *loadmgr.LoadConfig, stats *coordview.ShardStats) bool {
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

func loadInfoDiffer(snap balanceInput, desired *loadmgr.LoadConfig, stats *coordview.ShardStats) bool {
	if stats == nil {
		return true
	}
	if stats.UpLoadInfoVersion == 0 {
		return true
	}
	loadInfoVersion := snap.ConfigVersion(desired.CollectionID)
	if loadInfoVersion == 0 {
		return true
	}
	return stats.UpLoadInfoVersion != loadInfoVersion
}
