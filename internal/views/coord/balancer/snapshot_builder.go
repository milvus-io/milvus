package balancer

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// SnapshotBuilder assembles a BalancerSnapshot from the various sources:
// LoadConfigStore (desired state), ShardViewRegistry (actual placements),
// and external providers (node topology, data views, segment metadata).
//
// A builder is typically held by the Balancer and invoked at the start of
// each reconcile cycle. It is stateless apart from its configured sources;
// calling Build(ctx) twice produces two independent snapshots.
type SnapshotBuilder struct {
	configStore      *loadmgr.LoadConfigStore
	viewRegistry     *coordview.ShardViewRegistry
	nodeProvider     NodeProvider
	dataViewProvider DataViewProvider
	config           *BalanceConfig
}

// NewSnapshotBuilder constructs a builder. All arguments must be non-nil.
func NewSnapshotBuilder(
	configStore *loadmgr.LoadConfigStore,
	viewRegistry *coordview.ShardViewRegistry,
	nodeProvider NodeProvider,
	dataViewProvider DataViewProvider,
	config *BalanceConfig,
) *SnapshotBuilder {
	return &SnapshotBuilder{
		configStore:      configStore,
		viewRegistry:     viewRegistry,
		nodeProvider:     nodeProvider,
		dataViewProvider: dataViewProvider,
		config:           config,
	}
}

// Build collects a fresh BalancerSnapshot. The flow is:
//  1. LoadConfigSnapshot ← LoadConfigStore
//  2. ShardViewSnapshot  ← ShardViewRegistry
//  3. DataViewSnapshot   ← DataViewProvider
//  4. NodeSnapshot       ← NodeProvider
//  5. Nodes              ← NodeSnapshot plus registry node segment overview.
//
// The returned snapshot is owned by the caller and not shared with the
// builder's sources.
func (b *SnapshotBuilder) Build(ctx context.Context) *BalancerSnapshot {
	snap := &BalancerSnapshot{
		Config: b.config,
	}

	// 1. LoadConfigs + replica index.
	loadSnapshot := b.configStore.Snapshot()
	snap.LoadConfigSnapshot = loadSnapshot

	// 2. ShardStats.
	viewSnapshot := b.viewRegistry.Snapshot()
	snap.ShardViewSnapshot = viewSnapshot

	// 3. DataView + segment metadata snapshot.
	snap.DataViewSnapshot = b.dataViewProvider.DataViewSnapshot(ctx)

	// 5. Nodes: start from provider infos, then aggregate per-node load.
	nodeSnapshot := b.nodeProvider.Snapshot()
	snap.NodeSnapshot = nodeSnapshot
	snap.Nodes = buildBalanceNodes(nodeSnapshot)
	aggregateNodeLoad(snap.Nodes, viewSnapshot.StatsMap(), snap)

	return snap
}

// buildBalanceNodes converts NodeInfos into BalanceNodes with zero aggregate
// load; callers fill in UpRowCount / PendingRowCount afterwards.
func buildBalanceNodes(snapshot *NodeSnapshot) map[int64]*BalanceNode {
	out := make(map[int64]*BalanceNode)
	snapshot.Range(func(id int64, info *NodeInfo) bool {
		out[id] = &BalanceNode{
			NodeID:        info.NodeID,
			Alive:         info.Alive,
			Stopping:      info.Stopping,
			ResourceGroup: info.ResourceGroup,
		}
		return true
	})
	return out
}

// aggregateNodeLoad walks shard segment stats and accumulates UpRowCount and
// PendingRowCount onto each BalanceNode. Segments missing from the metadata
// snapshot contribute zero rows, matching DataCoord's "not yet estimated"
// semantics.
func aggregateNodeLoad(
	nodes map[int64]*BalanceNode,
	shardStats map[qviews.ShardID]*coordview.ShardStats,
	snap *BalancerSnapshot,
) {
	for _, stats := range shardStats {
		if stats == nil {
			continue
		}
		for segmentID, segment := range stats.Segments {
			rowCount := int64(0)
			if info, ok := snap.SegmentInfo(segmentID); ok {
				rowCount = segmentRows(info)
			}
			for nodeID, state := range segment.Nodes {
				node, ok := nodes[nodeID]
				if !ok {
					// Node present in placements but not in provider (e.g.,
					// newly removed node with still-live views). Skip; the
					// Balancer's Phase 1 will detect the unavailability.
					continue
				}
				switch state {
				case coordview.SegmentStateUp:
					node.UpRowCount += rowCount
				case coordview.SegmentStateReady, coordview.SegmentStatePreparing:
					node.PendingRowCount += rowCount
				}
			}
		}
	}
}
