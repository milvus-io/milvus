package balancer

import (
	"context"
	"sync"

	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// SnapshotBuilder assembles a BalancerSnapshot from the various sources:
// LoadConfigStore (desired state), ShardViewRegistry (actual placements),
// and external providers (node topology, data views, segment metadata).
//
// A builder is typically held by the Balancer and invoked at the start of
// each reconcile cycle. It owns a row-count ledger that is rebuilt by full
// reconciles and incrementally refreshed from ShardStats changes in between.
type SnapshotBuilder struct {
	configStore      *loadmgr.LoadConfigStore
	viewRegistry     *coordview.ShardViewRegistry
	nodeProvider     NodeProvider
	dataViewProvider DataViewProvider
	config           *BalanceConfig

	// rowCountLedger is owned by the reconcile loop. Reconcile calls must be serialized.
	rowCountLedger rowCountLedger

	// rowCountDirtyMu protects rowCountDirtyShards.
	rowCountDirtyMu sync.Mutex
	// rowCountDirtyShards tracks shards whose row-count contribution needs refresh.
	rowCountDirtyShards map[qviews.ShardID]struct{}
}

// rowCountLedger owns the three indexes used by incremental snapshot builds.
// shardRowCount is the source of nodeRowCount, while segmentRowCounts caches
// the immutable RowNum metadata needed to construct each shard entry.
type rowCountLedger struct {
	segmentRowCounts map[int64]int64
	shardRowCount    map[qviews.ShardID]ShardRowStats
	nodeRowCount     map[int64]NodeRowStats
}

// ShardRowStats maps each QueryNode to the row-count load contributed by one shard.
type ShardRowStats map[int64]NodeRowStats

// NodeRowStats splits a node's row-count load by placement state.
type NodeRowStats struct {
	UpRowCount      int64
	PendingRowCount int64
}

func newRowCountLedger() rowCountLedger {
	return rowCountLedger{
		segmentRowCounts: make(map[int64]int64),
		shardRowCount:    make(map[qviews.ShardID]ShardRowStats),
		nodeRowCount:     make(map[int64]NodeRowStats),
	}
}

// shardRowStatsSnapshot returns a stable outer-map snapshot for the shards
// planned in this cycle. Row-stat entries themselves are immutable.
func (l *rowCountLedger) shardRowStatsSnapshot(shardIDs []qviews.ShardID) map[qviews.ShardID]ShardRowStats {
	rowStats := make(map[qviews.ShardID]ShardRowStats, len(shardIDs))
	for _, shardID := range shardIDs {
		if stats, ok := l.shardRowCount[shardID]; ok {
			rowStats[shardID] = stats
		}
	}
	return rowStats
}

// NewSnapshotBuilder constructs a builder. All arguments must be non-nil.
func NewSnapshotBuilder(
	configStore *loadmgr.LoadConfigStore,
	viewRegistry *coordview.ShardViewRegistry,
	nodeProvider NodeProvider,
	dataViewProvider DataViewProvider,
	config *BalanceConfig,
) *SnapshotBuilder {
	builder := &SnapshotBuilder{
		configStore:         configStore,
		viewRegistry:        viewRegistry,
		nodeProvider:        nodeProvider,
		dataViewProvider:    dataViewProvider,
		config:              config,
		rowCountLedger:      newRowCountLedger(),
		rowCountDirtyShards: make(map[qviews.ShardID]struct{}),
	}
	if viewRegistry != nil {
		viewRegistry.RegisterStatsObserver(builder.ObserveShardStats)
	}
	return builder
}

// ObserveShardStats marks one shard contribution dirty. It intentionally does
// not perform metadata I/O or trigger reconciliation; the next scoped or
// periodic reconcile consumes the mark.
func (b *SnapshotBuilder) ObserveShardStats(shardID qviews.ShardID, _ *coordview.ShardStats) {
	b.rowCountDirtyMu.Lock()
	b.rowCountDirtyShards[shardID] = struct{}{}
	b.rowCountDirtyMu.Unlock()
}

// build captures the trigger scope before reading DataView and shard details,
// refreshes the incremental row-count ledger, and returns the exact shard list
// that BalancePolicy should plan in this cycle.
func (b *SnapshotBuilder) build(ctx context.Context, pending triggerBatch) (*BalancerSnapshot, []qviews.ShardID) {
	// 1. Capture load configs and resolve the preliminary trigger scope.
	loadSnapshot := b.configStore.Snapshot()

	scope := pending.resolveScope(loadSnapshot, b.viewRegistry)

	// 2. Read scoped DataViews and expand collection triggers into target shards.
	dataViewSnapshot := b.dataViewProvider.DataViewSnapshotForCollections(ctx, scope.collectionIDs)
	scope.AddDataViewShards(loadSnapshot, dataViewSnapshot)
	targetShards := maps.Keys(scope.targetShards)

	// 3. Snapshot shard stats and refresh their contributions to the row-count ledger.
	rowCountDirtyShards := b.takeRowCountDirtyShards()
	targetSnapshot := b.viewRegistry.SnapshotForShards(targetShards)
	if pending.full {
		b.rebuildRowCountLedger(ctx, targetShards, targetSnapshot.StatsMap(), dataViewSnapshot, scope.collectionIDs)
	} else if len(rowCountDirtyShards) > 0 {
		rowCountSnapshot := b.viewRegistry.SnapshotForShards(rowCountDirtyShards)
		b.refreshRowCountLedger(ctx, rowCountDirtyShards, rowCountSnapshot.StatsMap())
	}

	// 4. Assemble the scoped snapshot consumed by BalancePolicy.
	snap := &BalancerSnapshot{
		Config:                b.config,
		LoadConfigSnapshot:    loadSnapshot,
		ShardViewSnapshot:     targetSnapshot,
		DataViewSnapshot:      dataViewSnapshot,
		ShardRowStatsSnapshot: b.rowCountLedger.shardRowStatsSnapshot(targetShards),
	}

	// 5. Attach cluster-wide row counts to the current node snapshot.
	nodeSnapshot := b.nodeProvider.Snapshot()
	snap.NodeSnapshot = nodeSnapshot
	snap.Nodes = buildBalanceNodes(nodeSnapshot)
	for nodeID, rows := range b.rowCountLedger.nodeRowCount {
		if node := snap.Nodes[nodeID]; node != nil {
			node.UpRowCount = rows.UpRowCount
			node.PendingRowCount = rows.PendingRowCount
		}
	}

	return snap, targetShards
}

// takeRowCountDirtyShards atomically swaps the observer-owned dirty set. Marks
// arriving after the swap remain in the newly installed set for the next Build.
func (b *SnapshotBuilder) takeRowCountDirtyShards() []qviews.ShardID {
	b.rowCountDirtyMu.Lock()
	dirtySet := b.rowCountDirtyShards
	b.rowCountDirtyShards = make(map[qviews.ShardID]struct{})
	b.rowCountDirtyMu.Unlock()

	return maps.Keys(dirtySet)
}

// cacheSegmentRowCounts preloads RowNum for segments referenced by the scoped
// DataView before a full ledger rebuild resolves placement-only segments.
func (b *SnapshotBuilder) cacheSegmentRowCounts(snapshot *DataViewSnapshot, collectionIDs map[int64]struct{}) {
	for collectionID := range collectionIDs {
		snapshot.RangeShards(collectionID, func(shard *viewpb.DataViewOfShard) bool {
			for _, partition := range shard.GetPartitions() {
				for _, segmentID := range partition.GetSegmentIds() {
					if info, ok := snapshot.SegmentInfo(segmentID); ok {
						b.rowCountLedger.segmentRowCounts[segmentID] = segmentRows(info)
					}
				}
			}
			return true
		})
	}
}

// rebuildRowCountLedger clears all cached ledger state and reconstructs it from
// the full reconcile scope.
func (b *SnapshotBuilder) rebuildRowCountLedger(
	ctx context.Context,
	shardIDs []qviews.ShardID,
	statsByShard map[qviews.ShardID]*coordview.ShardStats,
	dataSnapshot *DataViewSnapshot,
	collectionIDs map[int64]struct{},
) {
	b.rowCountLedger = newRowCountLedger()
	b.cacheSegmentRowCounts(dataSnapshot, collectionIDs)
	b.refreshRowCountLedger(ctx, shardIDs, statsByShard)
}

// refreshRowCountLedger requests uncached placement metadata in one batch,
// then replaces each shard's contribution in the node aggregates. Missing
// metadata remains uncached and contributes zero rows.
func (b *SnapshotBuilder) refreshRowCountLedger(
	ctx context.Context,
	shardIDs []qviews.ShardID,
	statsByShard map[qviews.ShardID]*coordview.ShardStats,
) {
	unknownSegments := make([]int64, 0)
	for _, shardID := range shardIDs {
		stats := statsByShard[shardID]
		if stats == nil {
			continue
		}
		for segmentID := range stats.Segments {
			if _, ok := b.rowCountLedger.segmentRowCounts[segmentID]; ok {
				continue
			}
			unknownSegments = append(unknownSegments, segmentID)
		}
	}

	if len(unknownSegments) > 0 {
		segmentSnapshot := b.dataViewProvider.SegmentSnapshot(ctx, unknownSegments)
		if segmentSnapshot != nil {
			for _, segmentID := range unknownSegments {
				if info, ok := segmentSnapshot.Get(segmentID); ok {
					b.rowCountLedger.segmentRowCounts[segmentID] = segmentRows(info)
				}
			}
		}
	}

	for _, shardID := range shardIDs {
		b.rowCountLedger.replaceShardRowStats(shardID, statsByShard[shardID])
	}
}

// replaceShardRowStats removes the previously applied row counts before adding
// the values derived from the current immutable stats snapshot.
func (l *rowCountLedger) replaceShardRowStats(shardID qviews.ShardID, stats *coordview.ShardStats) {
	if old, ok := l.shardRowCount[shardID]; ok {
		l.applyShardRowStats(old, -1)
		delete(l.shardRowCount, shardID)
	}
	if stats == nil || len(stats.Segments) == 0 {
		return
	}

	rowStats := make(ShardRowStats)
	for segmentID, segment := range stats.Segments {
		rows := l.segmentRowCounts[segmentID]
		for nodeID, state := range segment.Nodes {
			nodeRows := rowStats[nodeID]
			switch state {
			case coordview.SegmentStateUp:
				nodeRows.UpRowCount += rows
			case coordview.SegmentStateReady, coordview.SegmentStatePreparing:
				nodeRows.PendingRowCount += rows
			}
			rowStats[nodeID] = nodeRows
		}
	}
	l.shardRowCount[shardID] = rowStats
	l.applyShardRowStats(rowStats, 1)
}

// applyShardRowStats adds row stats for direction=1 and subtracts them for
// direction=-1.
func (l *rowCountLedger) applyShardRowStats(rowStats ShardRowStats, direction int64) {
	for nodeID, rows := range rowStats {
		stats := l.nodeRowCount[nodeID]
		stats.UpRowCount += direction * rows.UpRowCount
		stats.PendingRowCount += direction * rows.PendingRowCount
		l.storeNodeRowStats(nodeID, stats)
	}
}

// storeNodeRowStats keeps the aggregate ledger sparse by removing zero entries.
func (l *rowCountLedger) storeNodeRowStats(nodeID int64, stats NodeRowStats) {
	if stats.UpRowCount == 0 && stats.PendingRowCount == 0 {
		delete(l.nodeRowCount, nodeID)
		return
	}
	l.nodeRowCount[nodeID] = stats
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
