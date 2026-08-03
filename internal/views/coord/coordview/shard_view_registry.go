package coordview

import (
	"context"
	"sync"

	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// ShardViewRegistry owns the lifecycle of every ShardViewManager on this Coord
// and exposes the immutable per-shard stats snapshot consumed by Balancer.
//
// The registry does not interpret views itself; it is a thin container around
// per-shard ShardViewManagers. Each manager holds its own mutex, so per-shard
// operations (AddPreparing, Stats) can run concurrently with registry lookups.
//
// All methods are safe for concurrent use.
type ShardViewRegistry struct {
	mu                 sync.RWMutex
	ctx                context.Context
	flushScheduler     *DirtyViewFlushScheduler
	dataViewReferences qviews.DataViewReferenceManager

	version  uint64
	shards   map[qviews.ShardID]*ShardViewManager
	stats    map[qviews.ShardID]*ShardStats
	snapshot *ShardViewSnapshot

	// Reverse indexes cover resident shards. The collection index changes with
	// shard lifecycle, while the node index tracks current Stats placements.
	collectionShards map[int64]map[qviews.ShardID]struct{}
	nodeShards       map[int64]map[qviews.ShardID]struct{}

	statsObservers []func(qviews.ShardID, *ShardStats)
}

// RecoverShardViewRegistry constructs a ShardViewRegistry and rebuilds every
// ShardViewManager from ETCD-persisted views. This is the sole constructor:
// the registry is always fully recovered before any operation.
//
// The provided ctx becomes the lifecycle context for every ShardViewManager's
// callback-driven I/O.
func RecoverShardViewRegistry(
	ctx context.Context,
	catalog queryview.QueryViewCatalog,
	s syncer.ReliableSyncer,
	dataViewReferences ...qviews.DataViewReferenceManager,
) (*ShardViewRegistry, error) {
	refs := dataViewReferenceManagerOrNoop(dataViewReferences)
	views, err := catalog.ListQueryViews(ctx)
	if err != nil {
		return nil, err
	}

	byShardID := make(map[qviews.ShardID][]*viewpb.QueryViewOfShard)
	for _, v := range views {
		sid := qviews.ShardID{
			ReplicaID: v.GetMeta().GetReplicaId(),
			VChannel:  v.GetMeta().GetVchannel(),
		}
		byShardID[sid] = append(byShardID[sid], v)
	}

	flushScheduler := newDirtyViewFlushScheduler(
		catalog,
		s,
		paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt(),
		nodescheduler.Get(),
	)
	batch := flushScheduler.Begin()
	shards := make(map[qviews.ShardID]*ShardViewManager, len(byShardID))
	for sid, recovered := range byShardID {
		manager, err := RecoverShardViewManager(ctx, sid, flushScheduler, refs, recovered)
		if err != nil {
			for _, recoveredManager := range shards {
				recoveredManager.unpinAllReferences()
			}
			flushScheduler.Close()
			return nil, err
		}
		shards[sid] = manager
	}

	registry := &ShardViewRegistry{
		ctx:                ctx,
		flushScheduler:     flushScheduler,
		dataViewReferences: refs,
		version:            1,
		shards:             shards,
		stats:              make(map[qviews.ShardID]*ShardStats, len(shards)),
		collectionShards:   make(map[int64]map[qviews.ShardID]struct{}),
		nodeShards:         make(map[int64]map[qviews.ShardID]struct{}),
	}
	for sid, mgr := range shards {
		stats := mgr.Stats()
		registry.stats[sid] = stats
		registry.addCollectionShardLocked(sid)
		registry.addNodeShardsLocked(sid, stats)
		mgr.SetStatsObserver(registry.onShardStatsChanged)
		mgr.setOnEmpty(registry.removeEmptyManager)
	}
	// Recovery sync callbacks may update manager stats immediately. Install all
	// observers and indexes before releasing the held recovery events so those
	// updates cannot be lost between the initial Stats call and observer setup.
	batch.Commit()
	if err := flushScheduler.Flush(ctx); err != nil {
		for _, manager := range shards {
			manager.unpinAllReferences()
		}
		flushScheduler.Close()
		return nil, err
	}
	return registry, nil
}

// Ensure returns the ShardViewManager for shardID, creating a fresh one if
// none exists. Safe to call repeatedly.
func (r *ShardViewRegistry) Ensure(shardID qviews.ShardID) *ShardViewManager {
	// Fast path: already present.
	r.mu.RLock()
	if mgr, ok := r.shards[shardID]; ok {
		r.mu.RUnlock()
		return mgr
	}
	r.mu.RUnlock()

	mgr := newShardViewManager(r.ctx, shardID, r.flushScheduler, nil, r.dataViewReferences)
	mgr.SetStatsObserver(r.onShardStatsChanged)
	mgr.setOnEmpty(r.removeEmptyManager)
	stats := emptyShardStats()

	r.mu.Lock()
	// Re-check under the write lock.
	if mgr, ok := r.shards[shardID]; ok {
		r.mu.Unlock()
		return mgr
	}
	r.shards[shardID] = mgr
	r.stats[shardID] = stats
	r.addCollectionShardLocked(shardID)
	r.version++
	r.mu.Unlock()
	return mgr
}

func (r *ShardViewRegistry) Close() {
	if r == nil || r.flushScheduler == nil {
		return
	}
	r.flushScheduler.Close()
}

// Begin opens an explicit cross-shard QueryView flush batch. Existing flush
// tasks continue running; events emitted before the returned token is committed
// are held and dispatched together as disjoint ShardID-lane tasks.
func (r *ShardViewRegistry) Begin() DirtyViewBatch {
	return r.flushScheduler.Begin()
}

// Get returns the ShardViewManager for shardID, or nil if absent.
func (r *ShardViewRegistry) Get(shardID qviews.ShardID) *ShardViewManager {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.shards[shardID]
}

// removeEmptyManager reclaims a manager after its last QueryView has completed
// durable removal. Recheck both emptiness and identity because the callback is
// invoked after releasing the manager lock.
func (r *ShardViewRegistry) removeEmptyManager(shardID qviews.ShardID, manager *ShardViewManager) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.views) != 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shards[shardID] != manager {
		return
	}
	r.removeNodeShardsLocked(shardID, r.stats[shardID])
	r.removeCollectionShardLocked(shardID)
	delete(r.stats, shardID)
	delete(r.shards, shardID)
	r.version++
}

// Snapshot returns the current resident immutable shard-view snapshot. It
// refreshes the resident snapshot lazily when the live version has advanced.
func (r *ShardViewRegistry) Snapshot() *ShardViewSnapshot {
	r.mu.RLock()
	snapshot := r.snapshot
	if snapshot != nil && snapshot.Version() == r.version {
		r.mu.RUnlock()
		return snapshot
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.snapshot == nil || r.snapshot.Version() != r.version {
		r.publishSnapshotLocked()
	}
	return r.snapshot
}

// SnapshotForShards returns an immutable snapshot containing only the
// requested resident shards. It does not refresh the cached full snapshot.
func (r *ShardViewRegistry) SnapshotForShards(shardIDs []qviews.ShardID) *ShardViewSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := make(map[qviews.ShardID]*ShardStats, len(shardIDs))
	for _, shardID := range shardIDs {
		if shardStats, ok := r.stats[shardID]; ok {
			stats[shardID] = shardStats
		}
	}
	return &ShardViewSnapshot{
		version: r.version,
		stats:   stats,
	}
}

// CollectionShards returns the resident shards belonging to collectionID.
func (r *ShardViewRegistry) CollectionShards(collectionID int64) []qviews.ShardID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return maps.Keys(r.collectionShards[collectionID])
}

// NodeShards returns the resident shards with placements on nodeID.
func (r *ShardViewRegistry) NodeShards(nodeID int64) []qviews.ShardID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return maps.Keys(r.nodeShards[nodeID])
}

// ShardIDs returns all resident shard IDs.
func (r *ShardViewRegistry) ShardIDs() []qviews.ShardID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	shardIDs := make([]qviews.ShardID, 0, len(r.shards))
	for shardID := range r.shards {
		shardIDs = append(shardIDs, shardID)
	}
	return shardIDs
}

// RegisterStatsObserver registers an observer for future per-shard stats
// updates. The current snapshot is not replayed; callers that need recovery
// state should read Snapshot explicitly.
func (r *ShardViewRegistry) RegisterStatsObserver(observer func(qviews.ShardID, *ShardStats)) {
	if observer == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statsObservers = append(r.statsObservers, observer)
}

type ShardViewSnapshot struct {
	version uint64
	stats   map[qviews.ShardID]*ShardStats
}

func NewShardViewSnapshot(version uint64, stats map[qviews.ShardID]*ShardStats) *ShardViewSnapshot {
	statsCopy := make(map[qviews.ShardID]*ShardStats, len(stats))
	for sid, st := range stats {
		statsCopy[sid] = st
	}
	return &ShardViewSnapshot{
		version: version,
		stats:   statsCopy,
	}
}

func (s *ShardViewSnapshot) Version() uint64 {
	if s == nil {
		return 0
	}
	return s.version
}

func (s *ShardViewSnapshot) StatsMap() map[qviews.ShardID]*ShardStats {
	if s == nil {
		return nil
	}
	return s.stats
}

func (r *ShardViewRegistry) onShardStatsChanged(shardID qviews.ShardID, stats *ShardStats) {
	r.mu.Lock()
	if _, ok := r.shards[shardID]; !ok {
		r.mu.Unlock()
		return
	}
	r.removeNodeShardsLocked(shardID, r.stats[shardID])
	r.stats[shardID] = stats
	r.addNodeShardsLocked(shardID, stats)
	r.version++
	observers := append([]func(qviews.ShardID, *ShardStats){}, r.statsObservers...)
	r.mu.Unlock()

	for _, observer := range observers {
		observer(shardID, stats)
	}
}

func (r *ShardViewRegistry) publishSnapshotLocked() {
	r.snapshot = NewShardViewSnapshot(r.version, r.stats)
}

func (r *ShardViewRegistry) addCollectionShardLocked(shardID qviews.ShardID) {
	channel, err := metautil.ParseChannel(shardID.VChannel, metautil.NewDynChannelMapper())
	if err != nil {
		return
	}
	shards := r.collectionShards[channel.CollectionID()]
	if shards == nil {
		shards = make(map[qviews.ShardID]struct{})
		r.collectionShards[channel.CollectionID()] = shards
	}
	shards[shardID] = struct{}{}
}

func (r *ShardViewRegistry) removeCollectionShardLocked(shardID qviews.ShardID) {
	channel, err := metautil.ParseChannel(shardID.VChannel, metautil.NewDynChannelMapper())
	if err != nil {
		return
	}
	collectionID := channel.CollectionID()
	shards := r.collectionShards[collectionID]
	delete(shards, shardID)
	if len(shards) == 0 {
		delete(r.collectionShards, collectionID)
	}
}

func (r *ShardViewRegistry) addNodeShardsLocked(shardID qviews.ShardID, stats *ShardStats) {
	if stats == nil {
		return
	}
	for _, segment := range stats.Segments {
		if segment == nil {
			continue
		}
		for nodeID := range segment.Nodes {
			shards := r.nodeShards[nodeID]
			if shards == nil {
				shards = make(map[qviews.ShardID]struct{})
				r.nodeShards[nodeID] = shards
			}
			shards[shardID] = struct{}{}
		}
	}
}

func (r *ShardViewRegistry) removeNodeShardsLocked(shardID qviews.ShardID, stats *ShardStats) {
	if stats == nil {
		return
	}
	for _, segment := range stats.Segments {
		if segment == nil {
			continue
		}
		for nodeID := range segment.Nodes {
			shards := r.nodeShards[nodeID]
			delete(shards, shardID)
			if len(shards) == 0 {
				delete(r.nodeShards, nodeID)
			}
		}
	}
}

func emptyShardStats() *ShardStats {
	return &ShardStats{Segments: make(map[int64]*SegmentStats)}
}
