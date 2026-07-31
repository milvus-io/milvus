package coordview

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/internal/views/coord/coordview/syncer"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
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
	mu             sync.RWMutex
	ctx            context.Context
	flushScheduler *DirtyViewFlushScheduler

	version  uint64
	shards   map[qviews.ShardID]*ShardViewManager
	stats    map[qviews.ShardID]*ShardStats
	snapshot *ShardViewSnapshot

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
) (*ShardViewRegistry, error) {
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
		shards[sid] = newShardViewManager(ctx, sid, flushScheduler, recovered)
	}
	batch.Commit()

	registry := &ShardViewRegistry{
		ctx:            ctx,
		flushScheduler: flushScheduler,
		version:        1,
		shards:         shards,
		stats:          make(map[qviews.ShardID]*ShardStats, len(shards)),
	}
	for sid, mgr := range shards {
		mgr.SetStatsObserver(registry.onShardStatsChanged)
		registry.stats[sid] = mgr.Stats()
	}
	if err := flushScheduler.Flush(ctx); err != nil {
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

	mgr := newShardViewManager(r.ctx, shardID, r.flushScheduler, nil)
	mgr.SetStatsObserver(r.onShardStatsChanged)
	stats := emptyShardStats()

	r.mu.Lock()
	// Re-check under the write lock.
	if mgr, ok := r.shards[shardID]; ok {
		r.mu.Unlock()
		return mgr
	}
	r.shards[shardID] = mgr
	r.stats[shardID] = stats
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
	r.stats[shardID] = stats
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

func emptyShardStats() *ShardStats {
	return &ShardStats{Segments: make(map[int64]*SegmentStats)}
}
