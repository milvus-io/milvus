package balancer

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// TriggerScope describes the external event scope that dirtied the Balancer.
//
// Empty Trigger() means a full scan. NodeChanged without DirtyNodes also means
// a full scan, because the caller did not provide enough information to narrow
// the affected shards.
type TriggerScope struct {
	NodeChanged      bool
	DirtyNodes       []int64
	DirtyShards      []qviews.ShardID
	DirtyCollections []int64
}

type triggerQueue struct {
	mu sync.Mutex

	full        bool
	dirtyNodes  map[int64]struct{}
	dirtyShards map[qviews.ShardID]struct{}
	dirtyColls  map[int64]struct{}

	signal chan struct{}
}

type triggerBatch struct {
	full        bool
	dirtyNodes  map[int64]struct{}
	dirtyShards map[qviews.ShardID]struct{}
	dirtyColls  map[int64]struct{}
}

// reconcileScope is the scoped DataView-read and Policy-planning boundary
// resolved from one trigger batch.
type reconcileScope struct {
	// collectionIDs selects collections whose DataViews are fetched.
	collectionIDs map[int64]struct{}

	// collectionWideIDs selects collections whose DataView shards are all
	// expanded into Policy targets.
	collectionWideIDs map[int64]struct{}

	// targetShards is the exact ShardStats scope and final Policy input.
	targetShards map[qviews.ShardID]struct{}
}

func (b triggerBatch) empty() bool {
	return !b.full && len(b.dirtyNodes) == 0 && len(b.dirtyShards) == 0 && len(b.dirtyColls) == 0
}

// resolveScope converts queued collection and shard events into scoped reads
// and Policy targets. Collection events include resident residual shards;
// malformed shard channels conservatively fall back to a full reconcile.
func (b triggerBatch) resolveScope(
	loadSnapshot *loadmgr.LoadConfigSnapshot,
	registry *coordview.ShardViewRegistry,
) reconcileScope {
	if b.full {
		return fullReconcileScope(loadSnapshot, registry)
	}

	scope := newReconcileScope()
	for collectionID := range b.dirtyColls {
		scope.collectionIDs[collectionID] = struct{}{}
		scope.collectionWideIDs[collectionID] = struct{}{}
		if registry != nil {
			scope.addShards(registry.CollectionShards(collectionID))
		}
	}

	for nodeID := range b.dirtyNodes {
		if registry == nil {
			return fullReconcileScope(loadSnapshot, registry)
		}
		for _, shardID := range registry.NodeShards(nodeID) {
			collectionID, ok := parseShardCollection(shardID)
			if !ok {
				return fullReconcileScope(loadSnapshot, registry)
			}
			scope.collectionIDs[collectionID] = struct{}{}
			scope.targetShards[shardID] = struct{}{}
		}
	}

	for shardID := range b.dirtyShards {
		collectionID, ok := parseShardCollection(shardID)
		if !ok {
			return fullReconcileScope(loadSnapshot, registry)
		}
		scope.collectionIDs[collectionID] = struct{}{}
		scope.targetShards[shardID] = struct{}{}
	}

	return scope
}

// AddDataViewShards expands collection-triggered scope into replica-by-vchannel
// targets from the latest DataView. Direct dirty-shard scope is not expanded.
func (s *reconcileScope) AddDataViewShards(
	loadSnapshot *loadmgr.LoadConfigSnapshot,
	dataSnapshot *DataViewSnapshot,
) {
	for collectionID := range s.collectionWideIDs {
		cfg := loadSnapshot.ConfigsMap()[collectionID]
		if cfg == nil {
			continue
		}
		dataSnapshot.RangeShards(collectionID, func(shard *viewpb.DataViewOfShard) bool {
			if shard == nil {
				return true
			}
			for _, replica := range cfg.Replicas {
				if replica == nil {
					continue
				}
				s.targetShards[qviews.ShardID{
					ReplicaID: replica.ReplicaID,
					VChannel:  shard.GetVchannel(),
				}] = struct{}{}
			}
			return true
		})
	}
}

func newReconcileScope() reconcileScope {
	return reconcileScope{
		collectionIDs:     make(map[int64]struct{}),
		collectionWideIDs: make(map[int64]struct{}),
		targetShards:      make(map[qviews.ShardID]struct{}),
	}
}

// fullReconcileScope combines configured collections with all resident shards,
// so residual views remain visible after their load config has been removed.
func fullReconcileScope(
	loadSnapshot *loadmgr.LoadConfigSnapshot,
	registry *coordview.ShardViewRegistry,
) reconcileScope {
	scope := newReconcileScope()
	if loadSnapshot != nil {
		for collectionID := range loadSnapshot.ConfigsMap() {
			scope.collectionIDs[collectionID] = struct{}{}
			scope.collectionWideIDs[collectionID] = struct{}{}
		}
	}
	if registry != nil {
		shardIDs := registry.ShardIDs()
		scope.addShards(shardIDs)
		for _, shardID := range shardIDs {
			if collectionID, ok := parseShardCollection(shardID); ok {
				scope.collectionIDs[collectionID] = struct{}{}
			}
		}
	}
	return scope
}

func (s *reconcileScope) addShards(shardIDs []qviews.ShardID) {
	for _, shardID := range shardIDs {
		s.targetShards[shardID] = struct{}{}
	}
}

// parseShardCollection extracts the collection ID encoded in a shard vchannel.
// resolveScope uses a false result to fall back to a full reconcile.
func parseShardCollection(shardID qviews.ShardID) (int64, bool) {
	channel, err := metautil.ParseChannel(shardID.VChannel, metautil.NewDynChannelMapper())
	if err != nil {
		return 0, false
	}
	return channel.CollectionID(), true
}

func newTriggerQueue() *triggerQueue {
	return &triggerQueue{
		dirtyNodes:  make(map[int64]struct{}),
		dirtyShards: make(map[qviews.ShardID]struct{}),
		dirtyColls:  make(map[int64]struct{}),
		signal:      make(chan struct{}, 1),
	}
}

func (q *triggerQueue) add(scopes ...TriggerScope) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(scopes) == 0 {
		q.full = true
		q.notifyLocked()
		return
	}

	for _, scope := range scopes {
		if scope.NodeChanged && len(scope.DirtyNodes) == 0 {
			q.full = true
		}
		for _, nodeID := range scope.DirtyNodes {
			q.dirtyNodes[nodeID] = struct{}{}
		}
		for _, shardID := range scope.DirtyShards {
			q.dirtyShards[shardID] = struct{}{}
		}
		for _, collectionID := range scope.DirtyCollections {
			q.dirtyColls[collectionID] = struct{}{}
		}
	}
	q.notifyLocked()
}

func (q *triggerQueue) notifyLocked() {
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

func (q *triggerQueue) signalCh() <-chan struct{} {
	return q.signal
}

func (q *triggerQueue) takePending() triggerBatch {
	q.mu.Lock()
	defer q.mu.Unlock()

	pending := triggerBatch{
		full:        q.full,
		dirtyNodes:  q.dirtyNodes,
		dirtyShards: q.dirtyShards,
		dirtyColls:  q.dirtyColls,
	}

	q.full = false
	q.dirtyNodes = make(map[int64]struct{})
	q.dirtyShards = make(map[qviews.ShardID]struct{})
	q.dirtyColls = make(map[int64]struct{})
	return pending
}
