package balancer

import (
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
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

func (b triggerBatch) empty() bool {
	return !b.full && len(b.dirtyNodes) == 0 && len(b.dirtyShards) == 0 && len(b.dirtyColls) == 0
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

func (b triggerBatch) expand(snap *BalancerSnapshot) []qviews.ShardID {
	if snap == nil {
		return nil
	}

	out := make(map[qviews.ShardID]struct{})
	if b.full {
		for _, shardID := range allSnapshotShards(snap) {
			out[shardID] = struct{}{}
		}
	}
	for shardID := range b.dirtyShards {
		out[shardID] = struct{}{}
	}
	for nodeID := range b.dirtyNodes {
		for _, shardID := range snapshotShardsByNode(snap, nodeID) {
			out[shardID] = struct{}{}
		}
	}
	for collectionID := range b.dirtyColls {
		for _, shardID := range snapshotShardsByCollection(snap, collectionID) {
			out[shardID] = struct{}{}
		}
	}

	shards := make([]qviews.ShardID, 0, len(out))
	for shardID := range out {
		shards = append(shards, shardID)
	}
	sort.Slice(shards, func(i, j int) bool {
		return shardLess(shards[i], shards[j])
	})
	return shards
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

func allSnapshotShards(snap *BalancerSnapshot) []qviews.ShardID {
	seen := make(map[qviews.ShardID]struct{})
	for shardID := range snap.ShardStatsMap() {
		seen[shardID] = struct{}{}
	}
	for collectionID := range snap.ConfigsMap() {
		snap.RangeDataShards(collectionID, func(shardID qviews.ShardID) bool {
			seen[shardID] = struct{}{}
			return true
		})
	}
	out := make([]qviews.ShardID, 0, len(seen))
	for shardID := range seen {
		out = append(out, shardID)
	}
	return out
}

func snapshotShardsByNode(snap *BalancerSnapshot, nodeID int64) []qviews.ShardID {
	seen := make(map[qviews.ShardID]struct{})
	for shardID, stats := range snap.ShardStatsMap() {
		if stats == nil {
			continue
		}
		for _, segment := range stats.Segments {
			if _, ok := segment.Nodes[nodeID]; ok {
				seen[shardID] = struct{}{}
				break
			}
		}
	}
	out := make([]qviews.ShardID, 0, len(seen))
	for shardID := range seen {
		out = append(out, shardID)
	}
	return out
}

func snapshotShardsByCollection(snap *BalancerSnapshot, collectionID int64) []qviews.ShardID {
	seen := make(map[qviews.ShardID]struct{})
	for shardID := range snap.ShardStatsMap() {
		cfg := snap.ConfigForShard(shardID)
		if cfg != nil && cfg.CollectionID == collectionID {
			seen[shardID] = struct{}{}
			continue
		}
		// ReleaseCollection removes the load config before triggering the
		// collection reconcile. Recover the collection from the vchannel so
		// residual shard views are released without waiting for a full scan.
		if cfg == nil && funcutil.GetCollectionIDFromVChannel(shardID.VChannel) == collectionID {
			seen[shardID] = struct{}{}
		}
	}
	snap.RangeDataShards(collectionID, func(shardID qviews.ShardID) bool {
		seen[shardID] = struct{}{}
		return true
	})
	out := make([]qviews.ShardID, 0, len(seen))
	for shardID := range seen {
		out = append(out, shardID)
	}
	return out
}
