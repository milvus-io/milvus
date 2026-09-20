package balancer

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/qviews"
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

func (b triggerBatch) empty() bool {
	return !b.full && len(b.dirtyNodes) == 0 && len(b.dirtyShards) == 0 && len(b.dirtyColls) == 0
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
