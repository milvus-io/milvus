package balancer

import (
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

type TriggerScope = api.TriggerScope

type triggerQueue struct {
	mu sync.Mutex

	scan        bool
	dirtyNodes  map[int64]struct{}
	dirtyShards map[qviews.ShardID]struct{}
	dirtyColls  map[int64]struct{}

	signal     chan struct{}
	retries    triggerBatch
	retryAt    time.Time
	retryDelay time.Duration
}

type triggerBatch struct {
	scan        bool
	dirtyNodes  map[int64]struct{}
	dirtyShards map[qviews.ShardID]struct{}
	dirtyColls  map[int64]struct{}
}

func (b triggerBatch) empty() bool {
	return !b.scan && !b.hasScopes()
}

func (b triggerBatch) hasScopes() bool {
	return len(b.dirtyNodes) != 0 || len(b.dirtyShards) != 0 || len(b.dirtyColls) != 0
}

// parseShardCollection extracts the collection ID encoded in a shard vchannel.
// Planning falls back to the replica index when the channel cannot be parsed.
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
		retryDelay:  100 * time.Millisecond,
	}
}

func (q *triggerQueue) add(scopes ...TriggerScope) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(scopes) == 0 {
		q.scan = true
		q.notifyLocked()
		return
	}

	for _, scope := range scopes {
		if scope.NodeChanged && len(scope.DirtyNodes) == 0 {
			q.scan = true
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
		scan:        q.scan,
		dirtyNodes:  q.dirtyNodes,
		dirtyShards: q.dirtyShards,
		dirtyColls:  q.dirtyColls,
	}

	q.scan = false
	q.dirtyNodes = make(map[int64]struct{})
	q.dirtyShards = make(map[qviews.ShardID]struct{})
	q.dirtyColls = make(map[int64]struct{})
	// Fresh publications take precedence over failed work. Retries retain
	// their own deadline, so neither backoff nor scan continuation blocks APIs.
	if !pending.hasScopes() && q.retries.hasScopes() && !time.Now().Before(q.retryAt) {
		pending.dirtyShards, pending.dirtyColls = q.retries.dirtyShards, q.retries.dirtyColls
		q.retries = triggerBatch{}
		q.retryDelay = min(5*time.Second, 2*q.retryDelay)
	}
	return pending
}

func (q *triggerQueue) addRetry(scope TriggerScope) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.retries.hasScopes() {
		q.retryAt = time.Now().Add(q.retryDelay)
		q.retries.dirtyShards = make(map[qviews.ShardID]struct{})
		q.retries.dirtyColls = make(map[int64]struct{})
	}
	for _, id := range scope.DirtyShards {
		q.retries.dirtyShards[id] = struct{}{}
	}
	for _, id := range scope.DirtyCollections {
		q.retries.dirtyColls[id] = struct{}{}
	}
	q.notifyLocked()
}

func (q *triggerQueue) retryWait() (time.Duration, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return max(0, time.Until(q.retryAt)), q.retries.hasScopes()
}

func (q *triggerQueue) resetRetryDelay() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.retries.hasScopes() {
		q.retryDelay = 100 * time.Millisecond
	}
}

func (q *triggerQueue) wake() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.notifyLocked()
}
