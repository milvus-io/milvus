package balancer

import (
	"container/list"
	"io"
	"iter"
	"slices"
	"sync"
	"sync/atomic"

	balancercache "github.com/milvus-io/milvus/internal/views/coord/balancer/cache"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

const (
	discoveryShardLimit = 100
	scanSlotBudget      = 256
)

type scopeKey struct {
	kind    string
	id      int64
	channel string
}

func discoveryKey(scope TriggerScope) (scopeKey, bool) {
	switch {
	case scope.NodeChanged && len(scope.DirtyNodes) == 0:
		return scopeKey{kind: "scan"}, true
	case len(scope.DirtyCollections) == 1:
		return scopeKey{kind: "collection", id: scope.DirtyCollections[0]}, true
	case len(scope.DirtyShards) == 1:
		id := scope.DirtyShards[0]
		return scopeKey{kind: "shard", id: id.ReplicaID, channel: id.VChannel}, true
	case len(scope.DirtyResourceGroups) == 1:
		return scopeKey{kind: "group", channel: scope.DirtyResourceGroups[0]}, true
	case len(scope.DirtyNodes) == 1:
		return scopeKey{kind: "node", id: scope.DirtyNodes[0]}, true
	}
	return scopeKey{}, false
}

type discoveryScope struct {
	scope     TriggerScope
	skip      map[qviews.ShardID]struct{}
	active    bool
	repeat    bool
	remaining func() (discoverySlice, bool)
	stop      func()
}

type discoverySlice struct {
	ids  []qviews.ShardID
	last bool
}

// Local changes, topology changes and the periodic scan have separate producers.
// Discovery returns the queue that owns a scope so it resumes on the same input.
type discoverySource int

const (
	localChanges discoverySource = iota
	topologyChanges
	periodicScan
	// The local producer serves both localChanges (collections) and localShards.
	localShards
	queueCount
	producerCount                 = periodicScan + 1
	anySource     discoverySource = -1
)

type scopeQueue struct {
	fresh, resumed list.List
	resumeNext     bool
	signal         chan struct{}
}

type discovery struct {
	mu              sync.Mutex
	queues          [queueCount]scopeQueue
	shardsNext      bool
	pending         map[scopeKey]*discoveryScope
	nextSync        discoverySource
	refreshingNodes atomic.Int64 // node walks with target accounting still in progress
}

func newDiscovery() *discovery {
	d := &discovery{pending: make(map[scopeKey]*discoveryScope)}
	for i := range producerCount {
		d.queues[i].signal = make(chan struct{}, 1)
	}
	d.queues[localShards].signal = d.queues[localChanges].signal
	return d
}

func (d *discovery) add(source discoverySource, scope TriggerScope) {
	d.mu.Lock()
	defer d.mu.Unlock()
	job := &discoveryScope{scope: scope}
	if key, ok := discoveryKey(scope); ok {
		if existing := d.pending[key]; existing != nil {
			existing.repeat = existing.active
			return
		}
		d.pending[key] = job
	}
	if len(scope.DirtyNodes) > 0 {
		d.refreshingNodes.Add(1)
	}
	q := &d.queues[source]
	q.fresh.PushBack(job)
	wakeBalancer(q.signal)
}

// All shards in the selected batch already saw the new layout.
func (d *discovery) layout(collections []int64, selected []qviews.ShardID) {
	skip := make(map[qviews.ShardID]struct{}, len(selected))
	for _, id := range selected {
		skip[id] = struct{}{}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	q := &d.queues[localChanges]
	q.fresh.PushBack(&discoveryScope{scope: TriggerScope{DirtyCollections: collections}, skip: skip})
	wakeBalancer(q.signal)
}

// When called synchronously before Start, rotate over the same source queues.
func (d *discovery) next(source discoverySource) (*discoveryScope, discoverySource) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if source != anySource {
		return d.take(source)
	}
	for range producerCount {
		source = d.nextSync
		d.nextSync = (source + 1) % producerCount
		if job, queue := d.take(source); job != nil {
			return job, queue
		}
	}
	return nil, anySource
}

func (d *discovery) take(source discoverySource) (*discoveryScope, discoverySource) {
	if source == localChanges {
		collections, shards := &d.queues[localChanges], &d.queues[localShards]
		if !shards.empty() && (d.shardsNext || collections.empty()) {
			source = localShards
			d.shardsNext = false
		} else if !collections.empty() {
			d.shardsNext = true
		}
	}
	return d.queues[source].take(), source
}

func (q *scopeQueue) empty() bool {
	return q.fresh.Len() == 0 && q.resumed.Len() == 0
}

func (q *scopeQueue) take() *discoveryScope {
	if q.resumed.Len() > 0 && (q.resumeNext || q.fresh.Len() == 0) {
		q.resumeNext = false
		return q.resumed.Remove(q.resumed.Front()).(*discoveryScope)
	}
	if q.fresh.Len() == 0 {
		return nil
	}
	job := q.fresh.Remove(q.fresh.Front()).(*discoveryScope)
	job.active = true
	q.resumeNext = q.resumed.Len() > 0
	return job
}

func (d *discovery) finish(source discoverySource, job *discoveryScope) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if job.repeat {
		d.queues[source].restart(job)
		return
	}
	if len(job.scope.DirtyNodes) > 0 {
		d.refreshingNodes.Add(-1)
	}
	if key, ok := discoveryKey(job.scope); ok && job.skip == nil {
		delete(d.pending, key)
	}
}

// The caller holds d.mu and has stopped any active iterator.
func (q *scopeQueue) restart(job *discoveryScope) {
	job.remaining, job.stop = nil, nil
	job.active, job.repeat = false, false
	q.fresh.PushBack(job)
}

// Release suspended iterator stacks on Stop. A restart rediscovers unfinished
// scopes against current cache state.
func (d *discovery) suspend() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.queues {
		q := &d.queues[i]
		for q.resumed.Len() > 0 {
			job := q.resumed.Remove(q.resumed.Front()).(*discoveryScope)
			job.stop()
			q.restart(job)
			wakeBalancer(q.signal)
		}
	}
}

// The scanner and radix indexes pin immutable roots. No cache lock survives a
// yield, including when a slice ends in the middle of a replica's channels.
func (b *DefaultBalancer) discover(scope TriggerScope) iter.Seq[discoverySlice] {
	return func(yield func(discoverySlice) bool) {
		walk := scopeWalk{balancer: b, yield: yield}
		if walk.visitScope(scope) {
			yield(discoverySlice{ids: walk.ids, last: true})
		}
	}
}

type scopeWalk struct {
	balancer *DefaultBalancer
	yield    func(discoverySlice) bool
	ids      []qviews.ShardID
	steps    int
}

func (w *scopeWalk) flush() bool {
	keepGoing := w.yield(discoverySlice{ids: w.ids})
	w.ids = nil
	w.steps = 0
	return keepGoing
}

func (w *scopeWalk) step() bool {
	w.steps++
	return w.steps < scanSlotBudget || w.flush()
}

func (w *scopeWalk) emit(id qviews.ShardID) bool {
	w.ids = append(w.ids, id)
	return len(w.ids) < discoveryShardLimit || w.flush()
}

func (w *scopeWalk) visitCollection(id int64) bool {
	c := w.balancer.cache.GetCollection(id)
	if c == nil || c.LoadConfig() == nil {
		return w.step()
	}
	cfg, data := c.LoadConfig(), c.DataView()
	if data != nil {
		for _, replica := range cfg.Replicas {
			for _, shard := range data.Shards {
				if !w.emit(qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: shard.VChannel}) {
					return false
				}
			}
		}
	}
	for resident := range c.RangeShards {
		shard := resident.ID()
		if data != nil && data.Shard(shard.VChannel) != nil && findReplica(cfg, shard.ReplicaID) != nil {
			if !w.step() {
				return false
			}
		} else if !w.emit(shard) {
			return false
		}
	}
	return w.step()
}

func (w *scopeWalk) visitScope(scope TriggerScope) bool {
	for _, id := range scope.DirtyShards {
		if !w.emit(id) {
			return false
		}
	}
	for _, id := range scope.DirtyCollections {
		if !w.visitCollection(id) {
			return false
		}
	}
	for _, id := range scope.DirtyNodes {
		if node := w.balancer.cache.GetNode(id); node != nil {
			for shard := range node.RangeShards {
				w.balancer.cache.RefreshShardTarget(shard)
				if !w.emit(shard) {
					return false
				}
			}
		}
		if !w.step() {
			return false
		}
	}
	for _, name := range scope.DirtyResourceGroups {
		if group := w.balancer.cache.GetResourceGroup(name); group != nil {
			for id := range group.RangeCollections {
				if !w.visitCollection(id) {
					return false
				}
			}
		}
		if !w.step() {
			return false
		}
	}
	if !scope.NodeChanged || len(scope.DirtyNodes) > 0 {
		return true
	}
	scanner := w.balancer.cache.NewScanner()
	for {
		ok := true
		err := scanner.Scan(scanSlotBudget, func(c *balancercache.CollectionEntry) bool {
			ok = w.visitCollection(c.ID())
			return ok
		})
		if !ok || err == io.EOF {
			return ok
		}
		if !w.flush() {
			return false
		}
	}
}

// dispatch advances one producer slice, independent of an in-flight placement.
func (b *DefaultBalancer) dispatch(source discoverySource) int {
	job, source := b.discovery.next(source)
	if job == nil {
		return -1
	}
	slice := b.nextSlice(job)
	if job.skip != nil {
		slice.ids = slices.DeleteFunc(slice.ids, func(id qviews.ShardID) bool {
			_, seen := job.skip[id]
			return seen
		})
	}
	b.submitShards(slice.ids)
	if slice.last {
		b.discovery.finish(source, job)
	} else {
		b.discovery.mu.Lock()
		b.discovery.queues[source].resumed.PushBack(job)
		b.discovery.mu.Unlock()
	}
	wakeBalancer(b.discovery.queues[source].signal)
	return len(slice.ids)
}

func (b *DefaultBalancer) nextSlice(job *discoveryScope) discoverySlice {
	if ids := job.scope.DirtyShards; len(ids) > 0 && len(ids) <= discoveryShardLimit {
		return discoverySlice{ids: ids, last: true}
	}
	if job.remaining == nil {
		job.remaining, job.stop = iter.Pull(b.discover(job.scope))
	}
	// discover yields a final slice even for an empty scope.
	slice, _ := job.remaining()
	if slice.last {
		job.stop()
	}
	return slice
}
