// Package viewfeedback owns planning access feedback for configured replicas.
package viewfeedback

import (
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/views/coord/balancer/api"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

const retention = 2 * time.Minute

var rateWindowCounts = [...]int{2, 6, 12, 24}

// Snapshot contains replica-wide feedback. RequestRates is the mean over all shards.
// It is published copy-on-write. Callers must not modify it or its fields.
type Snapshot struct {
	LastAccessUnixNanos int64
	RequestRates        RequestRates
}

// RequestRates uses the actual Node time span of up to N most recent windows.
// Values are planning attempts per second, not successful queries or NQ.
type RequestRates struct {
	Last2Windows  float64
	Last6Windows  float64
	Last12Windows float64
	Last24Windows float64
}

// ReplicaFeedbackStore follows current load intent, independently of View state.
type ReplicaFeedbackStore struct {
	stop                chan struct{}
	done                chan struct{}
	closeOnce           sync.Once
	mu                  sync.RWMutex
	collections         map[int64][]int64
	replicas            map[int64]*replicaFeedback
	shardCounts         map[int64]int
	unregister          func()
	unregisterDataViews func()
}

type replicaFeedback struct {
	collectionID        int64
	lastAccessUnixNanos int64
	snapshot            *Snapshot
	shards              map[qviews.ShardID]*shardFeedback
}

type shardFeedback struct {
	rates   [4]float64
	windows []feedbackContribution // Sorted by the Node's report time.
	recent  [4]windowAggregate
}

type windowAggregate struct {
	requestCount uint64
	startedAt    int64
}

// Counts are reverted; LastAccess is a lifetime high-water mark.
type feedbackContribution struct {
	startedAt    int64
	reportedAt   int64
	requestCount uint64
}

// New replays configured replicas before the caller starts receiving reports.
func New(configs *loadmgr.LoadConfigStore, dataViews api.DataViewPublisher) *ReplicaFeedbackStore {
	s := &ReplicaFeedbackStore{collections: make(map[int64][]int64), replicas: make(map[int64]*replicaFeedback), shardCounts: make(map[int64]int), stop: make(chan struct{}), done: make(chan struct{})}
	s.unregisterDataViews = dataViews.RegisterDataViewListener(s.onDataView)
	s.unregister = configs.RegisterLoadConfigListener(s.onLoadConfig)
	go s.expireLoop()
	return s
}

// DataView supplies the total shard count, including shards with no reports.
// The listener never calls back into either metadata publisher.
func (s *ReplicaFeedbackStore) onDataView(collectionID int64, view *api.CollectionDataView) {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	if view != nil {
		count = len(view.Shards)
	}
	if count == s.shardCounts[collectionID] {
		return
	}
	if count == 0 {
		delete(s.shardCounts, collectionID)
	} else {
		s.shardCounts[collectionID] = count
	}
	for _, id := range s.collections[collectionID] {
		s.replicas[id].publishSnapshot(count)
	}
}

func (s *ReplicaFeedbackStore) onLoadConfig(collectionID int64, config *loadmgr.LoadConfig, _ uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []int64
	kept := make(map[int64]struct{})
	if config != nil {
		for _, replica := range config.Replicas {
			id := replica.ReplicaID
			ids = append(ids, id)
			kept[id] = struct{}{}
			if s.replicas[id] == nil {
				s.replicas[id] = &replicaFeedback{collectionID: collectionID, shards: make(map[qviews.ShardID]*shardFeedback)}
			}
		}
	}
	for _, id := range s.collections[collectionID] {
		if _, ok := kept[id]; !ok {
			delete(s.replicas, id)
		}
	}
	if len(ids) == 0 {
		delete(s.collections, collectionID)
	} else {
		s.collections[collectionID] = ids
	}
}

// OnViewFeedback accepts best-effort windows only for currently configured replicas.
func (s *ReplicaFeedbackStore) OnViewFeedback(window qviews.ViewFeedbackWindow) {
	// Use the Node's window end as report time, without a receipt timestamp.
	if window.StartUnixNanos <= 0 || window.EndUnixNanos <= window.StartUnixNanos || window.EndUnixNanos-window.StartUnixNanos > int64(retention) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := make(map[*replicaFeedback]struct{})
	for id, sample := range window.Shards {
		replica := s.replicas[id.ReplicaID]
		if replica == nil {
			continue
		}
		_, collectionID, _, err := funcutil.ParseVChannel(id.VChannel)
		if err != nil || collectionID != replica.collectionID {
			continue
		}
		shard := replica.shards[id]
		if shard == nil {
			shard = &shardFeedback{}
			replica.shards[id] = shard
		}
		// Reports can arrive out of order across stream reconnections.
		index := sort.Search(len(shard.windows), func(i int) bool {
			return shard.windows[i].reportedAt > window.EndUnixNanos
		})
		shard.windows = slices.Insert(shard.windows, index, feedbackContribution{
			startedAt: window.StartUnixNanos, reportedAt: window.EndUnixNanos, requestCount: sample.RequestCount,
		})
		for i, n := range rateWindowCounts {
			start := max(0, len(shard.windows)-n)
			if index >= start {
				shard.recent[i].requestCount += sample.RequestCount
				if len(shard.windows) > n {
					// The inserted window replaces the oldest member of this group.
					old := shard.windows[start-1]
					shard.recent[i].requestCount -= old.requestCount
				}
			}
		}
		replica.lastAccessUnixNanos = max(replica.lastAccessUnixNanos, sample.LastAccessUnixNanos)
		shard.updateRates()
		changed[replica] = struct{}{}
	}
	for replica := range changed {
		replica.publishSnapshot(s.shardCounts[replica.collectionID])
	}
}

// GetViewFeedback returns the published immutable snapshot pointer without copying.
// Expiration is applied by the cleanup worker, including when there are no reads.
func (s *ReplicaFeedbackStore) GetViewFeedback(replicaID int64) (*Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if replica := s.replicas[replicaID]; replica != nil && replica.snapshot != nil {
		return replica.snapshot, true
	}
	return nil, false
}

// Called under the write lock, once per affected replica per report or sweep.
// Missing shards contribute zero; the divisor comes from DataView, not reports.
func (r *replicaFeedback) publishSnapshot(shardCount int) {
	if shardCount == 0 || len(r.shards) == 0 {
		r.snapshot = nil
		return
	}
	var rates [4]float64
	for _, shard := range r.shards {
		for i, rate := range shard.rates {
			rates[i] += rate
		}
	}
	for i := range rates {
		rates[i] /= float64(shardCount)
	}
	r.snapshot = &Snapshot{
		LastAccessUnixNanos: r.lastAccessUnixNanos,
		RequestRates: RequestRates{
			Last2Windows: rates[0], Last6Windows: rates[1],
			Last12Windows: rates[2], Last24Windows: rates[3],
		},
	}
}

func (s *shardFeedback) updateRates() {
	s.refreshRateStarts()
	for i := range s.rates {
		s.rates[i] = s.requestRate(i)
	}
}

func (s *shardFeedback) requestRate(index int) float64 {
	if len(s.windows) == 0 {
		return 0
	}
	aggregate := s.recent[index]
	span := s.windows[len(s.windows)-1].reportedAt - aggregate.startedAt
	return float64(aggregate.requestCount) / time.Duration(span).Seconds()
}

// Refresh only the bounded recent groups, never the full retained history.
// Start times need not be ordered when reports overlap across owner changes.
func (s *shardFeedback) refreshRateStarts() {
	for i, n := range rateWindowCounts {
		start := max(0, len(s.windows)-n)
		s.recent[i].startedAt = 0
		for _, window := range s.windows[start:] {
			if s.recent[i].startedAt == 0 || window.startedAt < s.recent[i].startedAt {
				s.recent[i].startedAt = window.startedAt
			}
		}
	}
}

func (s *ReplicaFeedbackStore) expireLoop() {
	defer close(s.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case now := <-ticker.C:
			s.expire(now.UnixNano())
		}
	}
}

func (s *ReplicaFeedbackStore) expire(now int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := now - int64(retention)
	for _, replica := range s.replicas {
		changed := false
		for _, shard := range replica.shards {
			n := 0
			for n < len(shard.windows) && shard.windows[n].reportedAt <= cutoff {
				n++
			}
			for i, count := range rateWindowCounts {
				for j := max(0, len(shard.windows)-count); j < n; j++ {
					old := shard.windows[j]
					shard.recent[i].requestCount -= old.requestCount
				}
			}
			if n == len(shard.windows) {
				shard.windows = nil
			} else {
				shard.windows = shard.windows[n:]
			}
			if n > 0 {
				shard.updateRates()
				changed = true
			}
		}
		if changed {
			replica.publishSnapshot(s.shardCounts[replica.collectionID])
		}
	}
}

// Close follows Syncer shutdown and joins the expiration worker.
func (s *ReplicaFeedbackStore) Close() {
	s.closeOnce.Do(func() {
		s.unregister()
		s.unregisterDataViews()
		close(s.stop)
		<-s.done
		s.mu.Lock()
		clear(s.collections)
		clear(s.shardCounts)
		clear(s.replicas)
		s.mu.Unlock()
	})
}
