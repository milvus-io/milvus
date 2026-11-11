// Package viewfeedback owns planning access feedback for configured replicas.
package viewfeedback

import (
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

const retention = 2 * time.Minute

var rateWindowCounts = [...]int{2, 6, 12, 24}

// Snapshot combines the retained count and lifetime last access with recent rates.
type Snapshot struct {
	qviews.ViewFeedback
	RequestRates RequestRates
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
	stop        chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
	mu          sync.RWMutex
	collections map[int64][]int64
	replicas    map[int64]*replicaFeedback
	unregister  func()
}

type replicaFeedback struct {
	collectionID int64
	shards       map[qviews.ShardID]*shardFeedback
}

type shardFeedback struct {
	qviews.ViewFeedback
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
func New(configs *loadmgr.LoadConfigStore) *ReplicaFeedbackStore {
	s := &ReplicaFeedbackStore{collections: make(map[int64][]int64), replicas: make(map[int64]*replicaFeedback), stop: make(chan struct{}), done: make(chan struct{})}
	s.unregister = configs.RegisterLoadConfigListener(s.onLoadConfig)
	go s.expireLoop()
	return s
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
			shard = &shardFeedback{ViewFeedback: qviews.ViewFeedback{ShardID: id}}
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
		shard.refreshRateStarts()
		shard.RequestCount += sample.RequestCount
		shard.LastAccessUnixNanos = max(shard.LastAccessUnixNanos, sample.LastAccessUnixNanos)
	}
}

// GetViewFeedback returns the cached aggregate without scanning windows.
// Expiration is applied by the cleanup worker, including when there are no reads.
func (s *ReplicaFeedbackStore) GetViewFeedback(id qviews.ShardID) (Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if replica := s.replicas[id.ReplicaID]; replica != nil {
		if shard := replica.shards[id]; shard != nil {
			return Snapshot{ViewFeedback: shard.ViewFeedback, RequestRates: RequestRates{
				Last2Windows: shard.requestRate(0), Last6Windows: shard.requestRate(1),
				Last12Windows: shard.requestRate(2), Last24Windows: shard.requestRate(3),
			}}, true
		}
	}
	return Snapshot{}, false
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
		for _, shard := range replica.shards {
			n := 0
			for n < len(shard.windows) && shard.windows[n].reportedAt <= cutoff {
				shard.RequestCount -= shard.windows[n].requestCount
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
				shard.refreshRateStarts()
			}
		}
	}
}

// Close follows Syncer shutdown and joins the expiration worker.
func (s *ReplicaFeedbackStore) Close() {
	s.closeOnce.Do(func() {
		s.unregister()
		close(s.stop)
		<-s.done
		s.mu.Lock()
		clear(s.collections)
		clear(s.replicas)
		s.mu.Unlock()
	})
}
