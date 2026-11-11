// Package shardstats owns planning access statistics for configured replicas.
package shardstats

import (
	"slices"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

const retention = 2 * time.Minute

// ReplicaStatsStore follows current load intent, independently of View state.
type ReplicaStatsStore struct {
	stop        chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
	mu          sync.RWMutex
	collections map[int64][]int64
	replicas    map[int64]*replicaStats
	unregister  func()
}

type replicaStats struct {
	collectionID int64
	shards       map[qviews.ShardID][]shardWindow
}

type shardWindow struct {
	expiresAt int64
	sample    qviews.ShardStats
}

// New replays configured replicas before the caller starts receiving reports.
func New(configs *loadmgr.LoadConfigStore) *ReplicaStatsStore {
	s := &ReplicaStatsStore{collections: make(map[int64][]int64), replicas: make(map[int64]*replicaStats), stop: make(chan struct{}), done: make(chan struct{})}
	s.unregister = configs.RegisterLoadConfigListener(s.onLoadConfig)
	go s.expireLoop()
	return s
}

func (s *ReplicaStatsStore) onLoadConfig(collectionID int64, config *loadmgr.LoadConfig, _ uint64) {
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
				s.replicas[id] = &replicaStats{collectionID: collectionID, shards: make(map[qviews.ShardID][]shardWindow)}
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

// OnShardStats accepts best-effort windows only for currently configured replicas.
func (s *ReplicaStatsStore) OnShardStats(window qviews.ShardStatsWindow) {
	now := time.Now().UnixNano()
	// Legacy cumulative reports have no window. Future node clocks must not
	// retain a window longer than two minutes after local receipt.
	expiresAt := min(window.EndUnixNanos, now) + int64(retention)
	if window.StartUnixNanos <= 0 || window.EndUnixNanos <= window.StartUnixNanos || window.EndUnixNanos-window.StartUnixNanos > int64(retention) || expiresAt <= now {
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
		replica.shards[id] = append(replica.shards[id], shardWindow{
			expiresAt: expiresAt, sample: sample,
		})
	}
}

// GetShardStats sums unexpired received windows; missing windows are not zeros.
// Expiration is checked here as well as by the independent cleanup worker.
func (s *ReplicaStatsStore) GetShardStats(id qviews.ShardID) (qviews.ShardStats, bool) {
	now := time.Now().UnixNano()
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result qviews.ShardStats
	found := false
	if replica := s.replicas[id.ReplicaID]; replica != nil {
		for _, window := range replica.shards[id] {
			if window.expiresAt > now {
				result.ShardID = id
				result.RequestCount += window.sample.RequestCount
				result.LastAccessUnixNanos = max(result.LastAccessUnixNanos, window.sample.LastAccessUnixNanos)
				found = true
			}
		}
	}
	return result, found
}

func (s *ReplicaStatsStore) expireLoop() {
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

func (s *ReplicaStatsStore) expire(now int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, replica := range s.replicas {
		for id, windows := range replica.shards {
			windows = slices.DeleteFunc(windows, func(w shardWindow) bool { return w.expiresAt <= now })
			if len(windows) == 0 {
				delete(replica.shards, id)
			} else {
				replica.shards[id] = windows
			}
		}
	}
}

// Close follows Syncer shutdown and joins the expiration worker.
func (s *ReplicaStatsStore) Close() {
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
