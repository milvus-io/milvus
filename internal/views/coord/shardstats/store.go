// Package shardstats owns planning access statistics for configured replicas.
package shardstats

import (
	"sync"

	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// ReplicaStatsStore follows current load intent, independently of View state.
type ReplicaStatsStore struct {
	mu          sync.RWMutex
	collections map[int64][]int64
	replicas    map[int64]*replicaStats
	unregister  func()
}

type replicaStats struct {
	collectionID int64
	shards       map[qviews.ShardID]qviews.ShardStats
}

// New replays configured replicas before the caller starts receiving reports.
func New(configs *loadmgr.LoadConfigStore) *ReplicaStatsStore {
	s := &ReplicaStatsStore{collections: make(map[int64][]int64), replicas: make(map[int64]*replicaStats)}
	s.unregister = configs.RegisterLoadConfigListener(s.onLoadConfig)
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
				s.replicas[id] = &replicaStats{collectionID: collectionID, shards: make(map[qviews.ShardID]qviews.ShardStats)}
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

// OnShardStats overwrites counters rather than adding full snapshots together.
// Reports cannot create a replica after its load intent has been withdrawn.
func (s *ReplicaStatsStore) OnShardStats(samples []qviews.ShardStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sample := range samples {
		replica := s.replicas[sample.ShardID.ReplicaID]
		if replica == nil {
			continue
		}
		_, collectionID, _, err := funcutil.ParseVChannel(sample.ShardID.VChannel)
		if err != nil || collectionID != replica.collectionID {
			continue
		}
		sample.LastAccessUnixNanos = max(sample.LastAccessUnixNanos, replica.shards[sample.ShardID].LastAccessUnixNanos)
		replica.shards[sample.ShardID] = sample
	}
}

func (s *ReplicaStatsStore) GetShardStats(id qviews.ShardID) (qviews.ShardStats, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if replica := s.replicas[id.ReplicaID]; replica != nil {
		sample, ok := replica.shards[id]
		return sample, ok
	}
	return qviews.ShardStats{}, false
}

// Close follows Syncer shutdown; unregister before acquiring the stats lock.
func (s *ReplicaStatsStore) Close() {
	s.unregister()
	s.mu.Lock()
	clear(s.collections)
	clear(s.replicas)
	s.mu.Unlock()
}
