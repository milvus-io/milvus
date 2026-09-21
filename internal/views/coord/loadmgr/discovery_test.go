package loadmgr

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestManagedDiscoverySuspendRestoreAndRejectStaleCallbacks(t *testing.T) {
	cfg := &LoadConfig{CollectionID: 1, Replicas: []*ReplicaAssignment{{ReplicaID: 10}, {ReplicaID: 11}}}
	store := &LoadConfigStore{configs: map[int64]*LoadConfig{1: cfg}, versions: map[int64]uint64{1: 1}}
	m := NewCollectionLoadManager(store, nil)
	a := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	b := qviews.ShardID{ReplicaID: 11, VChannel: a.VChannel}
	m.ObserveShardUp(a)
	m.ObserveShardUp(b)
	updates := 0
	m.SetShardAssignmentNotifier(func() { updates++; m.ShardAssignmentsByPChannel() })
	update := CollectionDiscoveryUpdate{CollectionID: 1, ConfigVersion: 1, Revision: 1, Shards: []qviews.ShardID{a}}
	require.True(t, m.UpdateCollectionDiscovery(update))
	require.Len(t, m.ShardAssignmentsByPChannel()["by-dev-rootcoord-dml_0"], 1)
	m.ObserveShardUp(b)
	require.False(t, m.MarkShardDiscoverable(b))
	require.Equal(t, 1, updates)
	require.False(t, m.UpdateCollectionDiscovery(update))
	update.Revision = 2
	update.Shards = append(update.Shards, b)
	require.True(t, m.UpdateCollectionDiscovery(update))
	require.Len(t, m.ShardAssignmentsByPChannel()["by-dev-rootcoord-dml_0"], 2)
	require.Equal(t, 2, updates)
	update.Revision = 3
	update.ConfigVersion = 999
	require.False(t, m.UpdateCollectionDiscovery(update))
	update.ConfigVersion = 1
	update.Shards = []qviews.ShardID{{ReplicaID: 10, VChannel: "bad"}}
	require.False(t, m.UpdateCollectionDiscovery(update))
	update.Shards = []qviews.ShardID{{ReplicaID: 12, VChannel: a.VChannel}}
	require.False(t, m.UpdateCollectionDiscovery(update))
	update.Shards = []qviews.ShardID{{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_2v0"}}
	require.False(t, m.UpdateCollectionDiscovery(update))
	update.Shards = nil
	require.True(t, m.UpdateCollectionDiscovery(update))
	require.Empty(t, m.ShardAssignmentsByPChannel())
	store.mu.Lock()
	delete(store.configs, 1)
	store.mu.Unlock()
	update.Revision = 4
	update.Shards = []qviews.ShardID{a}
	require.False(t, m.UpdateCollectionDiscovery(update))
	update.Shards = nil
	require.True(t, m.UpdateCollectionDiscovery(update))
	require.Len(t, cfg.Replicas, 2, "suspension never changes desired config")
}

func TestManagedDiscoveryConcurrentUpCallbacks(t *testing.T) {
	cfg := &LoadConfig{CollectionID: 1, Replicas: []*ReplicaAssignment{{ReplicaID: 10}}}
	m := NewCollectionLoadManager(&LoadConfigStore{configs: map[int64]*LoadConfig{1: cfg}, versions: map[int64]uint64{1: 1}}, nil)
	id := qviews.ShardID{ReplicaID: 10, VChannel: "by-dev-rootcoord-dml_0_1v0"}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.ObserveShardUp(id)
				m.ShardAssignmentsByPChannel()
			}
		}()
	}
	require.True(t, m.UpdateCollectionDiscovery(CollectionDiscoveryUpdate{CollectionID: 1, ConfigVersion: 1, Revision: 1}))
	wg.Wait()
	require.Empty(t, m.ShardAssignmentsByPChannel())
}
