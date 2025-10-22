// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestShardClientMgr(t *testing.T) {
	ctx := context.Background()
	nodeInfo := NodeInfo{
		NodeID: 1,
	}

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Close().Return(nil)
	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}

	mixcoord := mocks.NewMockMixCoordClient(t)

	mgr := NewShardClientMgr(mixcoord)
	mgr.SetClientCreatorFunc(creator)
	_, err := mgr.GetClient(ctx, nodeInfo)
	assert.Nil(t, err)

	mgr.Close()
	assert.Equal(t, mgr.clients.Len(), 0)
}

func TestShardClient(t *testing.T) {
	nodeInfo := NodeInfo{
		NodeID: 1,
	}

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Close().Return(nil).Maybe()
	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}
	shardClient := newShardClient(nodeInfo, creator, 3*time.Second)
	assert.Equal(t, len(shardClient.clients), 0)
	assert.Equal(t, false, shardClient.initialized.Load())
	assert.Equal(t, false, shardClient.isClosed)

	ctx := context.Background()
	_, err := shardClient.getClient(ctx)
	assert.Nil(t, err)
	assert.Equal(t, len(shardClient.clients), paramtable.Get().ProxyCfg.QueryNodePoolingSize.GetAsInt())

	// test close
	closed := shardClient.Close(false)
	assert.False(t, closed)
	closed = shardClient.Close(true)
	assert.True(t, closed)
}

func TestPurgeClient(t *testing.T) {
	node := NodeInfo{
		NodeID: 1,
	}

	returnEmptyResult := atomic.NewBool(false)

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Close().Return(nil).Maybe()
	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}

	s := &shardClientMgrImpl{
		clients:         typeutil.NewConcurrentMap[UniqueID, *shardClient](),
		clientCreator:   creator,
		closeCh:         make(chan struct{}),
		purgeInterval:   1 * time.Second,
		expiredDuration: 3 * time.Second,

		collLeader: map[string]map[string]*shardLeaders{
			"default": {
				"test": {
					idx:          atomic.NewInt64(0),
					collectionID: 1,
					shardLeaders: map[string][]NodeInfo{
						"0": {node},
					},
				},
			},
		},
	}

	go s.PurgeClient()
	defer s.Close()
	_, err := s.GetClient(context.Background(), node)
	assert.Nil(t, err)
	qnClient, ok := s.clients.Get(1)
	assert.True(t, ok)
	assert.True(t, qnClient.lastActiveTs.Load() > 0)

	time.Sleep(2 * time.Second)
	// expected client should not been purged before expiredDuration
	assert.Equal(t, s.clients.Len(), 1)
	assert.True(t, time.Now().UnixNano()-qnClient.lastActiveTs.Load() >= 2*time.Second.Nanoseconds())

	_, err = s.GetClient(context.Background(), node)
	assert.Nil(t, err)
	time.Sleep(2 * time.Second)
	// GetClient should refresh lastActiveTs, expected client should not be purged
	assert.Equal(t, s.clients.Len(), 1)
	assert.True(t, time.Now().UnixNano()-qnClient.lastActiveTs.Load() < 3*time.Second.Nanoseconds())

	time.Sleep(2 * time.Second)
	// client reach the expiredDuration, expected client should not be purged
	assert.Equal(t, s.clients.Len(), 1)
	assert.True(t, time.Now().UnixNano()-qnClient.lastActiveTs.Load() > 3*time.Second.Nanoseconds())

	s.DeprecateShardCache("default", "test")
	returnEmptyResult.Store(true)
	time.Sleep(2 * time.Second)
	// remove client from shard location, expected client should be purged
	assert.Eventually(t, func() bool {
		return s.clients.Len() == 0
	}, 10*time.Second, 1*time.Second)
}

func TestDeprecateShardCache(t *testing.T) {
	node := NodeInfo{
		NodeID: 1,
	}

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Close().Return(nil).Maybe()
	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}

	mixcoord := mocks.NewMockMixCoordClient(t)
	mgr := NewShardClientMgr(mixcoord)
	mgr.SetClientCreatorFunc(creator)

	t.Run("Clear with no collection info", func(t *testing.T) {
		mgr.DeprecateShardCache("default", "collection_not_exist")
		// Should not panic or error
	})

	t.Run("Clear valid collection empty cache", func(t *testing.T) {
		// Add a collection to cache first
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]map[string]*shardLeaders{
			"default": {
				"test_collection": {
					idx:          atomic.NewInt64(0),
					collectionID: 100,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
			},
		}
		mgr.leaderMut.Unlock()

		mgr.DeprecateShardCache("default", "test_collection")

		// Verify cache is cleared
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["default"]["test_collection"]
		assert.False(t, exists)
	})

	t.Run("Clear one collection, keep others", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]map[string]*shardLeaders{
			"default": {
				"collection1": {
					idx:          atomic.NewInt64(0),
					collectionID: 100,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
				"collection2": {
					idx:          atomic.NewInt64(0),
					collectionID: 101,
					shardLeaders: map[string][]NodeInfo{
						"channel-2": {node},
					},
				},
			},
		}
		mgr.leaderMut.Unlock()

		mgr.DeprecateShardCache("default", "collection1")

		// Verify collection1 is cleared but collection2 remains
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["default"]["collection1"]
		assert.False(t, exists)
		_, exists = mgr.collLeader["default"]["collection2"]
		assert.True(t, exists)
	})

	t.Run("Clear last collection in database removes database", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]map[string]*shardLeaders{
			"test_db": {
				"last_collection": {
					idx:          atomic.NewInt64(0),
					collectionID: 200,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
			},
		}
		mgr.leaderMut.Unlock()

		mgr.DeprecateShardCache("test_db", "last_collection")

		// Verify database is also removed
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["test_db"]
		assert.False(t, exists)
	})

	mgr.Close()
}

func TestInvalidateShardLeaderCache(t *testing.T) {
	node := NodeInfo{
		NodeID: 1,
	}

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Close().Return(nil).Maybe()
	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}

	mixcoord := mocks.NewMockMixCoordClient(t)
	mgr := NewShardClientMgr(mixcoord)
	mgr.SetClientCreatorFunc(creator)

	t.Run("Invalidate single collection", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]map[string]*shardLeaders{
			"default": {
				"collection1": {
					idx:          atomic.NewInt64(0),
					collectionID: 100,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
				"collection2": {
					idx:          atomic.NewInt64(0),
					collectionID: 101,
					shardLeaders: map[string][]NodeInfo{
						"channel-2": {node},
					},
				},
			},
		}
		mgr.leaderMut.Unlock()

		mgr.InvalidateShardLeaderCache([]int64{100})

		// Verify collection with ID 100 is removed, but 101 remains
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["default"]["collection1"]
		assert.False(t, exists)
		_, exists = mgr.collLeader["default"]["collection2"]
		assert.True(t, exists)
	})

	t.Run("Invalidate multiple collections", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]map[string]*shardLeaders{
			"default": {
				"collection1": {
					idx:          atomic.NewInt64(0),
					collectionID: 100,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
				"collection2": {
					idx:          atomic.NewInt64(0),
					collectionID: 101,
					shardLeaders: map[string][]NodeInfo{
						"channel-2": {node},
					},
				},
				"collection3": {
					idx:          atomic.NewInt64(0),
					collectionID: 102,
					shardLeaders: map[string][]NodeInfo{
						"channel-3": {node},
					},
				},
			},
		}
		mgr.leaderMut.Unlock()

		mgr.InvalidateShardLeaderCache([]int64{100, 102})

		// Verify collections 100 and 102 are removed, but 101 remains
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["default"]["collection1"]
		assert.False(t, exists)
		_, exists = mgr.collLeader["default"]["collection2"]
		assert.True(t, exists)
		_, exists = mgr.collLeader["default"]["collection3"]
		assert.False(t, exists)
	})

	t.Run("Invalidate non-existent collection", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]map[string]*shardLeaders{
			"default": {
				"collection1": {
					idx:          atomic.NewInt64(0),
					collectionID: 100,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
			},
		}
		mgr.leaderMut.Unlock()

		mgr.InvalidateShardLeaderCache([]int64{999})

		// Verify collection1 still exists
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["default"]["collection1"]
		assert.True(t, exists)
	})

	t.Run("Invalidate all collections in database removes database", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]map[string]*shardLeaders{
			"test_db": {
				"collection1": {
					idx:          atomic.NewInt64(0),
					collectionID: 200,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
				"collection2": {
					idx:          atomic.NewInt64(0),
					collectionID: 201,
					shardLeaders: map[string][]NodeInfo{
						"channel-2": {node},
					},
				},
			},
		}
		mgr.leaderMut.Unlock()

		mgr.InvalidateShardLeaderCache([]int64{200, 201})

		// Verify database is removed
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["test_db"]
		assert.False(t, exists)
	})

	t.Run("Invalidate across multiple databases", func(t *testing.T) {
		mgr.leaderMut.Lock()
		mgr.collLeader = map[string]map[string]*shardLeaders{
			"db1": {
				"collection1": {
					idx:          atomic.NewInt64(0),
					collectionID: 100,
					shardLeaders: map[string][]NodeInfo{
						"channel-1": {node},
					},
				},
			},
			"db2": {
				"collection2": {
					idx:          atomic.NewInt64(0),
					collectionID: 100, // Same collection ID in different database
					shardLeaders: map[string][]NodeInfo{
						"channel-2": {node},
					},
				},
			},
		}
		mgr.leaderMut.Unlock()

		mgr.InvalidateShardLeaderCache([]int64{100})

		// Verify collection is removed from both databases
		mgr.leaderMut.RLock()
		defer mgr.leaderMut.RUnlock()
		_, exists := mgr.collLeader["db1"]
		assert.False(t, exists) // db1 should be removed
		_, exists = mgr.collLeader["db2"]
		assert.False(t, exists) // db2 should be removed
	})

	mgr.Close()
}

func TestShuffleShardLeaders(t *testing.T) {
	t.Run("Shuffle with multiple nodes", func(t *testing.T) {
		shards := map[string][]NodeInfo{
			"channel-1": {
				{NodeID: 1, Address: "localhost:9000", Serviceable: true},
				{NodeID: 2, Address: "localhost:9001", Serviceable: true},
				{NodeID: 3, Address: "localhost:9002", Serviceable: true},
			},
		}
		sl := &shardLeaders{
			idx:          atomic.NewInt64(5),
			collectionID: 100,
			shardLeaders: shards,
		}

		reader := sl.GetReader()
		result := reader.Shuffle()

		// Verify result has same channel
		assert.Len(t, result, 1)
		assert.Contains(t, result, "channel-1")

		// Verify all nodes are present
		assert.Len(t, result["channel-1"], 3)

		// Verify the first node is based on idx rotation (idx=6, 6%3=0, so nodeID 1 should be first)
		assert.Equal(t, int64(1), result["channel-1"][0].NodeID)

		// Verify all nodes are still present (shuffled)
		nodeIDs := make(map[int64]bool)
		for _, node := range result["channel-1"] {
			nodeIDs[node.NodeID] = true
		}
		assert.True(t, nodeIDs[1])
		assert.True(t, nodeIDs[2])
		assert.True(t, nodeIDs[3])
	})

	t.Run("Shuffle rotates first replica based on idx", func(t *testing.T) {
		shards := map[string][]NodeInfo{
			"channel-1": {
				{NodeID: 1, Address: "localhost:9000", Serviceable: true},
				{NodeID: 2, Address: "localhost:9001", Serviceable: true},
				{NodeID: 3, Address: "localhost:9002", Serviceable: true},
			},
		}
		sl := &shardLeaders{
			idx:          atomic.NewInt64(5),
			collectionID: 100,
			shardLeaders: shards,
		}

		// First read, idx will be 6 (5+1), 6%3=0, so first replica should be leaders[0] which is nodeID 1
		reader := sl.GetReader()
		result := reader.Shuffle()
		assert.Equal(t, int64(1), result["channel-1"][0].NodeID)

		// Second read, idx will be 7 (6+1), 7%3=1, so first replica should be leaders[1] which is nodeID 2
		reader = sl.GetReader()
		result = reader.Shuffle()
		assert.Equal(t, int64(2), result["channel-1"][0].NodeID)

		// Third read, idx will be 8 (7+1), 8%3=2, so first replica should be leaders[2] which is nodeID 3
		reader = sl.GetReader()
		result = reader.Shuffle()
		assert.Equal(t, int64(3), result["channel-1"][0].NodeID)
	})

	t.Run("Shuffle with single node", func(t *testing.T) {
		shards := map[string][]NodeInfo{
			"channel-1": {
				{NodeID: 1, Address: "localhost:9000", Serviceable: true},
			},
		}
		sl := &shardLeaders{
			idx:          atomic.NewInt64(0),
			collectionID: 100,
			shardLeaders: shards,
		}

		reader := sl.GetReader()
		result := reader.Shuffle()

		assert.Len(t, result["channel-1"], 1)
		assert.Equal(t, int64(1), result["channel-1"][0].NodeID)
	})

	t.Run("Shuffle with multiple channels", func(t *testing.T) {
		shards := map[string][]NodeInfo{
			"channel-1": {
				{NodeID: 1, Address: "localhost:9000", Serviceable: true},
				{NodeID: 2, Address: "localhost:9001", Serviceable: true},
			},
			"channel-2": {
				{NodeID: 3, Address: "localhost:9002", Serviceable: true},
				{NodeID: 4, Address: "localhost:9003", Serviceable: true},
			},
		}
		sl := &shardLeaders{
			idx:          atomic.NewInt64(0),
			collectionID: 100,
			shardLeaders: shards,
		}

		reader := sl.GetReader()
		result := reader.Shuffle()

		// Verify both channels are present
		assert.Len(t, result, 2)
		assert.Contains(t, result, "channel-1")
		assert.Contains(t, result, "channel-2")

		// Verify each channel has correct number of nodes
		assert.Len(t, result["channel-1"], 2)
		assert.Len(t, result["channel-2"], 2)
	})

	t.Run("Shuffle with empty leaders", func(t *testing.T) {
		shards := map[string][]NodeInfo{}
		sl := &shardLeaders{
			idx:          atomic.NewInt64(0),
			collectionID: 100,
			shardLeaders: shards,
		}

		reader := sl.GetReader()
		result := reader.Shuffle()

		assert.Len(t, result, 0)
	})
}

// func BenchmarkShardClientMgr(b *testing.B) {
// 	node := nodeInfo{
// 		nodeID: 1,
// 	}
// 	cache := NewMockCache(b)
// 	cache.EXPECT().ListShardLocation().Return(map[int64]nodeInfo{
// 		1: node,
// 	}).Maybe()
// 	globalMetaCache = cache
// 	qn := mocks.NewMockQueryNodeClient(b)
// 	qn.EXPECT().Close().Return(nil).Maybe()

// 	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
// 		return qn, nil
// 	}
// 	s := &shardClientMgrImpl{
// 		clients:         typeutil.NewConcurrentMap[UniqueID, *shardClient](),
// 		clientCreator:   creator,
// 		closeCh:         make(chan struct{}),
// 		purgeInterval:   1 * time.Second,
// 		expiredDuration: 10 * time.Second,
// 	}
// 	go s.PurgeClient()
// 	defer s.Close()

// 	b.ResetTimer()
// 	b.RunParallel(func(pb *testing.PB) {
// 		for pb.Next() {
// 			_, err := s.GetClient(context.Background(), node)
// 			assert.Nil(b, err)
// 		}
// 	})
// }
