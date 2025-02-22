package proxy

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
	nodeInfo := nodeInfo{
		nodeID: 1,
	}

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Close().Return(nil)
	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}

	mgr := newShardClientMgr()
	mgr.SetClientCreatorFunc(creator)
	_, err := mgr.GetClient(ctx, nodeInfo)
	assert.Nil(t, err)

	mgr.Close()
	assert.Equal(t, mgr.clients.Len(), 0)
}

func TestShardClient(t *testing.T) {
	nodeInfo := nodeInfo{
		nodeID: 1,
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
	node := nodeInfo{
		nodeID: 1,
	}

	returnEmptyResult := atomic.NewBool(false)

	cache := NewMockCache(t)
	cache.EXPECT().ListShardLocation().RunAndReturn(func() map[int64]nodeInfo {
		if returnEmptyResult.Load() {
			return map[int64]nodeInfo{}
		}
		return map[int64]nodeInfo{
			1: node,
		}
	})
	globalMetaCache = cache

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

	returnEmptyResult.Store(true)
	time.Sleep(2 * time.Second)
	// remove client from shard location, expected client should be purged
	assert.Equal(t, s.clients.Len(), 0)
}

func BenchmarkShardClientMgr(b *testing.B) {
	node := nodeInfo{
		nodeID: 1,
	}
	cache := NewMockCache(b)
	cache.EXPECT().ListShardLocation().Return(map[int64]nodeInfo{
		1: node,
	}).Maybe()
	globalMetaCache = cache
	qn := mocks.NewMockQueryNodeClient(b)
	qn.EXPECT().Close().Return(nil).Maybe()

	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}
	s := &shardClientMgrImpl{
		clients:         typeutil.NewConcurrentMap[UniqueID, *shardClient](),
		clientCreator:   creator,
		closeCh:         make(chan struct{}),
		purgeInterval:   1 * time.Second,
		expiredDuration: 10 * time.Second,
	}
	go s.PurgeClient()
	defer s.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := s.GetClient(context.Background(), node)
			assert.Nil(b, err)
		}
	})
}
