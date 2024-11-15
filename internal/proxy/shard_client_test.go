package proxy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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

	mgr.ReleaseClientRef(1)
	assert.Equal(t, len(mgr.clients.data), 1)
	mgr.Close()
	assert.Equal(t, len(mgr.clients.data), 0)
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
	shardClient, err := newShardClient(nodeInfo, creator)
	assert.Nil(t, err)
	assert.Equal(t, len(shardClient.clients), 0)
	assert.Equal(t, int64(1), shardClient.refCnt.Load())
	assert.Equal(t, false, shardClient.initialized.Load())

	ctx := context.Background()
	_, err = shardClient.getClient(ctx)
	assert.Nil(t, err)
	assert.Equal(t, len(shardClient.clients), paramtable.Get().ProxyCfg.QueryNodePoolingSize.GetAsInt())
	assert.Equal(t, int64(2), shardClient.refCnt.Load())
	assert.Equal(t, true, shardClient.initialized.Load())

	shardClient.DecRef()
	assert.Equal(t, int64(1), shardClient.refCnt.Load())

	// only shard client manager can close shard client
	shardClient.DecRef()
	shardClient.DecRef()
	shardClient.DecRef()
	shardClient.DecRef()
	assert.Equal(t, false, shardClient.isClosed)
}

func TestPurgeClient(t *testing.T) {
	nodeInfo := nodeInfo{
		nodeID: 1,
	}

	qn := mocks.NewMockQueryNodeClient(t)
	qn.EXPECT().Close().Return(nil).Maybe()
	creator := func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
		return qn, nil
	}

	s := &shardClientMgrImpl{
		clients: struct {
			sync.RWMutex
			data map[UniqueID]*shardClient
		}{data: make(map[UniqueID]*shardClient)},
		clientCreator:   creator,
		closeCh:         make(chan struct{}),
		purgeInterval:   1 * time.Second,
		purgeExpiredAge: 3,
	}
	mockQC := mocks.NewMockQueryCoordClient(t)
	mockRC := mocks.NewMockRootCoordClient(t)
	mockRC.EXPECT().ListPolicy(mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{}, nil)
	InitMetaCache(context.TODO(), mockRC, mockQC, s)

	go s.PurgeClient()
	defer s.Close()
	// test client has been used
	_, err := s.GetClient(context.Background(), nodeInfo)
	assert.Nil(t, err)
	time.Sleep(5 * time.Second)
	// expected client has not been purged
	assert.Equal(t, len(s.clients.data), 1)

	s.ReleaseClientRef(1)
	time.Sleep(5 * time.Second)
	// expected client has been purged
	assert.Equal(t, len(s.clients.data), 0)
}
