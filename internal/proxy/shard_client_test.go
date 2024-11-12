package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mocks"
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
	qn.EXPECT().Close().Return(nil)
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

	shardClient.DecRef()
	assert.Equal(t, int64(0), shardClient.refCnt.Load())
	assert.Equal(t, true, shardClient.isClosed)
}
