package proxy

import (
	"context"
	"testing"

	"github.com/pkg/errors"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/stretchr/testify/assert"
)

func TestProxy_InvalidateCollectionMetaCache_remove_stream(t *testing.T) {
	cache := globalMetaCache
	globalMetaCache = nil
	defer func() { globalMetaCache = cache }()

	chMgr := newMockChannelsMgr()
	chMgr.removeDMLStreamFuncType = func(collectionID UniqueID) error {
		log.Debug("TestProxy_InvalidateCollectionMetaCache_remove_stream, remove dml stream")
		return nil
	}

	node := &Proxy{chMgr: chMgr}

	ctx := context.Background()
	req := &proxypb.InvalidateCollMetaCacheRequest{
		Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
	}

	status, err := node.InvalidateCollectionMetaCache(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
}

func TestProxy_CheckHealth(t *testing.T) {
	t.Run("not healthy", func(t *testing.T) {
		node := &Proxy{session: &sessionutil.Session{ServerID: 1}}
		node.stateCode.Store(commonpb.StateCode_Abnormal)
		ctx := context.Background()
		resp, err := node.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.Equal(t, 1, len(resp.Reasons))
	})

	t.Run("proxy health check is ok", func(t *testing.T) {
		node := &Proxy{
			rootCoord:  NewRootCoordMock(),
			queryCoord: NewQueryCoordMock(),
			dataCoord:  NewDataCoordMock(),
			indexCoord: NewIndexCoordMock(),
			session:    &sessionutil.Session{ServerID: 1},
		}
		node.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()
		resp, err := node.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, true, resp.IsHealthy)
		assert.Empty(t, resp.Reasons)
	})

	t.Run("proxy health check is fail", func(t *testing.T) {
		checkHealthFunc1 := func(ctx context.Context,
			req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
			return &milvuspb.CheckHealthResponse{
				IsHealthy: false,
				Reasons:   []string{"unHealth"},
			}, nil
		}

		checkHealthFunc2 := func(ctx context.Context,
			req *milvuspb.CheckHealthRequest) (*milvuspb.CheckHealthResponse, error) {
			return nil, errors.New("test")
		}

		dataCoordMock := NewDataCoordMock()
		dataCoordMock.checkHealthFunc = checkHealthFunc1

		indexCoordMock := NewIndexCoordMock()
		indexCoordMock.checkHealthFunc = checkHealthFunc2
		node := &Proxy{
			session: &sessionutil.Session{ServerID: 1},
			rootCoord: NewRootCoordMock(func(mock *RootCoordMock) {
				mock.checkHealthFunc = checkHealthFunc1
			}),
			queryCoord: NewQueryCoordMock(func(mock *QueryCoordMock) {
				mock.checkHealthFunc = checkHealthFunc2
			}),
			dataCoord:  dataCoordMock,
			indexCoord: indexCoordMock}
		node.stateCode.Store(commonpb.StateCode_Healthy)
		ctx := context.Background()
		resp, err := node.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		assert.NoError(t, err)
		assert.Equal(t, false, resp.IsHealthy)
		assert.Equal(t, 4, len(resp.Reasons))
	})
}
