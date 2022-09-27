package proxy

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
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
