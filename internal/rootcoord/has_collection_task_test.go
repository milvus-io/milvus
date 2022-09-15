package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/metastore/model"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/stretchr/testify/assert"
)

func Test_hasCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &hasCollectionTask{
			Req: &milvuspb.HasCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Undefined,
				},
				CollectionName: "test coll",
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &hasCollectionTask{
			Req: &milvuspb.HasCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_HasCollection,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_hasCollectionTask_Execute(t *testing.T) {
	t.Run("failed", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &hasCollectionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.HasCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_HasCollection,
				},
			},
			Rsp: &milvuspb.BoolResponse{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
		assert.False(t, task.Rsp.GetValue())
	})

	t.Run("success", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return nil, nil
		}
		core := newTestCore(withMeta(meta))
		task := &hasCollectionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.HasCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_HasCollection,
				},
			},
			Rsp: &milvuspb.BoolResponse{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
		assert.True(t, task.Rsp.GetValue())
	})
}
