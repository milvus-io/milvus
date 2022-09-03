package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
)

func Test_showCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &showCollectionTask{
			Req: &milvuspb.ShowCollectionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Undefined,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &showCollectionTask{
			Req: &milvuspb.ShowCollectionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowCollections,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_showCollectionTask_Execute(t *testing.T) {
	t.Run("failed to list collections", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &showCollectionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.ShowCollectionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowCollections,
				},
			},
			Rsp: &milvuspb.ShowCollectionsResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.ListCollectionsFunc = func(ctx context.Context, ts Timestamp) ([]*model.Collection, error) {
			return []*model.Collection{
				{
					Name: "test coll",
				},
				{
					Name: "test coll2",
				},
			}, nil
		}
		core := newTestCore(withMeta(meta))
		task := &showCollectionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.ShowCollectionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowCollections,
				},
			},
			Rsp: &milvuspb.ShowCollectionsResponse{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, task.Rsp.GetStatus().GetErrorCode())
		assert.Equal(t, 2, len(task.Rsp.GetCollectionNames()))
	})
}
