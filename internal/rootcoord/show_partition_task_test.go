package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
)

func Test_showPartitionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &showPartitionTask{
			Req: &milvuspb.ShowPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Undefined,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &showPartitionTask{
			Req: &milvuspb.ShowPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowPartitions,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_showPartitionTask_Execute(t *testing.T) {
	t.Run("failed to list collections by name", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &showPartitionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.ShowPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowPartitions,
				},
				CollectionName: "test coll",
			},
			Rsp: &milvuspb.ShowPartitionsResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_CollectionNotExists)
	})

	t.Run("failed to list collections by id", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &showPartitionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.ShowPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowPartitions,
				},
				CollectionID: 1,
			},
			Rsp: &milvuspb.ShowPartitionsResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_CollectionNotExists)
	})

	t.Run("success", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.GetCollectionByIDFunc = func(ctx context.Context, collectionID typeutil.UniqueID, ts Timestamp) (*model.Collection, error) {
			return &model.Collection{
				CollectionID: collectionID,
				Name:         "test coll",
				Partitions: []*model.Partition{
					{
						PartitionID:   1,
						PartitionName: "test partition1",
					},
					{
						PartitionID:   2,
						PartitionName: "test partition2",
					},
				},
			}, nil
		}
		core := newTestCore(withMeta(meta))
		task := &showPartitionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.ShowPartitionsRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_ShowPartitions,
				},
				CollectionID: 1,
			},
			Rsp: &milvuspb.ShowPartitionsResponse{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, task.Rsp.GetStatus().GetErrorCode())
		assert.Equal(t, 2, len(task.Rsp.GetPartitionNames()))
	})
}
