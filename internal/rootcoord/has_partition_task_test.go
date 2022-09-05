package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
)

func Test_hasPartitionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &hasPartitionTask{
			Req: &milvuspb.HasPartitionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Undefined,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &hasPartitionTask{
			Req: &milvuspb.HasPartitionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_HasPartition,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_hasPartitionTask_Execute(t *testing.T) {
	t.Run("fail to get collection", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &hasPartitionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.HasPartitionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_HasPartition,
				},
				CollectionName: "test coll",
			},
			Rsp: &milvuspb.BoolResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_CollectionNotExists)
		assert.False(t, task.Rsp.GetValue())
	})

	t.Run("failed", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return &model.Collection{
				Partitions: []*model.Partition{
					{
						PartitionName: "invalid test partition",
					},
				},
			}, nil
		}
		core := newTestCore(withMeta(meta))
		task := &hasPartitionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.HasPartitionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_HasCollection,
				},
				CollectionName: "test coll",
				PartitionName:  "test partition",
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
			return &model.Collection{
				Partitions: []*model.Partition{
					{
						PartitionName: "invalid test partition",
					},
					{
						PartitionName: "test partition",
					},
				},
			}, nil
		}
		core := newTestCore(withMeta(meta))
		task := &hasPartitionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.HasPartitionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_HasCollection,
				},
				CollectionName: "test coll",
				PartitionName:  "test partition",
			},
			Rsp: &milvuspb.BoolResponse{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
		assert.True(t, task.Rsp.GetValue())
	})
}
