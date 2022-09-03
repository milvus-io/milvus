package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/internal/metastore/model"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
)

func Test_describeCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &describeCollectionTask{
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DropCollection,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		task := &describeCollectionTask{
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_describeCollectionTask_Execute(t *testing.T) {
	t.Run("failed to get collection by name", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &describeCollectionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
				CollectionName: "test coll",
			},
			Rsp: &milvuspb.DescribeCollectionResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_CollectionNotExists)
	})

	t.Run("failed to get collection by id", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &describeCollectionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
				CollectionID: 1,
			},
			Rsp: &milvuspb.DescribeCollectionResponse{},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_CollectionNotExists)
	})

	t.Run("success", func(t *testing.T) {
		meta := newMockMetaTable()
		meta.GetCollectionByIDFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp) (*model.Collection, error) {
			return &model.Collection{
				CollectionID: 1,
				Name:         "test coll",
			}, nil
		}
		alias1, alias2 := funcutil.GenRandomStr(), funcutil.GenRandomStr()
		meta.ListAliasesByIDFunc = func(collID UniqueID) []string {
			return []string{alias1, alias2}
		}

		core := newTestCore(withMeta(meta))
		task := &describeCollectionTask{
			baseTaskV2: baseTaskV2{
				core: core,
				done: make(chan error, 1),
			},
			Req: &milvuspb.DescribeCollectionRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_DescribeCollection,
				},
				CollectionID: 1,
			},
			Rsp: &milvuspb.DescribeCollectionResponse{},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, task.Rsp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
		assert.ElementsMatch(t, []string{alias1, alias2}, task.Rsp.GetAliases())
	})
}
