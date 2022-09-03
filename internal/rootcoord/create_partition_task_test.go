package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/internal/metastore/model"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

func Test_createPartitionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &createPartitionTask{
			Req: &milvuspb.CreatePartitionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection}},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to get collection meta", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &createPartitionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req:        &milvuspb.CreatePartitionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreatePartition}},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		meta := newMockMetaTable()
		collectionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName}
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return coll.Clone(), nil
		}
		core := newTestCore(withMeta(meta))
		task := &createPartitionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req:        &milvuspb.CreatePartitionRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_CreatePartition}},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		assert.True(t, coll.Equal(*task.collMeta))
	})
}

func Test_createPartitionTask_Execute(t *testing.T) {
	t.Run("create duplicate partition", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{{PartitionName: partitionName}}}
		task := &createPartitionTask{
			collMeta: coll,
			Req:      &milvuspb.CreatePartitionRequest{CollectionName: collectionName, PartitionName: partitionName},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("failed to allocate partition id", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{}}
		core := newTestCore(withInvalidIDAllocator())
		task := &createPartitionTask{
			baseTaskV2: baseTaskV2{core: core},
			collMeta:   coll,
			Req:        &milvuspb.CreatePartitionRequest{CollectionName: collectionName, PartitionName: partitionName},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to expire cache", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{}}
		core := newTestCore(withValidIDAllocator(), withInvalidProxyManager())
		task := &createPartitionTask{
			baseTaskV2: baseTaskV2{core: core},
			collMeta:   coll,
			Req:        &milvuspb.CreatePartitionRequest{CollectionName: collectionName, PartitionName: partitionName},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to add partition meta", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{}}
		core := newTestCore(withValidIDAllocator(), withValidProxyManager(), withInvalidMeta())
		task := &createPartitionTask{
			baseTaskV2: baseTaskV2{core: core},
			collMeta:   coll,
			Req:        &milvuspb.CreatePartitionRequest{CollectionName: collectionName, PartitionName: partitionName},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{}}
		meta := newMockMetaTable()
		meta.AddPartitionFunc = func(ctx context.Context, partition *model.Partition) error {
			return nil
		}
		core := newTestCore(withValidIDAllocator(), withValidProxyManager(), withMeta(meta))
		task := &createPartitionTask{
			baseTaskV2: baseTaskV2{core: core},
			collMeta:   coll,
			Req:        &milvuspb.CreatePartitionRequest{CollectionName: collectionName, PartitionName: partitionName},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})
}
