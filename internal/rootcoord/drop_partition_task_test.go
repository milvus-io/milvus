package rootcoord

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/etcdpb"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
)

func Test_dropPartitionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &dropPartitionTask{
			Req: &milvuspb.DropPartitionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("drop default partition", func(t *testing.T) {
		task := &dropPartitionTask{
			Req: &milvuspb.DropPartitionRequest{
				Base:          &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				PartitionName: Params.CommonCfg.DefaultPartitionName,
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to get collection meta", func(t *testing.T) {
		core := newTestCore(withInvalidMeta())
		task := &dropPartitionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropPartitionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		Params.InitOnce()

		collectionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName}
		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return coll.Clone(), nil
		}
		core := newTestCore(withMeta(meta))
		task := &dropPartitionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropPartitionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				CollectionName: collectionName,
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
		assert.True(t, coll.Equal(*task.collMeta))
	})
}

func Test_dropPartitionTask_Execute(t *testing.T) {
	t.Run("drop non-existent partition", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{}}
		task := &dropPartitionTask{
			Req: &milvuspb.DropPartitionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				CollectionName: collectionName,
				PartitionName:  partitionName,
			},
			collMeta: coll.Clone(),
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("failed to expire cache", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{{PartitionName: partitionName}}}
		core := newTestCore(withInvalidProxyManager())
		task := &dropPartitionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropPartitionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				CollectionName: collectionName,
				PartitionName:  partitionName,
			},
			collMeta: coll.Clone(),
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to change partition state", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{{PartitionName: partitionName}}}
		core := newTestCore(withValidProxyManager(), withInvalidMeta())
		task := &dropPartitionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropPartitionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				CollectionName: collectionName,
				PartitionName:  partitionName,
			},
			collMeta: coll.Clone(),
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		partitionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName, Partitions: []*model.Partition{{PartitionName: partitionName}}}
		removePartitionMetaCalled := false
		removePartitionMetaChan := make(chan struct{}, 1)
		meta := newMockMetaTable()
		meta.ChangePartitionStateFunc = func(ctx context.Context, collectionID UniqueID, partitionID UniqueID, state etcdpb.PartitionState, ts Timestamp) error {
			return nil
		}
		meta.RemovePartitionFunc = func(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
			removePartitionMetaCalled = true
			removePartitionMetaChan <- struct{}{}
			return nil
		}

		gc := newMockGarbageCollector()
		deletePartitionCalled := false
		deletePartitionChan := make(chan struct{}, 1)
		gc.GcPartitionDataFunc = func(ctx context.Context, pChannels []string, coll *model.Partition) (Timestamp, error) {
			deletePartitionChan <- struct{}{}
			deletePartitionCalled = true
			return 0, nil
		}

		core := newTestCore(withValidProxyManager(), withMeta(meta), withGarbageCollector(gc))

		task := &dropPartitionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropPartitionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropPartition},
				CollectionName: collectionName,
				PartitionName:  partitionName,
			},
			collMeta: coll.Clone(),
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
		// check if redo worked.
		<-removePartitionMetaChan
		assert.True(t, removePartitionMetaCalled)
		<-deletePartitionChan
		assert.True(t, deletePartitionCalled)
	})
}
