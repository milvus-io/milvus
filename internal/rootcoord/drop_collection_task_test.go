package rootcoord

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/etcdpb"

	"github.com/milvus-io/milvus/internal/metastore/model"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/stretchr/testify/assert"
)

func Test_dropCollectionTask_Prepare(t *testing.T) {
	t.Run("invalid msg type", func(t *testing.T) {
		task := &dropCollectionTask{
			Req: &milvuspb.DropCollectionRequest{
				Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_DescribeCollection},
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("drop via alias", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		meta := newMockMetaTable()
		meta.IsAliasFunc = func(name string) bool {
			return true
		}
		core := newTestCore(withMeta(meta))
		task := &dropCollectionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Prepare(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		meta := newMockMetaTable()
		meta.IsAliasFunc = func(name string) bool {
			return false
		}
		core := newTestCore(withMeta(meta))
		task := &dropCollectionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Prepare(context.Background())
		assert.NoError(t, err)
	})
}

func Test_dropCollectionTask_Execute(t *testing.T) {
	t.Run("drop non-existent collection", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		core := newTestCore(withInvalidMeta())
		task := &dropCollectionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)
	})

	t.Run("failed to expire cache", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName}
		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return coll.Clone(), nil
		}
		meta.ListAliasesByIDFunc = func(collID UniqueID) []string {
			return []string{}
		}
		core := newTestCore(withInvalidProxyManager(), withMeta(meta))
		task := &dropCollectionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("failed to change collection state", func(t *testing.T) {
		collectionName := funcutil.GenRandomStr()
		coll := &model.Collection{Name: collectionName}
		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return coll.Clone(), nil
		}
		meta.ChangeCollectionStateFunc = func(ctx context.Context, collectionID UniqueID, state etcdpb.CollectionState, ts Timestamp) error {
			return errors.New("error mock ChangeCollectionState")
		}
		meta.ListAliasesByIDFunc = func(collID UniqueID) []string {
			return []string{}
		}
		core := newTestCore(withValidProxyManager(), withMeta(meta))
		task := &dropCollectionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Execute(context.Background())
		assert.Error(t, err)
	})

	t.Run("normal case, redo", func(t *testing.T) {
		defer cleanTestEnv()

		collectionName := funcutil.GenRandomStr()
		shardNum := 2

		ticker := newRocksMqTtSynchronizer()
		pchans := ticker.getDmlChannelNames(shardNum)
		ticker.addDmlChannels(pchans...)

		coll := &model.Collection{Name: collectionName, ShardsNum: int32(shardNum), PhysicalChannelNames: pchans}
		meta := newMockMetaTable()
		meta.GetCollectionByNameFunc = func(ctx context.Context, collectionName string, ts Timestamp) (*model.Collection, error) {
			return coll.Clone(), nil
		}
		meta.ChangeCollectionStateFunc = func(ctx context.Context, collectionID UniqueID, state etcdpb.CollectionState, ts Timestamp) error {
			return nil
		}
		meta.ListAliasesByIDFunc = func(collID UniqueID) []string {
			return []string{}
		}
		removeCollectionMetaCalled := false
		removeCollectionMetaChan := make(chan struct{}, 1)
		meta.RemoveCollectionFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
			removeCollectionMetaCalled = true
			removeCollectionMetaChan <- struct{}{}
			return nil
		}

		broker := newMockBroker()
		releaseCollectionCalled := false
		releaseCollectionChan := make(chan struct{}, 1)
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			releaseCollectionCalled = true
			releaseCollectionChan <- struct{}{}
			return nil
		}
		dropIndexCalled := false
		dropIndexChan := make(chan struct{}, 1)
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
			dropIndexCalled = true
			dropIndexChan <- struct{}{}
			return nil
		}

		gc := newMockGarbageCollector()
		deleteCollectionCalled := false
		deleteCollectionChan := make(chan struct{}, 1)
		gc.GcCollectionDataFunc = func(ctx context.Context, coll *model.Collection) (Timestamp, error) {
			deleteCollectionCalled = true
			deleteCollectionChan <- struct{}{}
			return 0, nil
		}

		core := newTestCore(
			withValidProxyManager(),
			withMeta(meta),
			withBroker(broker),
			withGarbageCollector(gc),
			withTtSynchronizer(ticker))

		task := &dropCollectionTask{
			baseTaskV2: baseTaskV2{core: core},
			Req: &milvuspb.DropCollectionRequest{
				Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
				CollectionName: collectionName,
			},
		}
		err := task.Execute(context.Background())
		assert.NoError(t, err)

		// check if redo worked.

		<-releaseCollectionChan
		assert.True(t, releaseCollectionCalled)

		<-dropIndexChan
		assert.True(t, dropIndexCalled)

		<-deleteCollectionChan
		assert.True(t, deleteCollectionCalled)

		<-removeCollectionMetaChan
		assert.True(t, removeCollectionMetaCalled)
	})
}
