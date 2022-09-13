package rootcoord

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/metastore/model"
)

func TestGarbageCollectorCtx_ReDropCollection(t *testing.T) {
	t.Run("failed to release collection", func(t *testing.T) {
		broker := newMockBroker()
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			return errors.New("error mock ReleaseCollection")
		}
		ticker := newTickerWithMockNormalStream()
		core := newTestCore(withBroker(broker), withTtSynchronizer(ticker))
		gc := newGarbageCollectorCtx(core)
		gc.ReDropCollection(&model.Collection{}, 1000)
	})

	t.Run("failed to DropCollectionIndex", func(t *testing.T) {
		broker := newMockBroker()
		releaseCollectionCalled := false
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			releaseCollectionCalled = true
			return nil
		}
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID) error {
			return errors.New("error mock DropCollectionIndex")
		}
		ticker := newTickerWithMockNormalStream()
		core := newTestCore(withBroker(broker), withTtSynchronizer(ticker))
		gc := newGarbageCollectorCtx(core)
		gc.ReDropCollection(&model.Collection{}, 1000)
		assert.True(t, releaseCollectionCalled)
	})

	t.Run("failed to GcCollectionData", func(t *testing.T) {
		broker := newMockBroker()
		releaseCollectionCalled := false
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			releaseCollectionCalled = true
			return nil
		}
		dropCollectionIndexCalled := false
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID) error {
			dropCollectionIndexCalled = true
			return nil
		}
		ticker := newTickerWithMockFailStream() // failed to broadcast drop msg.
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 100, nil
		}
		core := newTestCore(withBroker(broker), withTtSynchronizer(ticker), withTsoAllocator(tsoAllocator))
		core.ddlTsLockManager = newDdlTsLockManager(core)
		gc := newGarbageCollectorCtx(core)
		shardsNum := 2
		pchans := ticker.getDmlChannelNames(shardsNum)
		gc.ReDropCollection(&model.Collection{PhysicalChannelNames: pchans}, 1000)
		assert.True(t, releaseCollectionCalled)
		assert.True(t, dropCollectionIndexCalled)
	})

	t.Run("failed to remove collection", func(t *testing.T) {
		broker := newMockBroker()
		releaseCollectionCalled := false
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			releaseCollectionCalled = true
			return nil
		}
		dropCollectionIndexCalled := false
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID) error {
			dropCollectionIndexCalled = true
			return nil
		}
		meta := newMockMetaTable()
		meta.RemoveCollectionFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
			return errors.New("error mock RemoveCollection")
		}
		ticker := newTickerWithMockNormalStream()
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 100, nil
		}
		core := newTestCore(withBroker(broker),
			withTtSynchronizer(ticker),
			withTsoAllocator(tsoAllocator),
			withMeta(meta))
		core.ddlTsLockManager = newDdlTsLockManager(core)
		gc := newGarbageCollectorCtx(core)
		gc.ReDropCollection(&model.Collection{}, 1000)
		assert.True(t, releaseCollectionCalled)
		assert.True(t, dropCollectionIndexCalled)
	})

	t.Run("normal case", func(t *testing.T) {
		broker := newMockBroker()
		releaseCollectionCalled := false
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			releaseCollectionCalled = true
			return nil
		}
		dropCollectionIndexCalled := false
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID) error {
			dropCollectionIndexCalled = true
			return nil
		}
		meta := newMockMetaTable()
		removeCollectionCalled := false
		meta.RemoveCollectionFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
			removeCollectionCalled = true
			return nil
		}
		ticker := newTickerWithMockNormalStream()
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 100, nil
		}
		core := newTestCore(withBroker(broker),
			withTtSynchronizer(ticker),
			withTsoAllocator(tsoAllocator),
			withMeta(meta))
		core.ddlTsLockManager = newDdlTsLockManager(core)
		gc := newGarbageCollectorCtx(core)
		gc.ReDropCollection(&model.Collection{}, 1000)
		assert.True(t, releaseCollectionCalled)
		assert.True(t, dropCollectionIndexCalled)
		assert.True(t, removeCollectionCalled)
	})
}

func TestGarbageCollectorCtx_RemoveCreatingCollection(t *testing.T) {
	t.Run("failed to UnwatchChannels", func(t *testing.T) {
		broker := newMockBroker()
		broker.UnwatchChannelsFunc = func(ctx context.Context, info *watchInfo) error {
			return errors.New("error mock UnwatchChannels")
		}
		core := newTestCore(withBroker(broker))
		gc := newGarbageCollectorCtx(core)
		gc.RemoveCreatingCollection(&model.Collection{})
	})

	t.Run("failed to RemoveCollection", func(t *testing.T) {
		broker := newMockBroker()
		unwatchChannelsCalled := false
		broker.UnwatchChannelsFunc = func(ctx context.Context, info *watchInfo) error {
			unwatchChannelsCalled = true
			return nil
		}
		meta := newMockMetaTable()
		meta.RemoveCollectionFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
			return errors.New("error mock RemoveCollection")
		}
		core := newTestCore(withBroker(broker), withMeta(meta))
		gc := newGarbageCollectorCtx(core)
		gc.RemoveCreatingCollection(&model.Collection{})
		assert.True(t, unwatchChannelsCalled)
	})

	t.Run("normal case", func(t *testing.T) {
		broker := newMockBroker()
		unwatchChannelsCalled := false
		broker.UnwatchChannelsFunc = func(ctx context.Context, info *watchInfo) error {
			unwatchChannelsCalled = true
			return nil
		}
		meta := newMockMetaTable()
		removeCollectionCalled := false
		meta.RemoveCollectionFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
			removeCollectionCalled = true
			return nil
		}
		core := newTestCore(withBroker(broker), withMeta(meta))
		gc := newGarbageCollectorCtx(core)
		gc.RemoveCreatingCollection(&model.Collection{})
		assert.True(t, unwatchChannelsCalled)
		assert.True(t, removeCollectionCalled)
	})
}

func TestGarbageCollectorCtx_ReDropPartition(t *testing.T) {
	t.Run("failed to GcPartitionData", func(t *testing.T) {
		ticker := newTickerWithMockFailStream() // failed to broadcast drop msg.
		shardsNum := 2
		pchans := ticker.getDmlChannelNames(shardsNum)
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 100, nil
		}
		core := newTestCore(withTtSynchronizer(ticker), withTsoAllocator(tsoAllocator))
		core.ddlTsLockManager = newDdlTsLockManager(core)
		gc := newGarbageCollectorCtx(core)
		gc.ReDropPartition(pchans, &model.Partition{}, 100000)
	})

	t.Run("failed to RemovePartition", func(t *testing.T) {
		ticker := newTickerWithMockNormalStream()
		shardsNum := 2
		pchans := ticker.getDmlChannelNames(shardsNum)
		meta := newMockMetaTable()
		meta.RemovePartitionFunc = func(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
			return errors.New("error mock RemovePartition")
		}
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 100, nil
		}
		core := newTestCore(withMeta(meta), withTtSynchronizer(ticker), withTsoAllocator(tsoAllocator))
		core.ddlTsLockManager = newDdlTsLockManager(core)
		gc := newGarbageCollectorCtx(core)
		gc.ReDropPartition(pchans, &model.Partition{}, 100000)
	})

	t.Run("normal case", func(t *testing.T) {
		ticker := newTickerWithMockNormalStream()
		shardsNum := 2
		pchans := ticker.getDmlChannelNames(shardsNum)
		meta := newMockMetaTable()
		removePartitionCalled := false
		meta.RemovePartitionFunc = func(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
			removePartitionCalled = true
			return nil
		}
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 100, nil
		}
		core := newTestCore(withMeta(meta), withTtSynchronizer(ticker), withTsoAllocator(tsoAllocator))
		core.ddlTsLockManager = newDdlTsLockManager(core)
		gc := newGarbageCollectorCtx(core)
		gc.ReDropPartition(pchans, &model.Partition{}, 100000)
		assert.True(t, removePartitionCalled)
	})
}
