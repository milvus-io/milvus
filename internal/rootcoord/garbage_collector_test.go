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
		gc := newBgGarbageCollector(core)
		gc.ReDropCollection(&model.Collection{}, 1000)
	})

	t.Run("failed to DropCollectionIndex", func(t *testing.T) {
		broker := newMockBroker()
		releaseCollectionCalled := false
		releaseCollectionChan := make(chan struct{}, 1)
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			releaseCollectionCalled = true
			releaseCollectionChan <- struct{}{}
			return nil
		}
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID) error {
			return errors.New("error mock DropCollectionIndex")
		}
		ticker := newTickerWithMockNormalStream()
		core := newTestCore(withBroker(broker), withTtSynchronizer(ticker))
		gc := newBgGarbageCollector(core)
		core.garbageCollector = gc
		gc.ReDropCollection(&model.Collection{}, 1000)
		<-releaseCollectionChan
		assert.True(t, releaseCollectionCalled)
	})

	t.Run("failed to GcCollectionData", func(t *testing.T) {
		broker := newMockBroker()
		releaseCollectionCalled := false
		releaseCollectionChan := make(chan struct{}, 1)
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			releaseCollectionCalled = true
			releaseCollectionChan <- struct{}{}
			return nil
		}
		dropCollectionIndexCalled := false
		dropCollectionIndexChan := make(chan struct{}, 1)
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID) error {
			dropCollectionIndexCalled = true
			dropCollectionIndexChan <- struct{}{}
			return nil
		}
		ticker := newTickerWithMockFailStream() // failed to broadcast drop msg.
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 100, nil
		}
		core := newTestCore(withBroker(broker), withTtSynchronizer(ticker), withTsoAllocator(tsoAllocator))
		core.ddlTsLockManager = newDdlTsLockManagerV2(core.tsoAllocator)
		gc := newBgGarbageCollector(core)
		core.garbageCollector = gc
		shardsNum := 2
		pchans := ticker.getDmlChannelNames(shardsNum)
		gc.ReDropCollection(&model.Collection{PhysicalChannelNames: pchans}, 1000)
		<-releaseCollectionChan
		assert.True(t, releaseCollectionCalled)
		<-dropCollectionIndexChan
		assert.True(t, dropCollectionIndexCalled)
	})

	t.Run("failed to remove collection", func(t *testing.T) {
		broker := newMockBroker()
		releaseCollectionCalled := false
		releaseCollectionChan := make(chan struct{}, 1)
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			releaseCollectionCalled = true
			releaseCollectionChan <- struct{}{}
			return nil
		}
		dropCollectionIndexCalled := false
		dropCollectionIndexChan := make(chan struct{}, 1)
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID) error {
			dropCollectionIndexCalled = true
			dropCollectionIndexChan <- struct{}{}
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
		core.ddlTsLockManager = newDdlTsLockManagerV2(core.tsoAllocator)
		gc := newBgGarbageCollector(core)
		core.garbageCollector = gc
		gc.ReDropCollection(&model.Collection{}, 1000)
		<-releaseCollectionChan
		assert.True(t, releaseCollectionCalled)
		<-dropCollectionIndexChan
		assert.True(t, dropCollectionIndexCalled)
	})

	t.Run("normal case", func(t *testing.T) {
		broker := newMockBroker()
		releaseCollectionCalled := false
		releaseCollectionChan := make(chan struct{}, 1)
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			releaseCollectionCalled = true
			releaseCollectionChan <- struct{}{}
			return nil
		}
		dropCollectionIndexCalled := false
		dropCollectionIndexChan := make(chan struct{}, 1)
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID) error {
			dropCollectionIndexCalled = true
			dropCollectionIndexChan <- struct{}{}
			return nil
		}
		meta := newMockMetaTable()
		removeCollectionCalled := false
		removeCollectionChan := make(chan struct{}, 1)
		meta.RemoveCollectionFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
			removeCollectionCalled = true
			removeCollectionChan <- struct{}{}
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
		core.ddlTsLockManager = newDdlTsLockManagerV2(core.tsoAllocator)
		gc := newBgGarbageCollector(core)
		core.garbageCollector = gc
		gc.ReDropCollection(&model.Collection{}, 1000)
		<-releaseCollectionChan
		assert.True(t, releaseCollectionCalled)
		<-dropCollectionIndexChan
		assert.True(t, dropCollectionIndexCalled)
		<-removeCollectionChan
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
		gc := newBgGarbageCollector(core)
		core.garbageCollector = gc
		gc.RemoveCreatingCollection(&model.Collection{})
	})

	t.Run("failed to RemoveCollection", func(t *testing.T) {
		broker := newMockBroker()
		unwatchChannelsCalled := false
		unwatchChannelsChan := make(chan struct{}, 1)
		broker.UnwatchChannelsFunc = func(ctx context.Context, info *watchInfo) error {
			unwatchChannelsCalled = true
			unwatchChannelsChan <- struct{}{}
			return nil
		}
		meta := newMockMetaTable()
		meta.RemoveCollectionFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
			return errors.New("error mock RemoveCollection")
		}
		core := newTestCore(withBroker(broker), withMeta(meta))
		gc := newBgGarbageCollector(core)
		gc.RemoveCreatingCollection(&model.Collection{})
		<-unwatchChannelsChan
		assert.True(t, unwatchChannelsCalled)
	})

	t.Run("normal case", func(t *testing.T) {
		broker := newMockBroker()
		unwatchChannelsCalled := false
		unwatchChannelsChan := make(chan struct{}, 1)
		broker.UnwatchChannelsFunc = func(ctx context.Context, info *watchInfo) error {
			unwatchChannelsCalled = true
			unwatchChannelsChan <- struct{}{}
			return nil
		}
		meta := newMockMetaTable()
		removeCollectionCalled := false
		removeCollectionChan := make(chan struct{}, 1)
		meta.RemoveCollectionFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
			removeCollectionCalled = true
			removeCollectionChan <- struct{}{}
			return nil
		}
		core := newTestCore(withBroker(broker), withMeta(meta))
		gc := newBgGarbageCollector(core)
		gc.RemoveCreatingCollection(&model.Collection{})
		<-unwatchChannelsChan
		assert.True(t, unwatchChannelsCalled)
		<-removeCollectionChan
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
		core.ddlTsLockManager = newDdlTsLockManagerV2(core.tsoAllocator)
		gc := newBgGarbageCollector(core)
		core.garbageCollector = gc
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
		core.ddlTsLockManager = newDdlTsLockManagerV2(core.tsoAllocator)
		gc := newBgGarbageCollector(core)
		core.garbageCollector = gc
		gc.ReDropPartition(pchans, &model.Partition{}, 100000)
	})

	t.Run("normal case", func(t *testing.T) {
		ticker := newTickerWithMockNormalStream()
		shardsNum := 2
		pchans := ticker.getDmlChannelNames(shardsNum)
		meta := newMockMetaTable()
		removePartitionCalled := false
		removePartitionChan := make(chan struct{}, 1)
		meta.RemovePartitionFunc = func(ctx context.Context, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error {
			removePartitionCalled = true
			removePartitionChan <- struct{}{}
			return nil
		}
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 100, nil
		}
		core := newTestCore(withMeta(meta), withTtSynchronizer(ticker), withTsoAllocator(tsoAllocator))
		core.ddlTsLockManager = newDdlTsLockManagerV2(core.tsoAllocator)
		gc := newBgGarbageCollector(core)
		core.garbageCollector = gc
		gc.ReDropPartition(pchans, &model.Partition{}, 100000)
		<-removePartitionChan
		assert.True(t, removePartitionCalled)
	})
}
