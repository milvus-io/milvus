// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"

	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"

	mocktso "github.com/milvus-io/milvus/internal/tso/mocks"
	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/metastore/model"
)

func TestGarbageCollectorCtx_ReDropCollection(t *testing.T) {
	t.Run("failed to expire cache", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ListAliasesByID",
			mock.AnythingOfType("int64")).
			Return([]string{"alias1"})
		ticker := newTickerWithMockNormalStream()
		core := newTestCore(withInvalidProxyManager(),
			withTtSynchronizer(ticker), withMeta(meta))
		gc := newBgGarbageCollector(core)
		gc.ReDropCollection(&model.Collection{}, 1000)
	})

	t.Run("failed to release collection", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ListAliasesByID",
			mock.AnythingOfType("int64")).
			Return([]string{"alias1"})
		broker := newMockBroker()
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			return errors.New("error mock ReleaseCollection")
		}
		ticker := newTickerWithMockNormalStream()
		core := newTestCore(withBroker(broker), withTtSynchronizer(ticker), withMeta(meta), withValidProxyManager())
		gc := newBgGarbageCollector(core)
		gc.ReDropCollection(&model.Collection{}, 1000)
	})

	t.Run("failed to DropCollectionIndex", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ListAliasesByID",
			mock.AnythingOfType("int64")).
			Return([]string{"alias1"})
		broker := newMockBroker()
		releaseCollectionCalled := false
		releaseCollectionChan := make(chan struct{}, 1)
		broker.ReleaseCollectionFunc = func(ctx context.Context, collectionID UniqueID) error {
			releaseCollectionCalled = true
			releaseCollectionChan <- struct{}{}
			return nil
		}
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
			return errors.New("error mock DropCollectionIndex")
		}
		ticker := newTickerWithMockNormalStream()
		core := newTestCore(withBroker(broker), withTtSynchronizer(ticker), withMeta(meta), withValidProxyManager())
		gc := newBgGarbageCollector(core)
		core.garbageCollector = gc
		gc.ReDropCollection(&model.Collection{}, 1000)
		<-releaseCollectionChan
		assert.True(t, releaseCollectionCalled)
	})

	t.Run("failed to GcCollectionData", func(t *testing.T) {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ListAliasesByID",
			mock.AnythingOfType("int64")).
			Return([]string{"alias1"})
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
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
			dropCollectionIndexCalled = true
			dropCollectionIndexChan <- struct{}{}
			return nil
		}
		ticker := newTickerWithMockFailStream() // failed to broadcast drop msg.
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 100, nil
		}
		core := newTestCore(withBroker(broker), withTtSynchronizer(ticker), withTsoAllocator(tsoAllocator),
			withMeta(meta), withValidProxyManager())
		core.ddlTsLockManager = newDdlTsLockManager(core.tsoAllocator)
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
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
			dropCollectionIndexCalled = true
			dropCollectionIndexChan <- struct{}{}
			return nil
		}

		dropMetaChan := make(chan struct{}, 1)
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ListAliasesByID",
			mock.AnythingOfType("int64")).
			Return([]string{"alias1"})
		meta.On("RemoveCollection",
			mock.Anything, // context.Context
			mock.AnythingOfType("int64"),
			mock.AnythingOfType("uint64")).
			Run(func(args mock.Arguments) {
				dropMetaChan <- struct{}{}
			}).
			Return(errors.New("error mock RemoveCollection"))
		ticker := newTickerWithMockNormalStream()
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 100, nil
		}
		core := newTestCore(withBroker(broker),
			withTtSynchronizer(ticker),
			withTsoAllocator(tsoAllocator),
			withValidProxyManager(),
			withMeta(meta))
		core.ddlTsLockManager = newDdlTsLockManager(core.tsoAllocator)
		gc := newBgGarbageCollector(core)
		core.garbageCollector = gc
		gc.ReDropCollection(&model.Collection{}, 1000)
		<-releaseCollectionChan
		assert.True(t, releaseCollectionCalled)
		<-dropCollectionIndexChan
		assert.True(t, dropCollectionIndexCalled)
		<-dropMetaChan
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
		broker.DropCollectionIndexFunc = func(ctx context.Context, collID UniqueID, partIDs []UniqueID) error {
			dropCollectionIndexCalled = true
			dropCollectionIndexChan <- struct{}{}
			return nil
		}
		meta := mockrootcoord.NewIMetaTable(t)
		meta.On("ListAliasesByID",
			mock.AnythingOfType("int64")).
			Return([]string{"alias1"})
		removeCollectionCalled := false
		removeCollectionChan := make(chan struct{}, 1)
		meta.On("RemoveCollection",
			mock.Anything, // context.Context
			mock.AnythingOfType("int64"),
			mock.AnythingOfType("uint64")).
			Return(func(ctx context.Context, collectionID int64, ts uint64) error {
				removeCollectionCalled = true
				removeCollectionChan <- struct{}{}
				return nil
			})
		ticker := newTickerWithMockNormalStream()
		tsoAllocator := newMockTsoAllocator()
		tsoAllocator.GenerateTSOF = func(count uint32) (uint64, error) {
			return 100, nil
		}
		core := newTestCore(withBroker(broker),
			withTtSynchronizer(ticker),
			withTsoAllocator(tsoAllocator),
			withValidProxyManager(),
			withMeta(meta))
		core.ddlTsLockManager = newDdlTsLockManager(core.tsoAllocator)
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
		defer cleanTestEnv()

		shardNum := 2

		ticker := newRocksMqTtSynchronizer()
		pchans := ticker.getDmlChannelNames(shardNum)

		tsoAllocator := mocktso.NewAllocator(t)
		tsoAllocator.
			On("GenerateTSO", mock.AnythingOfType("uint32")).
			Return(Timestamp(0), errors.New("error mock GenerateTSO"))

		executed := make(chan struct{}, 1)
		executor := newMockStepExecutor()
		executor.AddStepsFunc = func(s *stepStack) {
			s.Execute(context.Background())
			executed <- struct{}{}
		}

		core := newTestCore(withTtSynchronizer(ticker), withTsoAllocator(tsoAllocator), withStepExecutor(executor))
		gc := newBgGarbageCollector(core)
		core.ddlTsLockManager = newDdlTsLockManager(tsoAllocator)
		core.garbageCollector = gc

		gc.RemoveCreatingCollection(&model.Collection{PhysicalChannelNames: pchans})
		<-executed
	})

	t.Run("failed to RemoveCollection", func(t *testing.T) {
		defer cleanTestEnv()

		shardNum := 2

		ticker := newRocksMqTtSynchronizer()
		pchans := ticker.getDmlChannelNames(shardNum)

		tsoAllocator := mocktso.NewAllocator(t)
		tsoAllocator.
			On("GenerateTSO", mock.AnythingOfType("uint32")).
			Return(Timestamp(100), nil)

		for _, pchan := range pchans {
			ticker.syncedTtHistogram.update(pchan, 101)
		}

		meta := newMockMetaTable()
		removeCollectionCalled := false
		removeCollectionChan := make(chan struct{}, 1)
		meta.RemoveCollectionFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
			removeCollectionCalled = true
			removeCollectionChan <- struct{}{}
			return fmt.Errorf("error mock RemoveCollection")
		}

		core := newTestCore(withTtSynchronizer(ticker), withMeta(meta), withTsoAllocator(tsoAllocator))
		gc := newBgGarbageCollector(core)
		core.ddlTsLockManager = newDdlTsLockManager(tsoAllocator)
		core.garbageCollector = gc

		gc.RemoveCreatingCollection(&model.Collection{PhysicalChannelNames: pchans})
		<-removeCollectionChan
		assert.True(t, removeCollectionCalled) // though it fail.
	})

	t.Run("normal case", func(t *testing.T) {
		defer cleanTestEnv()

		shardNum := 2

		ticker := newRocksMqTtSynchronizer()
		pchans := ticker.getDmlChannelNames(shardNum)

		tsoAllocator := mocktso.NewAllocator(t)
		tsoAllocator.
			On("GenerateTSO", mock.AnythingOfType("uint32")).
			Return(Timestamp(100), nil)

		for _, pchan := range pchans {
			ticker.syncedTtHistogram.update(pchan, 101)
		}

		meta := newMockMetaTable()
		removeCollectionCalled := false
		removeCollectionChan := make(chan struct{}, 1)
		meta.RemoveCollectionFunc = func(ctx context.Context, collectionID UniqueID, ts Timestamp) error {
			removeCollectionCalled = true
			removeCollectionChan <- struct{}{}
			return nil
		}

		core := newTestCore(withTtSynchronizer(ticker), withMeta(meta), withTsoAllocator(tsoAllocator))
		gc := newBgGarbageCollector(core)
		core.ddlTsLockManager = newDdlTsLockManager(tsoAllocator)
		core.garbageCollector = gc

		gc.RemoveCreatingCollection(&model.Collection{PhysicalChannelNames: pchans})
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
		core := newTestCore(withTtSynchronizer(ticker), withTsoAllocator(tsoAllocator), withDropIndex())
		core.ddlTsLockManager = newDdlTsLockManager(core.tsoAllocator)
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
		core := newTestCore(withMeta(meta), withTtSynchronizer(ticker), withTsoAllocator(tsoAllocator), withDropIndex())
		core.ddlTsLockManager = newDdlTsLockManager(core.tsoAllocator)
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
		core := newTestCore(withMeta(meta), withTtSynchronizer(ticker), withTsoAllocator(tsoAllocator), withDropIndex())
		core.ddlTsLockManager = newDdlTsLockManager(core.tsoAllocator)
		gc := newBgGarbageCollector(core)
		core.garbageCollector = gc
		gc.ReDropPartition(pchans, &model.Partition{}, 100000)
		<-removePartitionChan
		assert.True(t, removePartitionCalled)
	})
}
