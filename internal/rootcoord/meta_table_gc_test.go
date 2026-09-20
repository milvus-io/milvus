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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newCollectionGCMeta(t *testing.T, state pb.CollectionState) (*MetaTable, *mocks.RootCoordCatalog) {
	t.Helper()
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager(nil)
	t.Cleanup(channel.ResetStaticPChannelStatsManager)
	catalog := mocks.NewRootCoordCatalog(t)
	meta := &MetaTable{
		catalog: catalog,
		dbName2Meta: map[string]*model.Database{
			util.DefaultDBName: {ID: util.DefaultDBID, Name: util.DefaultDBName},
		},
		collID2Meta:        make(map[int64]*model.Collection),
		partitionName2ID:   make(map[int64]map[string]int64),
		fileResourceRefCnt: make(map[int64]int),
		names:              newNameDb(),
		aliases:            newNameDb(),
	}
	for id, name := range map[int64]string{100: "first", 200: "second"} {
		coll := &model.Collection{
			CollectionID: id, DBID: util.DefaultDBID, DBName: util.DefaultDBName,
			Name: name, State: pb.CollectionState_CollectionCreated, ShardsNum: 2,
			FileResourceIds: []int64{1},
			Partitions: []*model.Partition{
				{CollectionID: id, PartitionID: id * 10, PartitionName: "live", State: pb.PartitionState_PartitionCreated},
				{CollectionID: id, PartitionID: id*10 + 1, PartitionName: "old", State: pb.PartitionState_PartitionDropping},
			},
		}
		if id == 100 {
			coll.State = state
		}
		meta.collID2Meta[id] = coll
		meta.names.insert(coll.DBName, coll.Name, id)
		meta.partitionName2ID[id] = map[string]int64{"live": id * 10}
		if coll.Available() {
			meta.generalCnt += 2
			meta.fileResourceRefCnt[1]++
		}
	}
	meta.rebuildAvailableCollectionCountLocked()
	return meta, catalog
}

// Release blocked catalog calls and join workers even when an assertion fails.
func startCollectionGCCall(t *testing.T, release func(), call func() error) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- call()
	}()
	t.Cleanup(func() {
		release()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("metadata operation did not finish after releasing the catalog")
		}
	})
	return result
}

func waitCollectionGCCall(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("metadata operation blocked")
	}
}

func waitCollectionGCCatalog(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("catalog operation did not start")
	}
}

// Both catalog calls must start before either returns. This fails when the
// global metadata write lock is held across collection-state persistence.
func TestMetaTable_DropCollectionConcurrentPersistence(t *testing.T) {
	ctx := context.Background()
	meta, catalog := newCollectionGCMeta(t, pb.CollectionState_CollectionCreated)
	started, proceed := make(chan struct{}, 2), make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	catalog.On("AlterCollection", mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).
		Run(func(mock.Arguments) { started <- struct{}{}; <-proceed }).Return(nil).Twice()
	catalog.On("DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()
	first := startCollectionGCCall(t, release, func() error { return meta.DropCollection(ctx, 100, 30) })
	second := startCollectionGCCall(t, release, func() error { return meta.DropCollection(ctx, 200, 40) })
	waitCollectionGCCatalog(t, started)
	waitCollectionGCCatalog(t, started)

	reader := startCollectionGCCall(t, release, func() error {
		coll, err := meta.GetCollectionByID(ctx, util.DefaultDBName, 100, typeutil.MaxTimestamp, true)
		if assert.NoError(t, err) {
			assert.Equal(t, pb.CollectionState_CollectionCreated, coll.State)
		}
		return err
	})
	waitCollectionGCCall(t, reader)
	release()
	waitCollectionGCCall(t, first)
	waitCollectionGCCall(t, second)
	assert.Zero(t, meta.generalCnt)
	assert.Zero(t, meta.availableCollectionCount)
	assert.Zero(t, meta.fileResourceRefCnt[1])
}

func TestMetaTable_DropCollectionPreservesPartitionGC(t *testing.T) {
	ctx := context.Background()
	meta, catalog := newCollectionGCMeta(t, pb.CollectionState_CollectionCreated)
	started, proceed := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	stored := meta.collID2Meta[100].Clone()
	catalog.On("AlterCollection", mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).
		Run(func(args mock.Arguments) {
			stored = args.Get(2).(*model.Collection).Clone()
			close(started)
			<-proceed
		}).Return(nil).Once()
	catalog.On("DropPartition", mock.Anything, util.DefaultDBID, int64(100), int64(1001), mock.Anything).
		Run(func(mock.Arguments) {
			// Legacy partition GC rewrites the persisted collection record.
			assert.Equal(t, pb.CollectionState_CollectionDropping, stored.State)
			stored.Partitions = stored.Partitions[:1]
		}).Return(nil).Once()
	catalog.On("DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	drop := startCollectionGCCall(t, release, func() error { return meta.DropCollection(ctx, 100, 30) })
	waitCollectionGCCatalog(t, started)
	partition := startCollectionGCCall(t, release, func() error { return meta.RemovePartition(ctx, 100, 1001, 0) })
	// Queue the GC writer while persistence still owns the read lock. GC then
	// runs before Drop can acquire its write lock to publish the state.
	require.Eventually(t, func() bool {
		if meta.ddLock.TryRLock() {
			meta.ddLock.RUnlock()
			return false
		}
		return true
	}, 5*time.Second, time.Millisecond)
	release()
	waitCollectionGCCall(t, partition)
	waitCollectionGCCall(t, drop)
	require.Len(t, meta.collID2Meta[100].Partitions, 1)
	assert.Equal(t, int64(1000), meta.collID2Meta[100].Partitions[0].PartitionID)
	assert.Equal(t, stored.Partitions, meta.collID2Meta[100].Partitions)
	assert.Equal(t, stored.State, meta.collID2Meta[100].State)
	assert.Equal(t, 2, meta.generalCnt)
	assert.Equal(t, 1, meta.availableCollectionCount)
}

func TestMetaTable_DropCollectionConcurrentPublicationIsIdempotent(t *testing.T) {
	ctx := context.Background()
	meta, catalog := newCollectionGCMeta(t, pb.CollectionState_CollectionCreated)
	started, proceed := make(chan struct{}, 2), make(chan struct{})
	release := sync.OnceFunc(func() { close(proceed) })
	catalog.On("AlterCollection", mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).
		Run(func(mock.Arguments) { started <- struct{}{}; <-proceed }).Return(nil).Twice()
	catalog.On("DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	first := startCollectionGCCall(t, release, func() error { return meta.DropCollection(ctx, 100, 30) })
	second := startCollectionGCCall(t, release, func() error { return meta.DropCollection(ctx, 100, 30) })
	waitCollectionGCCatalog(t, started)
	waitCollectionGCCatalog(t, started)
	release()
	waitCollectionGCCall(t, first)
	waitCollectionGCCall(t, second)
	require.NoError(t, meta.DropCollection(ctx, 100, 30))
	assert.Equal(t, pb.CollectionState_CollectionDropping, meta.collID2Meta[100].State)
	assert.Equal(t, 2, meta.generalCnt)
	assert.Equal(t, 1, meta.availableCollectionCount)
	assert.Equal(t, 1, meta.fileResourceRefCnt[1])
}

func TestMetaTable_DropCollectionCatalogFailureAndRetry(t *testing.T) {
	for _, err := range []error{context.DeadlineExceeded, context.Canceled} {
		t.Run(err.Error(), func(t *testing.T) {
			ctx := context.Background()
			meta, catalog := newCollectionGCMeta(t, pb.CollectionState_CollectionCreated)
			before := meta.collID2Meta[100].Clone()
			count, refs := meta.generalCnt, meta.fileResourceRefCnt[1]
			stored := before.Clone()
			// Persistence can succeed even when its response is lost.
			catalog.On("AlterCollection", mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).
				Run(func(args mock.Arguments) { stored = args.Get(2).(*model.Collection).Clone() }).Return(err).Once()
			catalog.On("AlterCollection", mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).Return(nil).Once()
			catalog.On("DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			require.ErrorIs(t, meta.DropCollection(ctx, 100, 30), err)
			assert.Equal(t, pb.CollectionState_CollectionDropping, stored.State)
			assert.Equal(t, before, meta.collID2Meta[100].Clone())
			assert.Equal(t, count, meta.generalCnt)
			assert.Equal(t, refs, meta.fileResourceRefCnt[1])
			catalog.AssertNotCalled(t, "DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			require.NoError(t, meta.DropCollection(ctx, 100, 30))
			require.NoError(t, meta.DropCollection(ctx, 100, 30))
			assert.Equal(t, stored, meta.collID2Meta[100].Clone())
			assert.Equal(t, 2, meta.generalCnt)
			assert.Equal(t, 1, meta.availableCollectionCount)
			assert.Equal(t, 1, meta.fileResourceRefCnt[1])
		})
	}
}

func TestMetaTable_CollectionGCLeavesRecreatedCollectionGrants(t *testing.T) {
	ctx := context.Background()
	meta, catalog := newCollectionGCMeta(t, pb.CollectionState_CollectionDropping)
	catalog.On("CreateCollection", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, meta.AddCollection(ctx, &model.Collection{
		CollectionID: 300, DBID: util.DefaultDBID, DBName: util.DefaultDBName,
		Name: "first", State: pb.CollectionState_CollectionCreated,
	}))
	catalog.On("DropCollection", mock.Anything, mock.MatchedBy(func(coll *model.Collection) bool {
		return coll.CollectionID == 100
	}), mock.Anything).Return(nil).Once()
	require.NoError(t, meta.RemoveCollection(ctx, 100, 30))
	catalog.AssertNotCalled(t, "DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	assert.Equal(t, int64(300), meta.GetCollectionID(ctx, util.DefaultDBName, "first"))
	assert.NotContains(t, meta.collID2Meta, int64(100))
}

func TestMetaTable_CollectionGCCatalogAllowsMetadataProgress(t *testing.T) {
	for _, change := range []string{"create", "drop", "recreate", "rename", "legacy recreate"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			meta, catalog := newCollectionGCMeta(t, pb.CollectionState_CollectionDropping)
			if change == "legacy recreate" {
				meta.collID2Meta[100].DBName = ""
				meta.collID2Meta[100].DBID = util.NonDBID
			}
			started, proceed := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(proceed) })
			catalog.On("DropCollection", mock.Anything, mock.MatchedBy(func(coll *model.Collection) bool {
				return coll.CollectionID == 100 && len(coll.Aliases) == 0 && len(coll.Partitions) == 2
			}), mock.Anything).Run(func(mock.Arguments) { close(started); <-proceed }).Return(nil).Once()
			if change == "create" || change == "drop" {
				catalog.On("DeleteGrantByCollectionName", mock.Anything, util.DefaultTenant, util.DefaultDBName, "first").Return(nil).Once()
			}
			gc := startCollectionGCCall(t, release, func() error { return meta.RemoveCollection(ctx, 100, 0) })
			waitCollectionGCCatalog(t, started)
			reader := startCollectionGCCall(t, release, func() error {
				_, err := meta.GetDatabaseByName(ctx, util.DefaultDBName, typeutil.MaxTimestamp)
				return err
			})
			waitCollectionGCCall(t, reader)
			var publish func() error
			replacementID := int64(300)
			switch change {
			case "drop":
				catalog.On("AlterCollection", mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).Return(nil).Once()
				catalog.On("DeleteGrantByCollectionName", mock.Anything, util.DefaultTenant, util.DefaultDBName, "second").Return(nil).Once()
				publish = func() error { return meta.DropCollection(ctx, 200, 40) }
			case "rename":
				replacementID = 200
				catalog.On("AlterCollection", mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).Return(nil).Once()
				catalog.On("MigrateGrantCollectionName", mock.Anything, util.DefaultTenant, util.DefaultDBName, "second", util.DefaultDBName, "first").Return(nil).Once()
				control := funcutil.GetControlChannel("gc-test")
				result := message.BroadcastResultAlterCollectionMessageV2{
					Message: message.MustAsBroadcastAlterCollectionMessageV2(message.NewAlterCollectionMessageBuilderV2().
						WithHeader(&message.AlterCollectionMessageHeader{
							CollectionId: 200, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionName}},
						}).WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{CollectionName: "first"}}).
						WithBroadcast([]string{control}).MustBuildBroadcast()),
					Results: map[string]*message.AppendResult{control: {TimeTick: 40}},
				}
				publish = func() error { return meta.AlterCollection(ctx, result) }
			default:
				name := "first"
				if change == "create" {
					name = "new"
				}
				catalog.On("CreateCollection", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
				publish = func() error {
					return meta.AddCollection(ctx, &model.Collection{
						CollectionID: replacementID, DBID: util.DefaultDBID, DBName: util.DefaultDBName,
						Name: name, State: pb.CollectionState_CollectionCreated,
					})
				}
			}
			writer := startCollectionGCCall(t, release, publish)
			waitCollectionGCCall(t, writer)
			release()
			waitCollectionGCCall(t, gc)
			assert.NotContains(t, meta.collID2Meta, int64(100))
			assert.NotContains(t, meta.partitionName2ID, int64(100))
			if change != "create" && change != "drop" {
				assert.Equal(t, replacementID, meta.GetCollectionID(ctx, util.DefaultDBName, "first"))
				catalog.AssertNotCalled(t, "DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			} else {
				_, exists := meta.names.get(util.DefaultDBName, "first")
				assert.False(t, exists)
			}
			if change == "drop" {
				assert.Equal(t, pb.CollectionState_CollectionDropping, meta.collID2Meta[200].State)
			}
		})
	}
}

func TestMetaTable_CollectionGCCatalogFailureKeepsMetadata(t *testing.T) {
	for _, failure := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			ctx := context.Background()
			meta, catalog := newCollectionGCMeta(t, pb.CollectionState_CollectionDropping)
			before := meta.collID2Meta[100].Clone()
			catalog.On("DropCollection", mock.Anything, mock.Anything, mock.Anything).Return(failure).Once()
			require.ErrorIs(t, meta.RemoveCollection(ctx, 100, 0), failure)
			assert.Equal(t, before, meta.collID2Meta[100].Clone())
			assert.Equal(t, int64(100), meta.GetCollectionID(ctx, util.DefaultDBName, "first"))
			assert.Contains(t, meta.partitionName2ID, int64(100))
			catalog.AssertNotCalled(t, "DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			// A timeout can arrive after the catalog committed. Retrying must be
			// safe even if the persistent keys have already disappeared.
			catalog.On("DropCollection", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			catalog.On("DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
			require.NoError(t, meta.RemoveCollection(ctx, 100, 0))
			require.NoError(t, meta.RemoveCollection(ctx, 100, 0))
			assert.NotContains(t, meta.collID2Meta, int64(100))
		})
	}
}

func TestMetaTable_CollectionGCHistoricalAliasesKeepLock(t *testing.T) {
	ctx := context.Background()
	meta, catalog := newCollectionGCMeta(t, pb.CollectionState_CollectionDropping)
	meta.aliases.insert(util.DefaultDBName, "old-alias", 100)
	meta.aliases.insert("other-db", "old-alias", 200)
	// A dropping collection's name can already be an alias of another owner.
	meta.aliases.insert(util.DefaultDBName, "first", 200)
	catalog.On("DropCollection", mock.Anything, mock.MatchedBy(func(coll *model.Collection) bool {
		return len(coll.Aliases) == 1 && coll.Aliases[0] == "old-alias"
	}), mock.Anything).Run(func(mock.Arguments) {
		if meta.ddLock.TryLock() {
			meta.ddLock.Unlock()
			t.Error("alias catalog deletion must exclude name rebinding")
		}
	}).Return(nil).Once()
	catalog.On("DeleteGrantByCollectionName", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, meta.RemoveCollection(ctx, 100, 0))
	_, exists := meta.aliases.get(util.DefaultDBName, "old-alias")
	assert.False(t, exists)
	id, exists := meta.aliases.get("other-db", "old-alias")
	assert.True(t, exists)
	assert.Equal(t, int64(200), id)
	id, exists = meta.aliases.get(util.DefaultDBName, "first")
	assert.True(t, exists)
	assert.Equal(t, int64(200), id)
}
