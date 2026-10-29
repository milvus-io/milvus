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

package meta

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metacache"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	syncCollection = int64(4200)
	syncPartition  = int64(4201)
)

// newSyncFixture is a manager holding one collection with one partition, both
// at replicaNumber, whose catalog accepts exactly saves writes - the one that
// puts the collection there included.
func newSyncFixture(t *testing.T, saves int, replicaNumber int32, userSpecified bool) *CollectionManager {
	t.Helper()
	catalog := catalogmocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(saves)
	mgr := NewCollectionManager(catalog, metacache.NewMetaStore(nil))
	require.NoError(t, mgr.PutCollection(context.Background(),
		&Collection{CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID: syncCollection, ReplicaNumber: replicaNumber, UserSpecifiedReplicaMode: userSpecified,
		}},
		&Partition{PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: syncCollection, PartitionID: syncPartition, ReplicaNumber: replicaNumber,
		}}))
	return mgr
}

// The point of SyncReplicaNumber: the replicas are counted while the manager
// holds its lock, so no other writer of the number can land between the count
// and the write. A reader that cannot get in while count runs is the proof.
func TestSyncReplicaNumberCountsInsideTheCriticalSection(t *testing.T) {
	ctx := context.Background()
	mgr := newSyncFixture(t, 2, 3, true)

	counted := false
	got, err := mgr.SyncReplicaNumber(ctx, syncCollection, func() int32 {
		counted = true
		if mgr.rwmutex.TryRLock() {
			mgr.rwmutex.RUnlock()
			t.Error("the manager's lock is free while the replicas are counted")
		}
		return 2
	}, nil)
	require.NoError(t, err)
	require.True(t, counted)
	assert.EqualValues(t, 2, got)

	assert.EqualValues(t, 2, mgr.GetReplicaNumber(ctx, syncCollection))
	partitions := mgr.GetPartitionsByCollection(ctx, syncCollection)
	require.Len(t, partitions, 1)
	assert.EqualValues(t, 2, partitions[0].GetReplicaNumber(), "the partitions follow the collection")
	assert.True(t, mgr.GetCollection(ctx, syncCollection).GetUserSpecifiedReplicaMode(),
		"nil keeps the stored mode: a writer that only follows the replicas has no say in how they were asked for")
}

func TestSyncReplicaNumberWritesTheModeItIsGiven(t *testing.T) {
	ctx := context.Background()
	mgr := newSyncFixture(t, 2, 1, false)

	userSpecified := true
	got, err := mgr.SyncReplicaNumber(ctx, syncCollection, func() int32 { return 1 }, &userSpecified)
	require.NoError(t, err)
	assert.EqualValues(t, 1, got)
	assert.True(t, mgr.GetCollection(ctx, syncCollection).GetUserSpecifiedReplicaMode(),
		"a changed mode is written even when the number is not")
}

// The catalog accepts one write, the one that put the collection there: a
// sync that changes nothing must not make another.
func TestSyncReplicaNumberWritesNothingWhenNothingChanges(t *testing.T) {
	ctx := context.Background()
	mgr := newSyncFixture(t, 1, 2, true)

	got, err := mgr.SyncReplicaNumber(ctx, syncCollection, func() int32 { return 2 }, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 2, got)

	userSpecified := true
	_, err = mgr.SyncReplicaNumber(ctx, syncCollection, func() int32 { return 2 }, &userSpecified)
	require.NoError(t, err)
}

func TestSyncReplicaNumberOfAMissingCollection(t *testing.T) {
	mgr := NewCollectionManager(catalogmocks.NewQueryCoordCatalog(t), metacache.NewMetaStore(nil))

	_, err := mgr.SyncReplicaNumber(context.Background(), syncCollection, func() int32 {
		t.Error("there is nothing to count replicas for")
		return 0
	}, nil)
	assert.ErrorIs(t, err, merr.ErrCollectionNotFound)
}

func TestSyncReplicaNumberLeavesTheNumberAloneWhenTheCatalogRefuses(t *testing.T) {
	ctx := context.Background()
	catalog := catalogmocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil).Once()
	mgr := NewCollectionManager(catalog, metacache.NewMetaStore(nil))
	require.NoError(t, mgr.PutCollection(ctx, &Collection{CollectionLoadInfo: &querypb.CollectionLoadInfo{
		CollectionID: syncCollection, ReplicaNumber: 3,
	}}))

	refused := errors.New("catalog is away")
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(refused).Once()
	_, err := mgr.SyncReplicaNumber(ctx, syncCollection, func() int32 { return 2 }, nil)
	assert.ErrorIs(t, err, refused)
	assert.EqualValues(t, 3, mgr.GetReplicaNumber(ctx, syncCollection), "so the caller's retry still has something to correct")
}

// The race this exists for, in both orders. A resource group's teardown has
// removed a replica and an expansion adds one: each changes the replicas
// before it syncs, so whichever syncs last has counted the other's, and the
// number ends at the replicas that exist. With the count read outside the
// lock and written afterwards, the teardown's stale two could land last.
func TestWhicheverWriterSyncsLastHasCountedTheOthersReplicas(t *testing.T) {
	ctx := context.Background()

	t.Run("the expansion syncs last", func(t *testing.T) {
		// Three writes: the put, the teardown's three to two, the expansion's two to three.
		mgr := newSyncFixture(t, 3, 3, false)
		replicas := int32(2) // the teardown has removed one of three
		count := func() int32 { return replicas }

		_, err := mgr.SyncReplicaNumber(ctx, syncCollection, count, nil)
		require.NoError(t, err)
		replicas++ // the expansion spawns its replica, then syncs
		_, err = mgr.SyncReplicaNumber(ctx, syncCollection, count, nil)
		require.NoError(t, err)
		assert.EqualValues(t, 3, mgr.GetReplicaNumber(ctx, syncCollection))
	})

	t.Run("the teardown syncs last", func(t *testing.T) {
		// One write, the put: three were stored, and three is what both count.
		mgr := newSyncFixture(t, 1, 3, false)
		replicas := int32(2)
		count := func() int32 { return replicas }

		replicas++ // the expansion spawns and syncs first
		_, err := mgr.SyncReplicaNumber(ctx, syncCollection, count, nil)
		require.NoError(t, err)
		_, err = mgr.SyncReplicaNumber(ctx, syncCollection, count, nil)
		require.NoError(t, err)
		assert.EqualValues(t, 3, mgr.GetReplicaNumber(ctx, syncCollection))
	})
}
