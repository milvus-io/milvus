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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// withFailedLoadCache makes sure the global failed-load cache ShowLoadCollections
// expires on every call exists for the test.
func withFailedLoadCache(t *testing.T) {
	t.Helper()
	if meta.GlobalFailedLoadCache == nil {
		meta.GlobalFailedLoadCache = meta.NewFailedLoadCache()
	}
}

func TestShowLoadCollectionsWithoutAResourceGroupKeepsTheCollectionWideFigure(t *testing.T) {
	withFailedLoadCache(t)
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 100, 1000, "100-dmc0", 1, 2)
	f.putReplica(t, 100, 10, "rg-a")
	f.putReplica(t, 100, 20, "rg-b")
	f.putDelegator(100, 10, "100-dmc0", 1, 2) // rg-a fully loaded, rg-b has nothing yet

	resp, err := f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
		CollectionIDs: []int64{100},
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, []int64{100}, resp.GetCollectionIDs())
	assert.Equal(t, f.meta.CalculateLoadPercentage(context.Background(), 100), int32(resp.GetInMemoryPercentages()[0]),
		"without a resource group the answer is the collection-wide percentage, as before")
}

func TestShowLoadCollectionsScopedToAResourceGroup(t *testing.T) {
	withFailedLoadCache(t)
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 100, 1000, "100-dmc0", 1, 2)
	f.putReplica(t, 100, 10, "rg-a")
	f.putReplica(t, 100, 20, "rg-b")
	f.putDelegator(100, 10, "100-dmc0", 1, 2) // rg-a fully loaded, rg-b has nothing yet
	f.putResourceGroup(t, "rg-empty")

	show := func(rg string) *querypb.ShowCollectionsResponse {
		resp, err := f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
			CollectionIDs: []int64{100},
			ResourceGroup: rg,
		})
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		require.Equal(t, []int64{100}, resp.GetCollectionIDs())
		return resp
	}

	assert.EqualValues(t, 100, show("rg-a").GetInMemoryPercentages()[0],
		"the group whose replica serves every target is at 100 whatever its sibling does")
	assert.EqualValues(t, 0, show("rg-b").GetInMemoryPercentages()[0],
		"the group whose replica serves nothing yet is at 0, not at the collection-wide figure")
	assert.EqualValues(t, -1, show("rg-empty").GetInMemoryPercentages()[0],
		"a group that holds no replica of the collection answers -1, which is not 0")
}

func TestShowLoadCollectionsScopedToAnUnknownResourceGroupIsRefused(t *testing.T) {
	withFailedLoadCache(t)
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 100, 1000, "100-dmc0", 1, 2)
	f.putReplica(t, 100, 10, "rg-a")
	f.putDelegator(100, 10, "100-dmc0", 1, 2)

	resp, err := f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
		CollectionIDs: []int64{100},
		ResourceGroup: "rg-missing",
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrResourceGroupNotFound)

	// The reviewer's finding: a collection that is not loaded took the -1
	// branch before anything looked at the group, so a client polling
	// ShowLoadCollections{C, "rg_typo"} after a refused load waited on -1
	// forever. The group is validated before the collections are.
	const neverLoaded = int64(777)
	resp, err = f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
		CollectionIDs: []int64{neverLoaded},
		ResourceGroup: "rg-missing",
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrResourceGroupNotFound,
		"an unknown group is refused whether or not the collection is loaded")

	resp, err = f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
		ResourceGroup: "rg-missing",
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrResourceGroupNotFound,
		"and so is a request that names no collection at all")
}

func TestShowLoadCollectionsScopedAnswersMinusOneForAnUnloadedCollection(t *testing.T) {
	withFailedLoadCache(t)
	f := newRGLoadPercentageFixture(t)
	f.putResourceGroup(t, "rg-a")
	const neverLoaded = int64(777)

	resp, err := f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
		CollectionIDs: []int64{neverLoaded},
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrCollectionNotLoaded,
		"unscoped, an unloaded collection is refused exactly as before")

	resp, err = f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
		CollectionIDs: []int64{neverLoaded},
		ResourceGroup: "rg-a",
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Equal(t, []int64{neverLoaded}, resp.GetCollectionIDs())
	assert.EqualValues(t, -1, resp.GetInMemoryPercentages()[0],
		"scoped, an unloaded collection with no recorded failure holds no replica in the group: -1, not a refusal")
	assert.False(t, resp.GetQueryServiceAvailable()[0])
}

// putReplicaWithRONode registers a replica of collectionID in rgName homed on
// nodeID, with roNodeID as a read-only node the node manager does not know:
// under the availability rule ShowLoadCollections answers with, which is
// master's - a replica is available when every one of its read-only nodes is
// known to the node manager - that is a replica that is not available.
func (f *rgLoadPercentageFixture) putReplicaWithRONode(t *testing.T, collectionID, nodeID, roNodeID int64, rgName string) {
	t.Helper()
	f.putResourceGroup(t, rgName)
	require.Nil(t, f.nodeMgr.Get(roNodeID), "the read-only node must be unknown to the node manager")
	require.NoError(t, f.meta.Put(context.Background(), meta.NewReplica(&querypb.Replica{
		ID:            nodeID,
		CollectionID:  collectionID,
		ResourceGroup: rgName,
		Nodes:         []int64{nodeID},
		RoNodes:       []int64{roNodeID},
	})))
}

// A scoped row's query_service_available answers from the group's
// shard-leader readiness - every shard of the collection has a serviceable
// leader in the group's replicas, on a node the coordinator knows - not
// from the collection-wide rule (every read-only node alive, a replica with
// none counting as available), which read true for a replica that had
// loaded nothing. A collection loaded in rg-a and just expanded into rg-b,
// whose fresh replica holds no delegator, answers 0 and false for rg-b,
// 100 and true for rg-a, and the collection-wide 100 and true - by the
// unchanged rule - without a scope.
func TestShowLoadCollectionsScopedAnswersQueryServiceAvailableFromTheGroupsShardLeaders(t *testing.T) {
	withFailedLoadCache(t)
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 100, 1000, "100-dmc0", 1, 2)
	f.putReplica(t, 100, 10, "rg-a")
	f.registerNode(10)
	f.putServiceableDelegator(100, 10, "100-dmc0", 1, 2)
	f.promoteTarget(t, 100)
	// The collection-wide figure is the mean of its partitions' own: rg-a
	// carries everything, so the collection reads as loaded.
	require.NoError(t, f.meta.PutPartitionWithoutSave(context.Background(), &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{CollectionID: 100, PartitionID: 1000},
		LoadPercentage:    100,
	}))
	// rg-b's replica was just spawned: no delegator, nothing loaded, and no
	// read-only node either, which is what made the old rule call it available.
	f.putReplica(t, 100, 20, "rg-b")

	show := func(rg string) *querypb.ShowCollectionsResponse {
		resp, err := f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
			CollectionIDs: []int64{100},
			ResourceGroup: rg,
		})
		require.NoError(t, err)
		require.NoError(t, merr.Error(resp.GetStatus()))
		require.Equal(t, []int64{100}, resp.GetCollectionIDs())
		return resp
	}

	fresh := show("rg-b")
	assert.EqualValues(t, 0, fresh.GetInMemoryPercentages()[0], "sanity: the group's own progress")
	assert.False(t, fresh.GetQueryServiceAvailable()[0],
		"a replica that serves no shard is not available, whatever its read-only nodes say")

	serving := show("rg-a")
	assert.EqualValues(t, 100, serving.GetInMemoryPercentages()[0])
	assert.True(t, serving.GetQueryServiceAvailable()[0], "every shard has a serviceable leader in rg-a")

	unscoped := show("")
	assert.EqualValues(t, 100, unscoped.GetInMemoryPercentages()[0])
	assert.True(t, unscoped.GetQueryServiceAvailable()[0],
		"unscoped, the collection-wide answer is unchanged: rg-a serves")
}

// A group whose leader is not serviceable, or sits on a node the
// coordinator does not know, is not serving, however far its load is.
func TestShowLoadCollectionsScopedIsNotAvailableWithoutAServiceableLeader(t *testing.T) {
	withFailedLoadCache(t)
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 100, 1000, "100-dmc0", 1, 2)
	f.putReplica(t, 100, 10, "rg-a")
	f.putDelegator(100, 10, "100-dmc0", 1, 2) // every segment there, but the view does not report serviceable
	f.promoteTarget(t, 100)

	resp, err := f.server().ShowLoadCollections(context.Background(), &querypb.ShowCollectionsRequest{
		CollectionIDs: []int64{100},
		ResourceGroup: "rg-a",
	})
	require.NoError(t, err)
	require.NoError(t, merr.Error(resp.GetStatus()))
	assert.EqualValues(t, 100, resp.GetInMemoryPercentages()[0], "sanity: fully loaded by the percentage")
	assert.False(t, resp.GetQueryServiceAvailable()[0], "loaded is not serving: the leader is not serviceable")
}
