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

// The reviewer's L2: a scoped row's percentage spoke for the group, but its
// query_service_available still spoke for the whole collection, so a group
// that could not serve was reported as serving whenever any other group
// could. A collection loaded in rg-a and expanding into rg-b, whose replica
// is at 40 with no serving delegator yet and is not available, answers 40 and
// false for rg-b, and the collection-wide 100 and true without a scope.
//
// What makes rg-b's replica unavailable is a read-only node the node manager
// does not know. The rule looks at nothing else - not at the delegator, whose
// absence only shows in the percentage - and the scoped answer is that same
// rule restricted to the group's replicas.
func TestShowLoadCollectionsScopedAnswersQueryServiceAvailableForTheGroupOnly(t *testing.T) {
	withFailedLoadCache(t)
	f := newRGLoadPercentageFixture(t)
	f.putTarget(t, 100, 1000, "100-dmc0", 1, 2, 3, 4)
	f.putReplica(t, 100, 10, "rg-a")
	f.putDelegator(100, 10, "100-dmc0", 1, 2, 3, 4)
	// The collection-wide figure is the mean of its partitions' own: rg-a
	// carries everything, so the collection reads as loaded.
	require.NoError(t, f.meta.PutPartitionWithoutSave(context.Background(), &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{CollectionID: 100, PartitionID: 1000},
		LoadPercentage:    100,
	}))
	f.putReplicaWithRONode(t, 100, 20, 21, "rg-b")
	// The channel is watched and one segment of four is there: two of the
	// five targets, 40, and no serving delegator yet.
	f.putDelegator(100, 20, "100-dmc0", 1)

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

	scoped := show("rg-b")
	assert.EqualValues(t, 40, scoped.GetInMemoryPercentages()[0], "sanity: the group's own progress")
	assert.False(t, scoped.GetQueryServiceAvailable()[0],
		"scoped, the answer is whether THIS group can serve, and rg-b cannot")

	unscoped := show("")
	assert.EqualValues(t, 100, unscoped.GetInMemoryPercentages()[0])
	assert.True(t, unscoped.GetQueryServiceAvailable()[0],
		"unscoped, the collection-wide answer is unchanged: rg-a serves")

	assert.True(t, show("rg-a").GetQueryServiceAvailable()[0], "and rg-a answers for itself")
}
