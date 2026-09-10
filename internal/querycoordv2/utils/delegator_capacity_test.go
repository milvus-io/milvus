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

package utils

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// putReplicasIn registers the collection as already holding one replica in
// each of the given resource groups: the layout a scoped load adds to.
func putReplicasIn(t *testing.T, ctx context.Context, m *meta.Meta, collectionID int64, rgNames ...string) {
	t.Helper()
	for i, rgName := range rgNames {
		require.NoError(t, m.Put(ctx, meta.NewReplica(&querypb.Replica{
			ID:            collectionID*100 + int64(i+1),
			CollectionID:  collectionID,
			ResourceGroup: rgName,
		})))
	}
}

// The reviewer's failure. With strict isolation off, rg_a holds one regular
// query node and no streaming node, and the default group holds two
// streaming nodes. The replica manager serves rg_a's replica from the default
// group's pool - a group without streaming nodes of its own is taken in by
// the legacy default pool - so the default group's two nodes are shared, and
// its own count is not its capacity. A scoped load of one replica into rg_a
// is admitted (one replica in the default pool, two nodes); a scoped load of
// two replicas into the default group after it would put three replicas on
// two nodes, leaving one replica with no delegator forever, and is refused;
// one replica is admitted.
func TestTheDefaultPoolIsSharedWithTheReplicasOfGroupsWithoutStreamingNodes(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, false)
	ctx, m := metaWithResourceGroup(t, "rg_a", 1)
	const collectionID = int64(41)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		meta.DefaultResourceGroupName: typeutil.NewUniqueSet(901, 902),
	})()

	assert.NoError(t, CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_a": 1}, true),
		"one replica in the default pool of two nodes")
	putReplicasIn(t, ctx, m, collectionID, "rg_a")

	err := CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{meta.DefaultResourceGroupName: 2}, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough,
		"rg_a's replica already sits in the default pool: three replicas on two nodes")
	assert.Contains(t, err.Error(), meta.DefaultResourceGroupName, "the refusal names the pool")

	assert.NoError(t, CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{meta.DefaultResourceGroupName: 1}, true),
		"two replicas on two nodes")
	require.NoError(t, m.Put(ctx, meta.NewReplica(&querypb.Replica{
		ID: collectionID*100 + 50, CollectionID: collectionID, ResourceGroup: meta.DefaultResourceGroupName,
	})))

	// The operator loading into rg_a is refused by the default pool, and the
	// refusal says which group the load was into as well as which pool
	// refused it.
	err = CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_a": 2}, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough, "rg_a's two and the default group's one make three on two")
	assert.Contains(t, err.Error(), meta.DefaultResourceGroupName, "the pool that refused")
	assert.Contains(t, err.Error(), "[rg_a]", "the group the load was into")
}

// With no default pool to take them in, a replica in a group without
// streaming nodes makes the replica manager pool EVERY streaming node for
// EVERY replica of the collection (flat allocation), so the bound is the
// collection's whole replica count against the cluster's whole streaming
// node count, whichever groups they sit in.
func TestFlatAllocationBoundsTheWholeCollectionByEveryStreamingNode(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, false)
	ctx, m := metaWithResourceGroup(t, "rg_a", 1)
	const collectionID = int64(42)
	putReplicasIn(t, ctx, m, collectionID, "rg_a")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_b": typeutil.NewUniqueSet(201, 202),
		"rg_c": typeutil.NewUniqueSet(301),
	})()

	err := CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_b": 3}, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough,
		"rg_a's replica and rg_b's three make four on three streaming nodes")

	assert.NoError(t, CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_b": 2}, true),
		"three replicas on three streaming nodes, rg_b's own count notwithstanding")
}

// Under strict isolation a replica whose group has no streaming node is
// served from nothing at all, and is refused rather than left waiting.
func TestUnderStrictIsolationAReplicaInAGroupWithoutStreamingNodesIsRefused(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, true)
	ctx, m := metaWithResourceGroup(t, "rg_a", 1)
	const collectionID = int64(43)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_b": typeutil.NewUniqueSet(201),
	})()

	err := CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_a": 1}, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough)
	assert.Contains(t, err.Error(), "rg_a")
}

// A group that has streaming nodes is served from them and nothing else,
// whatever the rest of the cluster holds: its bound is its own count.
func TestAnIsolatedGroupIsBoundedByItsOwnStreamingNodes(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, false)
	ctx, m := metaWithResourceGroup(t, "rg_b")
	const collectionID = int64(44)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_b":                        typeutil.NewUniqueSet(201, 202),
		meta.DefaultResourceGroupName: typeutil.NewUniqueSet(901, 902, 903),
	})()

	err := CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_b": 3}, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough,
		"the default group's nodes are not rg_b's: three replicas on rg_b's two nodes")

	assert.NoError(t, CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_b": 2}, true))
}

// Replicas the collection already holds elsewhere are counted in the pool
// they share: a scoped load adds them across requests, and no single request
// sees them all.
func TestExistingReplicasInOtherGroupsAreCountedInTheirPool(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, false)
	ctx, m := metaWithResourceGroup(t, "rg_a", 1, 2)
	const collectionID = int64(45)
	putReplicasIn(t, ctx, m, collectionID, "rg_a", "rg_a")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		meta.DefaultResourceGroupName: typeutil.NewUniqueSet(901, 902, 903),
	})()

	err := CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{meta.DefaultResourceGroupName: 2}, true)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough, "two of rg_a's and two more make four on three")

	assert.NoError(t, CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{meta.DefaultResourceGroupName: 1}, true))
}

// A scoped request states the count of the groups it names; it does not add
// to the replicas already there. Re-sending the load that placed two
// replicas in rg_b asks for two, not four.
func TestAScopedRequestStatesTheNamedGroupsCountRatherThanAddingToIt(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, false)
	ctx, m := metaWithResourceGroup(t, "rg_b")
	const collectionID = int64(46)
	putReplicasIn(t, ctx, m, collectionID, "rg_b", "rg_b")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_b": typeutil.NewUniqueSet(201, 202),
	})()

	assert.NoError(t, CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_b": 2}, true),
		"the same two replicas, not two more")
}

// Only what a request ADDS is admitted. The collection's two replicas in
// rg_b sit on rg_b's two streaming nodes; one node restarts. The load that
// placed them, re-sent, adds nothing and is not refused - the collection is
// serving and the pool will be whole again - while the same request asking a
// third replica adds one to a pool that has no node for it, and is refused.
func TestOnlyTheReplicasARequestAddsAreAdmitted(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, false)
	ctx, m := metaWithResourceGroup(t, "rg_b")
	const collectionID = int64(50)
	putReplicasIn(t, ctx, m, collectionID, "rg_b", "rg_b")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_b": typeutil.NewUniqueSet(201),
	})()

	assert.NoError(t, CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_b": 2}, true),
		"the re-send adds nothing")
	err := CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_b": 3}, true)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough, "the third replica has no node")
}

// A pool that is over capacity is not the request's doing unless the request
// adds to it: an expansion into another pool is judged on its own pool.
func TestAnExpansionIsNotRefusedForAPoolItDoesNotAddTo(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, false)
	ctx, m := metaWithResourceGroup(t, "rg_a")
	const collectionID = int64(51)
	putReplicasIn(t, ctx, m, collectionID, "rg_a", "rg_a")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_a": typeutil.NewUniqueSet(101),
		"rg_b": typeutil.NewUniqueSet(201),
	})()

	assert.NoError(t, CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_b": 1}, true),
		"rg_a's pool is short a node, but this request adds only to rg_b's")
}

// Under strict isolation a replica the collection already holds in a group
// without streaming nodes is not the request's to answer for either.
func TestUnderStrictIsolationAnExistingUnservedReplicaDoesNotRefuseAnExpansionElsewhere(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, true)
	ctx, m := metaWithResourceGroup(t, "rg_a", 1)
	const collectionID = int64(52)
	putReplicasIn(t, ctx, m, collectionID, "rg_a")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_b": typeutil.NewUniqueSet(201),
	})()

	assert.NoError(t, CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_b": 1}, true))
}

// A request that states the whole placement replaces the layout: replicas in
// groups it does not name are on their way out and take no delegator.
func TestAWholePlacementRequestReplacesTheLayout(t *testing.T) {
	installForm(t)
	withStrictResourceGroupIsolation(t, false)
	ctx, m := metaWithResourceGroup(t, "rg_a", 1)
	const collectionID = int64(47)
	putReplicasIn(t, ctx, m, collectionID, "rg_a")

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		meta.DefaultResourceGroupName: typeutil.NewUniqueSet(901, 902),
	})()

	assert.NoError(t, CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{meta.DefaultResourceGroupName: 2}, false),
		"rg_a's replica is not carried by a whole-placement request, so the default pool holds two on two")
	err := CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{meta.DefaultResourceGroupName: 2}, true)
	assert.ErrorIs(t, err, merr.ErrResourceGroupNodeNotEnough, "scoped, rg_a's replica stays and is counted")
}

// A stock binary runs no pool check at all: its admission is exactly what it
// was, and a load that master admits is not refused by a bound master never
// had.
func TestAStockBinaryRunsNoPoolCheck(t *testing.T) {
	stockBinary(t)
	ctx, m := metaWithResourceGroup(t, "rg_b")
	const collectionID = int64(48)

	defer withStreamingQueryNodes(map[string]typeutil.UniqueSet{
		"rg_b": typeutil.NewUniqueSet(201),
	})()

	assert.NoError(t, CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_b": 5}, true))
}

// With the streaming service off there are no delegator pools to check.
func TestThePoolCheckIsOffWithTheStreamingServiceOff(t *testing.T) {
	installForm(t)
	ctx, m := metaWithResourceGroup(t, "rg_b")
	const collectionID = int64(49)

	disabled := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(false).Build()
	defer disabled.UnPatch()

	assert.NoError(t, CheckDelegatorCapacity(ctx, m, collectionID, map[string]int{"rg_b": 5}, true))
}
