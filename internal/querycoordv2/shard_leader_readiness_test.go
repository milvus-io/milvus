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

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// shardLeaderReadinessFixture wires up the four read-only stores
// GetShardLeaderReadinessByResourceGroup composes over -- CollectionManager
// and ReplicaManager (as meta.Meta), TargetManager, DistributionManager and
// NodeManager -- with no etcd and no query node behind them, mirroring the
// in-memory style of rgLoadPercentageFixture in this package.
type shardLeaderReadinessFixture struct {
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	dist      *meta.DistributionManager
	nodeMgr   *session.NodeManager
	broker    *meta.MockBroker
}

func newShardLeaderReadinessFixture(t *testing.T) *shardLeaderReadinessFixture {
	paramtable.Init() // AddResourceGroup reads the resource-group quota
	catalog := mocks.NewQueryCoordCatalog(t)
	catalog.On("SaveReplica", mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.On("SaveReplica", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.On("SaveResourceGroup", mock.Anything, mock.Anything).Return(nil).Maybe()

	nodeMgr := session.NewNodeManager()
	m := &meta.Meta{
		CollectionManager: meta.NewCollectionManager(catalog),
		ReplicaManager:    meta.NewReplicaManager(params.RandomIncrementIDAllocator(), catalog),
		// See rgLoadPercentageFixture: the surfaces validate rgName against
		// the ResourceManager first, so every group a test names must exist.
		ResourceManager: meta.NewResourceManager(catalog, nodeMgr),
	}
	broker := meta.NewMockBroker(t)

	return &shardLeaderReadinessFixture{
		meta:      m,
		targetMgr: meta.NewTargetManager(broker, m),
		dist:      meta.NewDistributionManager(nodeMgr),
		nodeMgr:   nodeMgr,
		broker:    broker,
	}
}

// server builds a *Server wired to this fixture's stores. The status has to
// be set explicitly: the entry points gate on merr.CheckHealthy(s.State()),
// and a zero Server reports StateCode_Initializing, so without this every
// test would be exercising that gate instead of the computation.
func (f *shardLeaderReadinessFixture) server() *Server {
	s := &Server{meta: f.meta, targetMgr: f.targetMgr, dist: f.dist, nodeMgr: f.nodeMgr}
	s.status.Store(int32(commonpb.StateCode_Healthy))
	return s
}

// putLoadedCollection registers collectionID as fully loaded -- collection
// status Loaded and its single partition at 100% -- and promotes the given
// channels into the CURRENT target. Both halves matter: the current target is
// what readiness is measured against, and the fully-loaded registration is
// exactly the state under which the native, collection-wide gate
// (checkLoadStatus) reports the collection ready.
func (f *shardLeaderReadinessFixture) putLoadedCollection(t *testing.T, collectionID, partitionID int64, channelNames ...string) {
	ctx := context.Background()
	require.NoError(t, f.meta.PutCollectionWithoutSave(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID: collectionID,
			Status:       querypb.LoadStatus_Loaded,
		},
		LoadPercentage: 100,
	}))
	require.NoError(t, f.meta.PutPartitionWithoutSave(ctx, &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: collectionID,
			PartitionID:  partitionID,
			Status:       querypb.LoadStatus_Loaded,
		},
		LoadPercentage: 100,
	}))

	vChannels := make([]*datapb.VchannelInfo, 0, len(channelNames))
	for _, name := range channelNames {
		vChannels = append(vChannels, &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: name})
	}
	f.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vChannels, nil, nil).Once()
	require.NoError(t, f.targetMgr.UpdateCollectionNextTarget(ctx, collectionID))
	require.True(t, f.targetMgr.UpdateCollectionCurrentTarget(ctx, collectionID),
		"the fixture must leave the collection with a current target, which is what readiness reads")
}

// putLoadingCollection registers collectionID mid-load -- collection status
// Loading with its single partition at 50% -- and still promotes the given
// channels into the CURRENT target, which is exactly what happens when one
// resource group's replicas finish first: shouldUpdateCurrentTarget pools
// ready delegators across every replica and requires only that each channel
// be covered by SOME replica, so the leading group's delegators alone promote
// the target while the collection-wide load percentage stays below 100.
func (f *shardLeaderReadinessFixture) putLoadingCollection(t *testing.T, collectionID, partitionID int64, channelNames ...string) {
	ctx := context.Background()
	require.NoError(t, f.meta.PutCollectionWithoutSave(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID: collectionID,
			Status:       querypb.LoadStatus_Loading,
		},
		LoadPercentage: 50,
	}))
	require.NoError(t, f.meta.PutPartitionWithoutSave(ctx, &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: collectionID,
			PartitionID:  partitionID,
			Status:       querypb.LoadStatus_Loading,
		},
		LoadPercentage: 50,
	}))

	vChannels := make([]*datapb.VchannelInfo, 0, len(channelNames))
	for _, name := range channelNames {
		vChannels = append(vChannels, &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: name})
	}
	f.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vChannels, nil, nil).Once()
	require.NoError(t, f.targetMgr.UpdateCollectionNextTarget(ctx, collectionID))
	require.True(t, f.targetMgr.UpdateCollectionCurrentTarget(ctx, collectionID),
		"the fixture must leave the mid-load collection with a promoted current target")
}

// putLoadedCollectionWithoutPartitions registers collectionID as Loaded and
// promotes its channels into the current target, but records NO partition --
// the state job_load.go leaves behind between RemovePartition and
// PutCollection whenever the incoming partition set is disjoint from the
// loaded one. RemovePartition is an independent etcd commit that does not
// touch the collection key, so the window is observable by a concurrent
// reader and survives a crash inside it: etcd keeps the collection key with
// zero partition keys, and CollectionManager.Recover restores the Loaded
// record while the partition loop has nothing to iterate.
//
// This is the state on which m.Exist and CalculateLoadPercentage disagree.
func (f *shardLeaderReadinessFixture) putLoadedCollectionWithoutPartitions(t *testing.T, collectionID int64, channelNames ...string) {
	ctx := context.Background()
	require.NoError(t, f.meta.PutCollectionWithoutSave(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID: collectionID,
			Status:       querypb.LoadStatus_Loaded,
		},
		LoadPercentage: 100,
	}))

	vChannels := make([]*datapb.VchannelInfo, 0, len(channelNames))
	for _, name := range channelNames {
		vChannels = append(vChannels, &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: name})
	}
	f.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vChannels, nil, nil).Maybe()
	f.targetMgr.UpdateCollectionNextTarget(ctx, collectionID)
	f.targetMgr.UpdateCollectionCurrentTarget(ctx, collectionID)
	require.True(t, f.meta.Exist(ctx, collectionID),
		"the fixture must reproduce the state Exist calls loaded")
	require.Negative(t, f.meta.CalculateLoadPercentage(ctx, collectionID),
		"...and on which the load percentage reads negative, which is the disagreement under test")
}

// putReplica registers a replica of collectionID in rgName holding nodeIDs.
// The replica ID is the first node id, so distinct replicas in one test need
// distinct nodes; ReplicaManager indexes by ID and a repeated ID would
// silently clobber the earlier replica.
func (f *shardLeaderReadinessFixture) putReplica(t *testing.T, collectionID int64, rgName string, nodeIDs ...int64) {
	f.putResourceGroup(t, rgName)
	require.NotEmpty(t, nodeIDs)
	require.NoError(t, f.meta.Put(context.Background(), meta.NewReplica(&querypb.Replica{
		ID:            nodeIDs[0],
		CollectionID:  collectionID,
		ResourceGroup: rgName,
		Nodes:         nodeIDs,
	})))
}

// putInvisibleReplica registers a replica of collectionID in rgName holding
// nodeIDs, in the not-yet-query-visible state load-config updates spawn
// replicas in (job_update.go passes WithQueryInvisible; visibility is flipped
// later, all-or-nothing, by tryPromoteReadyLoadConfigReplicas).
func (f *shardLeaderReadinessFixture) putInvisibleReplica(t *testing.T, collectionID int64, rgName string, nodeIDs ...int64) {
	f.putResourceGroup(t, rgName)
	require.NotEmpty(t, nodeIDs)
	mutable := meta.NewReplica(&querypb.Replica{
		ID:            nodeIDs[0],
		CollectionID:  collectionID,
		ResourceGroup: rgName,
		Nodes:         nodeIDs,
	}).CopyForWrite()
	mutable.SetQueryInvisible(true)
	require.NoError(t, f.meta.Put(context.Background(), mutable.IntoReplica()))
}

// registerNode makes nodeID one the coordinator knows about. A leader on an
// unregistered node is not usable, which the native shard-leader builder
// enforces by dropping any leader whose NodeManager entry is missing.
// putResourceGroup registers rgName so the existence check passes for a
// group that deliberately holds no replica (the NoReplicaInResourceGroup
// state, as opposed to the ErrResourceGroupNotFound one).
func (f *shardLeaderReadinessFixture) putResourceGroup(t *testing.T, rgName string) {
	t.Helper()
	putResourceGroup(t, f.meta, rgName)
}

func (f *shardLeaderReadinessFixture) registerNode(nodeID int64) {
	f.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:  nodeID,
		Address: "localhost:0",
		Version: semver.MustParse("2.6.0"),
	}))
}

// putLeader records nodeID as the delegator for channelName and registers the
// node. serviceable drives the leader view's Serviceable flag, i.e. whether
// the delegator reports itself able to answer a query.
func (f *shardLeaderReadinessFixture) putLeader(collectionID, nodeID int64, channelName string, serviceable bool) {
	f.registerNode(nodeID)
	f.dist.ChannelDistManager.Update(nodeID, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: channelName},
		Node:         nodeID,
		View: &meta.LeaderView{
			ID:           nodeID,
			CollectionID: collectionID,
			Channel:      channelName,
			Status:       &querypb.LeaderViewStatus{Serviceable: serviceable},
		},
	})
}

func (f *shardLeaderReadinessFixture) readiness(t *testing.T, collectionID int64, rgName string) utils.ShardLeaderReadiness {
	t.Helper()
	got, err := f.server().GetShardLeaderReadinessByResourceGroup(context.Background(), collectionID, rgName)
	require.NoError(t, err)
	return got
}

// TestShardLeaderReadinessByRG_LeaderInOneRGDoesNotMakeAnotherReady is the
// defect this method exists to close, stated directly: collection 100 is
// loaded into rg-a and rg-b, only rg-a's replica has a serviceable leader on
// the one shard, and rg-b must be reported not ready.
//
// The assertion pair matters more than either half alone. Any implementation
// that answers per collection -- including the obvious one, calling
// GetShardLeaders and looking at whether the shard has any leader at all --
// gives rg-a and rg-b the same answer, and admitting a query to rg-b is
// exactly the bug: rg-b has no leader to serve it.
func TestShardLeaderReadinessByRG_LeaderInOneRGDoesNotMakeAnotherReady(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 100, 1000, "100-dmc0")
	f.putReplica(t, 100, "rg-a", 10)
	f.putReplica(t, 100, "rg-b", 11)
	f.putLeader(100, 10, "100-dmc0", true)
	f.registerNode(11) // rg-b's node is up; it just holds no leader

	rgA := f.readiness(t, 100, "rg-a")
	rgB := f.readiness(t, 100, "rg-b")

	assert.True(t, rgA.Ready, "rg-a holds the serviceable leader of the only shard and must be ready")
	assert.Empty(t, rgA.UnreadyShards)

	assert.False(t, rgB.Ready,
		"rg-b holds no leader of the shard: a leader serving rg-a must not make rg-b look ready")
	assert.Equal(t, utils.ShardLeadersReasonShardsWithoutLeader, rgB.Reason)
	assert.Equal(t, []string{"100-dmc0"}, rgB.UnreadyShards,
		"the shard rg-b cannot serve must be named, not merely counted")
	assert.Equal(t, 1, rgB.TotalShards)
}

// TestShardLeaderReadinessByRG_DoesNotInheritCollectionWideGate pins the
// second, independent route to the same admission bug. checkLoadStatus, the
// gate on the native GetShardLeaders path, is collection-wide: it reads
// CalculateLoadPercentage(collectionID) and then short-circuits to ready
// whenever the collection's own status is LoadStatus_Loaded. That status is
// set only when the collection-wide average reaches 100, and nothing resets
// it when a later resource group is added, so once a collection has been
// Loaded the short-circuit is permanently armed for every resource group
// that comes after -- which is the state this fixture reproduces.
//
// This test puts the collection in exactly that state and asserts both sides
// of the contrast in one place: the native collection-wide path reports the
// shard served, while the per-resource-group answer for the resource group
// that has no leader is still not ready. If this method ever grows a
// checkLoadStatus call, or otherwise consults the collection's aggregate
// status, rg-b flips to ready here.
func TestShardLeaderReadinessByRG_DoesNotInheritCollectionWideGate(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 200, 2000, "200-dmc0")
	f.putReplica(t, 200, "rg-a", 20)
	f.putReplica(t, 200, "rg-b", 21)
	f.putLeader(200, 20, "200-dmc0", true)
	f.registerNode(21)

	// The collection-wide view: fully loaded, shard served, no error.
	require.EqualValues(t, 100, f.meta.CalculateLoadPercentage(ctx, 200),
		"the fixture must reproduce the state in which the collection-wide gate passes")
	shards, err := utils.GetShardLeaders(ctx, f.meta, f.targetMgr, f.dist, f.nodeMgr, 200, false)
	require.NoError(t, err, "the native collection-wide path must admit this state")
	require.Len(t, shards, 1)
	require.Equal(t, []int64{20}, shards[0].GetNodeIds(),
		"the native path reports the leader without recording which resource group it serves")

	rgB := f.readiness(t, 200, "rg-b")

	assert.False(t, rgB.Ready,
		"the collection-wide gate passing must not make a resource group without a leader ready")
	assert.Equal(t, utils.ShardLeadersReasonShardsWithoutLeader, rgB.Reason)
}

// TestShardLeaderReadinessByRG_NoReplicaInRG asserts that a resource group
// that holds no replica of the collection is reported as such, rather than as
// a resource group whose shards happen to be lagging. The two are different
// situations for the caller: nothing is loading in this resource group, so
// waiting will never help.
func TestShardLeaderReadinessByRG_NoReplicaInRG(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 300, 3000, "300-dmc0")
	f.putReplica(t, 300, "rg-a", 30)
	f.putLeader(300, 30, "300-dmc0", true)
	f.putResourceGroup(t, "rg-absent") // exists, holds nothing: not the ErrResourceGroupNotFound state

	got := f.readiness(t, 300, "rg-absent")

	assert.False(t, got.Ready)
	assert.Equal(t, utils.ShardLeadersReasonNoReplicaInResourceGroup, got.Reason,
		"a resource group with no replica must be distinguishable from one whose shards are lagging")
	assert.Zero(t, got.TotalShards)
	assert.Empty(t, got.UnreadyShards)
}

// TestShardLeaderReadinessByRG_UnserviceableLeaderIsNotReady asserts that the
// leader has to be serviceable, not merely present. A delegator that has been
// assigned the shard but reports itself unable to answer -- still catching up
// with streaming data, say -- cannot admit a query, and the native path drops
// it for the same reason.
func TestShardLeaderReadinessByRG_UnserviceableLeaderIsNotReady(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 400, 4000, "400-dmc0")
	f.putReplica(t, 400, "rg-a", 40)
	f.putLeader(400, 40, "400-dmc0", false)

	got := f.readiness(t, 400, "rg-a")

	assert.False(t, got.Ready,
		"a leader that reports itself unserviceable must not count as serving the shard")
	assert.Equal(t, utils.ShardLeadersReasonShardsWithoutLeader, got.Reason)
	assert.Equal(t, []string{"400-dmc0"}, got.UnreadyShards)
}

// TestShardLeaderReadinessByRG_LeaderOnUnknownNodeIsNotReady asserts that a
// serviceable leader sitting on a node the coordinator no longer knows about
// does not count. This is the third condition the native shard-leader builder
// applies per leader, and dropping it would make a resource group look ready
// across a query node the session manager has already evicted.
func TestShardLeaderReadinessByRG_LeaderOnUnknownNodeIsNotReady(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 500, 5000, "500-dmc0")
	f.putReplica(t, 500, "rg-a", 50)
	f.putLeader(500, 50, "500-dmc0", true)
	f.nodeMgr.Remove(50)

	got := f.readiness(t, 500, "rg-a")

	assert.False(t, got.Ready,
		"a leader on a node the coordinator has evicted must not count as serving the shard")
	assert.Equal(t, []string{"500-dmc0"}, got.UnreadyShards)
}

// TestShardLeaderReadinessByRG_PartiallyServedCollection asserts that every
// shard has to be served, that the ones that are not are named individually,
// and that the names come back sorted so one state always prints one line --
// the channel set arrives as a map, whose iteration order is random.
func TestShardLeaderReadinessByRG_PartiallyServedCollection(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 600, 6000, "600-dmc0", "600-dmc1", "600-dmc2")
	f.putReplica(t, 600, "rg-a", 60, 61)
	f.putLeader(600, 60, "600-dmc1", true)
	f.registerNode(61)

	got := f.readiness(t, 600, "rg-a")

	assert.False(t, got.Ready, "one shard out of three being served is not a servable resource group")
	assert.Equal(t, 3, got.TotalShards)
	assert.Equal(t, []string{"600-dmc0", "600-dmc2"}, got.UnreadyShards,
		"the unserved shards must be named, sorted, and must not include the one that is served")
}

// TestShardLeaderReadinessByRG_UnreadyShardsAreStable asserts that one state
// always produces one answer. The shards come out of a map, whose iteration
// order Go deliberately varies between calls, so an implementation that
// forwards that order straight to the caller makes the same unchanged state
// print a different log line each second and turns any comparison of two
// answers into noise. Repeating the call pins that down: with four unserved
// shards, an unsorted implementation has to hit the one right order every
// time to survive.
func TestShardLeaderReadinessByRG_UnreadyShardsAreStable(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 1200, 12000, "1200-dmc0", "1200-dmc1", "1200-dmc2", "1200-dmc3")
	f.putReplica(t, 1200, "rg-a", 120)
	f.registerNode(120)

	want := []string{"1200-dmc0", "1200-dmc1", "1200-dmc2", "1200-dmc3"}
	for i := 0; i < 30; i++ {
		got := f.readiness(t, 1200, "rg-a")
		require.Equal(t, want, got.UnreadyShards,
			"the unserved shards must come back in a stable order on every call, attempt %d", i)
	}
}

// TestShardLeaderReadinessByRG_LeadersAcrossReplicasInSameRG asserts that when
// a resource group holds more than one replica of the collection, leaders from
// any of them count: the question is whether the resource group can serve the
// shard, not whether one particular replica can. Here neither replica serves
// both shards on its own, but between them every shard is covered.
func TestShardLeaderReadinessByRG_LeadersAcrossReplicasInSameRG(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 700, 7000, "700-dmc0", "700-dmc1")
	f.putReplica(t, 700, "rg-shared", 70)
	f.putReplica(t, 700, "rg-shared", 71)
	f.putLeader(700, 70, "700-dmc0", true)
	f.putLeader(700, 71, "700-dmc1", true)

	got := f.readiness(t, 700, "rg-shared")

	assert.True(t, got.Ready,
		"shards served by different replicas of the same resource group must together make it ready")
	assert.Empty(t, got.UnreadyShards)
	assert.Equal(t, 2, got.TotalShards)
}

// TestShardLeaderReadinessByRG_EmptyRGSpansEveryReplica asserts that an empty
// resource group name is the absence of a filter rather than a filter that
// matches nothing, matching LoadPercentageByResourceGroup. The one replica
// that serves the shard lives in rg-a, so rg-b is not ready while "" is.
func TestShardLeaderReadinessByRG_EmptyRGSpansEveryReplica(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 800, 8000, "800-dmc0")
	f.putReplica(t, 800, "rg-a", 80)
	f.putReplica(t, 800, "rg-b", 81)
	f.putLeader(800, 80, "800-dmc0", true)
	f.registerNode(81)

	all := f.readiness(t, 800, "")

	assert.True(t, all.Ready,
		"an empty resource group must span every replica of the collection, not match none of them")
	assert.False(t, f.readiness(t, 800, "rg-b").Ready,
		"the unfiltered answer must not be what a specific resource group gets")
}

// TestShardLeaderReadinessByRG_NoChannelTarget asserts that a collection with
// a replica in the resource group but no shard in the current target -- what a
// collection under recovery looks like -- is reported with its own reason
// rather than as ready-by-vacuous-truth. Every shard of an empty shard set is
// trivially served, so an implementation that skips this guard reports Ready.
func TestShardLeaderReadinessByRG_NoChannelTarget(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	require.NoError(t, f.meta.PutCollectionWithoutSave(ctx, &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 900, Status: querypb.LoadStatus_Loaded},
		LoadPercentage:     100,
	}))
	// The partition record is what makes the collection count as registered:
	// the registration test is CalculateLoadPercentage, which needs a
	// non-empty partition set. Without it this fixture lands in the
	// not-loaded branch and would test that instead of the empty-shard-set
	// guard it exists for -- see
	// TestZeroPartitionCollectionReadsNotLoadedOnEveryScopedSurface.
	require.NoError(t, f.meta.PutPartitionWithoutSave(ctx, &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: 900, PartitionID: 9000, Status: querypb.LoadStatus_Loaded,
		},
		LoadPercentage: 100,
	}))
	f.putReplica(t, 900, "rg-a", 90)

	got := f.readiness(t, 900, "rg-a")

	assert.False(t, got.Ready, "a collection with no shard in the current target must not be called ready")
	assert.Equal(t, utils.ShardLeadersReasonNoChannelTarget, got.Reason)
	assert.Zero(t, got.TotalShards)
}

// TestShardLeaderReadinessByRG_FailedLoadIsSurfaced asserts that when a
// replica record exists but the collection is not registered as loaded, a
// recorded load failure reaches the caller as an error instead of leaving it
// to wait out its timeout on a load that is never coming back. This matches
// what LoadPercentageByResourceGroup does with the same cache.
func TestShardLeaderReadinessByRG_FailedLoadIsSurfaced(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	// Collection 1000 is never registered as loaded, so its load percentage
	// reads negative even though a replica record exists.
	f.putReplica(t, 1000, "rg-a", 100)

	loadErr := errors.New("mocked load failure")
	seedFailedLoadCache(t, 1000, loadErr)

	got, err := f.server().GetShardLeaderReadinessByResourceGroup(context.Background(), 1000, "rg-a")

	assert.ErrorIs(t, err, merr.ErrCollectionNotLoaded,
		"a recorded load failure must reach the caller, normalized to the terminal code")
	assert.Contains(t, err.Error(), loadErr.Error(),
		"normalizing the code must not throw away the recorded cause")
	assert.False(t, got.Ready)
	assert.Equal(t, utils.ShardLeadersReasonCollectionNotLoaded, got.Reason)
}

// TestShardLeaderReadinessByRG_FailedLoadIsNotRetriable mirrors the
// percentage surface: the cache stores whatever the failing load recorded,
// including retriable sentinels, and this surface must not hand one back --
// the struct's Reason disambiguates for a caller that reads it, but the error
// code is what a caller triaging on merr sees.
func TestShardLeaderReadinessByRG_FailedLoadIsNotRetriable(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putReplica(t, 1020, "rg-a", 102)

	seedFailedLoadCache(t, 1020, merr.WrapErrServiceNotReady("querynode", 102, "restarting"))

	got, err := f.server().GetShardLeaderReadinessByResourceGroup(context.Background(), 1020, "rg-a")

	assert.ErrorIs(t, err, merr.ErrCollectionNotLoaded)
	assert.NotErrorIs(t, err, merr.ErrServiceNotReady)
	assert.False(t, merr.IsRetryableErr(err))
	assert.False(t, got.Ready)
}

// TestShardLeaderReadinessByRG_FailedLoadSurvivesReplicaCleanup pins the
// terminal failed-load state, which is also the common one:
// CollectionObserver.observeTimeout removes BOTH the collection registration
// and every replica record, leaving only the GlobalFailedLoadCache entry
// behind. The recorded failure must still reach the caller from that state;
// checking replicas before consulting the cache would swallow it into
// (NoReplicaInResourceGroup, nil), which tells the caller nothing is loading
// here when the truth is that the load failed.
func TestShardLeaderReadinessByRG_FailedLoadSurvivesReplicaCleanup(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	// No putLoadedCollection, no putReplica: collection 1400 has been fully
	// cleaned up after its load timed out; only the failure record remains.
	// The resource group itself outlives the load, so it is registered.
	f.putResourceGroup(t, "rg-a")
	loadErr := errors.New("mocked load failure")
	seedFailedLoadCache(t, 1400, loadErr)

	got, err := f.server().GetShardLeaderReadinessByResourceGroup(context.Background(), 1400, "rg-a")

	assert.ErrorIs(t, err, merr.ErrCollectionNotLoaded,
		"the recorded load failure must survive the removal of the replica records")
	assert.Contains(t, err.Error(), loadErr.Error())
	assert.False(t, got.Ready)
	assert.Equal(t, utils.ShardLeadersReasonCollectionNotLoaded, got.Reason)
}

// TestShardLeaderReadinessByRG_NilFailedLoadCache asserts the same init
// window TestGetLoadPercentageByResourceGroup_NilFailedLoadCache pins:
// GlobalFailedLoadCache is the last dependency initQueryCoord wires, after
// the stores this function's guard checks, and Get on the nil global panics.
// In that window an unregistered collection simply reads as not loaded,
// without the recorded-failure detail the cache would have added.
func TestShardLeaderReadinessByRG_NilFailedLoadCache(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putResourceGroup(t, "rg-a")
	nilFailedLoadCache(t)

	assert.NotPanics(t, func() {
		got, err := f.server().GetShardLeaderReadinessByResourceGroup(context.Background(), 1500, "rg-a")
		assert.NoError(t, err)
		assert.False(t, got.Ready)
		assert.Equal(t, utils.ShardLeadersReasonCollectionNotLoaded, got.Reason)
	})
}

// TestShardLeaderReadinessByRG_QueryInvisibleReplicaDoesNotCount asserts that
// readiness agrees with the routing surface about which replicas exist for
// queries. The GetShardLeaders routing path
// filter on replica.IsQueryVisible(), so a query-invisible replica's leader is
// one the proxy can never route to; counting it here would report Ready for a
// resource group that does not appear in the GetShardLeaders answer at all — exactly the
// load-config switch window this check exists to keep honest. Once visibility
// is flipped (the same all-or-nothing promotion tryPromoteReadyLoadConfigReplicas
// performs), the same leader must start counting.
func TestShardLeaderReadinessByRG_QueryInvisibleReplicaDoesNotCount(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 1300, 13000, "1300-dmc0")
	f.putInvisibleReplica(t, 1300, "rg-a", 130)
	f.putLeader(1300, 130, "1300-dmc0", true)

	got := f.readiness(t, 1300, "rg-a")

	assert.False(t, got.Ready,
		"a serviceable leader on a query-invisible replica must not make the resource group ready: no query can route to it")
	assert.Equal(t, utils.ShardLeadersReasonShardsWithoutLeader, got.Reason)
	assert.Equal(t, []string{"1300-dmc0"}, got.UnreadyShards)

	require.NotEmpty(t, f.meta.SetReplicasQueryVisible(ctx, 130),
		"the fixture must actually flip the replica visible")
	promoted := f.readiness(t, 1300, "rg-a")
	assert.True(t, promoted.Ready,
		"after visibility is flipped the very same leader must count, proving visibility was the only gate")
}

// TestShardLeaderReadinessByRG_NotHealthy asserts what actually protects this
// entry point from a Server that is still coming up: the
// merr.CheckHealthy(s.State()) gate, the same one the rest of Server's
// surface takes -- see the matching note on
// TestGetLoadPercentageByResourceGroup_NotHealthy for why the nil checks
// downstream cannot supply that ordering on their own. The Server here is
// fully wired; only its status says it is not serving yet.
func TestShardLeaderReadinessByRG_NotHealthy(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	s := &Server{meta: f.meta, targetMgr: f.targetMgr, dist: f.dist, nodeMgr: f.nodeMgr}
	require.Equal(t, commonpb.StateCode_Initializing, s.State())

	got, err := s.GetShardLeaderReadinessByResourceGroup(context.Background(), 1, "rg-a")
	assert.ErrorIs(t, err, merr.ErrServiceNotReady,
		"a coordinator restart must be distinguishable from a resource group whose shards have no leader yet -- the sibling percentage surface answers the same code for the same condition")
	assert.False(t, got.Ready)
	assert.Equal(t, utils.ShardLeadersReasonCoordinatorNotReady, got.Reason,
		"the reason stays set alongside the error, so a caller that only logs is unaffected")
}

// TestShardLeaderReadinessByRG_NilStores covers the utils-level nil guard on
// its own terms: defense in depth for a direct caller (the observers hold
// these stores and bypass Server), deliberately not reached through Server,
// whose health gate would answer first.
//
// It answers ErrServiceNotReady alongside the Reason, the same as the
// percentage surface and the Server entry points do for the same condition.
// A direct caller reading only the struct cannot otherwise tell "the
// coordinator cannot answer" -- retriable, and not about this resource group
// -- from any other not-ready verdict, all of which are about it.
func TestShardLeaderReadinessByRG_NilStores(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	ctx := context.Background()

	for name, call := range map[string]func() (utils.ShardLeaderReadiness, error){
		"nil meta": func() (utils.ShardLeaderReadiness, error) {
			return utils.ShardLeaderReadinessByResourceGroup(ctx, nil, f.targetMgr, f.dist, f.nodeMgr, 1, "rg")
		},
		"nil targetMgr": func() (utils.ShardLeaderReadiness, error) {
			return utils.ShardLeaderReadinessByResourceGroup(ctx, f.meta, nil, f.dist, f.nodeMgr, 1, "rg")
		},
		"nil dist": func() (utils.ShardLeaderReadiness, error) {
			return utils.ShardLeaderReadinessByResourceGroup(ctx, f.meta, f.targetMgr, nil, f.nodeMgr, 1, "rg")
		},
		"nil resource manager": func() (utils.ShardLeaderReadiness, error) {
			partial := &meta.Meta{CollectionManager: f.meta.CollectionManager, ReplicaManager: f.meta.ReplicaManager}
			return utils.ShardLeaderReadinessByResourceGroup(ctx, partial, f.targetMgr, f.dist, f.nodeMgr, 1, "rg")
		},
		"nil nodeMgr": func() (utils.ShardLeaderReadiness, error) {
			return utils.ShardLeaderReadinessByResourceGroup(ctx, f.meta, f.targetMgr, f.dist, nil, 1, "rg")
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				got, err := call()
				assert.ErrorIs(t, err, merr.ErrServiceNotReady,
					"the utils layer must answer the same sentinel its siblings and the Server entry points do")
				assert.False(t, got.Ready)
				assert.Equal(t, utils.ShardLeadersReasonCoordinatorNotReady, got.Reason,
					"the reason stays set alongside the error")
			})
		})
	}
}

// TestShardLeaderReadinessByRG_LeavesNativeShardLeadersUnchanged is the
// zero-behavior-change proof on the querycoord side: asking the
// per-resource-group question is a pure read, so the native, collection-wide
// shard-leader answer is byte-for-byte the same before and after, for every
// resource group asked about including ones that do not exist.
func TestShardLeaderReadinessByRG_LeavesNativeShardLeadersUnchanged(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 1100, 11000, "1100-dmc0", "1100-dmc1")
	f.putReplica(t, 1100, "rg-a", 110)
	f.putReplica(t, 1100, "rg-b", 111)
	f.putLeader(1100, 110, "1100-dmc0", true)
	f.putLeader(1100, 111, "1100-dmc1", true)

	// The native answer is built by walking a map of channels, so its order is
	// not stable across calls. Compare it keyed by channel, or this test would
	// fail on shuffling rather than on a behavior change.
	nativeShards := func() map[string]*querypb.ShardLeadersList {
		lists, err := utils.GetShardLeaders(ctx, f.meta, f.targetMgr, f.dist, f.nodeMgr, 1100, false)
		require.NoError(t, err)
		byChannel := make(map[string]*querypb.ShardLeadersList, len(lists))
		for _, list := range lists {
			byChannel[list.GetChannelName()] = list
		}
		return byChannel
	}

	before := nativeShards()
	require.Len(t, before, 2)

	for _, rg := range []string{"", "rg-a", "rg-b", "rg-does-not-exist"} {
		_, err := f.server().GetShardLeaderReadinessByResourceGroup(ctx, 1100, rg)
		if rg == "rg-does-not-exist" {
			require.ErrorIs(t, err, merr.ErrResourceGroupNotFound, "an unknown group is refused, and must still leave the native answer alone")
			continue
		}
		require.NoError(t, err)
	}

	assert.Equal(t, before, nativeShards(),
		"the per-resource-group readiness read must not disturb the native shard-leader answer")
	assert.EqualValues(t, 100, f.meta.CalculateLoadPercentage(ctx, 1100),
		"the per-resource-group readiness read must not disturb the collection's load registration")
}

// TestShardLeaderReadinessByRG_UnknownResourceGroupIsInputError pins that a
// resource group which does not exist is refused as the request's own mistake
// -- ErrResourceGroupNotFound, an InputError, with its own Reason -- rather
// than folded into NoReplicaInResourceGroup. Both mean "waiting will never
// help", but only one of them tells the caller it misspelled the group.
func TestShardLeaderReadinessByRG_UnknownResourceGroupIsInputError(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 1300, 13000, "1300-dmc0")
	f.putReplica(t, 1300, "rg-a", 130)
	f.putLeader(1300, 130, "1300-dmc0", true)

	got, err := f.server().GetShardLeaderReadinessByResourceGroup(context.Background(), 1300, "rg-typo")

	assert.ErrorIs(t, err, merr.ErrResourceGroupNotFound)
	assert.Equal(t, merr.InputError, merr.GetErrorType(err))
	assert.False(t, got.Ready)
	assert.Equal(t, utils.ShardLeadersReasonResourceGroupNotFound, got.Reason)

	// The name is checked before the collection: the same typo on a
	// collection that is not loaded at all is still the typo's fault.
	_, err = f.server().GetShardLeaderReadinessByResourceGroup(context.Background(), 999999, "rg-typo")
	assert.ErrorIs(t, err, merr.ErrResourceGroupNotFound)
}

// TestShardLeaderReadinessByRG_EmptyRGWithNoReplicaReadsNoReplica pins the
// rgName == "" wording of the no-replica verdict. The reason strings are
// compared by callers, so "no replica lives in this resource group" would be
// a false statement when no group was named: the condition is that the
// collection has no replica at all.
func TestShardLeaderReadinessByRG_EmptyRGWithNoReplicaReadsNoReplica(t *testing.T) {
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 1400, 14000, "1400-dmc0")
	// registered as loaded, but every replica record is gone

	all := f.readiness(t, 1400, "")
	assert.False(t, all.Ready)
	assert.Equal(t, utils.ShardLeadersReasonNoReplica, all.Reason,
		"with no filter the verdict is about the collection, not a group")

	f.putResourceGroup(t, "rg-empty")
	named := f.readiness(t, 1400, "rg-empty")
	assert.Equal(t, utils.ShardLeadersReasonNoReplicaInResourceGroup, named.Reason,
		"a named group keeps the group-scoped wording")
}

// TestShardLeaderReadinessByRG_UnscopedNeedsNoResourceManager is the readiness
// half of TestLoadPercentageByResourceGroup_UnscopedNeedsNoResourceManager: a
// Meta without a ResourceManager still answers the unscoped question, and only
// a named group is refused with the coordinator-not-ready verdict.
func TestShardLeaderReadinessByRG_UnscopedNeedsNoResourceManager(t *testing.T) {
	ctx := context.Background()
	f := newShardLeaderReadinessFixture(t)
	f.putLoadedCollection(t, 1600, 16000, "1600-dmc0")
	f.putReplica(t, 1600, "rg-a", 160)
	f.putLeader(1600, 160, "1600-dmc0", true)
	partial := &meta.Meta{CollectionManager: f.meta.CollectionManager, ReplicaManager: f.meta.ReplicaManager}

	all, err := utils.ShardLeaderReadinessByResourceGroup(ctx, partial, f.targetMgr, f.dist, f.nodeMgr, 1600, "")
	assert.NoError(t, err, "the unscoped form never consults the resource manager")
	assert.True(t, all.Ready)

	scoped, err := utils.ShardLeaderReadinessByResourceGroup(ctx, partial, f.targetMgr, f.dist, f.nodeMgr, 1600, "rg-a")
	assert.ErrorIs(t, err, merr.ErrServiceNotReady)
	assert.False(t, scoped.Ready)
	assert.Equal(t, utils.ShardLeadersReasonCoordinatorNotReady, scoped.Reason)
}
