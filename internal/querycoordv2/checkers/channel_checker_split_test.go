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

package checkers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func servingChannel(name string, node int64) *meta.DmChannel {
	return &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: name},
		Node:         node,
		Version:      1,
		View: &meta.LeaderView{
			ID: node, Channel: name, Version: 1,
			Status: &querypb.LeaderViewStatus{Serviceable: true},
		},
	}
}

// A collection whose split has been adopted: the post-image names only the
// targets, so the retired source v0 is delisted. A listed Dropped shard is
// refused by every routing commit, so delisting is the only way a source goes.
func delistedSplitResp() *milvuspb.DescribeCollectionResponse {
	return &milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v1", "v2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardNormal, VchannelName: "v1"},
			{State: schemapb.ShardState_ShardNormal, VchannelName: "v2"},
		},
	}
}

// releasedSourcesAfter runs one round of the diff over a collection whose
// retired source v0 and both targets are loaded and serving, and reports which
// channels it chose to release.
func releasedSourcesAfter(t *testing.T, adopted *milvuspb.DescribeCollectionResponse, currentTarget map[string]*meta.DmChannel) []string {
	next := map[string]*meta.DmChannel{"v1": servingChannel("v1", 1), "v2": servingChannel("v2", 1)}
	_, released := channelDiff(t, adopted, next, currentTarget, "v0", "v1", "v2")
	return released
}

// channelDiff runs one round of the channel checker's diff for collection 1,
// replica 1 on node 1, with the given shard states, targets and delegators in
// dist, and reports the channels it chose to load and to release.
func channelDiff(t *testing.T, states *milvuspb.DescribeCollectionResponse,
	nextTarget, currentTarget map[string]*meta.DmChannel, inDist ...string,
) (loaded, released []string) {
	return channelDiffWith(t, func(broker *meta.MockBroker) {
		broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).Return(states, nil).Maybe()
	}, nil, nextTarget, currentTarget, inDist...)
}

// channelDiffWith is channelDiff with the broker's DescribeCollection set up by
// describe, and the next target marking window as its split window targets.
func channelDiffWith(t *testing.T, describe func(*meta.MockBroker), window typeutil.Set[string],
	nextTarget, currentTarget map[string]*meta.DmChannel, inDist ...string,
) (loaded, released []string) {
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: 1, Address: "localhost", Hostname: "localhost",
	}))
	catalog := catalogmocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Maybe()
	m := meta.NewMeta(RandomIncrementIDAllocator(), catalog, nodeMgr)
	ctx := context.Background()
	require.NoError(t, m.PutCollection(ctx, utils.CreateTestCollection(1, 1)))
	require.NoError(t, m.Put(ctx, utils.CreateTestReplica(1, 1, []int64{1})))

	dist := meta.NewDistributionManager(nodeMgr)
	delegators := make([]*meta.DmChannel, 0, len(inDist))
	for _, name := range inDist {
		delegators = append(delegators, servingChannel(name, 1))
	}
	dist.ChannelDistManager.Update(1, delegators...)

	targetMgr := meta.NewMockTargetManager(t)
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), meta.NextTarget).Return(nextTarget).Maybe()
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), meta.CurrentTarget).Return(currentTarget).Maybe()
	targetMgr.EXPECT().GetSplitWindowTargets(mock.Anything, int64(1), meta.NextTarget).Return(window).Maybe()

	broker := meta.NewMockBroker(t)
	describe(broker)

	// The checker's constructor reaches for the global assign policy factory.
	scheduler := task.NewMockScheduler(t)
	assign.InitGlobalAssignPolicyFactory(scheduler, nodeMgr, dist, m, targetMgr)
	t.Cleanup(assign.ResetGlobalAssignPolicyFactoryForTest)

	checker := NewChannelChecker(m, dist, targetMgr, nodeMgr, scheduler,
		meta.NewShardSplitStateCache(broker, time.Minute))

	toLoad, toRelease := checker.getDmChannelDiff(ctx, 1, 1)
	for _, ch := range toLoad {
		loaded = append(loaded, ch.GetChannelName())
	}
	for _, ch := range toRelease {
		released = append(released, ch.GetChannelName())
	}
	return loaded, released
}

// A split target still Creating is fronted in process by its source's
// delegator, so querycoord must not watch it: that would build a second,
// fresh delegator for the channel. Once adoption makes it Normal it is
// watched like any other channel of the next target.
func TestACreatingSplitTargetIsNotWatchedUntilAdopted(t *testing.T) {
	next := map[string]*meta.DmChannel{
		"v0": servingChannel("v0", 1),
		"v1": servingChannel("v1", 1),
		"v2": servingChannel("v2", 1),
	}
	current := map[string]*meta.DmChannel{"v0": servingChannel("v0", 1)}
	window := &milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v0", "v1", "v2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardSplitting},
			{State: schemapb.ShardState_ShardCreating},
			{State: schemapb.ShardState_ShardCreating},
		},
	}
	loaded, released := channelDiff(t, window, next, current, "v0")
	assert.Empty(t, loaded, "a Creating split target must not be watched")
	assert.Empty(t, released)

	loaded, released = channelDiff(t, delistedSplitResp(), next, current, "v0")
	assert.ElementsMatch(t, []string{"v1", "v2"}, loaded, "an adopted target is watched")
	assert.Empty(t, released)
}

// No split-state rule names a delisted source. The release order holds by the
// checker's plain rule: a channel is released only once it is in neither the
// next nor the current target, and the current target advances past the source
// only after every target has a serviceable delegator. GetShardLeaders
// enumerates the current target, so releasing the source while it is still
// listed there would fail every read of the collection until the flip.
func TestADelistedSourceIsHeldWhileReadsStillRouteToIt(t *testing.T) {
	current := map[string]*meta.DmChannel{"v0": servingChannel("v0", 1)}
	assert.NotContains(t, releasedSourcesAfter(t, delistedSplitResp(), current), "v0",
		"the delisted source must stay loaded while the current target still lists it")
}

func TestADelistedSourceIsReleasedOnceTheCurrentTargetDropsIt(t *testing.T) {
	current := map[string]*meta.DmChannel{
		"v1": servingChannel("v1", 1),
		"v2": servingChannel("v2", 1),
	}
	assert.Equal(t, []string{"v0"}, releasedSourcesAfter(t, delistedSplitResp(), current))
}

// C1: between adoption and the window-end re-pull the next target is still the
// window snapshot, which lists the retired source v0 next to its targets, and
// the current target still lists v0. If the source's node stops then, v0 drops
// out of dist. Re-watching it would rebuild the source delegator without its
// in-process children -- adoption delisted it, so the rebuild re-derives no
// target to front -- and the current target would keep routing reads to that
// childless source until the flip. A channel the collection no longer lists is
// never watched.
func TestADelistedSourceIsNotReWatchedAfterItsNodeStops(t *testing.T) {
	window := map[string]*meta.DmChannel{
		"v0": servingChannel("v0", 1),
		"v1": servingChannel("v1", 1),
		"v2": servingChannel("v2", 1),
	}
	current := map[string]*meta.DmChannel{"v0": servingChannel("v0", 1)}

	// the source's node stopped: nothing of the collection is in dist.
	loaded, released := channelDiff(t, delistedSplitResp(), window, current)
	assert.NotContains(t, loaded, "v0", "a delisted source must never be re-watched")
	assert.ElementsMatch(t, []string{"v1", "v2"}, loaded)
	assert.Empty(t, released)
}

// I2: the next target pulled just after a fence lists the new targets, while
// the cached shard states were read just before it and do not list them yet.
// The window mark, taken from a fresh read before that same pull, catches the
// targets either way.
func TestAFenceRaceWithAStaleCacheDoesNotWatchTheTargets(t *testing.T) {
	window := map[string]*meta.DmChannel{
		"v0": servingChannel("v0", 1),
		"v1": servingChannel("v1", 1),
		"v2": servingChannel("v2", 1),
	}
	current := map[string]*meta.DmChannel{"v0": servingChannel("v0", 1)}
	preFence := func(broker *meta.MockBroker) {
		broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"v0"},
			ShardInfos:          []*schemapb.CollectionShardInfo{{State: schemapb.ShardState_ShardNormal}},
		}, nil).Maybe()
	}
	loaded, released := channelDiffWith(t, preFence, typeutil.NewSet("v1", "v2"), window, current, "v0")
	assert.Empty(t, loaded)
	assert.Empty(t, released)
}

// I2: a target adopted while the next target is still the window snapshot is
// watched only from the pull taken after the adoption. The window snapshot
// attributes the target's flushed data to its source and carries no seek or
// segment of the target's own, so the shard states calling it Normal is not
// enough.
func TestAnAdoptedTargetIsNotWatchedFromTheWindowSnapshot(t *testing.T) {
	window := map[string]*meta.DmChannel{
		"v0": servingChannel("v0", 1),
		"v1": servingChannel("v1", 1),
		"v2": servingChannel("v2", 1),
	}
	current := map[string]*meta.DmChannel{"v0": servingChannel("v0", 1)}
	adopted := func(broker *meta.MockBroker) {
		broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).Return(delistedSplitResp(), nil).Maybe()
	}
	loaded, _ := channelDiffWith(t, adopted, typeutil.NewSet("v1", "v2"), window, current, "v0")
	assert.Empty(t, loaded, "a window target is watched only from a pull that no longer marks it")

	repulled := map[string]*meta.DmChannel{"v1": servingChannel("v1", 1), "v2": servingChannel("v2", 1)}
	loaded, _ = channelDiffWith(t, adopted, nil, repulled, current, "v0")
	assert.ElementsMatch(t, []string{"v1", "v2"}, loaded)
}

// I2: after a querycoord restart the cache is empty, and while the coordinator
// cannot describe the collection nothing tells a not-yet-adopted target from
// any other channel. Nothing of the collection is watched until it can: the
// watch itself describes the collection first, so it could not succeed anyway.
func TestNothingIsWatchedWhileTheShardStatesAreUnknown(t *testing.T) {
	next := map[string]*meta.DmChannel{
		"v0": servingChannel("v0", 1),
		"v1": servingChannel("v1", 1),
	}
	describeFails := func(broker *meta.MockBroker) {
		broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).
			Return(nil, merr.WrapErrServiceUnavailable("rootcoord not ready")).Maybe()
	}
	loaded, released := channelDiffWith(t, describeFails, nil, next, nil)
	assert.Empty(t, loaded)
	assert.Empty(t, released)
}
