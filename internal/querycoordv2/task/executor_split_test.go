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

package task

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// subscribeWith runs one subscribe of channel on node 1 against the given
// DescribeCollection answer. The cluster mock expects no call at all, so a
// subscribe that reaches WatchDmChannels fails the test.
func subscribeWith(t *testing.T, describe *milvuspb.DescribeCollectionResponse, channel string) error {
	broker := meta.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).Return(describe, nil)
	ex := NewExecutor(1, nil, nil, broker, nil, session.NewMockCluster(t), nil)

	channelTask, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(0), 1,
		meta.NilReplica, NewChannelAction(1, ActionTypeGrow, channel))
	require.NoError(t, err)
	defer channelTask.Cancel(nil)
	return ex.subscribeChannel(channelTask, 0)
}

// C1: the channel checker decides what to watch from a shard-state view that
// may be seconds old, so a retired split source can still look listed to it
// right after adoption. The executor re-reads the collection just before the
// watch, and a vchannel it no longer lists is never watched: a rebuilt source
// has no children to front, and would serve its key range without them.
func TestSubscribeRefusesAChannelTheCollectionNoLongerLists(t *testing.T) {
	adopted := &milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v1", "v2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardNormal},
			{State: schemapb.ShardState_ShardNormal},
		},
	}
	err := subscribeWith(t, adopted, "v0")
	assert.ErrorIs(t, err, merr.ErrChannelNotFound)
}

// I2: the channel checker's shard-state view may be seconds old. The executor's
// fresh describe refuses a split target that is still Creating, which only its
// source's delegator may serve until adoption.
func TestSubscribeRefusesASplitTargetNotYetAdopted(t *testing.T) {
	window := &milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v0", "v1"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardSplitting},
			{State: schemapb.ShardState_ShardCreating},
		},
	}
	err := subscribeWith(t, window, "v1")
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
}

// splitWindowDescribe is a collection mid split: v0 is the fenced source, v1
// and v2 its not-yet-adopted targets, v9 a shard the split does not touch.
func splitWindowDescribe() *milvuspb.DescribeCollectionResponse {
	return &milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v0", "v1", "v2", "v9"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardSplitting},
			{State: schemapb.ShardState_ShardCreating},
			{State: schemapb.ShardState_ShardCreating},
			{State: schemapb.ShardState_ShardNormal},
		},
	}
}

// moveWith runs the Grow step of a move of channel from node 2 to node 1,
// with the channel's delegator currently on node 2 of the task's replica.
func moveWith(t *testing.T, describe *milvuspb.DescribeCollectionResponse, channel string) error {
	broker := meta.NewMockBroker(t)
	broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).Return(describe, nil)
	targetMgr := meta.NewMockTargetManager(t)
	targetMgr.EXPECT().GetSplitWindowTargets(mock.Anything, int64(1), meta.NextTarget).Return(nil).Maybe()
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), mock.Anything).Return(map[string]*meta.DmChannel{
		"v0": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v0"}},
		"v9": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v9"}},
	}).Maybe()
	dist := meta.NewDistributionManager(session.NewNodeManager())
	dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: channel},
		Node:         2,
		View:         &meta.LeaderView{ID: 2, CollectionID: 1, Channel: channel},
	})
	replica := meta.NewReplica(&querypb.Replica{ID: 10, CollectionID: 1, Nodes: []int64{1, 2}}, typeutil.NewUniqueSet(1, 2))
	ex := NewExecutor(1, nil, dist, broker, targetMgr, session.NewMockCluster(t), nil)

	channelTask, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(0), 1, replica,
		NewChannelAction(1, ActionTypeGrow, channel), NewChannelAction(2, ActionTypeReduce, channel))
	require.NoError(t, err)
	defer channelTask.Cancel(nil)
	return ex.subscribeChannel(channelTask, 0)
}

// AV-L6-H1: the balance freeze is decided from a shard-state view that may be
// seconds old, so a move of a fenced split source can reach the executor. A
// rebuilt source on another node has no in-process children, so nobody would
// serve the targets' writes after the fence. The executor's fresh describe
// refuses such a move, retriably, before anything is watched.
func TestSubscribeRefusesMovingAFencedSplitSource(t *testing.T) {
	err := moveWith(t, splitWindowDescribe(), "v0")
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.True(t, merr.IsRetryableErr(err))
}

// A re-watch of a fenced source that no node of the replica serves any more is
// recovery, not a move: it is left to the shard's own recovery path.
func TestShardSplitMoveCheckAllowsRecoveryAndUnrelatedMoves(t *testing.T) {
	states := meta.ShardStatesOf(splitWindowDescribe())
	targetMgr := meta.NewMockTargetManager(t)
	targetMgr.EXPECT().GetSplitWindowTargets(mock.Anything, int64(1), meta.NextTarget).Return(nil).Maybe()
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), mock.Anything).Return(map[string]*meta.DmChannel{
		"v0": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v0"}},
		"v9": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v9"}},
	}).Maybe()
	dist := meta.NewDistributionManager(session.NewNodeManager())
	dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v9"},
		Node:         2,
		View:         &meta.LeaderView{ID: 2, CollectionID: 1, Channel: "v9"},
	})
	replica := meta.NewReplica(&querypb.Replica{ID: 10, CollectionID: 1, Nodes: []int64{1, 2}}, typeutil.NewUniqueSet(1, 2))
	ex := NewExecutor(1, nil, dist, nil, targetMgr, nil, nil)
	grow := func(channel string) *ChannelTask {
		channelTask, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(0), 1, replica,
			NewChannelAction(1, ActionTypeGrow, channel))
		require.NoError(t, err)
		t.Cleanup(func() { channelTask.Cancel(nil) })
		return channelTask
	}

	assert.NoError(t, ex.checkShardSplitMove(context.Background(), grow("v0"), 1, "v0", states),
		"no node of the replica serves the source: a watch is recovery")
	assert.NoError(t, ex.checkShardSplitMove(context.Background(), grow("v9"), 1, "v9", states),
		"a shard the split does not touch moves freely")
}

// RR-L6-N1: the distribution can miss a live delegator -- a first pull after a
// QueryCoord restart that failed, for one. A split family channel missing from
// the replica's distribution counts as recovery only once every node of the
// replica still in the node manager has had a distribution pull succeed; until
// then the "missing" source may still be served, with its children, by a node
// QueryCoord has not heard from yet.
func TestShardSplitMoveCheckTrustsOnlyAPulledDistribution(t *testing.T) {
	states := meta.ShardStatesOf(splitWindowDescribe())
	targetMgr := meta.NewMockTargetManager(t)
	targetMgr.EXPECT().GetSplitWindowTargets(mock.Anything, int64(1), meta.NextTarget).Return(nil).Maybe()
	targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), mock.Anything).Return(map[string]*meta.DmChannel{
		"v0": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v0"}},
		"v9": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v9"}},
	}).Maybe()
	replica := meta.NewReplica(&querypb.Replica{ID: 10, CollectionID: 1, Nodes: []int64{1, 2}}, typeutil.NewUniqueSet(1, 2))

	check := func(t *testing.T, channel string, node2 func(*session.NodeManager)) error {
		nodeMgr := session.NewNodeManager()
		node1 := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 1})
		node1.SetLastHeartbeat(time.Now())
		nodeMgr.Add(node1)
		node2(nodeMgr)
		dist := meta.NewDistributionManager(nodeMgr)
		ex := NewExecutor(1, nil, dist, nil, targetMgr, nil, nodeMgr)
		channelTask, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(0), 1, replica,
			NewChannelAction(1, ActionTypeGrow, channel))
		require.NoError(t, err)
		defer channelTask.Cancel(nil)
		return ex.checkShardSplitMove(context.Background(), channelTask, 1, channel, states)
	}
	neverPulled := func(nodeMgr *session.NodeManager) {
		nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}))
	}
	pulled := func(nodeMgr *session.NodeManager) {
		node := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2})
		node.SetLastHeartbeat(time.Now())
		nodeMgr.Add(node)
	}
	gone := func(*session.NodeManager) {}

	err := check(t, "v0", neverPulled)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable, "node 2 may still serve the source")
	assert.True(t, merr.IsRetryableErr(err))
	assert.NoError(t, check(t, "v0", pulled), "every node reported: the source is truly unserved")
	assert.NoError(t, check(t, "v0", gone), "the previous holder is gone from the node manager")
	assert.NoError(t, check(t, "v9", neverPulled), "a shard the split does not touch is not held back")
}

// The VchannelInfo a QueryNode is watched with is the next target's, and a
// next target recovered after a QueryCoord restart must still carry the split
// signal DataCoord reported with its seek position.
func TestWatchRequestCarriesTheSplitSignalOfARestoredTarget(t *testing.T) {
	restored := meta.FromPbCollectionTarget(&querypb.CollectionTarget{
		CollectionID: 1,
		ChannelTargets: []*querypb.ChannelTarget{{
			ChannelName:         "src",
			SeekPosition:        &msgpb.MsgPosition{ChannelName: "src", Timestamp: 200},
			SplitTargetChannels: []string{"t1", "t2"},
		}},
	})
	channelTask, err := NewChannelTask(context.Background(), time.Minute, WrapIDSource(0), 1, meta.NilReplica,
		NewChannelAction(1, ActionTypeGrow, "src"))
	require.NoError(t, err)
	defer channelTask.Cancel(nil)

	req := packSubChannelRequest(channelTask, channelTask.Actions()[0], &schemapb.CollectionSchema{}, nil, &querypb.LoadMetaInfo{},
		restored.GetAllDmChannels()["src"], nil, nil, 1)
	require.Len(t, req.GetInfos(), 1)
	assert.Equal(t, []string{"t1", "t2"}, req.GetInfos()[0].GetSplitTargetChannels())
	assert.Equal(t, uint64(200), req.GetInfos()[0].GetSeekPosition().GetTimestamp())
}
