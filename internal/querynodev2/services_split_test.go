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

package querynodev2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSpawnSplitChildIdempotent(t *testing.T) {
	node := &QueryNode{
		ctx:        context.Background(),
		delegators: typeutil.NewConcurrentMap[string, delegator.ShardDelegator](),
	}
	existing := delegator.NewMockShardDelegator(t)
	node.delegators.Insert("v1", existing)

	// the target already has a registered child: return it without touching the
	// coordinator, so a re-consume of the fence never double-spawns.
	got, err := node.SpawnSplitChild(context.Background(), delegator.SpawnChildParams{
		CollectionID: 1,
		Target:       &messagespb.SplitShardTarget{Vchannel: "v1"},
	})
	assert.NoError(t, err)
	assert.Equal(t, existing, got)
}

func TestWaitSplitTargetRecovery(t *testing.T) {
	t.Run("retries until the target appears with a seek position", func(t *testing.T) {
		mixCoord := mocks.NewMockMixCoordClient(t)
		// first round: the target vchannel is not in the recovery info yet.
		mixCoord.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponseV2{
			Status: merr.Success(),
		}, nil).Once()
		// second round: the target is created and its channel checkpoint seeded.
		mixCoord.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponseV2{
			Status: merr.Success(),
			Channels: []*datapb.VchannelInfo{{
				ChannelName:  "v1",
				SeekPosition: &msgpb.MsgPosition{Timestamp: 42, MsgID: []byte{1}},
			}},
		}, nil).Once()

		future := syncutil.NewFuture[types.MixCoordClient]()
		future.Set(mixCoord)
		node := &QueryNode{ctx: context.Background(), mixCoord: future}

		pos, err := node.waitSplitTargetRecovery(1, "v1")
		assert.NoError(t, err)
		assert.Equal(t, uint64(42), pos.GetTimestamp())
	})

	t.Run("keeps waiting while the position cannot be seeked from", func(t *testing.T) {
		// A vchannel created moments ago has no checkpoint, so datacoord falls
		// back to the earliest segment's DML position -- on a target nothing has
		// been written to that carries neither a message ID nor a WAL name.
		// Accepting it builds a dispatcher that skips the seek, and the first
		// read then panics the whole querynode.
		mixCoord := mocks.NewMockMixCoordClient(t)
		mixCoord.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponseV2{
			Status: merr.Success(),
			Channels: []*datapb.VchannelInfo{{
				ChannelName:  "v1",
				SeekPosition: &msgpb.MsgPosition{Timestamp: 42},
			}},
		}, nil).Once()
		mixCoord.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(&datapb.GetRecoveryInfoResponseV2{
			Status: merr.Success(),
			Channels: []*datapb.VchannelInfo{{
				ChannelName:  "v1",
				SeekPosition: &msgpb.MsgPosition{Timestamp: 99, MsgID: []byte{7}},
			}},
		}, nil).Once()

		future := syncutil.NewFuture[types.MixCoordClient]()
		future.Set(mixCoord)
		node := &QueryNode{ctx: context.Background(), mixCoord: future}

		pos, err := node.waitSplitTargetRecovery(1, "v1")
		assert.NoError(t, err)
		assert.Equal(t, uint64(99), pos.GetTimestamp(),
			"the unseekable position must be skipped, not returned")
	})
}

func TestRespawnSplitChildrenOnRecovery(t *testing.T) {
	makeNode := func(mc types.MixCoordClient) *QueryNode {
		future := syncutil.NewFuture[types.MixCoordClient]()
		future.Set(mc)
		return &QueryNode{ctx: context.Background(), mixCoord: future}
	}
	// vchannels src/t1/t2 with the given per-shard states.
	descResp := func(states ...schemapb.ShardState) *milvuspb.DescribeCollectionResponse {
		infos := make([]*schemapb.CollectionShardInfo, len(states))
		for i, s := range states {
			infos[i] = &schemapb.CollectionShardInfo{State: s}
		}
		return &milvuspb.DescribeCollectionResponse{
			Status:              merr.Success(),
			VirtualChannelNames: []string{"src", "t1", "t2"},
			ShardInfos:          infos,
		}
	}

	t.Run("respawns the creating targets when the source is splitting", func(t *testing.T) {
		mc := mocks.NewMockMixCoordClient(t)
		mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(
			descResp(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating), nil)
		source := delegator.NewMockShardDelegator(t)
		var got []string
		source.EXPECT().ProcessSplitShard(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, targets []*messagespb.SplitShardTarget) error {
				for _, tg := range targets {
					got = append(got, tg.GetVchannel())
				}
				return nil
			})

		makeNode(mc).respawnSplitChildrenOnRecovery(context.Background(), source, 1, "src")
		// only the not-yet-adopted (Creating) targets are re-fronted.
		assert.ElementsMatch(t, []string{"t1", "t2"}, got)
	})

	t.Run("refuses to guess the fronting host under concurrent splits", func(t *testing.T) {
		mc := mocks.NewMockMixCoordClient(t)
		// Two shards split at once. Which source fronts which target is the
		// coordinator's choice and lives in the split task, so it is not
		// derivable from a DescribeCollection -- and guessing would let two
		// sources front one target and return its rows twice.
		mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			Status:              merr.Success(),
			VirtualChannelNames: []string{"src", "other", "t1", "t2", "o1"},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardSplitting},
				{State: schemapb.ShardState_ShardSplitting},
				{State: schemapb.ShardState_ShardCreating},
				{State: schemapb.ShardState_ShardCreating},
				{State: schemapb.ShardState_ShardCreating},
			},
		}, nil)
		source := delegator.NewMockShardDelegator(t)
		// ProcessSplitShard is never set up, so the mock fails the test if the
		// rebuild fronts anything at all. The targets stay unfronted until they
		// are adopted, which reads as a channel not yet serving -- never as rows
		// returned twice.
		makeNode(mc).respawnSplitChildrenOnRecovery(context.Background(), source, 1, "src")
	})

	t.Run("no-op when the source is not splitting", func(t *testing.T) {
		mc := mocks.NewMockMixCoordClient(t)
		mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(
			descResp(schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardNormal), nil)
		source := delegator.NewMockShardDelegator(t)
		// ProcessSplitShard is never set up, so the mock fails the test if it is called.
		makeNode(mc).respawnSplitChildrenOnRecovery(context.Background(), source, 1, "src")
	})
}

// With one splitting source the fronting choice is forced -- every Creating
// target is fronted by it -- so the rebuild is exact and needs no provenance.
func TestRespawnSplitChildrenFrontsEveryTargetOfTheOnlySource(t *testing.T) {
	mc := mocks.NewMockMixCoordClient(t)
	mc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:              merr.Success(),
		VirtualChannelNames: []string{"src", "normal", "t0", "t1", "t2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardSplitting},
			{State: schemapb.ShardState_ShardNormal},
			{State: schemapb.ShardState_ShardCreating},
			{State: schemapb.ShardState_ShardCreating},
			{State: schemapb.ShardState_ShardCreating},
		},
	}, nil)

	source := delegator.NewMockShardDelegator(t)
	var got []string
	source.EXPECT().ProcessSplitShard(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, targets []*messagespb.SplitShardTarget) error {
			for _, tg := range targets {
				got = append(got, tg.GetVchannel())
			}
			return nil
		})

	future := syncutil.NewFuture[types.MixCoordClient]()
	future.Set(mc)
	node := &QueryNode{ctx: context.Background(), mixCoord: future}
	node.respawnSplitChildrenOnRecovery(context.Background(), source, 1, "src")

	// Every Creating target, and only those: the Normal shard is not a target.
	assert.ElementsMatch(t, []string{"t0", "t1", "t2"}, got)
}

// adoptionNode builds the smallest QueryNode that can reach the adoption branch
// of WatchDmChannels.
func adoptionNode() *QueryNode {
	node := &QueryNode{
		ctx:                   context.Background(),
		lifetime:              lifetime.NewLifetime(commonpb.StateCode_Healthy),
		delegators:            typeutil.NewConcurrentMap[string, delegator.ShardDelegator](),
		subscribingChannels:   typeutil.NewConcurrentSet[string](),
		unsubscribingChannels: typeutil.NewConcurrentSet[string](),
		distDeltaTracker:      newDataDistributionDeltaTracker(),
	}
	return node
}

func adoptionRequest(channel string) *querypb.WatchDmChannelsRequest {
	return &querypb.WatchDmChannelsRequest{
		CollectionID:  1,
		Infos:         []*datapb.VchannelInfo{{CollectionID: 1, ChannelName: channel}},
		Schema:        &schemapb.CollectionSchema{Name: "adoption"},
		IndexInfoList: []*indexpb.IndexInfo{{CollectionID: 1}},
	}
}

func (c *dataDistributionDeltaTracker) isChannelDirty(channel string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.dirtyChannels[channel]
	return ok
}

func TestAdoptingASplitChildMakesItVisibleToQuerycoord(t *testing.T) {
	// Lifting the visibility flag is not enough: the distribution report is a
	// DELTA, so a channel reaches querycoord only if the tracker was told it
	// changed. A child is deliberately never marked while un-adopted, so without
	// a mark at adoption it stays out of every delta report and querycoord never
	// learns the channel exists -- which freezes the collection's current target
	// (it advances only once every next-target channel has a delegator in dist),
	// and a frozen current target is why a partition created after the split was
	// never loaded and the retired source was never released.
	node := adoptionNode()
	child := delegator.NewMockShardDelegator(t)
	child.EXPECT().IsUnadoptedSplitChild().Return(true).Once()
	child.EXPECT().MarkAdopted().Once()
	node.delegators.Insert("v1", child)

	require.False(t, node.distDeltaTracker.isChannelDirty("v1"))

	status, err := node.WatchDmChannels(context.Background(), adoptionRequest("v1"))
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
	assert.True(t, node.distDeltaTracker.isChannelDirty("v1"),
		"an adopted child must enter the next delta report, or querycoord never sees it")
}

func TestReWatchingAnAlreadyAdoptedChannelChangesNothing(t *testing.T) {
	// The watch is retried until querycoord sees the channel in dist, so the
	// adoption branch runs many times. Only the first one is an adoption; the
	// rest must not re-mark or re-promote.
	node := adoptionNode()
	child := delegator.NewMockShardDelegator(t)
	child.EXPECT().IsUnadoptedSplitChild().Return(false).Once()
	node.delegators.Insert("v1", child)

	status, err := node.WatchDmChannels(context.Background(), adoptionRequest("v1"))
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
	assert.False(t, node.distDeltaTracker.isChannelDirty("v1"))
}
