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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func splittingCollectionResp() *milvuspb.DescribeCollectionResponse {
	return &milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v0", "v1", "v2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardSplitting}, // v0 is the fenced source
			{State: schemapb.ShardState_ShardCreating},  // v1 split target
			{State: schemapb.ShardState_ShardCreating},  // v2 split target
		},
	}
}

func TestShardSplitStateCache(t *testing.T) {
	ctx := context.Background()

	t.Run("reports the splitting source channels and caches within the TTL", func(t *testing.T) {
		broker := NewMockBroker(t)
		// only one DescribeCollection despite several queries: the TTL caches it.
		broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).Return(splittingCollectionResp(), nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		assert.True(t, cache.IsShardSplitting(ctx, 1))
		assert.Equal(t, []string{"v0"}, cache.SplittingSourceChannels(ctx, 1))
		assert.Equal(t, []string{"v0"}, cache.SplittingSourceChannels(ctx, 1))
	})

	t.Run("a collection with no splitting shard is not frozen", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(2)).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"v0", "v1"},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardNormal},
				{State: schemapb.ShardState_ShardNormal},
			},
		}, nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		assert.False(t, cache.IsShardSplitting(ctx, 2))
		assert.Empty(t, cache.SplittingSourceChannels(ctx, 2))
	})

	t.Run("a transient error falls back to the last known value", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(3)).Return(splittingCollectionResp(), nil).Once()
		broker.EXPECT().DescribeCollection(mock.Anything, int64(3)).Return(nil, errors.New("coord down")).Once()
		cache := NewShardSplitStateCache(broker, 0) // TTL 0 forces a refetch every query

		assert.Equal(t, []string{"v0"}, cache.SplittingSourceChannels(ctx, 3))
		// the refetch errors; the stale source set is served instead of flapping off.
		assert.Equal(t, []string{"v0"}, cache.SplittingSourceChannels(ctx, 3))
	})

	t.Run("reports sources and creating targets by state", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(5)).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"src", "t1", "t2", "other"},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardSplitting}, // src: fenced source
				{State: schemapb.ShardState_ShardCreating},  // t1: not-yet-adopted target
				{State: schemapb.ShardState_ShardCreating},  // t2: not-yet-adopted target
				{State: schemapb.ShardState_ShardNormal},    // other: untouched shard
			},
		}, nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		assert.Equal(t, []string{"src"}, cache.SplittingSourceChannels(ctx, 5))
		assert.ElementsMatch(t, []string{"t1", "t2"}, cache.CreatingTargetChannels(ctx, 5))
	})

	t.Run("adoption lifts the freeze", func(t *testing.T) {
		broker := NewMockBroker(t)
		// adoption makes the targets Normal and delists the source in one commit.
		broker.EXPECT().DescribeCollection(mock.Anything, int64(6)).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"t1", "t2"},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardNormal},
				{State: schemapb.ShardState_ShardNormal},
			},
		}, nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		assert.False(t, cache.IsShardSplitting(ctx, 6))
		assert.Empty(t, cache.CreatingTargetChannels(ctx, 6))
	})

	t.Run("ReadShardStates reads past the TTL and refreshes the cache", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(8)).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"v0"},
			ShardInfos:          []*schemapb.CollectionShardInfo{{State: schemapb.ShardState_ShardNormal}},
		}, nil).Once()
		broker.EXPECT().DescribeCollection(mock.Anything, int64(8)).Return(splittingCollectionResp(), nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		assert.Empty(t, cache.CreatingTargetChannels(ctx, 8))
		// a fresh read ignores the still-valid cached entry.
		states, err := cache.ReadShardStates(ctx, 8)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"v1", "v2"}, states.SplitWindowTargets([]string{"v0", "v1", "v2"}))
		// and later cached queries see what it read.
		assert.ElementsMatch(t, []string{"v1", "v2"}, cache.CreatingTargetChannels(ctx, 8))
	})

	t.Run("ReadShardStates falls back to the last cached read when the fresh read fails", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(9)).Return(splittingCollectionResp(), nil).Once()
		broker.EXPECT().DescribeCollection(mock.Anything, int64(9)).Return(nil, errors.New("coord down")).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		assert.NotEmpty(t, cache.CreatingTargetChannels(ctx, 9))
		states, err := cache.ReadShardStates(ctx, 9)
		assert.NoError(t, err)
		// the cached read's complement: v1, v2 Creating, and v3 never listed.
		assert.ElementsMatch(t, []string{"v1", "v2", "v3"}, states.SplitWindowTargets([]string{"v0", "v1", "v2", "v3"}))
	})

	t.Run("ReadShardStates reports the error when nothing is cached", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(12)).Return(nil, errors.New("coord down")).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		states, err := cache.ReadShardStates(ctx, 12)
		assert.Error(t, err)
		assert.Nil(t, states)
	})

	t.Run("SplitWindowTargets marks every pulled channel the read did not see settled", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(10)).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"normal", "splitting", "dropped", "creating"},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardNormal},
				{State: schemapb.ShardState_ShardSplitting},
				{State: schemapb.ShardState_ShardDropped},
				{State: schemapb.ShardState_ShardCreating},
			},
		}, nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		states, err := cache.ReadShardStates(ctx, 10)
		assert.NoError(t, err)
		// Normal, Splitting and Dropped never return to Creating; a Creating
		// channel and one the read never listed (fenced after it) are marked.
		assert.ElementsMatch(t, []string{"creating", "fenced"},
			states.SplitWindowTargets([]string{"normal", "splitting", "dropped", "creating", "fenced"}))
		assert.Empty(t, states.SplitWindowTargets([]string{"normal", "splitting"}))
		assert.Empty(t, states.SplitWindowTargets(nil))
	})

	t.Run("a listed vchannel without a shard info is a Normal legacy shard", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(11)).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"a", "b"},
			ShardInfos:          []*schemapb.CollectionShardInfo{{State: schemapb.ShardState_ShardCreating}},
		}, nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		states, err := cache.ReadShardStates(ctx, 11)
		assert.NoError(t, err)
		assert.Equal(t, []string{"a"}, states.SplitWindowTargets([]string{"a", "b"}))
		assert.Equal(t, []string{"a"}, cache.CreatingTargetChannels(ctx, 11))
		assert.False(t, cache.IsShardSplitting(ctx, 11))
	})

	t.Run("CreatingTargetChannelsAsOf reports not-known for an entry that predates the pull", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(13)).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"src"},
			ShardInfos:          []*schemapb.CollectionShardInfo{{State: schemapb.ShardState_ShardNormal}},
		}, nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		// populate the cache with a pre-fence read.
		assert.Empty(t, cache.CreatingTargetChannels(ctx, 13))

		// the pull this entry is asked about happened strictly after that read.
		channels, ok := cache.CreatingTargetChannelsAsOf(ctx, 13, time.Now().Add(time.Millisecond))
		assert.False(t, ok)
		assert.Nil(t, channels)
	})

	t.Run("CreatingTargetChannelsAsOf reports the creating channels for an entry no older than the pull", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(14)).Return(splittingCollectionResp(), nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		before := time.Now()
		channels, ok := cache.CreatingTargetChannelsAsOf(ctx, 14, before)
		assert.True(t, ok)
		assert.ElementsMatch(t, []string{"v1", "v2"}, channels)
	})

	t.Run("CreatingTargetChannelsAsOf reports not-known when nothing is cached", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(15)).Return(nil, errors.New("coord down")).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		channels, ok := cache.CreatingTargetChannelsAsOf(ctx, 15, time.Time{})
		assert.False(t, ok)
		assert.Nil(t, channels)
	})

	t.Run("ChannelStates returns one read of every channel's state", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(16)).Return(splittingCollectionResp(), nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		states, ok := cache.ChannelStates(ctx, 16)
		assert.True(t, ok)
		assert.Equal(t, ShardStates{
			"v0": schemapb.ShardState_ShardSplitting,
			"v1": schemapb.ShardState_ShardCreating,
			"v2": schemapb.ShardState_ShardCreating,
		}, states)

		// the map is a copy: mutating it cannot corrupt the cache.
		states["v0"] = schemapb.ShardState_ShardNormal
		again, ok := cache.ChannelStates(ctx, 16)
		assert.True(t, ok)
		assert.Equal(t, schemapb.ShardState_ShardSplitting, again["v0"])
	})

	t.Run("ChannelStates reports not-known when nothing is cached", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(17)).Return(nil, errors.New("coord down")).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		states, ok := cache.ChannelStates(ctx, 17)
		assert.False(t, ok)
		assert.Nil(t, states)
	})

	t.Run("Invalidate forces a refetch", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(4)).Return(splittingCollectionResp(), nil).Once()
		// after the split completes the shards are Normal again.
		broker.EXPECT().DescribeCollection(mock.Anything, int64(4)).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"v0", "v1", "v2"},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardNormal},
				{State: schemapb.ShardState_ShardNormal},
				{State: schemapb.ShardState_ShardNormal},
			},
		}, nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		assert.True(t, cache.IsShardSplitting(ctx, 4))
		cache.Invalidate(4)
		assert.False(t, cache.IsShardSplitting(ctx, 4))
	})
}

func TestShardStatesListingRules(t *testing.T) {
	adopted := ShardStatesOf(&milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v1", "v2"},
		ShardInfos:          []*schemapb.CollectionShardInfo{{State: schemapb.ShardState_ShardNormal}},
	})
	// a listed vchannel without a shard info is a legacy Normal shard.
	assert.Equal(t, ShardStates{"v1": schemapb.ShardState_ShardNormal, "v2": schemapb.ShardState_ShardNormal}, adopted)
	assert.False(t, adopted.Splitting())
	assert.True(t, ShardStatesOf(splittingCollectionResp()).Splitting())

	assert.True(t, adopted.Lists("v1"))
	assert.False(t, adopted.Lists("v0"), "adoption delisted the source")
	assert.NoError(t, adopted.CheckWatchable("v1"))
	assert.ErrorIs(t, adopted.CheckWatchable("v0"), merr.ErrChannelNotFound)

	window := ShardStatesOf(splittingCollectionResp())
	assert.NoError(t, window.CheckWatchable("v0"))
	assert.ErrorIs(t, window.CheckWatchable("v1"), merr.ErrServiceUnavailable, "a Creating target is not adopted yet")

	current := map[string]*DmChannel{
		"v0": {VchannelInfo: &datapb.VchannelInfo{ChannelName: "v0"}},
		"v1": {VchannelInfo: &datapb.VchannelInfo{ChannelName: "v1"}},
	}
	assert.Equal(t, []string{"v0"}, adopted.Delisted(current))

	// no listing at all carries no routing information: nothing is delisted.
	var none ShardStates
	assert.True(t, none.Lists("v0"))
	assert.Empty(t, none.Delisted(current))
	assert.NoError(t, none.CheckWatchable("v0"))
}

func TestCheckShardSplitMovable(t *testing.T) {
	ctx := context.Background()
	check := func(t *testing.T, resp *milvuspb.DescribeCollectionResponse, describeErr error, window []string, current ...string) error {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(1)).Return(resp, describeErr).Maybe()
		targetMgr := NewMockTargetManager(t)
		var marks typeutil.Set[string]
		if len(window) > 0 {
			marks = typeutil.NewSet(window...)
		}
		targetMgr.EXPECT().GetSplitWindowTargets(mock.Anything, int64(1), NextTarget).Return(marks).Maybe()
		channels := make(map[string]*DmChannel)
		for _, name := range current {
			channels[name] = &DmChannel{VchannelInfo: &datapb.VchannelInfo{ChannelName: name}}
		}
		targetMgr.EXPECT().GetDmChannelsByCollection(mock.Anything, int64(1), CurrentTarget).Return(channels).Maybe()
		return CheckShardSplitMovable(ctx, NewShardSplitStateCache(broker, time.Minute), targetMgr, 1)
	}
	normal := &milvuspb.DescribeCollectionResponse{VirtualChannelNames: []string{"v0", "v9"}}
	adopted := &milvuspb.DescribeCollectionResponse{VirtualChannelNames: []string{"v1", "v2", "v9"}}

	assert.NoError(t, check(t, normal, nil, nil, "v0", "v9"), "a collection that is not splitting moves freely")
	assert.NoError(t, check(t, adopted, nil, nil, "v1", "v2", "v9"), "after the flip")
	for name, err := range map[string]error{
		"window marks":   check(t, normal, nil, []string{"v1"}, "v0", "v9"),
		"states unknown": check(t, nil, errors.New("coord down"), nil, "v0"),
		"splitting":      check(t, splittingCollectionResp(), nil, nil, "v0"),
		"retired source": check(t, adopted, nil, nil, "v0", "v9"),
	} {
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable, name)
		assert.True(t, merr.IsRetryableErr(err), name)
	}
	assert.NoError(t, CheckShardSplitMovable(ctx, nil, nil, 1), "no cache, no rule")
}
