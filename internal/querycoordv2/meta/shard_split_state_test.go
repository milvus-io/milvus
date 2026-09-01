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

	t.Run("reports creating targets and dropped sources by state", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(5)).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"src", "t1", "t2", "old"},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardSplitting}, // src: fenced source
				{State: schemapb.ShardState_ShardCreating},  // t1: not-yet-adopted target
				{State: schemapb.ShardState_ShardCreating},  // t2: not-yet-adopted target
				{State: schemapb.ShardState_ShardDropped},   // old: released source
			},
		}, nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		assert.Equal(t, []string{"src"}, cache.SplittingSourceChannels(ctx, 5))
		assert.ElementsMatch(t, []string{"t1", "t2"}, cache.CreatingTargetChannels(ctx, 5))
		assert.Equal(t, []string{"old"}, cache.DroppedSourceChannels(ctx, 5))
	})

	t.Run("a dropped-but-not-cleaned source keeps balance frozen", func(t *testing.T) {
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(6)).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"t1", "old"},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardNormal},  // adopted target, now normal
				{State: schemapb.ShardState_ShardDropped}, // source dropped, pending release
			},
		}, nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		// no fenced source remains, but the dropped source still holds the freeze
		// until datacoord removes it from the collection meta.
		assert.Empty(t, cache.SplittingSourceChannels(ctx, 6))
		assert.True(t, cache.IsShardSplitting(ctx, 6))
	})

	t.Run("LiveChannels are the Normal and Creating ones", func(t *testing.T) {
		// Which target replaced which source is provenance and lives in the split
		// task, so what the cache offers is the collection-wide view: the shards
		// that currently serve reads.
		broker := NewMockBroker(t)
		broker.EXPECT().DescribeCollection(mock.Anything, int64(7)).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"src1", "src2", "t1a", "t1b", "t2a"},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardDropped},
				{State: schemapb.ShardState_ShardSplitting},
				{State: schemapb.ShardState_ShardNormal},
				{State: schemapb.ShardState_ShardCreating},
				{State: schemapb.ShardState_ShardNormal},
			},
		}, nil).Once()
		cache := NewShardSplitStateCache(broker, time.Minute)

		assert.ElementsMatch(t, []string{"t1a", "t1b", "t2a"}, cache.LiveChannels(ctx, 7))
		// and the retired and fenced ones are not live.
		assert.ElementsMatch(t, []string{"src1"}, cache.DroppedSourceChannels(ctx, 7))
		assert.ElementsMatch(t, []string{"src2"}, cache.SplittingSourceChannels(ctx, 7))
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
