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

package proxy

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/util/routing"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// orderNamespaces returns two namespaces and the routing key that separates
// them, with the first namespace strictly below the split key and the second at
// or above it. It hides the hash-prefixed encoding ordering from the assertions.
func orderNamespaces(a, b string) (low, high string, splitKey []byte) {
	ka, kb := routing.EncodeNamespace(a), routing.EncodeNamespace(b)
	if bytes.Compare(ka, kb) <= 0 {
		return a, b, kb
	}
	return b, a, ka
}

func TestBuildRangeRoutingTable(t *testing.T) {
	low, high, splitKey := orderNamespaces("tenant-a", "tenant-b")

	t.Run("hash mode returns nil table", func(t *testing.T) {
		tbl, err := buildRangeRoutingTable(schemapb.RoutingMode_RoutingModeHash,
			[]string{"ch0", "ch1"},
			[]*schemapb.CollectionShardInfo{{}, {}})
		assert.NoError(t, err)
		assert.Nil(t, tbl)
	})

	t.Run("range mode builds a covering table", func(t *testing.T) {
		tbl, err := buildRangeRoutingTable(schemapb.RoutingMode_RoutingModeRange,
			[]string{"ch0", "ch1"},
			[]*schemapb.CollectionShardInfo{
				{RoutingKeyLower: nil, RoutingKeyUpper: splitKey, State: schemapb.ShardState_ShardNormal},
				{RoutingKeyLower: splitKey, RoutingKeyUpper: nil, State: schemapb.ShardState_ShardNormal},
			})
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, 2, tbl.NumShards())
		assert.Equal(t, "ch0", tbl.LookupNamespace(low))
		assert.Equal(t, "ch1", tbl.LookupNamespace(high))
	})

	t.Run("fenced source and dropped source are excluded", func(t *testing.T) {
		// The source shard (ch0) is fenced/splitting and its old whole-space range is
		// now owned by the two creating targets ch1 and ch2. The excluded source must
		// not break coverage.
		tbl, err := buildRangeRoutingTable(schemapb.RoutingMode_RoutingModeRange,
			[]string{"ch0", "ch1", "ch2"},
			[]*schemapb.CollectionShardInfo{
				{RoutingKeyLower: nil, RoutingKeyUpper: nil, State: schemapb.ShardState_ShardSplitting},
				{RoutingKeyLower: nil, RoutingKeyUpper: splitKey, State: schemapb.ShardState_ShardCreating},
				{RoutingKeyLower: splitKey, RoutingKeyUpper: nil, State: schemapb.ShardState_ShardCreating},
			})
		assert.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, 2, tbl.NumShards())
		assert.Equal(t, "ch1", tbl.LookupNamespace(low))
		assert.Equal(t, "ch2", tbl.LookupNamespace(high))

		// A dropped source is excluded the same way.
		tbl, err = buildRangeRoutingTable(schemapb.RoutingMode_RoutingModeRange,
			[]string{"ch0", "ch1", "ch2"},
			[]*schemapb.CollectionShardInfo{
				{RoutingKeyLower: nil, RoutingKeyUpper: nil, State: schemapb.ShardState_ShardDropped},
				{RoutingKeyLower: nil, RoutingKeyUpper: splitKey, State: schemapb.ShardState_ShardNormal},
				{RoutingKeyLower: splitKey, RoutingKeyUpper: nil, State: schemapb.ShardState_ShardNormal},
			})
		assert.NoError(t, err)
		assert.Equal(t, 2, tbl.NumShards())
	})

	t.Run("shard info count mismatch is an error", func(t *testing.T) {
		_, err := buildRangeRoutingTable(schemapb.RoutingMode_RoutingModeRange,
			[]string{"ch0", "ch1"},
			[]*schemapb.CollectionShardInfo{{}})
		assert.Error(t, err)
	})

	t.Run("a gap in coverage is an error", func(t *testing.T) {
		// ch0 ends at splitKey but ch1 starts at a strictly larger key: a gap.
		gapStart := append(append([]byte{}, splitKey...), 0x00)
		_, err := buildRangeRoutingTable(schemapb.RoutingMode_RoutingModeRange,
			[]string{"ch0", "ch1"},
			[]*schemapb.CollectionShardInfo{
				{RoutingKeyLower: nil, RoutingKeyUpper: splitKey, State: schemapb.ShardState_ShardNormal},
				{RoutingKeyLower: gapStart, RoutingKeyUpper: nil, State: schemapb.ShardState_ShardNormal},
			})
		assert.Error(t, err)
	})
}

func TestResolveRangeRoutingChannels(t *testing.T) {
	low, high, splitKey := orderNamespaces("tenant-a", "tenant-b")
	tbl, err := buildRangeRoutingTable(schemapb.RoutingMode_RoutingModeRange,
		[]string{"ch0", "ch1"},
		[]*schemapb.CollectionShardInfo{
			{RoutingKeyLower: nil, RoutingKeyUpper: splitKey, State: schemapb.ShardState_ShardNormal},
			{RoutingKeyLower: splitKey, RoutingKeyUpper: nil, State: schemapb.ShardState_ShardNormal},
		})
	assert.NoError(t, err)

	base := []string{"ch0", "ch1"}

	t.Run("nil info keeps the hash channel set", func(t *testing.T) {
		out, err := resolveRangeRoutingChannels(nil, "tenant-a", base)
		assert.NoError(t, err)
		assert.Equal(t, base, out)
	})

	t.Run("hash collection keeps the hash channel set", func(t *testing.T) {
		info := &collectionInfo{collID: 1, routingMode: schemapb.RoutingMode_RoutingModeHash}
		out, err := resolveRangeRoutingChannels(info, "tenant-a", base)
		assert.NoError(t, err)
		assert.Equal(t, base, out)
	})

	t.Run("range collection narrows to the namespace shard", func(t *testing.T) {
		info := &collectionInfo{collID: 1, routingMode: schemapb.RoutingMode_RoutingModeRange, rangeRoutingTable: tbl}
		out, err := resolveRangeRoutingChannels(info, low, base)
		assert.NoError(t, err)
		assert.Equal(t, []string{"ch0"}, out)

		out, err = resolveRangeRoutingChannels(info, high, base)
		assert.NoError(t, err)
		assert.Equal(t, []string{"ch1"}, out)
	})

	t.Run("range collection without a table is a retriable error", func(t *testing.T) {
		info := &collectionInfo{collID: 1, routingMode: schemapb.RoutingMode_RoutingModeRange}
		_, err := resolveRangeRoutingChannels(info, "tenant-a", base)
		assert.Error(t, err)
	})
}

// TestMetaCache_RangeRoutingSurfaced verifies update() surfaces routing_mode and
// shard_infos from DescribeCollection and derives a usable range routing table.
func TestMetaCache_RangeRoutingSurfaced(t *testing.T) {
	cache := globalMetaCache
	defer func() { globalMetaCache = cache }()
	ctx := context.Background()
	rootCoord := mocks.NewMockMixCoordClient(t)
	rootCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{Status: merr.Success()}, nil)
	err := InitMetaCache(ctx, rootCoord)
	assert.NoError(t, err)

	low, high, splitKey := orderNamespaces("tenant-a", "tenant-b")
	rootCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		Status:       merr.Success(),
		CollectionID: 7,
		Schema: &schemapb.CollectionSchema{
			Name:            "ns_coll",
			EnableNamespace: true,
			Fields:          []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", IsPrimaryKey: true}},
		},
		ShardsNum:            2,
		PhysicalChannelNames: []string{"pch0", "pch1"},
		VirtualChannelNames:  []string{"vch0", "vch1"},
		RoutingMode:          schemapb.RoutingMode_RoutingModeRange,
		ShardInfos: []*schemapb.CollectionShardInfo{
			{RoutingKeyUpper: splitKey, State: schemapb.ShardState_ShardNormal},
			{RoutingKeyLower: splitKey, State: schemapb.ShardState_ShardNormal},
		},
	}, nil).Once()

	info, err := globalMetaCache.GetCollectionInfo(ctx, "db", "ns_coll", 7)
	assert.NoError(t, err)
	assert.Equal(t, schemapb.RoutingMode_RoutingModeRange, info.routingMode)
	assert.NotNil(t, info.rangeRoutingTable)
	assert.Equal(t, "vch0", info.rangeRoutingTable.LookupNamespace(low))
	assert.Equal(t, "vch1", info.rangeRoutingTable.LookupNamespace(high))
}

// TestInsertTask_ShardFencedRefetch verifies the reject-refetch closed loop: an
// insert to a range-routed shard that was fenced by a split is retried after the
// proxy drops its stale routing cache, and the retry succeeds.
func TestInsertTask_ShardFencedRefetch(t *testing.T) {
	oldCache := globalMetaCache
	defer func() { globalMetaCache = oldCache }()

	// A single-shard range table: the whole key space maps to vch0.
	tbl, err := buildRangeRoutingTable(schemapb.RoutingMode_RoutingModeRange,
		[]string{"vch0"},
		[]*schemapb.CollectionShardInfo{{State: schemapb.ShardState_ShardNormal}})
	assert.NoError(t, err)
	rangeInfo := &collectionInfo{collID: 100, routingMode: schemapb.RoutingMode_RoutingModeRange, rangeRoutingTable: tbl}

	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionID(mock.Anything, "db", "coll").Return(int64(100), nil)
	cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "coll", int64(100)).Return(rangeInfo, nil)
	cache.EXPECT().GetPartitionID(mock.Anything, "db", "coll", "_default").Return(int64(200), nil)
	// The fence rejection must drop the stale routing cache exactly once.
	removed := 0
	cache.EXPECT().RemoveCollection(mock.Anything, "db", "coll", uint64(0)).Run(func(_ context.Context, _ string, _ string, _ uint64) {
		removed++
	}).Return()
	globalMetaCache = cache

	chMgr := NewMockChannelsMgr(t)
	chMgr.EXPECT().getVChannels(int64(100)).Return([]string{"vch0"}, nil)

	// First append is fenced; the second (post-refetch) append succeeds.
	prevWAL := streaming.WAL()
	defer streaming.SetWALForTest(prevWAL)
	mockWAL := mock_streaming.NewMockWALAccesser(t)
	fenced := types.NewAppendResponseN(1)
	fenced.FillAllError(streamingstatus.NewShardFenced("vch0"))
	ok := types.NewAppendResponseN(1)
	ok.FillAllResponse(types.AppendResponse{AppendResult: &types.AppendResult{TimeTick: 100}})
	mockWAL.EXPECT().AppendMessages(mock.Anything, mock.Anything).Return(fenced).Once()
	mockWAL.EXPECT().AppendMessages(mock.Anything, mock.Anything).Return(ok).Once()
	streaming.SetWALForTest(mockWAL)

	namespace := "tenant-a"
	insertMsg := buildMinimalInsertMsg(100, "db", "coll", "_default")
	insertMsg.Namespace = &namespace
	it := &insertTask{
		ctx:          context.Background(),
		insertMsg:    insertMsg,
		result:       buildMutationResult(42),
		chMgr:        chMgr,
		collectionID: 100,
	}

	err = it.Execute(context.Background())
	assert.NoError(t, err)
	assert.True(t, merr.Ok(it.result.GetStatus()))
	assert.Equal(t, 1, removed, "fenced append must invalidate the routing cache once")
	assert.Equal(t, uint64(100), it.result.Timestamp)
}
