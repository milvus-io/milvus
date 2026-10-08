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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Since #53595 a Flush is a ManualFlush broadcast to every vchannel the
// collection lists, with OptBuildBroadcastAckSyncUp: completion is the
// consuming side's ack, not a channel checkpoint. That makes
// BroadcastAlteredCollection's vchannel refresh load-bearing for flush
// routing, not just for the split trigger -- the broadcast goes to
// datacoord's CACHED list.
//
// Over a split's lifetime:
//
//   - before the write switch, the list is the source alone;
//   - after it, the source (fenced, still listed until adoption) AND the two
//     targets. The targets must be there or their growing segments are never
//     persisted while Flush reports success; the fenced source is harmless
//     because the shard interceptor's name gate appends a ManualFlush
//     addressed to a fenced vchannel without effect
//     (appendManualFlushWithoutEffect) instead of refusing it, so its replica
//     still lands and can be acked and the broadcaster is never wedged;
//   - after the adoption, the targets alone: a retired source is no longer
//     named at all.
//
// Nothing here is reachable if the cache keeps the pre-split list, which is
// what this pins.
func TestManualFlushFollowsASplitsVChannelList(t *testing.T) {
	ctx := context.Background()
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(splitMgrCollection, &collectionInfo{
		ID:            splitMgrCollection,
		Schema:        &schemapb.CollectionSchema{Name: "coll"},
		VChannelNames: []string{splitMgrV0},
	})
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	m.collections = collections
	svr := &Server{meta: m}
	svr.handler = &ServerHandler{svr}
	svr.stateCode.Store(commonpb.StateCode_Healthy)

	var broadcast []string
	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			assert.Equal(t, message.MessageTypeManualFlush, msg.MessageType())
			broadcast = msg.BroadcastHeader().VChannels
			return &types.BroadcastAppendResult{}, nil
		})

	flushedVChannels := func(listed []string) []string {
		coll := svr.meta.GetClonedCollectionInfo(splitMgrCollection)
		coll.VChannelNames = listed
		svr.meta.AddCollection(coll)
		broadcast = nil
		_, err := svr.flushCollection(ctx, splitMgrCollection, nil, bapi)
		require.NoError(t, err)
		return broadcast
	}

	assert.ElementsMatch(t, []string{splitMgrV0}, flushedVChannels([]string{splitMgrV0}))
	// The write switch's own AlterCollection broadcast refreshed the list.
	assert.ElementsMatch(t, []string{splitMgrV0, splitMgrV1, splitMgrV2},
		flushedVChannels([]string{splitMgrV0, splitMgrV1, splitMgrV2}),
		"a Flush during the split window must reach the targets, and reaches the fenced source too")
	// The adoption delisted the source.
	assert.ElementsMatch(t, []string{splitMgrV1, splitMgrV2}, flushedVChannels([]string{splitMgrV1, splitMgrV2}),
		"a retired source is no longer named by a Flush")
}

// A Flush during the split window reports the state of the shards the
// collection lists at that moment, including the targets: their flushed
// segments are in the response and the channel checkpoints the response
// carries are keyed by the listed vchannels. A target whose checkpoint has
// been seeded by CommitShardSplit is therefore reported with its genesis
// position rather than with nothing.
func TestFlushDuringASplitReportsTheTargets(t *testing.T) {
	ctx := context.Background()
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(splitMgrCollection, &collectionInfo{
		ID:            splitMgrCollection,
		Schema:        &schemapb.CollectionSchema{Name: "coll"},
		VChannelNames: []string{splitMgrV0, splitMgrV1, splitMgrV2},
	})
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	m.collections = collections
	svr := &Server{meta: m, shardSplitTasks: newShardSplitTasks()}
	svr.handler = &ServerHandler{svr}
	svr.stateCode.Store(commonpb.StateCode_Healthy)
	require.NoError(t, svr.meta.UpdateChannelCheckpoints(ctx, []*msgpb.MsgPosition{
		splitTestPosition(splitMgrV1, 3000),
	}))
	m.segments.SetSegment(9100, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 9100, CollectionID: splitMgrCollection, InsertChannel: splitMgrV1,
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
	}})

	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(&types.BroadcastAppendResult{}, nil).Once()
	result, err := svr.flushCollection(ctx, splitMgrCollection, nil, bapi)
	require.NoError(t, err)
	assert.Contains(t, result.GetFlushSegmentIDs(), int64(9100))
	assert.EqualValues(t, 3000, result.GetChannelCps()[splitMgrV1].GetTimestamp())
	assert.Contains(t, result.GetChannelCps(), splitMgrV0)
	// FlushTs stays zero on the streaming path: completion is the ack, so
	// GetFlushState has no checkpoint to compare against and answers flushed.
	assert.Zero(t, result.GetFlushTs())
}
