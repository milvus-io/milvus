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

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// splitTopologyServer builds a Server whose mixCoord describes a collection
// mid-way through a hash split that has just been adopted: two sources handed
// off (ShardDropped) and three targets live.
func splitTopologyServer(t *testing.T, shardInfos []*schemapb.CollectionShardInfo) *Server {
	mixCoord := mocks2.NewMixCoord(t)
	mixCoord.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
		Return(&milvuspb.DescribeCollectionResponse{
			Status:              merr.Success(),
			VirtualChannelNames: []string{"src0", "src1", "tgt0", "tgt1", "tgt2"},
			ShardInfos:          shardInfos,
		}, nil).Maybe()
	return &Server{mixCoord: mixCoord}
}

func adoptedSplitShardInfos() []*schemapb.CollectionShardInfo {
	return []*schemapb.CollectionShardInfo{
		{VchannelName: "src0", State: schemapb.ShardState_ShardDropped},
		{VchannelName: "src1", State: schemapb.ShardState_ShardDropped},
		{VchannelName: "tgt0", State: schemapb.ShardState_ShardNormal},
		{VchannelName: "tgt1", State: schemapb.ShardState_ShardNormal},
		{VchannelName: "tgt2", State: schemapb.ShardState_ShardNormal},
	}
}

func TestServingChannelsExcludeHandedOffSplitSources(t *testing.T) {
	// The recovery info is what querycoord builds a collection's target from.
	// Leaving a handed-off source in it keeps a shard leader on the source, and
	// after a primary-key split the source's segments still hold every row the
	// targets were just given a rewritten copy of — so a read reaches both and
	// every primary key comes back twice.
	svr := splitTopologyServer(t, adoptedSplitShardInfos())

	serving, err := svr.getServingChannelsByCollectionID(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, []string{"tgt0", "tgt1", "tgt2"},
		lo.Map(serving, func(ch RWChannel, _ int) string { return ch.GetName() }))
}

func TestAllChannelsStillIncludeHandedOffSplitSources(t *testing.T) {
	// Flush state, snapshots and segment listing still have to see the source:
	// its segments exist until GC reclaims them, and a caller asking "what
	// channels does this collection have" is not asking about serving.
	svr := splitTopologyServer(t, adoptedSplitShardInfos())

	all, err := svr.getChannelsByCollectionID(context.Background(), 1)
	require.NoError(t, err)
	assert.Len(t, all, 5)
}

func TestServingChannelsKeepFencedAndCreatingShards(t *testing.T) {
	// Mid-window states are not a handoff: the source is still the only thing
	// answering for its key space (Splitting), and a target is already writable
	// and about to be adopted (Creating). Dropping either would lose reads.
	svr := splitTopologyServer(t, []*schemapb.CollectionShardInfo{
		{VchannelName: "src0", State: schemapb.ShardState_ShardSplitting},
		{VchannelName: "src1", State: schemapb.ShardState_ShardSplitting},
		{VchannelName: "tgt0", State: schemapb.ShardState_ShardCreating},
		{VchannelName: "tgt1", State: schemapb.ShardState_ShardCreating},
		{VchannelName: "tgt2", State: schemapb.ShardState_ShardCreating},
	})

	serving, err := svr.getServingChannelsByCollectionID(context.Background(), 1)
	require.NoError(t, err)
	assert.Len(t, serving, 5)
}

func TestServingChannelsWithoutShardInfos(t *testing.T) {
	// A collection that has never been split — and any response from a version
	// that predates shard_infos — carries none. Every channel serves.
	svr := splitTopologyServer(t, nil)

	serving, err := svr.getServingChannelsByCollectionID(context.Background(), 1)
	require.NoError(t, err)
	assert.Len(t, serving, 5)
}
