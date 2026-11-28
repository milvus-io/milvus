// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package message_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func newTestSplitShardBroadcast(opts ...message.OptBuildBroadcast) message.BroadcastMutableMessage {
	return message.NewSplitShardMessageBuilderV2().
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    10,
			SplitTaskId:     99,
			SourceVchannel:  "p0_10v0",
			TargetVchannels: []string{"p1_10v1", "p2_10v2"},
		}).
		WithBody(&message.SplitShardMessageBody{
			GenesisHeader: &message.CreateCollectionMessageHeader{CollectionId: 10, PartitionIds: []int64{11}},
			Genesis:       &message.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{Name: "c"}},
			Routing: &message.SplitShardRouting{
				RoutingModulus: 2,
				ShardBy:        "hash(pk)",
				Shards: []*schemapb.CollectionShardInfo{
					{VchannelName: "p1_10v1", Routing: &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: []uint64{0}}}},
					{VchannelName: "p2_10v2", Routing: &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: []uint64{1}}}},
				},
			},
		}).
		WithBroadcast([]string{"p0_10v0", "p1_10v1", "p2_10v2"}, opts...).
		MustBuildBroadcast()
}

func TestSplitShardMessageProperties(t *testing.T) {
	require.True(t, message.MessageTypeSplitShard.Valid())
	require.Equal(t, "SplitShard", message.MessageTypeSplitShard.String())
	// The fence must find no DML in flight on the source vchannel.
	require.True(t, message.MessageTypeSplitShard.IsExclusiveRequired())
	require.False(t, message.MessageTypeSplitShard.IsSelfControlled())
	require.False(t, message.MessageTypeSplitShard.IsSystem())
	require.False(t, message.MessageTypeSplitShard.IsDMLMessageType())
}

func TestSplitShardMessageRoundTrip(t *testing.T) {
	broadcast := newTestSplitShardBroadcast(message.OptBuildBroadcastAppendFirst("p0_10v0"))
	require.Equal(t, message.MessageTypeSplitShard, broadcast.MessageType())
	bh := broadcast.BroadcastHeader()
	require.ElementsMatch(t, []string{"p0_10v0", "p1_10v1", "p2_10v2"}, bh.VChannels)
	require.Equal(t, []string{"p0_10v0"}, bh.AppendFirstVChannels)
	require.False(t, bh.AckSyncUp)
	// A split is replicated: it must not be marked unreplicable.
	require.False(t, broadcast.IsUnreplicable())

	copies := broadcast.WithBroadcastID(7).SplitIntoMutableMessage()
	require.Len(t, copies, 3)
	for _, c := range copies {
		split := message.MustAsMutableSplitShardMessageV2(c)
		require.Equal(t, int64(99), split.Header().GetSplitTaskId())
		require.Equal(t, "p0_10v0", split.Header().GetSourceVchannel())
		require.Equal(t, uint64(2), split.MustBody().GetRouting().GetRoutingModulus())
		require.Len(t, split.MustBody().GetRouting().GetShards(), 2)
		// Every copy keeps the append-first vchannel in its broadcast header.
		require.Equal(t, []string{"p0_10v0"}, c.BroadcastHeader().AppendFirstVChannels)
		require.Equal(t, uint64(7), c.BroadcastHeader().BroadcastID)
	}
}

func TestOptBuildBroadcastAppendFirstOption(t *testing.T) {
	// No append-first vchannel by default.
	require.Empty(t, newTestSplitShardBroadcast().BroadcastHeader().AppendFirstVChannels)

	// The vchannel must be one of the broadcast.
	require.Panics(t, func() { newTestSplitShardBroadcast(message.OptBuildBroadcastAppendFirst("p9_10v9")) })
	// The control channel is never appended first.
	require.Panics(t, func() {
		message.OptBuildBroadcastAppendFirst("p0_vcchan")(&messagespb.BroadcastHeader{Vchannels: []string{"p0_vcchan"}})
	})
	// The option and ack sync up exclude each other, in both orders.
	require.Panics(t, func() {
		newTestSplitShardBroadcast(message.OptBuildBroadcastAppendFirst("p0_10v0"), message.OptBuildBroadcastAckSyncUp())
	})
	require.Panics(t, func() {
		newTestSplitShardBroadcast(message.OptBuildBroadcastAckSyncUp(), message.OptBuildBroadcastAppendFirst("p0_10v0"))
	})
	// Ack sync up alone is unchanged.
	require.True(t, newTestSplitShardBroadcast(message.OptBuildBroadcastAckSyncUp()).BroadcastHeader().AckSyncUp)
}

func TestOverwriteReplicateVChannelRenamesAppendFirst(t *testing.T) {
	copies := newTestSplitShardBroadcast(message.OptBuildBroadcastAppendFirst("p0_10v0")).WithBroadcastID(7).SplitIntoMutableMessage()
	for _, c := range copies {
		source := c.BroadcastHeader().VChannels
		id := walimplstest.NewTestMessageID(1)
		immutable := c.WithTimeTick(100).WithLastConfirmed(id).IntoImmutableMessage(id)
		replicated := message.MustNewReplicateMessage("primary", immutable.IntoImmutableMessageProto())
		renamed := make([]string, 0, len(source))
		for _, vchannel := range source {
			renamed = append(renamed, "s-"+vchannel)
		}
		replicated.OverwriteReplicateVChannel("s-"+immutable.VChannel(), renamed)
		bh := replicated.BroadcastHeader()
		require.Equal(t, renamed, bh.VChannels)
		require.Equal(t, []string{"s-p0_10v0"}, bh.AppendFirstVChannels)
	}
}

func TestOverwriteReplicateVChannelPanicsOnForeignAppendFirst(t *testing.T) {
	// A header whose append-first vchannel is not one of its vchannels cannot
	// be built by OptBuildBroadcastAppendFirst; only a foreign producer can.
	foreign := func(bh *messagespb.BroadcastHeader) { bh.AppendFirstVchannels = []string{"not-in-the-broadcast"} }
	c := newTestSplitShardBroadcast(foreign).WithBroadcastID(7).SplitIntoMutableMessage()[0]
	id := walimplstest.NewTestMessageID(1)
	immutable := c.WithTimeTick(100).WithLastConfirmed(id).IntoImmutableMessage(id)
	replicated := message.MustNewReplicateMessage("primary", immutable.IntoImmutableMessageProto())
	require.Panics(t, func() {
		replicated.OverwriteReplicateVChannel("s", []string{"a", "b", "c"})
	})
}

func TestReplicateMessageDropsTheBarrierTimeTick(t *testing.T) {
	c := newTestSplitShardBroadcast(message.OptBuildBroadcastAppendFirst("p0_10v0")).WithBroadcastID(7).SplitIntoMutableMessage()[1]
	id := walimplstest.NewTestMessageID(1)
	immutable := c.WithBarrierTimeTick(90).WithTimeTick(100).WithLastConfirmed(id).IntoImmutableMessage(id)
	require.Equal(t, uint64(90), immutable.BarrierTimeTick())
	// The barrier bounds a time tick of the source cluster. The receiving
	// cluster must not wait for its own TSO to pass it.
	replicated := message.MustNewReplicateMessage("primary", immutable.IntoImmutableMessageProto())
	require.Zero(t, replicated.BarrierTimeTick())
}
