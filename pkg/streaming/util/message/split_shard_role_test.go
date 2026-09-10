package message

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func TestSplitShardRoleOf(t *testing.T) {
	header := &messagespb.SplitShardMessageHeader{
		CollectionId:    1,
		SourceVchannels: []string{"p0_1v0"},
		Targets: []*messagespb.SplitShardTarget{
			{Vchannel: "p1_1v1", Routing: &schemapb.HashRouting{Buckets: []uint64{0}}},
			{Vchannel: "p2_1v2", Routing: &schemapb.HashRouting{Buckets: []uint64{1}}},
		},
	}
	assert.Equal(t, SplitShardRoleSource, SplitShardRoleOf(header, "p0_1v0"))
	assert.Equal(t, SplitShardRoleTarget, SplitShardRoleOf(header, "p1_1v1"))
	assert.Equal(t, SplitShardRoleTarget, SplitShardRoleOf(header, "p2_1v2"))
	assert.Equal(t, SplitShardRoleControl, SplitShardRoleOf(header, "p0_vcchan"))
	// "p9_2v9" belongs to a different collection (2, not 1): unrouted and unknown.
	assert.Equal(t, SplitShardRoleUnknown, SplitShardRoleOf(header, "p9_2v9"))

	assert.Equal(t, []uint64{1}, SplitShardTargetOf(header, "p2_1v2").GetRouting().GetBuckets())
	assert.Nil(t, SplitShardTargetOf(header, "p0_1v0"))
}

// TestSplitShardRoleOfBystander pins the role a replica plays when it lands on
// a vchannel that the split neither fences nor creates, but which still
// belongs to the same collection: it is a BYSTANDER, not a misroute, because
// the broadcast now covers every vchannel of the collection so that every
// replica can observe the split (even the ones it does not act on). A
// vchannel of a DIFFERENT collection stays Unknown -- that is genuinely a
// misroute.
func TestSplitShardRoleOfBystander(t *testing.T) {
	header := &messagespb.SplitShardMessageHeader{
		CollectionId:    1,
		SourceVchannels: []string{"p0_1v0"},
		Targets: []*messagespb.SplitShardTarget{
			{Vchannel: "p1_1v1", Routing: &schemapb.HashRouting{Buckets: []uint64{0}}},
		},
	}
	// "p2_1v2" is neither the source nor a target, but it is collection 1's
	// third shard: a bystander that must pass through without effect.
	assert.Equal(t, SplitShardRoleBystander, SplitShardRoleOf(header, "p2_1v2"))
	// "p2_9v2" carries the same shard index but belongs to collection 9: unknown.
	assert.Equal(t, SplitShardRoleUnknown, SplitShardRoleOf(header, "p2_9v2"))
}

func TestSplitShardTypeIsFreshTimeTickAndExclusive(t *testing.T) {
	assert.True(t, MessageTypeSplitShard.IsFreshTimeTick())
	assert.True(t, MessageTypeSplitShard.IsExclusiveRequired())
	assert.False(t, MessageTypeInsert.IsFreshTimeTick())
	assert.False(t, MessageTypeAlterCollection.IsFreshTimeTick())
}

func TestOptBuildBroadcastAppendFirst(t *testing.T) {
	msg := NewSplitShardMessageBuilderV2().
		WithHeader(&messagespb.SplitShardMessageHeader{CollectionId: 1}).
		WithBody(&messagespb.SplitShardMessageBody{}).
		WithBroadcast([]string{"p0_1v0", "p1_1v1", "p0_vcchan"}, OptBuildBroadcastAppendFirst("p0_1v0")).
		MustBuildBroadcast()
	assert.Equal(t, []string{"p0_1v0"}, msg.BroadcastHeader().AppendFirstVChannels)
	assert.Panics(t, func() {
		NewSplitShardMessageBuilderV2().
			WithHeader(&messagespb.SplitShardMessageHeader{CollectionId: 1}).
			WithBody(&messagespb.SplitShardMessageBody{}).
			WithBroadcast([]string{"p0_1v0"}, OptBuildBroadcastAppendFirst("p9_1v9")).
			MustBuildBroadcast()
	}, "append-first vchannel outside the broadcast list must be refused at build time")
	assert.Panics(t, func() {
		NewSplitShardMessageBuilderV2().
			WithHeader(&messagespb.SplitShardMessageHeader{CollectionId: 1}).
			WithBody(&messagespb.SplitShardMessageBody{}).
			WithBroadcast([]string{"p0_1v0", "p0_vcchan"}, OptBuildBroadcastAppendFirst("p0_vcchan")).
			MustBuildBroadcast()
	}, "the control channel can never be appended first")
}

// TestOptBuildBroadcastAppendFirstAndAckSyncUpAreMutuallyExclusive asserts
// that AppendFirst and AckSyncUp can never both be set on the same broadcast,
// regardless of the order the options are applied in.
func TestOptBuildBroadcastAppendFirstAndAckSyncUpAreMutuallyExclusive(t *testing.T) {
	assert.Panics(t, func() {
		NewSplitShardMessageBuilderV2().
			WithHeader(&messagespb.SplitShardMessageHeader{CollectionId: 1}).
			WithBody(&messagespb.SplitShardMessageBody{}).
			WithBroadcast([]string{"p0_1v0"}, OptBuildBroadcastAckSyncUp(), OptBuildBroadcastAppendFirst("p0_1v0")).
			MustBuildBroadcast()
	}, "append-first cannot be added to an already ack-sync-up broadcast")

	assert.Panics(t, func() {
		NewSplitShardMessageBuilderV2().
			WithHeader(&messagespb.SplitShardMessageHeader{CollectionId: 1}).
			WithBody(&messagespb.SplitShardMessageBody{}).
			WithBroadcast([]string{"p0_1v0"}, OptBuildBroadcastAppendFirst("p0_1v0"), OptBuildBroadcastAckSyncUp()).
			MustBuildBroadcast()
	}, "ack-sync-up cannot be added to an already append-first broadcast")
}

// TestOverwriteReplicateVChannelPanicsOnAForeignAppendFirst pins what happens
// when a broadcast header's append-first list is NOT a subset of its own
// vchannel list.
//
// The builder refuses to produce such a header, so this forges one: the case
// only reaches a secondary cluster from a malformed or corrupted remote header,
// and there is no name to map it to. Refusing loudly is the point -- carrying
// the source cluster's name through would make the secondary's append gate wait
// on a vchannel that exists nowhere here, silently and forever.
func TestOverwriteReplicateVChannelPanicsOnAForeignAppendFirst(t *testing.T) {
	msg := NewSplitShardMessageBuilderV2().
		WithHeader(&messagespb.SplitShardMessageHeader{CollectionId: 1}).
		WithBody(&messagespb.SplitShardMessageBody{}).
		WithBroadcast([]string{"p0_1v0", "p1_1v1"}, OptBuildBroadcastAppendFirst("p0_1v0")).
		MustBuildBroadcast().
		WithBroadcastID(9)

	replica := msg.SplitIntoMutableMessage()[0].(*messageImpl)
	bh := replica.broadcastHeader()
	bh.AppendFirstVchannels = []string{"foreign_1v9"}
	encoded, err := EncodeProto(bh)
	assert.NoError(t, err)
	replica.properties.Set(messageBroadcastHeader, encoded)

	assert.Panics(t, func() {
		replica.OverwriteReplicateVChannel(replica.VChannel(), []string{"q0_1v0", "q1_1v1"})
	})
}
