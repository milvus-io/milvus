package message

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSplitShardRoleOf(t *testing.T) {
	header := &messagespb.SplitShardMessageHeader{
		CollectionId:    1,
		SourceVchannel:  "p0_1v0",
		TargetVchannels: []string{"p1_1v1", "p2_1v2"},
	}
	assert.Equal(t, SplitShardRoleSource, SplitShardRoleOf(header, "p0_1v0"))
	assert.Equal(t, SplitShardRoleTarget, SplitShardRoleOf(header, "p1_1v1"))
	assert.Equal(t, SplitShardRoleTarget, SplitShardRoleOf(header, "p2_1v2"))
	assert.Equal(t, SplitShardRoleControl, SplitShardRoleOf(header, "p0_vcchan"))
	// "p9_2v9" belongs to a different collection (2, not 1): unrouted and unknown.
	assert.Equal(t, SplitShardRoleUnknown, SplitShardRoleOf(header, "p9_2v9"))
	// An empty vchannel never matches an unset source.
	assert.Equal(t, SplitShardRoleUnknown, SplitShardRoleOf(&messagespb.SplitShardMessageHeader{}, ""))
}

// TestSplitShardRoleOfAnotherShardOfTheCollectionIsUnknown pins that there is
// no bystander role: the broadcast reaches only the source, the targets and the
// control channel, so a replica on another shard of the SAME collection was
// never sent there and is a misroute, exactly like one on another collection.
func TestSplitShardRoleOfAnotherShardOfTheCollectionIsUnknown(t *testing.T) {
	header := &messagespb.SplitShardMessageHeader{
		CollectionId:    1,
		SourceVchannel:  "p0_1v0",
		TargetVchannels: []string{"p1_1v1"},
	}
	// "p2_1v2" is collection 1's third shard, neither the source nor a target.
	assert.Equal(t, SplitShardRoleUnknown, SplitShardRoleOf(header, "p2_1v2"))
	// "p2_9v2" belongs to collection 9.
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

// TestOverwriteReplicateVChannelRejectsAForeignAppendFirst pins what happens
// when a broadcast header's append-first list is NOT a subset of its own
// vchannel list.
//
// The builder refuses to produce such a header, so this forges one: the case
// only reaches a secondary cluster from a malformed or corrupted remote header,
// and there is no name to map it to. It must be refused -- carrying the source
// cluster's name through would make the secondary's append gate wait on a
// vchannel that exists nowhere here, silently and forever -- but refused as an
// ERROR, not a panic: the replicate stream re-delivers the same bytes after
// every reconnect, so a panic would crash-loop the proxy instead of failing the
// one message.
func TestOverwriteReplicateVChannelRejectsAForeignAppendFirst(t *testing.T) {
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

	var overwriteErr error
	assert.NotPanics(t, func() {
		overwriteErr = replica.OverwriteReplicateVChannel(replica.VChannel(), []string{"q0_1v0", "q1_1v1"})
	})
	assert.Error(t, overwriteErr)
	assert.Contains(t, overwriteErr.Error(), "foreign_1v9")
	// The primary wrote the header: a replication contract violation is a
	// System error, never an input error.
	assert.ErrorIs(t, overwriteErr, merr.ErrServiceInternal)
	assert.NotErrorIs(t, overwriteErr, merr.ErrParameterInvalid)

	// A well-formed header still succeeds through the same path.
	wellFormed := msg.SplitIntoMutableMessage()[1].(*messageImpl)
	assert.NoError(t, wellFormed.OverwriteReplicateVChannel(
		wellFormed.VChannel(), []string{"q0_1v0", "q1_1v1"}))
}
