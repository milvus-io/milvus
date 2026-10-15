package message_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func newAppendExtraTestExtra(t *testing.T) *anypb.Any {
	extra, err := anypb.New(&message.SplitShardExtraResponse{SplitTimeTick: 90})
	require.NoError(t, err)
	return extra
}

func newAppendExtraTestBroadcast() message.BroadcastMutableMessage {
	return message.NewSplitShardMessageBuilderV2().
		WithHeader(&message.SplitShardMessageHeader{CollectionId: 1, SplitTaskId: 2, SourceVchannel: "p0_1v0"}).
		WithBody(&message.SplitShardMessageBody{}).
		WithBroadcast([]string{"p0_1v0", "p1_1v1"}, message.OptBuildBroadcastAppendFirst("p0_1v0")).
		MustBuildBroadcast().
		WithBroadcastID(1)
}

// TestAppendExtraTravelsOnTheRecord: an extra append response set on a mutable
// message is on the record the WAL persists, where a consumer-side ack reads it.
func TestAppendExtraTravelsOnTheRecord(t *testing.T) {
	extra := newAppendExtraTestExtra(t)
	msg := newAppendExtraTestBroadcast().SplitIntoMutableMessage()[0]
	assert.Nil(t, message.AppendExtraOf(msg))

	message.SetAppendExtra(msg, extra)
	assert.True(t, proto.Equal(extra, message.AppendExtraOf(msg)))

	record := msg.WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	assert.True(t, proto.Equal(extra, message.AppendExtraOf(record)))

	// A nil extra clears it.
	message.SetAppendExtra(msg, nil)
	assert.Nil(t, message.AppendExtraOf(msg))

	// An extra that cannot be decoded reads as none: a reader that requires one
	// then refuses loudly instead of acting on garbage.
	properties := record.Properties().ToRawMap()
	properties["_ae"] = "not base64 !"
	corrupted := message.NewImmutableMesasge(walimplstest.NewTestMessageID(1), record.Payload(), properties)
	assert.Nil(t, message.AppendExtraOf(corrupted))

	// A MutableMessage that is not this package's implementation is left alone.
	assert.NotPanics(t, func() {
		message.SetAppendExtra(mock_message.NewMockMutableMessage(t), extra)
	})
}

// TestAppendExtraStaysWithTheReplicaItWasAppendedAs: the extra append response
// is what ONE cluster's WAL answered for ONE replica. A replicated message must
// not carry the source cluster's value into this cluster's WAL, and a broadcast
// rebuilt from one replica's record must not hand it to every other replica.
func TestAppendExtraStaysWithTheReplicaItWasAppendedAs(t *testing.T) {
	replica := newAppendExtraTestBroadcast().SplitIntoMutableMessage()[0]
	message.SetAppendExtra(replica, newAppendExtraTestExtra(t))
	record := replica.WithTimeTick(100).WithLastConfirmedUseMessageID().IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	require.NotNil(t, message.AppendExtraOf(record))

	replicated := message.MustNewReplicateMessage("primary", record.IntoImmutableMessageProto())
	assert.Nil(t, message.AppendExtraOf(replicated))

	rebuilt := record.IntoBroadcastMutableMessage()
	assert.Nil(t, message.AppendExtraOf(rebuilt))
	for _, again := range rebuilt.SplitIntoMutableMessage() {
		assert.Nil(t, message.AppendExtraOf(again), again.VChannel())
	}
	// The record itself is untouched.
	assert.NotNil(t, message.AppendExtraOf(record))
}
