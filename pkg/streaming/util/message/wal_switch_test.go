package message_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/wp"
)

type walSwitchEnvelope struct{ message.ImmutableMessage }

func TestWALSwitchLinkValidation(t *testing.T) {
	original := &message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker, Config: map[string]string{"keep": "value"}}
	msg := message.NewAlterWALMessageBuilderV2().WithHeader(original).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable()
	id := wp.NewWpID(&wplog.LogMessageId{SegmentId: 2, EntryId: 3})
	for _, m := range []message.BasicMessage{msg, msg.IntoImmutableMessage(id), message.CreateTestEmptyInsertMesage(1, nil)} {
		start, err := message.WALSwitchStart(m)
		require.NoError(t, err)
		require.Nil(t, start)
		require.False(t, message.IsWALSwitch(m))
	}
	typed := message.MustAsMutableAlterWALMessageV2(msg)
	previous := typed.Header()
	message.SetWALSwitchStart(typed.(message.MutableMessage), id)
	require.Nil(t, original.TargetStartMessageId)
	require.Nil(t, previous.TargetStartMessageId, "per-channel update must not mutate a shared header")
	require.Equal(t, original.Config, typed.Header().Config)
	require.False(t, msg.Properties().Exist("_ws"))
	// Reconstruct from serialized WAL fields, not the in-memory typed header.
	encoded, err := proto.Marshal(msg.IntoMessageProto())
	require.NoError(t, err)
	var stored messagespb.Message
	require.NoError(t, proto.Unmarshal(encoded, &stored))
	replayed := message.NewImmutableMesasge(id, stored.Payload, stored.Properties)
	for _, m := range []message.BasicMessage{msg, typed, msg.IntoImmutableMessage(id), replayed, &walSwitchEnvelope{replayed}, message.MustAsImmutableAlterWALMessageV2(replayed)} {
		start, err := message.WALSwitchStart(m)
		require.NoError(t, err)
		require.True(t, id.EQ(start))
		require.True(t, message.IsWALSwitch(m))
	}
	header := message.MustAsImmutableAlterWALMessageV2(replayed).Header()
	require.True(t, proto.Equal(id.IntoProto(), header.TargetStartMessageId))
	require.Equal(t, original.Config, header.Config)
	for _, bad := range []*commonpb.MessageID{
		{WALName: commonpb.WALName_WoodPecker, Id: "bad"},
		{},
	} {
		header := proto.Clone(typed.Header()).(*message.AlterWALMessageHeader)
		header.TargetStartMessageId = bad
		typed.OverwriteHeader(header)
		require.True(t, message.IsWALSwitch(typed), "present but invalid must fail validation, not become legacy")
		_, err := message.WALSwitchStart(typed)
		require.Error(t, err)
	}
	wrong := message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_Pulsar}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable()
	message.SetWALSwitchStart(wrong, id)
	_, err = message.WALSwitchStart(wrong)
	require.Error(t, err)
}

func TestWALSwitchEndsOldChunkRuns(t *testing.T) {
	id := wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 1})
	m := message.NewMutableMessageBeforeAppend(make([]byte, 100), message.CreateTestEmptyInsertMesage(1, nil).WithTimeTick(10).WithLastConfirmed(id).Properties().ToRawMap())
	chunks := message.SplitIntoChunks(m, 50)
	var assembler message.ChunkAssembler
	msg, handled, err := assembler.Push(chunks[0].IntoImmutableMessage(id))
	require.NoError(t, err)
	require.True(t, handled)
	require.Nil(t, msg)
	a := message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(20).WithLastConfirmed(id)
	message.SetWALSwitchStart(a, id)
	_, handled, err = assembler.Push(a.IntoImmutableMessage(id))
	require.NoError(t, err)
	require.False(t, handled)
	// The orphaned remainder cannot reconstruct a message across the boundary.
	msg, handled, err = assembler.Push(chunks[1].IntoImmutableMessage(id))
	require.NoError(t, err)
	require.True(t, handled)
	require.Nil(t, msg)
}

func TestWALSwitchRejectsMissingOrCorruptHeader(t *testing.T) {
	for _, raw := range []string{"", "invalid!"} {
		msg := message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable()
		if raw == "" {
			delete(msg.Properties().ToRawMap(), "_h")
		} else {
			msg.Properties().ToRawMap()["_h"] = raw
		}
		require.Panics(t, func() { message.IsWALSwitch(msg) })
	}
}
