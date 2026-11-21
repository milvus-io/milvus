package message_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func physicalChunk(tt uint64, index, total, id int, payload string) message.ImmutableMessage {
	return message.NewMutableMessageBeforeAppend([]byte(payload), map[string]string{
		"_v": "2", "_t": strconv.Itoa(int(message.MessageTypeInsert)), "_tt": message.EncodeUint64(tt),
		"_ci": strconv.Itoa(index), "_ct": strconv.Itoa(total), "_vc": "test_1v0",
	}).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(id)))
}

func TestChunkReaderInterleavingAndRetry(t *testing.T) {
	var reader message.ChunkAssembler
	for _, m := range []message.ImmutableMessage{
		physicalChunk(10, 0, 2, 1, "abc"), physicalChunk(20, 0, 2, 2, "xy"),
		physicalChunk(10, 0, 2, 3, "abc"),
	} {
		got, handled, err := reader.Push(m)
		require.NoError(t, err)
		require.True(t, handled)
		require.Nil(t, got)
	}
	got, handled, err := reader.Push(physicalChunk(10, 1, 2, 4, "def"))
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, []byte("abcdef"), got.IntoImmutableMessageProto().Payload)
	require.True(t, got.MessageID().EQ(walimplstest.NewTestMessageID(3)))
	require.False(t, message.IsChunkedPayload(got))
	require.Equal(t, "test_1v0", got.VChannel())
	got, handled, err = reader.Push(physicalChunk(20, 1, 2, 5, "z"))
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, []byte("xyz"), got.IntoImmutableMessageProto().Payload)
}

func TestChunkReaderIncompleteRunAndSeek(t *testing.T) {
	var reader message.ChunkAssembler
	// Starting in the middle cannot deliver a partial logical message.
	got, handled, err := reader.Push(physicalChunk(10, 1, 2, 1, "tail"))
	require.NoError(t, err)
	require.True(t, handled)
	require.Nil(t, got)
	_, _, err = reader.Push(physicalChunk(20, 0, 2, 2, "head"))
	require.NoError(t, err)
	reader.AdvanceTimeTick(20)
	got, handled, err = reader.Push(physicalChunk(20, 1, 2, 3, "tail"))
	require.NoError(t, err)
	require.True(t, handled)
	require.Nil(t, got)
	// An ordinary record remains ordinary after cleanup.
	ordinary := message.NewMutableMessageBeforeAppend([]byte("whole"), map[string]string{"_v": "1"}).IntoImmutableMessage(walimplstest.NewTestMessageID(4))
	got, handled, err = reader.Push(ordinary)
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, got)
}

func TestChunkReaderBarriersDiscardOnlyConfirmedRuns(t *testing.T) {
	for _, kind := range []message.MessageType{message.MessageTypeTimeTick, message.MessageTypeMVCCBarrier} {
		t.Run(kind.String(), func(t *testing.T) {
			var reader message.ChunkAssembler
			for _, tick := range []uint64{10, 20} {
				_, _, err := reader.Push(physicalChunk(tick, 0, 2, int(tick), "head"))
				require.NoError(t, err)
			}
			barrier := message.NewMutableMessageBeforeAppend(nil, map[string]string{
				"_v": "1", "_t": strconv.Itoa(int(kind)), "_tt": message.EncodeUint64(10),
			}).IntoImmutableMessage(walimplstest.NewTestMessageID(30))
			got, handled, err := reader.Push(barrier)
			require.NoError(t, err)
			require.False(t, handled)
			require.Nil(t, got)
			got, handled, err = reader.Push(physicalChunk(10, 1, 2, 31, "tail"))
			require.NoError(t, err)
			require.True(t, handled)
			require.Nil(t, got)
			got, handled, err = reader.Push(physicalChunk(20, 1, 2, 32, "tail"))
			require.NoError(t, err)
			require.True(t, handled)
			require.Equal(t, []byte("headtail"), got.IntoImmutableMessageProto().Payload)
		})
	}
}

func TestChunkReaderRejectsCorruption(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second message.ImmutableMessage
	}{
		{"changed total", physicalChunk(10, 0, 2, 1, "head"), physicalChunk(10, 1, 3, 2, "tail")},
		{"changed payload", physicalChunk(10, 0, 2, 1, "head"), physicalChunk(10, 0, 2, 2, "other")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reader message.ChunkAssembler
			_, _, err := reader.Push(tc.first)
			require.NoError(t, err)
			got, handled, err := reader.Push(tc.second)
			require.ErrorIs(t, err, message.ErrCorruptedChunk)
			require.True(t, handled)
			require.Nil(t, got)
		})
	}
	for _, props := range []map[string]string{
		{"_ci": "0"},
		{"_ci": "0", "_ct": "1", "_tt": message.EncodeUint64(10)},
		{"_ci": "01", "_ct": "2", "_tt": message.EncodeUint64(10)},
		{"_ci": "0", "_ct": "2", "_tt": "!"},
	} {
		var reader message.ChunkAssembler
		m := message.NewMutableMessageBeforeAppend([]byte("bad"), props).IntoImmutableMessage(walimplstest.NewTestMessageID(1))
		got, handled, err := reader.Push(m)
		require.ErrorIs(t, err, message.ErrCorruptedChunk)
		require.True(t, handled)
		require.Nil(t, got)
	}
}
