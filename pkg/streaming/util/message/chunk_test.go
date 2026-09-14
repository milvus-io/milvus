package message

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireChunkPush(t *testing.T, assembler *ChunkAssembler, msg ImmutableMessage) (ImmutableMessage, bool) {
	t.Helper()
	assembled, handled, err := assembler.Push(msg)
	require.NoError(t, err)
	return assembled, handled
}

func TestSplitIntoChunksFitsInOne(t *testing.T) {
	payload := []byte("hello world")
	msg := NewMutableMessageBeforeAppend(payload, map[string]string{"_t": "x", "_v": "1"})

	chunks := SplitIntoChunks(msg, 100)
	require.Len(t, chunks, 1)
	assert.Same(t, msg, chunks[0])
}

func TestSplitIntoChunksRoundTrip(t *testing.T) {
	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	props := map[string]string{
		"_t":  "insert",
		"_v":  "1",
		"_tt": "123",
		"_tx": "whatever",
	}
	msg := NewMutableMessageBeforeAppend(payload, props)

	chunks := SplitIntoChunks(msg, 300)
	// ceil(1000/300) = 4 chunks: 300+300+300+100.
	require.Len(t, chunks, 4)

	// Every chunk carries the original properties plus index/total markers.
	for i, c := range chunks {
		assert.True(t, IsChunkedPayload(c))
		assert.Equal(t, i, ChunkIndex(c))
		assert.Equal(t, 4, ChunkTotal(c))
	}

	// Round-trip the chunks through immutable messages (as the read side sees
	// them from the backend), then reassemble.
	immutables := make([]ImmutableMessage, len(chunks))
	for i, c := range chunks {
		immutables[i] = c.IntoImmutableMessage(testMessageID(strconv.Itoa(i)))
	}

	assembled := AssembleChunks(immutables)

	// The reassembled payload must be byte-identical to the original.
	assert.Equal(t, payload, assembled.IntoImmutableMessageProto().GetPayload())

	// The logical message ID is the first chunk's ID.
	assert.True(t, testMessageID("0").EQ(assembled.MessageID()))

	// The chunk markers are removed and the original properties preserved.
	assert.False(t, IsChunkedPayload(assembled))
	rawProps := assembled.IntoImmutableMessageProto().GetProperties()
	assert.Equal(t, "123", rawProps["_tt"])
	assert.Equal(t, "whatever", rawProps["_tx"])
	assert.NotContains(t, rawProps, "_ci")
	assert.NotContains(t, rawProps, "_ct")
}

func TestSplitIntoChunksDisabledOnNonPositiveChunkSize(t *testing.T) {
	payload := make([]byte, 100)
	msg := NewMutableMessageBeforeAppend(payload, map[string]string{"_t": "x", "_tt": "100"})

	chunks := SplitIntoChunks(msg, 0)
	require.Len(t, chunks, 1)
	assert.Same(t, msg, chunks[0])
}

func TestSplitIntoChunksExactBoundary(t *testing.T) {
	// payload exactly a multiple of chunkSize must split into exactly that many
	// chunks, with no empty trailing chunk.
	payload := make([]byte, 600)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	msg := NewMutableMessageBeforeAppend(payload, map[string]string{"_t": "x", "_tt": "100"})

	chunks := SplitIntoChunks(msg, 300)
	require.Len(t, chunks, 2)
	for _, c := range chunks {
		assert.Equal(t, 2, ChunkTotal(c))
	}
}

func TestSplitIntoChunksEmptyPayload(t *testing.T) {
	msg := NewMutableMessageBeforeAppend([]byte{}, map[string]string{"_t": "x", "_tt": "100"})
	chunks := SplitIntoChunks(msg, 300)
	require.Len(t, chunks, 1)
	assert.Same(t, msg, chunks[0])
}

func TestChunkAssemblerReassembles(t *testing.T) {
	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	msg := NewMutableMessageBeforeAppend(payload, map[string]string{"_t": "x", "_tt": "100"})
	chunks := SplitIntoChunks(msg, 300) // 4 chunks

	var a ChunkAssembler
	for i, c := range chunks {
		assembled, handled := requireChunkPush(t, &a, c.IntoImmutableMessage(testMessageID(strconv.Itoa(i))))
		require.True(t, handled)
		if i < 3 {
			assert.Nil(t, assembled, "intermediate chunks must be buffered, not assembled")
		} else {
			require.NotNil(t, assembled)
			assert.Equal(t, payload, assembled.IntoImmutableMessageProto().GetPayload())
			assert.True(t, testMessageID("0").EQ(assembled.MessageID()))
		}
	}
}

func TestChunkAssemblerPassesThroughNonChunk(t *testing.T) {
	var a ChunkAssembler
	other := NewMutableMessageBeforeAppend([]byte("plain"), map[string]string{"_t": "x", "_tt": "100"}).
		IntoImmutableMessage(testMessageID("7"))
	assembled, handled := requireChunkPush(t, &a, other)
	assert.False(t, handled, "a non-chunk message must not be swallowed")
	assert.Nil(t, assembled)
}

func TestChunkAssemblerRejectsMalformedMarkers(t *testing.T) {
	tests := []struct {
		name       string
		indexValue *string
		totalValue *string
	}{
		{name: "missing index", totalValue: stringPtr("2")},
		{name: "missing total", indexValue: stringPtr("0")},
		{name: "non numeric index", indexValue: stringPtr("x"), totalValue: stringPtr("2")},
		{name: "signed index", indexValue: stringPtr("+1"), totalValue: stringPtr("2")},
		{name: "negative index", indexValue: stringPtr("-1"), totalValue: stringPtr("2")},
		{name: "leading zero", indexValue: stringPtr("00"), totalValue: stringPtr("2")},
		{name: "overflow", indexValue: stringPtr("0"), totalValue: stringPtr("999999999999999999999999")},
		{name: "single chunk total", indexValue: stringPtr("0"), totalValue: stringPtr("1")},
		{name: "index out of range", indexValue: stringPtr("2"), totalValue: stringPtr("2")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			properties := map[string]string{messageTypeKey: "x", messageTimeTick: "100"}
			if test.indexValue != nil {
				properties[messageChunkIndex] = *test.indexValue
			}
			if test.totalValue != nil {
				properties[messageChunkTotal] = *test.totalValue
			}
			msg := NewMutableMessageBeforeAppend([]byte("x"), properties).
				IntoImmutableMessage(testMessageID("malformed"))

			var assembler ChunkAssembler
			assembled, handled, err := assembler.Push(msg)
			assert.True(t, handled)
			assert.Nil(t, assembled)
			require.ErrorIs(t, err, ErrCorruptedChunk)
		})
	}
}

func TestChunkAssemblerRejectsMalformedTimeTick(t *testing.T) {
	tests := []struct {
		name          string
		timeTickValue *string
	}{
		{name: "missing"},
		{name: "empty", timeTickValue: stringPtr("")},
		{name: "non numeric", timeTickValue: stringPtr("!")},
		{name: "signed", timeTickValue: stringPtr("+1")},
		{name: "leading zero", timeTickValue: stringPtr("01")},
		{name: "uppercase", timeTickValue: stringPtr("A")},
		{name: "overflow", timeTickValue: stringPtr("zzzzzzzzzzzzzzzzzzzzzzzz")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			properties := map[string]string{
				messageTypeKey:    "x",
				messageChunkIndex: "0",
				messageChunkTotal: "2",
			}
			if test.timeTickValue != nil {
				properties[messageTimeTick] = *test.timeTickValue
			}
			msg := NewMutableMessageBeforeAppend([]byte("x"), properties).
				IntoImmutableMessage(testMessageID("malformed-timetick"))

			var assembler ChunkAssembler
			assembled, handled, err := assembler.Push(msg)
			assert.True(t, handled)
			assert.Nil(t, assembled)
			require.ErrorIs(t, err, ErrCorruptedChunk)
		})
	}
}

func TestChunkAssemblerUsesSparseSlotsAndDiscardsOrphansAtTimeTick(t *testing.T) {
	oldHead := NewMutableMessageBeforeAppend([]byte("old"), map[string]string{
		messageTypeKey:    "x",
		messageTimeTick:   "100",
		messageChunkIndex: "0",
		messageChunkTotal: "1000000000",
	}).IntoImmutableMessage(testMessageID("old-head"))

	future := SplitIntoChunks(NewMutableMessageBeforeAppend(
		[]byte{0xAA, 0xBB},
		map[string]string{messageTypeKey: "x", messageTimeTick: "200"},
	), 1)

	var assembler ChunkAssembler
	requireChunkPush(t, &assembler, oldHead)
	requireChunkPush(t, &assembler, future[0].IntoImmutableMessage(testMessageID("future-head")))
	oldTimeTick := oldHead.TimeTick()
	futureTimeTick := future[0].TimeTick()
	require.Len(t, assembler.runs[oldTimeTick].slots, 1, "declared total must not preallocate slots")

	timeTick := NewMutableMessageBeforeAppend(nil, map[string]string{
		messageTypeKey:  MessageTypeTimeTick.marshal(),
		messageTimeTick: "150",
	}).IntoImmutableMessage(testMessageID("timetick"))
	assembled, handled := requireChunkPush(t, &assembler, timeTick)
	assert.False(t, handled)
	assert.Nil(t, assembled)
	assert.NotContains(t, assembler.runs, oldTimeTick)
	assert.Contains(t, assembler.runs, futureTimeTick, "a run newer than the TimeTick remains live")

	assembled, handled = requireChunkPush(t, &assembler, future[1].IntoImmutableMessage(testMessageID("future-tail")))
	assert.True(t, handled)
	require.NotNil(t, assembled)
	assert.Equal(t, []byte{0xAA, 0xBB}, assembled.Payload())
	assert.Empty(t, assembler.runs)
}

func stringPtr(value string) *string {
	return &value
}

func TestChunkAssemblerPassesThroughNonChunkDuringIncompleteRun(t *testing.T) {
	payload := make([]byte, 1000)
	msg := NewMutableMessageBeforeAppend(payload, map[string]string{"_t": "x", "_tt": "100"})
	chunks := SplitIntoChunks(msg, 300) // 4 chunks, only feed 2

	var a ChunkAssembler
	requireChunkPush(t, &a, chunks[0].IntoImmutableMessage(testMessageID("0")))
	requireChunkPush(t, &a, chunks[1].IntoImmutableMessage(testMessageID("1")))

	// Unrelated traffic may interleave with a live run. It passes through while
	// the buffered chunks remain available for their eventual tail.
	other := NewMutableMessageBeforeAppend([]byte("other"), map[string]string{"_t": "x", "_tt": "100"}).
		IntoImmutableMessage(testMessageID("2"))
	assembled, handled := requireChunkPush(t, &a, other)
	assert.False(t, handled)
	assert.Nil(t, assembled)

	assembled, handled = requireChunkPush(t, &a, chunks[2].IntoImmutableMessage(testMessageID("3")))
	require.True(t, handled)
	require.Nil(t, assembled)
	assembled, handled = requireChunkPush(t, &a, chunks[3].IntoImmutableMessage(testMessageID("4")))
	require.True(t, handled)
	require.NotNil(t, assembled)
	assert.Equal(t, payload, assembled.IntoImmutableMessageProto().GetPayload())
}

func TestChunkAssemblerAssemblesInterleavedPacksIndependently(t *testing.T) {
	// Pairing is keyed by time tick, not by log adjacency: two packs whose
	// chunks interleave in the stream must each assemble from their own slots.
	payloadA := bytes.Repeat([]byte{0xAA}, 1200) // 4 chunks
	payloadB := bytes.Repeat([]byte{0xBB}, 900)  // 3 chunks
	msgA := NewMutableMessageBeforeAppend(payloadA, map[string]string{"_t": "x", "_tt": "100"})
	msgB := NewMutableMessageBeforeAppend(payloadB, map[string]string{"_t": "x", "_tt": "200"})
	chunksA := SplitIntoChunks(msgA, 300)
	chunksB := SplitIntoChunks(msgB, 300)

	var a ChunkAssembler
	requireChunkPush(t, &a, chunksA[0].IntoImmutableMessage(testMessageID("0")))
	requireChunkPush(t, &a, chunksB[0].IntoImmutableMessage(testMessageID("1")))
	requireChunkPush(t, &a, chunksA[1].IntoImmutableMessage(testMessageID("2")))
	requireChunkPush(t, &a, chunksB[1].IntoImmutableMessage(testMessageID("3")))

	assembledB, handled := requireChunkPush(t, &a, chunksB[2].IntoImmutableMessage(testMessageID("4")))
	require.True(t, handled)
	require.NotNil(t, assembledB)
	assert.Equal(t, payloadB, assembledB.IntoImmutableMessageProto().GetPayload())
	assert.True(t, testMessageID("1").EQ(assembledB.MessageID()))

	assembledA, handled := requireChunkPush(t, &a, chunksA[2].IntoImmutableMessage(testMessageID("5")))
	assert.Nil(t, assembledA)
	require.True(t, handled)
	assembledA, handled = requireChunkPush(t, &a, chunksA[3].IntoImmutableMessage(testMessageID("6")))
	require.True(t, handled)
	require.NotNil(t, assembledA)
	assert.Equal(t, payloadA, assembledA.IntoImmutableMessageProto().GetPayload())
	assert.True(t, testMessageID("0").EQ(assembledA.MessageID()))
}

func TestChunkAssemblerDoesNotRejectManyInterleavedRuns(t *testing.T) {
	const runCount = 1025
	allChunks := make([][]MutableMessage, runCount)
	for i := range allChunks {
		msg := NewMutableMessageBeforeAppend(
			[]byte{byte(i), byte(i + 1)},
			map[string]string{"_t": "x", "_tt": strconv.Itoa(100 + i)},
		)
		allChunks[i] = SplitIntoChunks(msg, 1)
		require.Len(t, allChunks[i], 2)
	}

	var a ChunkAssembler
	for i, chunks := range allChunks {
		assembled, handled := requireChunkPush(t, &a, chunks[0].IntoImmutableMessage(testMessageID("head-"+strconv.Itoa(i))))
		require.True(t, handled)
		require.Nil(t, assembled)
	}

	for i, chunks := range allChunks {
		assembled, handled := requireChunkPush(t, &a, chunks[1].IntoImmutableMessage(testMessageID("tail-"+strconv.Itoa(i))))
		require.True(t, handled)
		require.NotNil(t, assembled, "live run %d was discarded", i)
		assert.Equal(t, []byte{byte(i), byte(i + 1)}, assembled.IntoImmutableMessageProto().GetPayload())
		assert.True(t, testMessageID("head-"+strconv.Itoa(i)).EQ(assembled.MessageID()))
	}
}

func TestChunkAssemblerSwallowsRedeliveredMiddleChunk(t *testing.T) {
	payload := make([]byte, 900)
	for i := range payload {
		payload[i] = byte(i % 253)
	}
	msg := NewMutableMessageBeforeAppend(payload, map[string]string{"_t": "x", "_tt": "100"})
	chunks := SplitIntoChunks(msg, 300) // 3 chunks

	var a ChunkAssembler
	requireChunkPush(t, &a, chunks[0].IntoImmutableMessage(testMessageID("0")))
	requireChunkPush(t, &a, chunks[1].IntoImmutableMessage(testMessageID("1")))

	// The backend persisted chunk 1 but its send surfaced as an error, so the
	// producer rewrote it under a new message ID. The duplicate must be
	// swallowed without corrupting the run.
	dup, handled := requireChunkPush(t, &a, chunks[1].IntoImmutableMessage(testMessageID("9")))
	require.True(t, handled)
	assert.Nil(t, dup)

	assembled, handled := requireChunkPush(t, &a, chunks[2].IntoImmutableMessage(testMessageID("2")))
	require.True(t, handled)
	require.NotNil(t, assembled)
	assert.Equal(t, payload, assembled.IntoImmutableMessageProto().GetPayload())
}

func TestChunkAssemblerDuplicateHeadUsesLatestObservation(t *testing.T) {
	msg := NewMutableMessageBeforeAppend(
		[]byte{0xAA, 0xBB},
		map[string]string{"_t": "x", "_tt": "100"},
	)
	chunks := SplitIntoChunks(msg, 1)
	require.Len(t, chunks, 2)

	headProto := chunks[0].IntoMessageProto()
	newHead := func(id, traceContext string) ImmutableMessage {
		props := make(map[string]string, len(headProto.GetProperties()))
		for key, value := range headProto.GetProperties() {
			props[key] = value
		}
		props[messageTraceContext] = traceContext
		return NewMutableMessageBeforeAppend(headProto.GetPayload(), props).
			IntoImmutableMessage(testMessageID(id))
	}

	var a ChunkAssembler
	assembled, handled := requireChunkPush(t, &a, newHead("persisted-before-error", "first-attempt"))
	require.True(t, handled)
	require.Nil(t, assembled)
	assembled, handled = requireChunkPush(t, &a, newHead("successful-retry", "successful-attempt"))
	require.True(t, handled)
	require.Nil(t, assembled)
	assembled, handled = requireChunkPush(t, &a, chunks[1].IntoImmutableMessage(testMessageID("tail")))
	require.True(t, handled)
	require.NotNil(t, assembled)
	assert.True(t, testMessageID("successful-retry").EQ(assembled.MessageID()))
	assert.Equal(t, "successful-attempt", assembled.IntoImmutableMessageProto().GetProperties()[messageTraceContext])
}

func TestChunkAssemblerRejectsNonDuplicateRegression(t *testing.T) {
	payloadA := bytes.Repeat([]byte{0xAA}, 900)
	payloadB := bytes.Repeat([]byte{0xBB}, 900)
	msgA := NewMutableMessageBeforeAppend(payloadA, map[string]string{"_t": "x", "_tt": "100"})
	msgB := NewMutableMessageBeforeAppend(payloadB, map[string]string{"_t": "x", "_tt": "100"})
	chunksA := SplitIntoChunks(msgA, 300)
	chunksB := SplitIntoChunks(msgB, 300)

	var a ChunkAssembler
	requireChunkPush(t, &a, chunksA[0].IntoImmutableMessage(testMessageID("0")))
	requireChunkPush(t, &a, chunksA[1].IntoImmutableMessage(testMessageID("1")))

	// Same index, same total, different bytes: not a redelivery but a broken
	// log. The scanner must stop rather than advance past the logical message.
	interloper, handled, err := a.Push(chunksB[1].IntoImmutableMessage(testMessageID("8")))
	require.True(t, handled)
	assert.Nil(t, interloper)
	require.ErrorIs(t, err, ErrCorruptedChunk)

	// The corrupt run was removed before the error was returned.
	tail, handled := requireChunkPush(t, &a, chunksA[2].IntoImmutableMessage(testMessageID("2")))
	require.True(t, handled)
	assert.Nil(t, tail, "stale tail of a discarded run must not complete it")

	// The next fresh run assembles normally.
	requireChunkPush(t, &a, chunksB[0].IntoImmutableMessage(testMessageID("3")))
	requireChunkPush(t, &a, chunksB[1].IntoImmutableMessage(testMessageID("4")))
	assembled, handled := requireChunkPush(t, &a, chunksB[2].IntoImmutableMessage(testMessageID("5")))
	require.True(t, handled)
	require.NotNil(t, assembled)
	assert.Equal(t, payloadB, assembled.IntoImmutableMessageProto().GetPayload())
}

func TestChunkAssemblerRejectsTotalMismatch(t *testing.T) {
	msg := NewMutableMessageBeforeAppend(
		bytes.Repeat([]byte{0xAA}, 900),
		map[string]string{messageTypeKey: "x", messageTimeTick: "100"},
	)
	chunks := SplitIntoChunks(msg, 300)

	var assembler ChunkAssembler
	requireChunkPush(t, &assembler, chunks[0].IntoImmutableMessage(testMessageID("head")))
	mismatched := chunks[1].IntoMessageProto()
	mismatched.Properties[messageChunkTotal] = "4"
	assembled, handled, err := assembler.Push(
		NewMutableMessageBeforeAppend(mismatched.Payload, mismatched.Properties).
			IntoImmutableMessage(testMessageID("mismatch")),
	)
	assert.True(t, handled)
	assert.Nil(t, assembled)
	require.ErrorIs(t, err, ErrCorruptedChunk)
}

func TestChunkAssemblerSwallowsOrphanMiddleChunk(t *testing.T) {
	payload := make([]byte, 600)
	msg := NewMutableMessageBeforeAppend(payload, map[string]string{"_t": "x", "_tt": "100"})
	chunks := SplitIntoChunks(msg, 300)

	var a ChunkAssembler
	// A middle chunk whose run head was never delivered must not be buffered:
	// without its head there is no complete logical-message identity to retain.
	orphan, handled := requireChunkPush(t, &a, chunks[1].IntoImmutableMessage(testMessageID("7")))
	require.True(t, handled)
	assert.Nil(t, orphan)

	// And it leaves nothing behind for the next run.
	requireChunkPush(t, &a, chunks[0].IntoImmutableMessage(testMessageID("0")))
	assembled, handled := requireChunkPush(t, &a, chunks[1].IntoImmutableMessage(testMessageID("1")))
	require.True(t, handled)
	require.NotNil(t, assembled)
	assert.Equal(t, payload, assembled.IntoImmutableMessageProto().GetPayload())
}

// TestChunkAssemblerEndToEndDeliveryStream simulates a realistic delivery
// stream as the read side sees it from a backend: several oversized messages,
// retry redeliveries injected right after their originals, and unrelated
// complete messages flowing between packs. Every pack must assemble exactly
// once, byte-identical, and every unrelated message must pass through untouched.
func TestChunkAssemblerEndToEndDeliveryStream(t *testing.T) {
	const packs = 6
	chunkSize := 256

	// Build the logical messages and their splits.
	type pack struct {
		payload []byte
		chunks  []MutableMessage
	}
	packsData := make([]pack, packs)
	for k := 0; k < packs; k++ {
		size := chunkSize*(k%3+2) - k*7 // varying sizes, always >= 2 chunks
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i*31 + k*7)
		}
		msg := NewMutableMessageBeforeAppend(payload, map[string]string{"_t": "insert", "_tt": strconv.Itoa(100 + k), "_k": strconv.Itoa(k)})
		chunks := SplitIntoChunks(msg, chunkSize)
		require.GreaterOrEqual(t, len(chunks), 2)
		packsData[k] = pack{payload: payload, chunks: chunks}
	}

	nextID := 0
	newID := func() string { nextID++; return strconv.Itoa(nextID) }

	// Produce the delivery sequence exactly as the log would hold it.
	var stream []ImmutableMessage
	var wantPassthrough []ImmutableMessage
	for k := 0; k < packs; k++ {
		for ci, c := range packsData[k].chunks {
			stream = append(stream, c.IntoImmutableMessage(testMessageID(newID())))
			if ci%2 == 1 { // persisted-but-unacked rewrite of the same record
				stream = append(stream, c.IntoImmutableMessage(testMessageID(newID())))
			}
		}
		if k != packs-1 { // unrelated traffic between packs
			m := NewMutableMessageBeforeAppend([]byte{byte(k)}, map[string]string{"_t": "delete", "_tt": "200"})
			imm := m.IntoImmutableMessage(testMessageID(newID()))
			stream = append(stream, imm)
			wantPassthrough = append(wantPassthrough, imm)
		}
	}

	// Drive the assembler.
	var a ChunkAssembler
	var gotPacks [][]byte
	var gotPassthrough []ImmutableMessage
	for _, m := range stream {
		assembled, handled := requireChunkPush(t, &a, m)
		if !handled {
			// Not a chunk: the caller processes it normally.
			gotPassthrough = append(gotPassthrough, m)
			continue
		}
		if assembled != nil {
			gotPacks = append(gotPacks, assembled.IntoImmutableMessageProto().GetPayload())
		}
	}

	// Every pack came out exactly once, byte-identical, in order.
	require.Len(t, gotPacks, packs)
	for k := range packsData {
		assert.Equal(t, packsData[k].payload, gotPacks[k], "pack %d corrupted", k)
	}
	// Every unrelated message passed through exactly once.
	require.Len(t, gotPassthrough, len(wantPassthrough))
	for i := range wantPassthrough {
		assert.True(t, wantPassthrough[i].MessageID().EQ(gotPassthrough[i].MessageID()))
	}
	// Nothing left buffered.
	_, handled := requireChunkPush(t, &a, NewMutableMessageBeforeAppend([]byte("x"), map[string]string{"_t": "insert", "_tt": "300"}).IntoImmutableMessage(testMessageID(newID())))
	assert.False(t, handled, "trailing non-chunk must process normally")
}

// TestChunkAssemblerSwallowsLateDuplicateAfterCompletion covers a defensive
// case: a redelivered copy arriving after its run already completed (e.g. a
// scanner racing ahead across a checkpoint). It must neither resurrect the
// old run nor pollute the next one.
func TestChunkAssemblerSwallowsLateDuplicateAfterCompletion(t *testing.T) {
	payload := make([]byte, 900)
	msg := NewMutableMessageBeforeAppend(payload, map[string]string{"_t": "x", "_tt": "100"})
	chunks := SplitIntoChunks(msg, 300)

	var a ChunkAssembler
	requireChunkPush(t, &a, chunks[0].IntoImmutableMessage(testMessageID("0")))
	requireChunkPush(t, &a, chunks[1].IntoImmutableMessage(testMessageID("1")))
	assembled, handled := requireChunkPush(t, &a, chunks[2].IntoImmutableMessage(testMessageID("2")))
	require.True(t, handled)
	require.NotNil(t, assembled)

	late, handled := requireChunkPush(t, &a, chunks[1].IntoImmutableMessage(testMessageID("9")))
	require.True(t, handled)
	assert.Nil(t, late)

	// The next run is unaffected.
	requireChunkPush(t, &a, chunks[0].IntoImmutableMessage(testMessageID("3")))
	requireChunkPush(t, &a, chunks[1].IntoImmutableMessage(testMessageID("4")))
	second, handled := requireChunkPush(t, &a, chunks[2].IntoImmutableMessage(testMessageID("5")))
	require.True(t, handled)
	require.NotNil(t, second)
	assert.Equal(t, payload, second.IntoImmutableMessageProto().GetPayload())
}
