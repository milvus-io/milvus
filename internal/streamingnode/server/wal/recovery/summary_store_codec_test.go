package recovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// reframeTestChunk re-writes a chunk object around a mutated footer, keeping the
// payload region byte-identical. It is how a test produces an object whose index
// disagrees with its payload — the failure the section refs must catch.
func reframeTestChunk(t *testing.T, payload []byte, mutate func(*streamingpb.PChannelSummaryChunkFooter)) []byte {
	t.Helper()
	footer, footerStart, err := unmarshalPChannelSummaryChunkTail(payload)
	require.NoError(t, err)
	mutate(footer)
	footerPayload, err := marshalPChannelSummaryChunkFooter(footer)
	require.NoError(t, err)

	buf := bytes.NewBuffer(append([]byte{}, payload[:footerStart]...))
	buf.Write(footerPayload)
	checksum := sha256.Sum256(footerPayload)
	buf.Write(checksum[:])
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(footerPayload)))
	buf.Write(length)
	buf.Write(pchannelSummaryChunkFooterMagic)
	return buf.Bytes()
}

func TestPChannelSummaryChunkRoundTripsSections(t *testing.T) {
	records := map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 10, 100, 101), newTestSummaryRecord("key-b", 12, 102)},
		"v2": {newTestSummaryRecord("key-c", 11, 200)},
	}

	payload, footer, err := marshalPChannelSummaryChunk("p1", 7, 3, records)
	require.NoError(t, err)
	require.Equal(t, "p1", footer.GetPchannel())
	require.Equal(t, uint64(7), footer.GetGeneration())
	require.Equal(t, int64(3), footer.GetTerm())
	// The chunk's span covers every vchannel in it.
	require.Equal(t, uint64(10), footer.GetStartTimetick())
	require.Equal(t, uint64(12), footer.GetEndTimetick())

	decoded, decodedFooter, err := unmarshalPChannelSummaryChunk(payload)
	require.NoError(t, err)
	require.True(t, proto.Equal(footer, decodedFooter))
	require.Len(t, decoded, 2)

	// The key and the row offsets come from one section, the identity and the
	// primary keys from another; a faithful round trip proves they were rejoined
	// by position.
	v1 := decoded["v1"]
	require.Len(t, v1, 2)
	require.Equal(t, "key-a", v1[0].IdempotencyKey)
	require.Equal(t, uint64(10), v1[0].SourceTimeTick)
	require.True(t, message.MustUnmarshalMessageID(v1[0].SourceMessageID).EQ(rmq.NewRmqID(10)))
	require.True(t, message.MustUnmarshalMessageID(v1[0].LastConfirmedMessageID).EQ(rmq.NewRmqID(9)))
	require.Equal(t, []uint32{0, 1}, v1[0].InsertResult.GetRowOffsets())
	require.Equal(t, []int64{100, 101}, v1[0].InsertResult.GetIds().GetIntId().GetData())
	require.Equal(t, "key-b", v1[1].IdempotencyKey)
	require.Equal(t, []int64{102}, v1[1].InsertResult.GetIds().GetIntId().GetData())

	require.Len(t, decoded["v2"], 1)
	require.Equal(t, "key-c", decoded["v2"][0].IdempotencyKey)
}

func TestPChannelSummaryChunkIndexesEachSectionSeparately(t *testing.T) {
	records := map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 10, 100), newTestSummaryRecord("key-b", 11, 101)},
	}
	payload, footer, err := marshalPChannelSummaryChunk("p1", 0, 0, records)
	require.NoError(t, err)

	require.Len(t, footer.GetChunks(), 1)
	index := footer.GetChunks()[0]
	require.Equal(t, "v1", index.GetVchannel())
	require.Equal(t, uint64(10), index.GetStartTimetick())
	require.Equal(t, uint64(11), index.GetEndTimetick())

	idempotency, inserts := index.GetIdempotency(), index.GetInserts()
	require.NotNil(t, idempotency)
	require.NotNil(t, inserts)
	// Position is the only link between the sections, so their counts must agree.
	require.Equal(t, uint64(2), idempotency.GetRecordCount())
	require.Equal(t, uint64(2), inserts.GetRecordCount())
	// The sections occupy disjoint ranges, which is what makes one readable
	// without the other.
	require.Equal(t, idempotency.GetOffset()+idempotency.GetLength(), inserts.GetOffset())

	// A consumer that wants only the primary keys reads exactly the insert
	// section's range and decodes it on its own. This is the lazy read the split
	// exists for.
	section := &streamingpb.VChannelSummaryInsertSection{}
	require.NoError(t, proto.Unmarshal(payload[inserts.GetOffset():inserts.GetOffset()+inserts.GetLength()], section))
	require.Len(t, section.GetRecords(), 2)
	require.Equal(t, []int64{100}, section.GetRecords()[0].GetIds().GetIntId().GetData())
}

func TestPChannelSummaryChunkOmitsIdempotencySectionWhenNoKeys(t *testing.T) {
	// Nothing on the current write path stages a keyless record, but the format
	// must not require the idempotency section: turning idempotency off has to
	// stop writing it without changing what an insert records.
	keyless := &SummaryRecord{SourceMessageID: rmq.NewRmqID(9).IntoProto(), SourceTimeTick: 9}
	payload, footer, err := marshalPChannelSummaryChunk("p1", 0, 0, map[string][]*SummaryRecord{"v1": {keyless}})
	require.NoError(t, err)
	require.Nil(t, footer.GetChunks()[0].GetIdempotency())
	require.NotNil(t, footer.GetChunks()[0].GetInserts())

	decoded, _, err := unmarshalPChannelSummaryChunk(payload)
	require.NoError(t, err)
	require.Len(t, decoded["v1"], 1)
	require.Empty(t, decoded["v1"][0].IdempotencyKey)
	require.Equal(t, uint64(9), decoded["v1"][0].SourceTimeTick)
}

func TestPChannelSummaryChunkRejectsMisalignedSections(t *testing.T) {
	records := map[string][]*SummaryRecord{
		"v1": {newTestSummaryRecord("key-a", 10, 100), newTestSummaryRecord("key-b", 11, 101)},
	}
	payload, _, err := marshalPChannelSummaryChunk("p1", 0, 0, records)
	require.NoError(t, err)

	// Shorten the idempotency section so it decodes to fewer records than the
	// insert section. Every pairing after the first would be wrong, so this must
	// fail rather than produce plausible records.
	tampered := reframeTestChunk(t, payload, func(footer *streamingpb.PChannelSummaryChunkFooter) {
		ref := footer.GetChunks()[0].GetIdempotency()
		ref.RecordCount = 1
	})
	_, _, err = unmarshalPChannelSummaryChunk(tampered)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
}

func TestPChannelSummaryChunkRejectsSectionOutsidePayload(t *testing.T) {
	records := map[string][]*SummaryRecord{"v1": {newTestSummaryRecord("key-a", 10, 100)}}
	payload, _, err := marshalPChannelSummaryChunk("p1", 0, 0, records)
	require.NoError(t, err)

	// A section that reaches into the footer is a mislocation, and the bounds
	// check is what catches it in the absence of a per-section checksum.
	tampered := reframeTestChunk(t, payload, func(footer *streamingpb.PChannelSummaryChunkFooter) {
		footer.GetChunks()[0].GetInserts().Length = uint64(len(payload))
	})
	_, _, err = unmarshalPChannelSummaryChunk(tampered)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
}

func TestPChannelSummaryChunkRejectsRecordCountMismatch(t *testing.T) {
	records := map[string][]*SummaryRecord{"v1": {newTestSummaryRecord("key-a", 10, 100)}}
	payload, _, err := marshalPChannelSummaryChunk("p1", 0, 0, records)
	require.NoError(t, err)

	tampered := reframeTestChunk(t, payload, func(footer *streamingpb.PChannelSummaryChunkFooter) {
		footer.GetChunks()[0].GetInserts().RecordCount = 5
	})
	_, _, err = unmarshalPChannelSummaryChunk(tampered)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
}

func TestPChannelSummaryChunkRejectsMissingInsertSection(t *testing.T) {
	records := map[string][]*SummaryRecord{"v1": {newTestSummaryRecord("key-a", 10, 100)}}
	payload, _, err := marshalPChannelSummaryChunk("p1", 0, 0, records)
	require.NoError(t, err)

	// The insert section is the write itself; a vchannel index without one
	// describes nothing.
	tampered := reframeTestChunk(t, payload, func(footer *streamingpb.PChannelSummaryChunkFooter) {
		footer.GetChunks()[0].Inserts = nil
	})
	_, _, err = unmarshalPChannelSummaryChunk(tampered)
	require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
}

func TestPChannelSummaryChunkRejectsDamagedFrame(t *testing.T) {
	records := map[string][]*SummaryRecord{"v1": {newTestSummaryRecord("key-a", 10, 100)}}
	payload, _, err := marshalPChannelSummaryChunk("p1", 0, 0, records)
	require.NoError(t, err)

	t.Run("header magic", func(t *testing.T) {
		damaged := append([]byte{}, payload...)
		damaged[0] ^= 0xff
		_, _, err := unmarshalPChannelSummaryChunk(damaged)
		require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	})

	t.Run("version", func(t *testing.T) {
		damaged := append([]byte{}, payload...)
		binary.BigEndian.PutUint16(damaged[8:10], pchannelSummaryCodecVersion+1)
		_, _, err := unmarshalPChannelSummaryChunk(damaged)
		require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	})

	t.Run("trailing magic", func(t *testing.T) {
		damaged := append([]byte{}, payload...)
		damaged[len(damaged)-1] ^= 0xff
		_, _, err := unmarshalPChannelSummaryChunk(damaged)
		require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	})

	t.Run("footer checksum", func(t *testing.T) {
		// The footer is the one region that keeps a checksum: every section offset
		// derives from it, so a silently wrong footer would send every read astray.
		damaged := append([]byte{}, payload...)
		checksumStart := len(damaged) - len(pchannelSummaryChunkFooterMagic) - 4 - pchannelSummaryChunkChecksumSize
		damaged[checksumStart] ^= 0xff
		_, _, err := unmarshalPChannelSummaryChunk(damaged)
		require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	})

	t.Run("truncated", func(t *testing.T) {
		_, _, err := unmarshalPChannelSummaryChunk(payload[:8])
		require.ErrorIs(t, err, ErrPChannelSummaryStoreCorrupted)
	})
}

func TestPChannelSummaryChunkKeyIsDeterministic(t *testing.T) {
	cm := newTestSummaryStoreChunkManager(t)
	first := buildPChannelSummaryChunkKey(cm, "by-dev-rootcoord-dml_0", 7, 3)
	require.Equal(t, first, buildPChannelSummaryChunkKey(cm, "by-dev-rootcoord-dml_0", 7, 3))
	// Generation and term both address the object; neither may be dropped from
	// the key or a fenced writer would overwrite the current owner's chunk.
	require.NotEqual(t, first, buildPChannelSummaryChunkKey(cm, "by-dev-rootcoord-dml_0", 8, 3))
	require.NotEqual(t, first, buildPChannelSummaryChunkKey(cm, "by-dev-rootcoord-dml_0", 7, 4))
}
