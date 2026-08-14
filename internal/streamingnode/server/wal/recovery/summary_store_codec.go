package recovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	// pchannelSummaryCodecVersion is the on-disk chunk format version. A chunk
	// whose version does not match is rejected rather than parsed on a guess.
	pchannelSummaryCodecVersion      = 1
	pchannelSummaryChunkHeaderSize   = 16
	pchannelSummaryChunkChecksumSize = sha256.Size
)

var (
	pchannelSummaryChunkHeaderMagic = []byte("PSCCH001")
	pchannelSummaryChunkFooterMagic = []byte("PSCFT001")
)

// summaryChunkMarshalOptions pins deterministic output. The persist path treats
// a byte-identical rewrite of a generation as an idempotent retry, so a stable
// encoding keeps the common retry cheap. It is only an optimization, not a
// correctness dependency: proto guarantees determinism within a build but not
// across versions, so writePChannelSummaryChunkIfAbsent falls back to comparing
// the decoded footer identity rather than trusting byte equality.
var summaryChunkMarshalOptions = proto.MarshalOptions{Deterministic: true}

type pchannelSummarySourceCheckpoint struct {
	MessageID *commonpb.MessageID
	TimeTick  uint64
}

func newPChannelSummarySourceCheckpoint(checkpoint *WALCheckpoint) *pchannelSummarySourceCheckpoint {
	if checkpoint == nil {
		return nil
	}
	sourceCheckpoint := &pchannelSummarySourceCheckpoint{
		TimeTick: checkpoint.TimeTick,
	}
	if checkpoint.MessageID != nil {
		sourceCheckpoint.MessageID = checkpoint.MessageID.IntoProto()
	}
	return sourceCheckpoint
}

func marshalPChannelSummaryChunk(
	pchannel string,
	generation uint64,
	term int64,
	sourceCheckpoint *WALCheckpoint,
	recordsByVChannel map[string][]*streamingpb.SummaryEntry,
) ([]byte, *streamingpb.PChannelSummaryChunkFooter, string, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.Write(newPChannelSummaryChunkHeader())

	vchannels := make([]string, 0, len(recordsByVChannel))
	for vchannel := range recordsByVChannel {
		vchannels = append(vchannels, vchannel)
	}
	sort.Strings(vchannels)

	footer := &streamingpb.PChannelSummaryChunkFooter{
		CodecVersion: uint32(pchannelSummaryCodecVersion),
		Pchannel:     pchannel,
		Generation:   generation,
		Term:         term,
		Chunks:       make([]*streamingpb.VChannelSummaryChunkIndex, 0, len(vchannels)),
	}
	if checkpoint := newPChannelSummarySourceCheckpoint(sourceCheckpoint); checkpoint != nil {
		footer.SourceCheckpointMessageId = cloneMessageIDProto(checkpoint.MessageID)
		footer.SourceCheckpointTimetick = checkpoint.TimeTick
	}

	for _, vchannel := range vchannels {
		records := sortedSummaryEntries(recordsByVChannel[vchannel])
		if len(records) == 0 {
			continue
		}
		payload, err := marshalVChannelSummaryChunk(&streamingpb.VChannelSummaryChunk{
			Vchannel: vchannel,
			Entries:  records,
		})
		if err != nil {
			return nil, nil, "", err
		}
		offset := uint64(buf.Len())
		buf.Write(payload)
		startTimetick, endTimetick := summaryEntrySourceRange(records)
		extendPChannelSummaryChunkFooterSourceRange(footer, startTimetick, endTimetick)
		footer.Chunks = append(footer.Chunks, &streamingpb.VChannelSummaryChunkIndex{
			Vchannel:            vchannel,
			Offset:              offset,
			Length:              uint64(len(payload)),
			Checksum:            chunkChecksum(payload),
			RecordCount:         uint64(len(records)),
			SourceStartTimetick: startTimetick,
			SourceEndTimetick:   endTimetick,
		})
	}

	footerPayload, err := marshalPChannelSummaryChunkFooter(footer)
	if err != nil {
		return nil, nil, "", err
	}
	// bytes.Buffer.Write never returns an error, so the trailer writes are unchecked.
	buf.Write(footerPayload)
	// Checksum the footer bytes exactly as written and carry it in the trailer,
	// so verification never re-marshals the parsed footer — proto marshaling is
	// not guaranteed byte-stable across library versions, and re-deriving would
	// then flag a healthy chunk as corrupt.
	footerChecksum := sha256.Sum256(footerPayload)
	buf.Write(footerChecksum[:])
	footerLen := make([]byte, 4)
	binary.BigEndian.PutUint32(footerLen, uint32(len(footerPayload)))
	buf.Write(footerLen)
	buf.Write(pchannelSummaryChunkFooterMagic)
	return buf.Bytes(), footer, hex.EncodeToString(footerChecksum[:]), nil
}

func unmarshalPChannelSummaryChunk(payload []byte) (map[string][]*streamingpb.SummaryEntry, *streamingpb.PChannelSummaryChunkFooter, string, error) {
	if len(payload) < pchannelSummaryChunkHeaderSize+pchannelSummaryChunkChecksumSize+len(pchannelSummaryChunkFooterMagic)+4 {
		return nil, nil, "", pchannelSummaryStoreCorruptedf("pchannel summary chunk payload too short")
	}
	if !bytes.Equal(payload[:len(pchannelSummaryChunkHeaderMagic)], pchannelSummaryChunkHeaderMagic) {
		return nil, nil, "", pchannelSummaryStoreCorruptedf("invalid pchannel summary chunk header magic")
	}
	if version := binary.BigEndian.Uint16(payload[8:10]); version != pchannelSummaryCodecVersion {
		return nil, nil, "", pchannelSummaryStoreCorruptedf("unsupported pchannel summary chunk version %d", version)
	}
	if headerSize := binary.BigEndian.Uint32(payload[12:16]); headerSize != pchannelSummaryChunkHeaderSize {
		return nil, nil, "", pchannelSummaryStoreCorruptedf("invalid pchannel summary chunk header size %d", headerSize)
	}
	footerMagicStart := len(payload) - len(pchannelSummaryChunkFooterMagic)
	if !bytes.Equal(payload[footerMagicStart:], pchannelSummaryChunkFooterMagic) {
		return nil, nil, "", pchannelSummaryStoreCorruptedf("invalid pchannel summary chunk footer magic")
	}
	footerLenStart := footerMagicStart - 4
	footerChecksumStart := footerLenStart - pchannelSummaryChunkChecksumSize
	if footerChecksumStart < pchannelSummaryChunkHeaderSize {
		return nil, nil, "", pchannelSummaryStoreCorruptedf("invalid pchannel summary chunk footer length offset")
	}
	footerLen := int(binary.BigEndian.Uint32(payload[footerLenStart:footerMagicStart]))
	footerStart := footerChecksumStart - footerLen
	if footerLen <= 0 || footerStart < pchannelSummaryChunkHeaderSize {
		return nil, nil, "", pchannelSummaryStoreCorruptedf("invalid pchannel summary chunk footer length")
	}
	footerPayload := payload[footerStart:footerChecksumStart]
	storedFooterChecksum := payload[footerChecksumStart:footerLenStart]
	if actual := sha256.Sum256(footerPayload); !bytes.Equal(storedFooterChecksum, actual[:]) {
		return nil, nil, "", pchannelSummaryStoreCorruptedf("pchannel summary chunk footer checksum mismatch")
	}
	footer, err := unmarshalPChannelSummaryChunkFooter(footerPayload)
	if err != nil {
		return nil, nil, "", markPChannelSummaryStoreCorrupted(err)
	}

	recordsByVChannel := make(map[string][]*streamingpb.SummaryEntry, len(footer.Chunks))
	for _, chunkIndex := range footer.Chunks {
		end := chunkIndex.Offset + chunkIndex.Length
		if chunkIndex.Offset < uint64(pchannelSummaryChunkHeaderSize) || end > uint64(footerStart) || chunkIndex.Offset > end {
			return nil, nil, "", pchannelSummaryStoreCorruptedf("invalid vchannel summary chunk range for vchannel %s", chunkIndex.Vchannel)
		}
		chunkPayload := payload[chunkIndex.Offset:end]
		if !bytes.Equal(chunkIndex.Checksum, chunkChecksum(chunkPayload)) {
			return nil, nil, "", pchannelSummaryStoreCorruptedf("vchannel summary chunk checksum mismatch for vchannel %s", chunkIndex.Vchannel)
		}
		chunk, err := unmarshalVChannelSummaryChunk(chunkPayload)
		if err != nil {
			return nil, nil, "", markPChannelSummaryStoreCorrupted(err)
		}
		// The record count is the footer's own tally, kept independently of the
		// payload, so it is worth cross-checking. Identity is not: the vchannel
		// comes from the index, and the checksum above already proved these are
		// the bytes that index describes.
		if uint64(len(chunk.Entries)) != chunkIndex.RecordCount {
			return nil, nil, "", pchannelSummaryStoreCorruptedf("vchannel summary chunk record count mismatch for vchannel %s", chunkIndex.Vchannel)
		}
		recordsByVChannel[chunkIndex.Vchannel] = chunk.Entries
	}
	return recordsByVChannel, footer, hex.EncodeToString(storedFooterChecksum), nil
}

// marshalVChannelSummaryChunk encodes one vchannel's chunk. Its records must
// already be cloned and sorted by the caller.
//
// No self-checksum: the bytes are protected by the footer's per-chunk index
// checksum (VChannelSummaryChunkIndex.checksum), computed over exactly these
// bytes and verified before the payload is ever decoded.
func marshalVChannelSummaryChunk(chunk *streamingpb.VChannelSummaryChunk) ([]byte, error) {
	if chunk == nil {
		return nil, merr.WrapErrServiceInternalMsg("nil vchannel summary chunk")
	}
	if len(chunk.GetEntries()) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("empty vchannel summary chunk")
	}
	return summaryChunkMarshalOptions.Marshal(chunk)
}

// unmarshalVChannelSummaryChunk decodes a chunk payload. Where the records
// belong is the chunk's own vchannel and the footer's pchannel; a record carries
// no destination of its own.
func unmarshalVChannelSummaryChunk(payload []byte) (*streamingpb.VChannelSummaryChunk, error) {
	pb := &streamingpb.VChannelSummaryChunk{}
	if err := proto.Unmarshal(payload, pb); err != nil {
		return nil, markPChannelSummaryStoreCorrupted(err)
	}
	pb.Entries = sortedSummaryEntries(pb.GetEntries())
	return pb, nil
}

func newPChannelSummaryChunkHeader() []byte {
	header := make([]byte, pchannelSummaryChunkHeaderSize)
	copy(header, pchannelSummaryChunkHeaderMagic)
	binary.BigEndian.PutUint16(header[8:10], pchannelSummaryCodecVersion)
	binary.BigEndian.PutUint16(header[10:12], 0)
	binary.BigEndian.PutUint32(header[12:16], pchannelSummaryChunkHeaderSize)
	return header
}

func marshalPChannelSummaryChunkFooter(footer *streamingpb.PChannelSummaryChunkFooter) ([]byte, error) {
	if footer == nil {
		return nil, merr.WrapErrServiceInternalMsg("nil pchannel summary chunk footer")
	}
	footer.CodecVersion = uint32(pchannelSummaryCodecVersion)
	sort.Slice(footer.Chunks, func(i, j int) bool {
		return footer.Chunks[i].Vchannel < footer.Chunks[j].Vchannel
	})
	// No self-checksum: integrity is verified against the trailer checksum over
	// these exact bytes (see marshal/unmarshalPChannelSummaryChunk).
	return summaryChunkMarshalOptions.Marshal(footer)
}

func unmarshalPChannelSummaryChunkFooter(payload []byte) (*streamingpb.PChannelSummaryChunkFooter, error) {
	pb := &streamingpb.PChannelSummaryChunkFooter{}
	if err := proto.Unmarshal(payload, pb); err != nil {
		return nil, markPChannelSummaryStoreCorrupted(err)
	}
	if pb.GetCodecVersion() != pchannelSummaryCodecVersion {
		return nil, pchannelSummaryStoreCorruptedf("unsupported pchannel summary chunk footer version %d", pb.GetCodecVersion())
	}
	return pb, nil
}

// sortedSummaryEntries returns the records in chunk order: by source
// timetick, then source message id, then idempotency key. It copies the slice
// but never the records — they are shared by pointer and nothing here mutates
// them.
func sortedSummaryEntries(records []*streamingpb.SummaryEntry) []*streamingpb.SummaryEntry {
	if len(records) == 0 {
		return nil
	}
	sorted := make([]*streamingpb.SummaryEntry, 0, len(records))
	for _, record := range records {
		if record == nil {
			continue
		}
		sorted = append(sorted, record)
	}
	sort.Slice(sorted, func(i, j int) bool {
		left, right := sorted[i], sorted[j]
		if left.GetSourceTimetick() != right.GetSourceTimetick() {
			return left.GetSourceTimetick() < right.GetSourceTimetick()
		}
		if left.GetSourceMessageId().GetId() != right.GetSourceMessageId().GetId() {
			return left.GetSourceMessageId().GetId() < right.GetSourceMessageId().GetId()
		}
		leftKey, rightKey := left.GetIdempotency().GetKey(), right.GetIdempotency().GetKey()
		if leftKey != rightKey {
			return leftKey < rightKey
		}
		return false
	})
	return sorted
}

// summaryEntrySourceRange returns the timetick span of an already sorted
// record slice. The span is a timetick range only: a vchannel never records a
// physical WAL position.
func summaryEntrySourceRange(records []*streamingpb.SummaryEntry) (uint64, uint64) {
	if len(records) == 0 {
		return 0, 0
	}
	return records[0].GetSourceTimetick(), records[len(records)-1].GetSourceTimetick()
}

func extendPChannelSummaryChunkFooterSourceRange(footer *streamingpb.PChannelSummaryChunkFooter, startTimetick, endTimetick uint64) {
	if footer == nil {
		return
	}
	if footer.SourceStartTimetick == 0 || startTimetick < footer.SourceStartTimetick {
		footer.SourceStartTimetick = startTimetick
	}
	if endTimetick > footer.SourceEndTimetick {
		footer.SourceEndTimetick = endTimetick
	}
}

func chunkChecksum(payload []byte) []byte {
	sum := sha256.Sum256(payload)
	return sum[:]
}
