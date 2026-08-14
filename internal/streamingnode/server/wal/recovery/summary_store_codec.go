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

type pchannelSummaryChunkFooter struct {
	CodecVersion int
	PChannel     string
	Generation   uint64
	// The WAL assignment term of the writer. Used to arbitrate a same-generation
	// write conflict between two owners (split-brain): the newer term wins, the
	// older term is fenced.
	Term                      int64
	SourceCheckpointMessageID *commonpb.MessageID
	SourceCheckpointTimetick  uint64
	SourceStartTimetick       uint64
	SourceEndTimetick         uint64
	Chunks                    []vchannelSummaryChunkIndex
}

// vchannelSummaryChunk is one vchannel's whole chunk inside the object.
//
// It holds records and nothing else today, but it stays a named payload rather
// than a bare record slice: block-level information — a key range, a filter,
// whatever lets a reader skip the entire block without decoding it — belongs
// here when it arrives. Do not collapse it back into []committedWriteRecord for
// being a single field.
//
// What it must never grow back is a self-describing header. The footer index
// names the vchannel and its checksum proves these bytes, so a pchannel,
// vchannel, generation or codec version here could only repeat what the reader
// already holds and has already verified.
type vchannelSummaryChunk struct {
	Records []committedWriteRecord
}

type vchannelSummaryChunkIndex struct {
	VChannel            string
	Offset              uint64
	Length              uint64
	Checksum            []byte
	RecordCount         uint64
	SourceStartTimetick uint64
	SourceEndTimetick   uint64
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
	recordsByVChannel map[string][]committedWriteRecord,
) ([]byte, *pchannelSummaryChunkFooter, string, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.Write(newPChannelSummaryChunkHeader())

	vchannels := make([]string, 0, len(recordsByVChannel))
	for vchannel := range recordsByVChannel {
		vchannels = append(vchannels, vchannel)
	}
	sort.Strings(vchannels)

	footer := &pchannelSummaryChunkFooter{
		CodecVersion: pchannelSummaryCodecVersion,
		PChannel:     pchannel,
		Generation:   generation,
		Term:         term,
		Chunks:       make([]vchannelSummaryChunkIndex, 0, len(vchannels)),
	}
	if checkpoint := newPChannelSummarySourceCheckpoint(sourceCheckpoint); checkpoint != nil {
		footer.SourceCheckpointMessageID = cloneMessageIDProto(checkpoint.MessageID)
		footer.SourceCheckpointTimetick = checkpoint.TimeTick
	}

	for _, vchannel := range vchannels {
		records := cloneAndSortCommittedWriteRecords(pchannel, vchannel, recordsByVChannel[vchannel])
		if len(records) == 0 {
			continue
		}
		payload, err := marshalVChannelSummaryChunk(&vchannelSummaryChunk{Records: records})
		if err != nil {
			return nil, nil, "", err
		}
		offset := uint64(buf.Len())
		buf.Write(payload)
		startTimetick, endTimetick := committedWriteRecordSourceRange(records)
		extendPChannelSummaryChunkFooterSourceRange(footer, startTimetick, endTimetick)
		footer.Chunks = append(footer.Chunks, vchannelSummaryChunkIndex{
			VChannel:            vchannel,
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

func unmarshalPChannelSummaryChunk(payload []byte) (map[string][]committedWriteRecord, *pchannelSummaryChunkFooter, string, error) {
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

	recordsByVChannel := make(map[string][]committedWriteRecord, len(footer.Chunks))
	for _, chunkIndex := range footer.Chunks {
		end := chunkIndex.Offset + chunkIndex.Length
		if chunkIndex.Offset < uint64(pchannelSummaryChunkHeaderSize) || end > uint64(footerStart) || chunkIndex.Offset > end {
			return nil, nil, "", pchannelSummaryStoreCorruptedf("invalid vchannel summary chunk range for vchannel %s", chunkIndex.VChannel)
		}
		chunkPayload := payload[chunkIndex.Offset:end]
		if !bytes.Equal(chunkIndex.Checksum, chunkChecksum(chunkPayload)) {
			return nil, nil, "", pchannelSummaryStoreCorruptedf("vchannel summary chunk checksum mismatch for vchannel %s", chunkIndex.VChannel)
		}
		chunk, err := unmarshalVChannelSummaryChunk(chunkPayload, footer.PChannel, chunkIndex.VChannel)
		if err != nil {
			return nil, nil, "", markPChannelSummaryStoreCorrupted(err)
		}
		// The record count is the footer's own tally, kept independently of the
		// payload, so it is worth cross-checking. Identity is not: the vchannel
		// comes from the index, and the checksum above already proved these are
		// the bytes that index describes.
		if uint64(len(chunk.Records)) != chunkIndex.RecordCount {
			return nil, nil, "", pchannelSummaryStoreCorruptedf("vchannel summary chunk record count mismatch for vchannel %s", chunkIndex.VChannel)
		}
		recordsByVChannel[chunkIndex.VChannel] = chunk.Records
	}
	return recordsByVChannel, footer, hex.EncodeToString(storedFooterChecksum), nil
}

// marshalVChannelSummaryChunk encodes one vchannel's chunk. Its records must
// already be cloned and sorted by the caller.
//
// No self-checksum: the bytes are protected by the footer's per-chunk index
// checksum (vchannelSummaryChunkIndex.Checksum), computed over exactly these
// bytes and verified before the payload is ever decoded.
func marshalVChannelSummaryChunk(chunk *vchannelSummaryChunk) ([]byte, error) {
	if chunk == nil {
		return nil, merr.WrapErrServiceInternalMsg("nil vchannel summary chunk")
	}
	if len(chunk.Records) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("empty vchannel summary chunk")
	}
	return summaryChunkMarshalOptions.Marshal(chunk.intoProto())
}

// unmarshalVChannelSummaryChunk decodes a chunk payload. The pchannel and
// vchannel come from the footer that located this payload rather than from the
// payload itself; they backfill records that carry no destination of their own.
func unmarshalVChannelSummaryChunk(payload []byte, pchannel, vchannel string) (*vchannelSummaryChunk, error) {
	pb := &streamingpb.VChannelSummaryChunk{}
	if err := proto.Unmarshal(payload, pb); err != nil {
		return nil, markPChannelSummaryStoreCorrupted(err)
	}
	chunk := newVChannelSummaryChunkFromProto(pb)
	chunk.Records = cloneAndSortCommittedWriteRecords(pchannel, vchannel, chunk.Records)
	return chunk, nil
}

func (chunk *vchannelSummaryChunk) intoProto() *streamingpb.VChannelSummaryChunk {
	pb := &streamingpb.VChannelSummaryChunk{
		Records: make([]*streamingpb.CommittedWriteRecord, 0, len(chunk.Records)),
	}
	for _, record := range chunk.Records {
		pb.Records = append(pb.Records, record.intoProto())
	}
	return pb
}

func newVChannelSummaryChunkFromProto(pb *streamingpb.VChannelSummaryChunk) *vchannelSummaryChunk {
	chunk := &vchannelSummaryChunk{
		Records: make([]committedWriteRecord, 0, len(pb.GetRecords())),
	}
	for _, record := range pb.GetRecords() {
		chunk.Records = append(chunk.Records, newCommittedWriteRecordFromProto(record))
	}
	return chunk
}

func newPChannelSummaryChunkHeader() []byte {
	header := make([]byte, pchannelSummaryChunkHeaderSize)
	copy(header, pchannelSummaryChunkHeaderMagic)
	binary.BigEndian.PutUint16(header[8:10], pchannelSummaryCodecVersion)
	binary.BigEndian.PutUint16(header[10:12], 0)
	binary.BigEndian.PutUint32(header[12:16], pchannelSummaryChunkHeaderSize)
	return header
}

func marshalPChannelSummaryChunkFooter(footer *pchannelSummaryChunkFooter) ([]byte, error) {
	if footer == nil {
		return nil, merr.WrapErrServiceInternalMsg("nil pchannel summary chunk footer")
	}
	footer.CodecVersion = pchannelSummaryCodecVersion
	sort.Slice(footer.Chunks, func(i, j int) bool {
		return footer.Chunks[i].VChannel < footer.Chunks[j].VChannel
	})
	// No self-checksum: integrity is verified against the trailer checksum over
	// these exact bytes (see marshal/unmarshalPChannelSummaryChunk).
	return summaryChunkMarshalOptions.Marshal(footer.intoProto())
}

func unmarshalPChannelSummaryChunkFooter(payload []byte) (*pchannelSummaryChunkFooter, error) {
	pb := &streamingpb.PChannelSummaryChunkFooter{}
	if err := proto.Unmarshal(payload, pb); err != nil {
		return nil, markPChannelSummaryStoreCorrupted(err)
	}
	if pb.GetCodecVersion() != pchannelSummaryCodecVersion {
		return nil, pchannelSummaryStoreCorruptedf("unsupported pchannel summary chunk footer version %d", pb.GetCodecVersion())
	}
	return newPChannelSummaryChunkFooterFromProto(pb), nil
}

func (footer *pchannelSummaryChunkFooter) intoProto() *streamingpb.PChannelSummaryChunkFooter {
	pb := &streamingpb.PChannelSummaryChunkFooter{
		CodecVersion:              uint32(footer.CodecVersion),
		Pchannel:                  footer.PChannel,
		Generation:                footer.Generation,
		Term:                      footer.Term,
		SourceCheckpointMessageId: cloneMessageIDProto(footer.SourceCheckpointMessageID),
		SourceCheckpointTimetick:  footer.SourceCheckpointTimetick,
		SourceStartTimetick:       footer.SourceStartTimetick,
		SourceEndTimetick:         footer.SourceEndTimetick,
		Chunks:                    make([]*streamingpb.VChannelSummaryChunkIndex, 0, len(footer.Chunks)),
	}
	for _, index := range footer.Chunks {
		pb.Chunks = append(pb.Chunks, &streamingpb.VChannelSummaryChunkIndex{
			Vchannel:            index.VChannel,
			Offset:              index.Offset,
			Length:              index.Length,
			Checksum:            index.Checksum,
			RecordCount:         index.RecordCount,
			SourceStartTimetick: index.SourceStartTimetick,
			SourceEndTimetick:   index.SourceEndTimetick,
		})
	}
	return pb
}

func newPChannelSummaryChunkFooterFromProto(pb *streamingpb.PChannelSummaryChunkFooter) *pchannelSummaryChunkFooter {
	footer := &pchannelSummaryChunkFooter{
		CodecVersion:              int(pb.GetCodecVersion()),
		PChannel:                  pb.GetPchannel(),
		Generation:                pb.GetGeneration(),
		Term:                      pb.GetTerm(),
		SourceCheckpointMessageID: cloneMessageIDProto(pb.GetSourceCheckpointMessageId()),
		SourceCheckpointTimetick:  pb.GetSourceCheckpointTimetick(),
		SourceStartTimetick:       pb.GetSourceStartTimetick(),
		SourceEndTimetick:         pb.GetSourceEndTimetick(),
		Chunks:                    make([]vchannelSummaryChunkIndex, 0, len(pb.GetChunks())),
	}
	for _, index := range pb.GetChunks() {
		footer.Chunks = append(footer.Chunks, vchannelSummaryChunkIndex{
			VChannel:            index.GetVchannel(),
			Offset:              index.GetOffset(),
			Length:              index.GetLength(),
			Checksum:            index.GetChecksum(),
			RecordCount:         index.GetRecordCount(),
			SourceStartTimetick: index.GetSourceStartTimetick(),
			SourceEndTimetick:   index.GetSourceEndTimetick(),
		})
	}
	return footer
}

func cloneAndSortCommittedWriteRecords(pchannel, vchannel string, records []committedWriteRecord) []committedWriteRecord {
	if len(records) == 0 {
		return nil
	}
	cloned := make([]committedWriteRecord, 0, len(records))
	for _, record := range records {
		record.SourcePChannel = firstNonEmpty(record.SourcePChannel, pchannel)
		record.VChannel = firstNonEmpty(record.VChannel, vchannel)
		record.SourceMessageID = cloneMessageIDProto(record.SourceMessageID)
		record.LastConfirmedMessageID = cloneMessageIDProto(record.LastConfirmedMessageID)
		if record.Idempotency != nil {
			idempotency := *record.Idempotency
			record.Idempotency = &idempotency
		}
		cloned = append(cloned, record)
	}
	sort.Slice(cloned, func(i, j int) bool {
		left, right := cloned[i], cloned[j]
		if left.SourceTimeTick != right.SourceTimeTick {
			return left.SourceTimeTick < right.SourceTimeTick
		}
		if left.SourceMessageID.GetId() != right.SourceMessageID.GetId() {
			return left.SourceMessageID.GetId() < right.SourceMessageID.GetId()
		}
		leftKey, rightKey := committedWriteRecordKey(left), committedWriteRecordKey(right)
		if leftKey != rightKey {
			return leftKey < rightKey
		}
		return false
	})
	return cloned
}

// committedWriteRecordSourceRange returns the timetick span of an already sorted
// record slice. The span is a timetick range only: a vchannel never records a
// physical WAL position.
func committedWriteRecordSourceRange(records []committedWriteRecord) (uint64, uint64) {
	if len(records) == 0 {
		return 0, 0
	}
	return records[0].SourceTimeTick, records[len(records)-1].SourceTimeTick
}

func extendPChannelSummaryChunkFooterSourceRange(footer *pchannelSummaryChunkFooter, startTimetick, endTimetick uint64) {
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

func committedWriteRecordKey(record committedWriteRecord) string {
	if record.Idempotency == nil {
		return ""
	}
	return record.Idempotency.Key
}

func firstNonEmpty(value string, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func chunkChecksum(payload []byte) []byte {
	sum := sha256.Sum256(payload)
	return sum[:]
}
