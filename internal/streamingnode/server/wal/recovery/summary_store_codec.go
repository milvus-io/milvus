package recovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	// pchannelSummaryCodecVersion is bumped to 2: the footer checksum moved out of
	// the JSON body into the binary trailer so integrity is verified against the
	// exact stored bytes, not a JSON re-serialization of the parsed struct.
	pchannelSummaryCodecVersion      = 2
	pchannelSummaryChunkHeaderSize   = 16
	pchannelSummaryChunkChecksumSize = sha256.Size
)

var (
	pchannelSummaryChunkHeaderMagic = []byte("PSCCH001")
	pchannelSummaryChunkFooterMagic = []byte("PSCFT001")
)

type pchannelSummarySourceCheckpoint struct {
	MessageID *commonpb.MessageID `json:"message_id,omitempty"`
	TimeTick  uint64              `json:"timetick"`
}

type pchannelSummaryChunkFooter struct {
	CodecVersion int    `json:"codec_version"`
	PChannel     string `json:"pchannel"`
	Generation   uint64 `json:"generation"`
	// The WAL assignment term of the writer. Used to arbitrate a same-generation
	// write conflict between two owners (split-brain): the newer term wins, the
	// older term is fenced. Chunks written before this field decode as term 0.
	Term                      int64                       `json:"term,omitempty"`
	SourceCheckpointMessageID *commonpb.MessageID         `json:"source_checkpoint_message_id,omitempty"`
	SourceCheckpointTimetick  uint64                      `json:"source_checkpoint_timetick,omitempty"`
	SourceStartMessageID      *commonpb.MessageID         `json:"source_start_message_id,omitempty"`
	SourceEndMessageID        *commonpb.MessageID         `json:"source_end_message_id,omitempty"`
	SourceStartTimetick       uint64                      `json:"source_start_timetick,omitempty"`
	SourceEndTimetick         uint64                      `json:"source_end_timetick,omitempty"`
	Chunks                    []vchannelSummaryChunkIndex `json:"chunks"`
}

type vchannelSummaryChunkIndex struct {
	VChannel             string              `json:"vchannel"`
	Offset               uint64              `json:"offset"`
	Length               uint64              `json:"length"`
	Checksum             string              `json:"checksum"`
	RecordCount          uint64              `json:"record_count"`
	SourceStartMessageID *commonpb.MessageID `json:"source_start_message_id,omitempty"`
	SourceEndMessageID   *commonpb.MessageID `json:"source_end_message_id,omitempty"`
	SourceStartTimetick  uint64              `json:"source_start_timetick,omitempty"`
	SourceEndTimetick    uint64              `json:"source_end_timetick,omitempty"`
}

type vchannelSummaryChunk struct {
	CodecVersion         int                    `json:"codec_version"`
	PChannel             string                 `json:"pchannel"`
	VChannel             string                 `json:"vchannel"`
	Generation           uint64                 `json:"generation"`
	SourceStartMessageID *commonpb.MessageID    `json:"source_start_message_id,omitempty"`
	SourceEndMessageID   *commonpb.MessageID    `json:"source_end_message_id,omitempty"`
	SourceStartTimetick  uint64                 `json:"source_start_timetick,omitempty"`
	SourceEndTimetick    uint64                 `json:"source_end_timetick,omitempty"`
	Records              []committedWriteRecord `json:"records"`
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
		chunk := &vchannelSummaryChunk{
			CodecVersion: pchannelSummaryCodecVersion,
			PChannel:     pchannel,
			VChannel:     vchannel,
			Generation:   generation,
			Records:      records,
		}
		payload, err := marshalVChannelSummaryChunk(chunk)
		if err != nil {
			return nil, nil, "", err
		}
		offset := uint64(buf.Len())
		buf.Write(payload)
		extendPChannelSummaryChunkFooterSourceRange(footer, chunk)
		footer.Chunks = append(footer.Chunks, vchannelSummaryChunkIndex{
			VChannel:             vchannel,
			Offset:               offset,
			Length:               uint64(len(payload)),
			Checksum:             checksumHex(payload),
			RecordCount:          uint64(len(records)),
			SourceStartMessageID: cloneMessageIDProto(chunk.SourceStartMessageID),
			SourceEndMessageID:   cloneMessageIDProto(chunk.SourceEndMessageID),
			SourceStartTimetick:  chunk.SourceStartTimetick,
			SourceEndTimetick:    chunk.SourceEndTimetick,
		})
	}

	footerPayload, err := marshalPChannelSummaryChunkFooter(footer)
	if err != nil {
		return nil, nil, "", err
	}
	// bytes.Buffer.Write never returns an error, so the trailer writes are unchecked.
	buf.Write(footerPayload)
	// Checksum the footer bytes exactly as written and carry it in the trailer,
	// so verification never re-marshals the parsed footer (which would break the
	// day a proto field's JSON encoding changes).
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
		if chunkIndex.Checksum != "" && chunkIndex.Checksum != checksumHex(chunkPayload) {
			return nil, nil, "", pchannelSummaryStoreCorruptedf("vchannel summary chunk checksum mismatch for vchannel %s", chunkIndex.VChannel)
		}
		decodedChunk, err := unmarshalVChannelSummaryChunk(chunkPayload)
		if err != nil {
			return nil, nil, "", markPChannelSummaryStoreCorrupted(err)
		}
		if decodedChunk.VChannel != chunkIndex.VChannel {
			return nil, nil, "", pchannelSummaryStoreCorruptedf("vchannel summary chunk vchannel mismatch, footer %s, payload %s", chunkIndex.VChannel, decodedChunk.VChannel)
		}
		if decodedChunk.Generation != footer.Generation {
			return nil, nil, "", pchannelSummaryStoreCorruptedf("vchannel summary chunk generation mismatch for vchannel %s, footer %d, payload %d", chunkIndex.VChannel, footer.Generation, decodedChunk.Generation)
		}
		if uint64(len(decodedChunk.Records)) != chunkIndex.RecordCount {
			return nil, nil, "", pchannelSummaryStoreCorruptedf("vchannel summary chunk record count mismatch for vchannel %s", chunkIndex.VChannel)
		}
		recordsByVChannel[chunkIndex.VChannel] = decodedChunk.Records
	}
	return recordsByVChannel, footer, hex.EncodeToString(storedFooterChecksum), nil
}

func marshalVChannelSummaryChunk(chunk *vchannelSummaryChunk) ([]byte, error) {
	if chunk == nil {
		return nil, merr.WrapErrServiceInternalMsg("nil vchannel summary chunk")
	}
	if len(chunk.Records) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("empty vchannel summary chunk")
	}
	chunk.CodecVersion = pchannelSummaryCodecVersion
	chunk.Records = cloneAndSortCommittedWriteRecords(chunk.PChannel, chunk.VChannel, chunk.Records)
	chunk.SourceStartMessageID, chunk.SourceEndMessageID, chunk.SourceStartTimetick, chunk.SourceEndTimetick = committedWriteRecordSourceRange(chunk.Records)
	// No self-checksum: the chunk's bytes are protected by the footer's per-chunk
	// index checksum (vchannelSummaryChunkIndex.Checksum), computed over these exact
	// bytes and verified before this chunk is ever decoded.
	return json.Marshal(chunk)
}

func unmarshalVChannelSummaryChunk(payload []byte) (*vchannelSummaryChunk, error) {
	chunk := &vchannelSummaryChunk{}
	if err := json.Unmarshal(payload, chunk); err != nil {
		return nil, markPChannelSummaryStoreCorrupted(err)
	}
	if chunk.CodecVersion != pchannelSummaryCodecVersion {
		return nil, pchannelSummaryStoreCorruptedf("unsupported vchannel summary chunk version %d", chunk.CodecVersion)
	}
	chunk.Records = cloneAndSortCommittedWriteRecords(chunk.PChannel, chunk.VChannel, chunk.Records)
	return chunk, nil
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
	return json.Marshal(footer)
}

func unmarshalPChannelSummaryChunkFooter(payload []byte) (*pchannelSummaryChunkFooter, error) {
	footer := &pchannelSummaryChunkFooter{}
	if err := json.Unmarshal(payload, footer); err != nil {
		return nil, markPChannelSummaryStoreCorrupted(err)
	}
	if footer.CodecVersion != pchannelSummaryCodecVersion {
		return nil, pchannelSummaryStoreCorruptedf("unsupported pchannel summary chunk footer version %d", footer.CodecVersion)
	}
	return footer, nil
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
		record.Rows = append([]committedWriteRow(nil), record.Rows...)
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

func committedWriteRecordSourceRange(records []committedWriteRecord) (*commonpb.MessageID, *commonpb.MessageID, uint64, uint64) {
	if len(records) == 0 {
		return nil, nil, 0, 0
	}
	start := records[0]
	end := records[len(records)-1]
	return cloneMessageIDProto(start.SourceMessageID), cloneMessageIDProto(end.SourceMessageID), start.SourceTimeTick, end.SourceTimeTick
}

func extendPChannelSummaryChunkFooterSourceRange(footer *pchannelSummaryChunkFooter, chunk *vchannelSummaryChunk) {
	if footer == nil || chunk == nil {
		return
	}
	if footer.SourceStartTimetick == 0 || chunk.SourceStartTimetick < footer.SourceStartTimetick {
		footer.SourceStartTimetick = chunk.SourceStartTimetick
		footer.SourceStartMessageID = cloneMessageIDProto(chunk.SourceStartMessageID)
	}
	if chunk.SourceEndTimetick > footer.SourceEndTimetick {
		footer.SourceEndTimetick = chunk.SourceEndTimetick
		footer.SourceEndMessageID = cloneMessageIDProto(chunk.SourceEndMessageID)
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

func checksumHex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
