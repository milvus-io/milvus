package recovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

const (
	// pchannelWindowCodecVersion is bumped to 2: the footer checksum moved out of
	// the JSON body into the binary trailer so integrity is verified against the
	// exact stored bytes, not a JSON re-serialization of the parsed struct.
	pchannelWindowCodecVersion      = 2
	pchannelWindowChunkHeaderSize   = 16
	pchannelWindowChunkChecksumSize = sha256.Size
)

var (
	pchannelWindowChunkHeaderMagic = []byte("PWCCH001")
	pchannelWindowChunkFooterMagic = []byte("PWCFT001")
)

type pchannelWindowSourceCheckpoint struct {
	MessageID *commonpb.MessageID `json:"message_id,omitempty"`
	TimeTick  uint64              `json:"timetick"`
}

type pchannelWindowChunkFooter struct {
	CodecVersion              int                        `json:"codec_version"`
	PChannel                  string                     `json:"pchannel"`
	Generation                uint64                     `json:"generation"`
	SourceCheckpointMessageID *commonpb.MessageID        `json:"source_checkpoint_message_id,omitempty"`
	SourceCheckpointTimetick  uint64                     `json:"source_checkpoint_timetick,omitempty"`
	SourceStartMessageID      *commonpb.MessageID        `json:"source_start_message_id,omitempty"`
	SourceEndMessageID        *commonpb.MessageID        `json:"source_end_message_id,omitempty"`
	SourceStartTimetick       uint64                     `json:"source_start_timetick,omitempty"`
	SourceEndTimetick         uint64                     `json:"source_end_timetick,omitempty"`
	Chunks                    []vchannelWindowChunkIndex `json:"chunks"`
}

type vchannelWindowChunkIndex struct {
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

type vchannelWindowChunk struct {
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

func newPChannelWindowSourceCheckpoint(checkpoint *WALCheckpoint) *pchannelWindowSourceCheckpoint {
	if checkpoint == nil {
		return nil
	}
	sourceCheckpoint := &pchannelWindowSourceCheckpoint{
		TimeTick: checkpoint.TimeTick,
	}
	if checkpoint.MessageID != nil {
		sourceCheckpoint.MessageID = checkpoint.MessageID.IntoProto()
	}
	return sourceCheckpoint
}

func marshalPChannelWindowChunk(
	pchannel string,
	generation uint64,
	sourceCheckpoint *WALCheckpoint,
	recordsByVChannel map[string][]committedWriteRecord,
) ([]byte, *pchannelWindowChunkFooter, string, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.Write(newPChannelWindowChunkHeader())

	vchannels := make([]string, 0, len(recordsByVChannel))
	for vchannel := range recordsByVChannel {
		vchannels = append(vchannels, vchannel)
	}
	sort.Strings(vchannels)

	footer := &pchannelWindowChunkFooter{
		CodecVersion: pchannelWindowCodecVersion,
		PChannel:     pchannel,
		Generation:   generation,
		Chunks:       make([]vchannelWindowChunkIndex, 0, len(vchannels)),
	}
	if checkpoint := newPChannelWindowSourceCheckpoint(sourceCheckpoint); checkpoint != nil {
		footer.SourceCheckpointMessageID = cloneMessageIDProto(checkpoint.MessageID)
		footer.SourceCheckpointTimetick = checkpoint.TimeTick
	}

	for _, vchannel := range vchannels {
		records := cloneAndSortCommittedWriteRecords(pchannel, vchannel, recordsByVChannel[vchannel])
		if len(records) == 0 {
			continue
		}
		chunk := &vchannelWindowChunk{
			CodecVersion: pchannelWindowCodecVersion,
			PChannel:     pchannel,
			VChannel:     vchannel,
			Generation:   generation,
			Records:      records,
		}
		payload, err := marshalVChannelWindowChunk(chunk)
		if err != nil {
			return nil, nil, "", err
		}
		offset := uint64(buf.Len())
		buf.Write(payload)
		extendPChannelWindowChunkFooterSourceRange(footer, chunk)
		footer.Chunks = append(footer.Chunks, vchannelWindowChunkIndex{
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

	footerPayload, err := marshalPChannelWindowChunkFooter(footer)
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
	buf.Write(pchannelWindowChunkFooterMagic)
	return buf.Bytes(), footer, hex.EncodeToString(footerChecksum[:]), nil
}

func unmarshalPChannelWindowChunk(payload []byte) (map[string][]committedWriteRecord, *pchannelWindowChunkFooter, string, error) {
	if len(payload) < pchannelWindowChunkHeaderSize+pchannelWindowChunkChecksumSize+len(pchannelWindowChunkFooterMagic)+4 {
		return nil, nil, "", pchannelWindowStoreCorruptedf("pchannel window chunk payload too short")
	}
	if !bytes.Equal(payload[:len(pchannelWindowChunkHeaderMagic)], pchannelWindowChunkHeaderMagic) {
		return nil, nil, "", pchannelWindowStoreCorruptedf("invalid pchannel window chunk header magic")
	}
	if version := binary.BigEndian.Uint16(payload[8:10]); version != pchannelWindowCodecVersion {
		return nil, nil, "", pchannelWindowStoreCorruptedf("unsupported pchannel window chunk version %d", version)
	}
	if headerSize := binary.BigEndian.Uint32(payload[12:16]); headerSize != pchannelWindowChunkHeaderSize {
		return nil, nil, "", pchannelWindowStoreCorruptedf("invalid pchannel window chunk header size %d", headerSize)
	}
	footerMagicStart := len(payload) - len(pchannelWindowChunkFooterMagic)
	if !bytes.Equal(payload[footerMagicStart:], pchannelWindowChunkFooterMagic) {
		return nil, nil, "", pchannelWindowStoreCorruptedf("invalid pchannel window chunk footer magic")
	}
	footerLenStart := footerMagicStart - 4
	footerChecksumStart := footerLenStart - pchannelWindowChunkChecksumSize
	if footerChecksumStart < pchannelWindowChunkHeaderSize {
		return nil, nil, "", pchannelWindowStoreCorruptedf("invalid pchannel window chunk footer length offset")
	}
	footerLen := int(binary.BigEndian.Uint32(payload[footerLenStart:footerMagicStart]))
	footerStart := footerChecksumStart - footerLen
	if footerLen <= 0 || footerStart < pchannelWindowChunkHeaderSize {
		return nil, nil, "", pchannelWindowStoreCorruptedf("invalid pchannel window chunk footer length")
	}
	footerPayload := payload[footerStart:footerChecksumStart]
	storedFooterChecksum := payload[footerChecksumStart:footerLenStart]
	if actual := sha256.Sum256(footerPayload); !bytes.Equal(storedFooterChecksum, actual[:]) {
		return nil, nil, "", pchannelWindowStoreCorruptedf("pchannel window chunk footer checksum mismatch")
	}
	footer, err := unmarshalPChannelWindowChunkFooter(footerPayload)
	if err != nil {
		return nil, nil, "", markPChannelWindowStoreCorrupted(err)
	}

	recordsByVChannel := make(map[string][]committedWriteRecord, len(footer.Chunks))
	for _, chunkIndex := range footer.Chunks {
		end := chunkIndex.Offset + chunkIndex.Length
		if chunkIndex.Offset < uint64(pchannelWindowChunkHeaderSize) || end > uint64(footerStart) || chunkIndex.Offset > end {
			return nil, nil, "", pchannelWindowStoreCorruptedf("invalid vchannel window chunk range for vchannel %s", chunkIndex.VChannel)
		}
		chunkPayload := payload[chunkIndex.Offset:end]
		if chunkIndex.Checksum != "" && chunkIndex.Checksum != checksumHex(chunkPayload) {
			return nil, nil, "", pchannelWindowStoreCorruptedf("vchannel window chunk checksum mismatch for vchannel %s", chunkIndex.VChannel)
		}
		decodedChunk, err := unmarshalVChannelWindowChunk(chunkPayload)
		if err != nil {
			return nil, nil, "", markPChannelWindowStoreCorrupted(err)
		}
		if decodedChunk.VChannel != chunkIndex.VChannel {
			return nil, nil, "", pchannelWindowStoreCorruptedf("vchannel window chunk vchannel mismatch, footer %s, payload %s", chunkIndex.VChannel, decodedChunk.VChannel)
		}
		if decodedChunk.Generation != footer.Generation {
			return nil, nil, "", pchannelWindowStoreCorruptedf("vchannel window chunk generation mismatch for vchannel %s, footer %d, payload %d", chunkIndex.VChannel, footer.Generation, decodedChunk.Generation)
		}
		if uint64(len(decodedChunk.Records)) != chunkIndex.RecordCount {
			return nil, nil, "", pchannelWindowStoreCorruptedf("vchannel window chunk record count mismatch for vchannel %s", chunkIndex.VChannel)
		}
		recordsByVChannel[chunkIndex.VChannel] = decodedChunk.Records
	}
	return recordsByVChannel, footer, hex.EncodeToString(storedFooterChecksum), nil
}

func marshalVChannelWindowChunk(chunk *vchannelWindowChunk) ([]byte, error) {
	if chunk == nil {
		return nil, errors.New("nil vchannel window chunk")
	}
	if len(chunk.Records) == 0 {
		return nil, errors.New("empty vchannel window chunk")
	}
	chunk.CodecVersion = pchannelWindowCodecVersion
	chunk.Records = cloneAndSortCommittedWriteRecords(chunk.PChannel, chunk.VChannel, chunk.Records)
	chunk.SourceStartMessageID, chunk.SourceEndMessageID, chunk.SourceStartTimetick, chunk.SourceEndTimetick = committedWriteRecordSourceRange(chunk.Records)
	// No self-checksum: the chunk's bytes are protected by the footer's per-chunk
	// index checksum (vchannelWindowChunkIndex.Checksum), computed over these exact
	// bytes and verified before this chunk is ever decoded.
	return json.Marshal(chunk)
}

func unmarshalVChannelWindowChunk(payload []byte) (*vchannelWindowChunk, error) {
	chunk := &vchannelWindowChunk{}
	if err := json.Unmarshal(payload, chunk); err != nil {
		return nil, markPChannelWindowStoreCorrupted(err)
	}
	if chunk.CodecVersion != pchannelWindowCodecVersion {
		return nil, pchannelWindowStoreCorruptedf("unsupported vchannel window chunk version %d", chunk.CodecVersion)
	}
	chunk.Records = cloneAndSortCommittedWriteRecords(chunk.PChannel, chunk.VChannel, chunk.Records)
	return chunk, nil
}

func newPChannelWindowChunkHeader() []byte {
	header := make([]byte, pchannelWindowChunkHeaderSize)
	copy(header, pchannelWindowChunkHeaderMagic)
	binary.BigEndian.PutUint16(header[8:10], pchannelWindowCodecVersion)
	binary.BigEndian.PutUint16(header[10:12], 0)
	binary.BigEndian.PutUint32(header[12:16], pchannelWindowChunkHeaderSize)
	return header
}

func marshalPChannelWindowChunkFooter(footer *pchannelWindowChunkFooter) ([]byte, error) {
	if footer == nil {
		return nil, errors.New("nil pchannel window chunk footer")
	}
	footer.CodecVersion = pchannelWindowCodecVersion
	sort.Slice(footer.Chunks, func(i, j int) bool {
		return footer.Chunks[i].VChannel < footer.Chunks[j].VChannel
	})
	// No self-checksum: integrity is verified against the trailer checksum over
	// these exact bytes (see marshal/unmarshalPChannelWindowChunk).
	return json.Marshal(footer)
}

func unmarshalPChannelWindowChunkFooter(payload []byte) (*pchannelWindowChunkFooter, error) {
	footer := &pchannelWindowChunkFooter{}
	if err := json.Unmarshal(payload, footer); err != nil {
		return nil, markPChannelWindowStoreCorrupted(err)
	}
	if footer.CodecVersion != pchannelWindowCodecVersion {
		return nil, pchannelWindowStoreCorruptedf("unsupported pchannel window chunk footer version %d", footer.CodecVersion)
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

func extendPChannelWindowChunkFooterSourceRange(footer *pchannelWindowChunkFooter, chunk *vchannelWindowChunk) {
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
