package recovery

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	// pchannelSummaryCodecVersion is the on-disk chunk format version. It is
	// written once, into the object's fixed binary header, and checked there
	// before anything else is parsed — a chunk whose version does not match is
	// rejected rather than parsed on a guess. Neither the footer nor the etcd
	// meta repeats it: a second copy could only ever agree with the first, and
	// the version has to be readable before any proto in the object is trusted.
	pchannelSummaryCodecVersion      = 1
	pchannelSummaryChunkHeaderSize   = 16
	pchannelSummaryChunkChecksumSize = sha256.Size
)

var (
	pchannelSummaryChunkHeaderMagic = []byte("PSCCH001")
	pchannelSummaryChunkFooterMagic = []byte("PSCFT001")
)

// summaryChunkMarshalOptions pins deterministic output so that a rewrite of the
// same generation is usually byte-identical and can be recognised as a retry
// without decoding. It is only an optimization: proto guarantees determinism
// within a build but not across versions, so the write path falls back to
// comparing decoded records rather than trusting byte equality.
var summaryChunkMarshalOptions = proto.MarshalOptions{Deterministic: true}

// marshalPChannelSummaryChunk frames one chunk object.
//
// A vchannel's records are written as SEPARATE SECTIONS, not one message. The
// footer indexes each section on its own, so a reader can range-read the section
// it needs and nothing else — a primary-key index reads the inserts without
// paying for the idempotency keys, and the keys can stop being written entirely
// without changing what an insert records.
//
// The sections of one vchannel correspond by position, which is why they are
// built here in one pass over one sorted record slice: there is no other way for
// them to disagree.
func marshalPChannelSummaryChunk(
	pchannel string,
	generation uint64,
	term int64,
	recordsByVChannel map[string][]*SummaryRecord,
) ([]byte, *streamingpb.PChannelSummaryChunkFooter, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.Write(newPChannelSummaryChunkHeader())

	vchannels := make([]string, 0, len(recordsByVChannel))
	for vchannel := range recordsByVChannel {
		vchannels = append(vchannels, vchannel)
	}
	sort.Strings(vchannels)

	footer := &streamingpb.PChannelSummaryChunkFooter{
		Pchannel:   pchannel,
		Generation: generation,
		Term:       term,
		Chunks:     make([]*streamingpb.VChannelSummaryChunkIndex, 0, len(vchannels)),
	}

	for _, vchannel := range vchannels {
		records := sortedSummaryRecords(recordsByVChannel[vchannel])
		if len(records) == 0 {
			continue
		}
		index := &streamingpb.VChannelSummaryChunkIndex{Vchannel: vchannel}

		// The idempotency section is written only when something in this vchannel
		// carries a key. A record without one still occupies its position, so the
		// two sections stay index-aligned whatever the mix.
		if summaryRecordsCarryIdempotencyKey(records) {
			section := &streamingpb.VChannelSummaryIdempotencySection{
				Records: make([]*streamingpb.VChannelSummaryIdempotencyRecord, 0, len(records)),
			}
			for _, record := range records {
				section.Records = append(section.Records, &streamingpb.VChannelSummaryIdempotencyRecord{
					Key:        record.IdempotencyKey,
					RowOffsets: record.InsertResult.GetRowOffsets(),
				})
			}
			ref, err := appendPChannelSummarySection(buf, section, len(records))
			if err != nil {
				return nil, nil, err
			}
			index.Idempotency = ref
		}

		insertSection := &streamingpb.VChannelSummaryInsertSection{
			Records: make([]*streamingpb.VChannelSummaryInsertRecord, 0, len(records)),
		}
		for _, record := range records {
			insertSection.Records = append(insertSection.Records, &streamingpb.VChannelSummaryInsertRecord{
				SourceMessageId:        record.SourceMessageID,
				SourceTimetick:         record.SourceTimeTick,
				LastConfirmedMessageId: record.LastConfirmedMessageID,
				Ids:                    record.InsertResult.GetIds(),
			})
		}
		ref, err := appendPChannelSummarySection(buf, insertSection, len(records))
		if err != nil {
			return nil, nil, err
		}
		index.Inserts = ref

		index.StartTimetick, index.EndTimetick = summaryRecordTimetickRange(records)
		extendPChannelSummaryChunkFooterRange(footer, index.StartTimetick, index.EndTimetick)
		footer.Chunks = append(footer.Chunks, index)
	}

	footerPayload, err := marshalPChannelSummaryChunkFooter(footer)
	if err != nil {
		return nil, nil, err
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
	return buf.Bytes(), footer, nil
}

// appendPChannelSummarySection writes one section and returns the ref that
// locates it. The offset is absolute within the object so a reader can turn it
// straight into a ranged read.
func appendPChannelSummarySection(buf *bytes.Buffer, section proto.Message, recordCount int) (*streamingpb.VChannelSummarySectionRef, error) {
	payload, err := summaryChunkMarshalOptions.Marshal(section)
	if err != nil {
		return nil, merr.WrapErrServiceInternalMsg("failed to marshal vchannel summary section: " + err.Error())
	}
	offset := uint64(buf.Len())
	buf.Write(payload)
	return &streamingpb.VChannelSummarySectionRef{
		Offset:      offset,
		Length:      uint64(len(payload)),
		RecordCount: uint64(recordCount),
	}, nil
}

func unmarshalPChannelSummaryChunk(payload []byte) (map[string][]*SummaryRecord, *streamingpb.PChannelSummaryChunkFooter, error) {
	footer, footerStart, err := unmarshalPChannelSummaryChunkTail(payload)
	if err != nil {
		return nil, nil, err
	}
	recordsByVChannel := make(map[string][]*SummaryRecord, len(footer.GetChunks()))
	for _, index := range footer.GetChunks() {
		records, err := unmarshalVChannelSummarySections(payload, footerStart, index)
		if err != nil {
			return nil, nil, err
		}
		recordsByVChannel[index.GetVchannel()] = records
	}
	return recordsByVChannel, footer, nil
}

// unmarshalPChannelSummaryChunkTail decodes the object's trailer and footer and
// returns where the payload region ends. Every section offset is bounded by that
// position, which is what keeps a corrupt index from addressing the footer.
func unmarshalPChannelSummaryChunkTail(payload []byte) (*streamingpb.PChannelSummaryChunkFooter, uint64, error) {
	if len(payload) < pchannelSummaryChunkHeaderSize+pchannelSummaryChunkChecksumSize+len(pchannelSummaryChunkFooterMagic)+4 {
		return nil, 0, pchannelSummaryStoreCorruptedf("pchannel summary chunk payload too short")
	}
	if !bytes.Equal(payload[:len(pchannelSummaryChunkHeaderMagic)], pchannelSummaryChunkHeaderMagic) {
		return nil, 0, pchannelSummaryStoreCorruptedf("invalid pchannel summary chunk header magic")
	}
	if version := binary.BigEndian.Uint16(payload[8:10]); version != pchannelSummaryCodecVersion {
		return nil, 0, pchannelSummaryStoreCorruptedf("unsupported pchannel summary chunk version %d", version)
	}
	if headerSize := binary.BigEndian.Uint32(payload[12:16]); headerSize != pchannelSummaryChunkHeaderSize {
		return nil, 0, pchannelSummaryStoreCorruptedf("invalid pchannel summary chunk header size %d", headerSize)
	}
	footerMagicStart := len(payload) - len(pchannelSummaryChunkFooterMagic)
	if !bytes.Equal(payload[footerMagicStart:], pchannelSummaryChunkFooterMagic) {
		return nil, 0, pchannelSummaryStoreCorruptedf("invalid pchannel summary chunk footer magic")
	}
	footerLenStart := footerMagicStart - 4
	footerChecksumStart := footerLenStart - pchannelSummaryChunkChecksumSize
	if footerChecksumStart < pchannelSummaryChunkHeaderSize {
		return nil, 0, pchannelSummaryStoreCorruptedf("invalid pchannel summary chunk footer length offset")
	}
	footerLen := int(binary.BigEndian.Uint32(payload[footerLenStart:footerMagicStart]))
	footerStart := footerChecksumStart - footerLen
	if footerLen <= 0 || footerStart < pchannelSummaryChunkHeaderSize {
		return nil, 0, pchannelSummaryStoreCorruptedf("invalid pchannel summary chunk footer length")
	}
	footerPayload := payload[footerStart:footerChecksumStart]
	if actual := sha256.Sum256(footerPayload); !bytes.Equal(payload[footerChecksumStart:footerLenStart], actual[:]) {
		return nil, 0, pchannelSummaryStoreCorruptedf("pchannel summary chunk footer checksum mismatch")
	}
	footer, err := unmarshalPChannelSummaryChunkFooter(footerPayload)
	if err != nil {
		return nil, 0, markPChannelSummaryStoreCorrupted(err)
	}
	return footer, uint64(footerStart), nil
}

// unmarshalVChannelSummarySections decodes one vchannel's sections and rejoins
// them into whole records.
//
// The sections carry no checksum of their own. The object store already
// guarantees the bytes read are the bytes written, so the failure worth catching
// here is a mislocated section — an offset or length computed wrong — and that
// is caught without one: the bounds check below rejects a ref that leaves the
// payload region, a decode of the wrong bytes fails to parse, and the record
// count stored on the ref must match what actually decoded.
func unmarshalVChannelSummarySections(
	payload []byte,
	payloadEnd uint64,
	index *streamingpb.VChannelSummaryChunkIndex,
) ([]*SummaryRecord, error) {
	vchannel := index.GetVchannel()
	insertsPayload, err := sliceVChannelSummarySection(payload, payloadEnd, vchannel, "inserts", index.GetInserts())
	if err != nil {
		return nil, err
	}
	inserts := &streamingpb.VChannelSummaryInsertSection{}
	if err := proto.Unmarshal(insertsPayload, inserts); err != nil {
		return nil, pchannelSummaryStoreCorruptedf("failed to decode insert section for vchannel %s: %s", vchannel, err.Error())
	}
	if uint64(len(inserts.GetRecords())) != index.GetInserts().GetRecordCount() {
		return nil, pchannelSummaryStoreCorruptedf("insert section record count mismatch for vchannel %s", vchannel)
	}

	var idempotency *streamingpb.VChannelSummaryIdempotencySection
	if ref := index.GetIdempotency(); ref != nil {
		idempotencyPayload, err := sliceVChannelSummarySection(payload, payloadEnd, vchannel, "idempotency", ref)
		if err != nil {
			return nil, err
		}
		idempotency = &streamingpb.VChannelSummaryIdempotencySection{}
		if err := proto.Unmarshal(idempotencyPayload, idempotency); err != nil {
			return nil, pchannelSummaryStoreCorruptedf("failed to decode idempotency section for vchannel %s: %s", vchannel, err.Error())
		}
		if uint64(len(idempotency.GetRecords())) != ref.GetRecordCount() {
			return nil, pchannelSummaryStoreCorruptedf("idempotency section record count mismatch for vchannel %s", vchannel)
		}
		// Position is the only link between the sections, so an unequal length is
		// not a recoverable mismatch: every pairing below it would be wrong.
		if len(idempotency.GetRecords()) != len(inserts.GetRecords()) {
			return nil, pchannelSummaryStoreCorruptedf("idempotency section is not aligned with the insert section for vchannel %s", vchannel)
		}
	}

	records := make([]*SummaryRecord, 0, len(inserts.GetRecords()))
	for i, insert := range inserts.GetRecords() {
		record := &SummaryRecord{
			SourceMessageID:        insert.GetSourceMessageId(),
			SourceTimeTick:         insert.GetSourceTimetick(),
			LastConfirmedMessageID: insert.GetLastConfirmedMessageId(),
		}
		var rowOffsets []uint32
		if idempotency != nil {
			annotation := idempotency.GetRecords()[i]
			record.IdempotencyKey = annotation.GetKey()
			rowOffsets = annotation.GetRowOffsets()
		}
		// The two halves of the duplicate response live in different sections;
		// rejoin them into the shape the append path replays back to the client.
		if insert.GetIds() != nil || len(rowOffsets) > 0 {
			record.InsertResult = &messagespb.IdempotentInsertResult{
				RowOffsets: rowOffsets,
				Ids:        insert.GetIds(),
			}
		}
		records = append(records, record)
	}
	return sortedSummaryRecords(records), nil
}

func sliceVChannelSummarySection(
	payload []byte,
	payloadEnd uint64,
	vchannel string,
	name string,
	ref *streamingpb.VChannelSummarySectionRef,
) ([]byte, error) {
	if ref == nil {
		return nil, pchannelSummaryStoreCorruptedf("missing %s section for vchannel %s", name, vchannel)
	}
	end := ref.GetOffset() + ref.GetLength()
	if ref.GetOffset() < uint64(pchannelSummaryChunkHeaderSize) || end > payloadEnd || ref.GetOffset() > end {
		return nil, pchannelSummaryStoreCorruptedf("invalid %s section range for vchannel %s", name, vchannel)
	}
	return payload[ref.GetOffset():end], nil
}

func summaryRecordsCarryIdempotencyKey(records []*SummaryRecord) bool {
	for _, record := range records {
		if record.IdempotencyKey != "" {
			return true
		}
	}
	return false
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
	return pb, nil
}

// summaryRecordTimetickRange returns the timetick span of an already sorted
// record slice. The span is a timetick range only: a vchannel never records a
// physical WAL position.
func summaryRecordTimetickRange(records []*SummaryRecord) (uint64, uint64) {
	if len(records) == 0 {
		return 0, 0
	}
	return records[0].SourceTimeTick, records[len(records)-1].SourceTimeTick
}

func extendPChannelSummaryChunkFooterRange(footer *streamingpb.PChannelSummaryChunkFooter, startTimetick, endTimetick uint64) {
	if footer == nil {
		return
	}
	if footer.StartTimetick == 0 || startTimetick < footer.StartTimetick {
		footer.StartTimetick = startTimetick
	}
	if endTimetick > footer.EndTimetick {
		footer.EndTimetick = endTimetick
	}
}
