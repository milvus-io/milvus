// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package walsummary implements the WALSummary: the WAL-level (pchannel-level)
// data summary. It observes the WAL messages and persists per-consumer summary
// sections into chunk objects, so consumers (the transform log, and future
// idempotency / insert views) can recover their durable state without
// re-reading the whole WAL.
//
// Three artifacts:
//   - PChannelSummaryManifest (object storage): the chunk index, the pending GC
//     work queue and the DDL invalidation map.
//   - chunk objects (object storage): one per generation, holding per-vchannel
//     per-consumer sections.
//
// The chunk is the only boundary between "already summarized" and "still in the
// WAL": a message handle is released only after the chunk covering it is
// durable and the dirty summary state is installed, so the global WAL
// checkpoint never advances past a fact that is not in a chunk.
package walsummary

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	// codecVersion is the on-disk chunk format version. It is written once, into
	// the object's fixed binary header, and checked there before anything else
	// is parsed.
	codecVersion      = 1
	chunkHeaderSize   = 16
	chunkChecksumSize = sha256.Size

	// manifestVersion is the on-disk manifest format version.
	manifestVersion = 1
	manifestHeader  = 16

	chunkObjectPrefix = "chunk."
	chunkObjectExt    = ".psc"
	manifestObjectExt = ".manifest."

	// summaryObjectDir is the object storage directory of the summary store,
	// mirrored by the etcd DirectorySummaryStore constant of the metastore.
	summaryObjectDir = "summary"
)

var (
	chunkHeaderMagic = []byte("PSCCH001")
	chunkFooterMagic = []byte("PSCFT001")
	manifestMagic    = []byte("PSMF0001")

	// marshalOptions pins deterministic output so that a rewrite of the same
	// generation is usually byte-identical and can be recognised as a retry
	// without decoding. It is only an optimization: proto guarantees
	// determinism within a build but not across versions, so the write path
	// falls back to comparing decoded records rather than trusting byte
	// equality.
	marshalOptions = proto.MarshalOptions{Deterministic: true}
)

// Store is the object storage layer of one pchannel's WALSummary. All objects
// are written under the owner's term, so a new owner never overwrites the
// objects it recovered from.
type Store struct {
	chunkManager storage.ChunkManager
	pchannel     string
	term         int64
}

// NewStore creates the object storage layer of a pchannel summary store.
func NewStore(chunkManager storage.ChunkManager, pchannel string, term int64) *Store {
	return &Store{
		chunkManager: chunkManager,
		pchannel:     pchannel,
		term:         term,
	}
}

// PChannel returns the pchannel of the store.
func (s *Store) PChannel() string {
	return s.pchannel
}

// Term returns the WAL assignment term the store writes under.
func (s *Store) Term() int64 {
	return s.term
}

// ChunkKey returns the object key of one generation's chunk.
func (s *Store) ChunkKey(generation uint64) string {
	return buildChunkKey(s.chunkManager, s.pchannel, generation, s.term)
}

// ManifestKey returns the object key of this term's manifest.
func (s *Store) ManifestKey() string {
	return s.ManifestKeyOfTerm(s.term)
}

// ManifestKeyOfTerm returns the object key of an arbitrary term's manifest.
func (s *Store) ManifestKeyOfTerm(term int64) string {
	return buildManifestKey(s.chunkManager, s.pchannel, term)
}

// WriteChunk writes one chunk object. It never overwrites a differing chunk of
// the same generation: a same-generation object with identical content is a
// retry (idempotent no-op), a same-generation object with different content is
// corruption, and a newer term's object fences this owner.
func (s *Store) WriteChunk(
	ctx context.Context,
	generation uint64,
	recordsByVChannel map[string][]*streamingpb.VChannelSummaryTransformRecord,
) (*streamingpb.PChannelSummaryChunkFooter, uint64, error) {
	payload, footer, err := marshalChunk(s.pchannel, generation, s.term, recordsByVChannel)
	if err != nil {
		return nil, 0, err
	}
	key := s.ChunkKey(generation)
	exists, err := s.chunkManager.Exist(ctx, key)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to probe summary chunk %s", key)
	}
	if !exists {
		if err := s.chunkManager.Write(ctx, key, payload); err != nil {
			return nil, 0, errors.Wrapf(err, "failed to write summary chunk %s", key)
		}
		return footer, uint64(len(payload)), nil
	}
	existingPayload, err := s.chunkManager.Read(ctx, key)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to read existing summary chunk %s", key)
	}
	if bytes.Equal(existingPayload, payload) {
		return footer, uint64(len(payload)), nil
	}
	// Same generation, different bytes. Either this owner is retrying its own
	// write, or another writer produced this chunk. The Exist->Write above is
	// not atomic, so under split-brain both owners can pass the absence check
	// and the last write would silently win. Arbitrate on the decoded footer:
	// the newer term is the current owner, the older term is fenced.
	if existingRecords, existingFooter, decodeErr := unmarshalChunk(existingPayload); decodeErr == nil {
		if existingFooter.GetTerm() > s.term {
			return nil, 0, storeFencedf("summary chunk %s already written by term %d, own term %d", key, existingFooter.GetTerm(), s.term)
		}
		if existingFooter.GetTerm() < s.term {
			if err := s.chunkManager.Write(ctx, key, payload); err != nil {
				return nil, 0, errors.Wrapf(err, "failed to overwrite fenced summary chunk %s", key)
			}
			return footer, uint64(len(payload)), nil
		}
		// Same term: this is our own chunk. Byte inequality alone does not prove
		// a conflict, because the payload encoding is not guaranteed to be
		// byte-stable across proto library versions. Compare what the chunk
		// actually contains instead, so an identical rewrite stays idempotent
		// and only genuinely different content is corruption.
		if existingFooter.GetGeneration() == footer.GetGeneration() &&
			transformRecordsByVChannelEqual(existingRecords, recordsByVChannel) {
			return footer, uint64(len(payload)), nil
		}
	}
	return nil, 0, storeCorruptedf("summary chunk already exists with different payload: %s", key)
}

// ReadChunk reads and decodes one chunk object. The term of the chunk is
// passed explicitly: the manifest may reference chunks written by a previous
// term (see Recover), and the object key is term-scoped.
func (s *Store) ReadChunk(
	ctx context.Context,
	generation uint64,
	term int64,
) (map[string][]*streamingpb.VChannelSummaryTransformRecord, *streamingpb.PChannelSummaryChunkFooter, error) {
	key := buildChunkKey(s.chunkManager, s.pchannel, generation, term)
	payload, err := s.chunkManager.Read(ctx, key)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to read summary chunk %s", key)
	}
	return unmarshalChunk(payload)
}

// ReadTransformSection decodes one vchannel's transform section of one chunk.
// The section location comes from the manifest's per-vchannel index, so the
// caller does not need the whole chunk decoded first. The chunk's own term is
// passed explicitly for the same reason as ReadChunk.
func (s *Store) ReadTransformSection(
	ctx context.Context,
	generation uint64,
	term int64,
	vchannel string,
	index *streamingpb.VChannelSummaryChunkIndex,
) ([]*streamingpb.VChannelSummaryTransformRecord, error) {
	key := buildChunkKey(s.chunkManager, s.pchannel, generation, term)
	payload, err := s.chunkManager.Read(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read summary chunk %s", key)
	}
	_, footerStart, err := unmarshalChunkTail(payload)
	if err != nil {
		return nil, err
	}
	return unmarshalTransformSection(payload, footerStart, index)
}

// DeleteChunk removes one chunk object. It is only called by the GC worker for
// chunks already released from the manifest; the chunk's own term is passed
// explicitly so an inherited (previous-term) chunk is deleted from the right
// object.
func (s *Store) DeleteChunk(ctx context.Context, generation uint64, term int64) error {
	key := buildChunkKey(s.chunkManager, s.pchannel, generation, term)
	if err := s.chunkManager.Remove(ctx, key); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return errors.Wrapf(err, "failed to delete summary chunk %s", key)
	}
	return nil
}

// ReadManifest reads this term's manifest. A missing object is reported as
// (nil, false, nil): that is the normal answer for a term that never wrote one.
func (s *Store) ReadManifest(ctx context.Context) (*streamingpb.PChannelSummaryManifest, bool, error) {
	return s.ReadManifestOfTerm(ctx, s.term)
}

// ReadManifestOfTerm reads the manifest of an arbitrary term. The object key
// is term-scoped, so a handoff can read the previous owner's manifest and
// inherit its chunk index (see Recover).
func (s *Store) ReadManifestOfTerm(ctx context.Context, term int64) (*streamingpb.PChannelSummaryManifest, bool, error) {
	key := s.ManifestKeyOfTerm(term)
	exists, err := s.chunkManager.Exist(ctx, key)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to probe summary manifest %s", key)
	}
	if !exists {
		return nil, false, nil
	}
	payload, err := s.chunkManager.Read(ctx, key)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to read summary manifest %s", key)
	}
	manifest, err := unmarshalManifest(payload)
	if err != nil {
		return nil, false, err
	}
	return manifest, true, nil
}

// WriteManifest publishes the manifest. The manifest is always written as the
// previous manifest plus amendments (see inheritManifest): recovery reads the
// prior term's manifest on a term handoff and seals the inherited index into
// the new term's manifest, so the chain never grows beyond one hop.
func (s *Store) WriteManifest(ctx context.Context, manifest *streamingpb.PChannelSummaryManifest) error {
	payload, err := marshalManifest(manifest)
	if err != nil {
		return err
	}
	key := s.ManifestKey()
	if err := s.chunkManager.Write(ctx, key, payload); err != nil {
		return errors.Wrapf(err, "failed to write summary manifest %s", key)
	}
	return nil
}

// ProbeChunkForward lists the chunk objects of this term at or after
// fromGeneration, in generation order. Recovery uses it to find chunks whose
// manifest record was lost to a crash between the chunk write and the manifest
// write.
func (s *Store) ProbeChunkForward(ctx context.Context, fromGeneration uint64) ([]*streamingpb.PChannelSummaryChunkIndexEntry, error) {
	return s.ProbeChunkForwardOfTerm(ctx, s.term, fromGeneration)
}

// ProbeChunkForwardOfTerm lists the chunk objects of an arbitrary term at or
// after fromGeneration. A term handoff probes the previous owner's chunks the
// same way this term's own tail is probed (see Recover).
func (s *Store) ProbeChunkForwardOfTerm(ctx context.Context, term int64, fromGeneration uint64) ([]*streamingpb.PChannelSummaryChunkIndexEntry, error) {
	prefix := buildChunkPrefix(s.chunkManager, s.pchannel)
	keys, _, err := storage.ListAllChunkWithPrefix(ctx, s.chunkManager, prefix, false)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, errors.Wrapf(err, "failed to list summary chunks under %s", prefix)
	}
	entries := make([]*streamingpb.PChannelSummaryChunkIndexEntry, 0, len(keys))
	termSuffix := ".term" + strconv.FormatInt(term, 10) + chunkObjectExt
	for _, key := range keys {
		base := strings.TrimPrefix(key, prefix)
		if !strings.HasPrefix(base, chunkObjectPrefix) || !strings.HasSuffix(base, termSuffix) {
			continue
		}
		middle := strings.TrimSuffix(strings.TrimPrefix(base, chunkObjectPrefix), termSuffix)
		generation, err := strconv.ParseUint(middle, 10, 64)
		if err != nil || generation < fromGeneration {
			continue
		}
		payload, err := s.chunkManager.Read(ctx, key)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read probed summary chunk %s", key)
		}
		_, footer, err := unmarshalChunk(payload)
		if err != nil {
			return nil, err
		}
		entries = append(entries, chunkIndexEntryFromFooter(footer, uint64(len(payload))))
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].GetGeneration() < entries[j].GetGeneration()
	})
	return entries, nil
}

// RemoveAllObjects deletes every object of the pchannel's summary store. It is
// only correct where no catalog meta references a chunk any more, such as
// dropping the store. A prefix removal over the whole store root reaps the
// per-term manifests, the chunks directory, and whatever an earlier partial
// removal left behind.
func (s *Store) RemoveAllObjects(ctx context.Context) error {
	prefix := buildStorePrefix(s.chunkManager, s.pchannel)
	if err := s.chunkManager.RemoveWithPrefix(ctx, prefix); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return errors.Wrapf(err, "failed to remove summary store with prefix %s", prefix)
	}
	return nil
}

// ---- key builders ----

func buildChunkKey(cm storage.ChunkManager, pchannel string, generation uint64, term int64) string {
	return buildChunkPrefix(cm, pchannel) +
		chunkObjectPrefix + strconv.FormatUint(generation, 10) +
		".term" + strconv.FormatInt(term, 10) + chunkObjectExt
}

func buildChunkPrefix(cm storage.ChunkManager, pchannel string) string {
	return path.Join(
		cm.RootPath(),
		"streamingnode",
		summaryObjectDir,
		sanitizePathPart(pchannel),
		"chunks",
	) + "/"
}

func buildManifestKey(cm storage.ChunkManager, pchannel string, term int64) string {
	return path.Join(
		cm.RootPath(),
		"streamingnode",
		summaryObjectDir,
		sanitizePathPart(pchannel),
	) + "/" + sanitizePathPart(pchannel) + manifestObjectExt + strconv.FormatInt(term, 10)
}

func buildStorePrefix(cm storage.ChunkManager, pchannel string) string {
	return path.Join(
		cm.RootPath(),
		"streamingnode",
		summaryObjectDir,
		sanitizePathPart(pchannel),
	) + "/"
}

func sanitizePathPart(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	return replacer.Replace(value)
}

// ---- chunk codec ----

// marshalChunk frames one chunk object: fixed header, then one transform
// section per vchannel (in vchannel order), then the indexed footer, then the
// footer checksum and trailer.
func marshalChunk(
	pchannel string,
	generation uint64,
	term int64,
	recordsByVChannel map[string][]*streamingpb.VChannelSummaryTransformRecord,
) ([]byte, *streamingpb.PChannelSummaryChunkFooter, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.Write(newChunkHeader())

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
		records := sortedTransformRecords(recordsByVChannel[vchannel])
		if len(records) == 0 {
			continue
		}
		section := &streamingpb.VChannelSummaryTransformSection{
			Records: make([]*streamingpb.VChannelSummaryTransformRecord, 0, len(records)),
		}
		for _, record := range records {
			section.Records = append(section.Records, cloneTransformRecord(record))
		}
		ref, err := appendSection(buf, section, len(records))
		if err != nil {
			return nil, nil, err
		}
		index := &streamingpb.VChannelSummaryChunkIndex{
			Vchannel:  vchannel,
			Transform: ref,
		}
		index.StartTimetick, index.EndTimetick = transformRecordTimetickRange(records)
		extendFooterRange(footer, index.StartTimetick, index.EndTimetick)
		footer.Chunks = append(footer.Chunks, index)
	}

	footerPayload, err := marshalOptions.Marshal(footer)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to marshal summary chunk footer")
	}
	// bytes.Buffer.Write never returns an error, so the trailer writes are
	// unchecked.
	buf.Write(footerPayload)
	// Checksum the footer bytes exactly as written and carry it in the trailer,
	// so verification never re-marshals the parsed footer — proto marshaling is
	// not guaranteed byte-stable across library versions.
	footerChecksum := sha256.Sum256(footerPayload)
	buf.Write(footerChecksum[:])
	footerLen := make([]byte, 4)
	binary.BigEndian.PutUint32(footerLen, uint32(len(footerPayload)))
	buf.Write(footerLen)
	buf.Write(chunkFooterMagic)
	return buf.Bytes(), footer, nil
}

// appendSection writes one section and returns the ref that locates it. The
// offset is absolute within the object so a reader can turn it straight into a
// ranged read.
func appendSection(buf *bytes.Buffer, section proto.Message, recordCount int) (*streamingpb.VChannelSummarySectionRef, error) {
	payload, err := marshalOptions.Marshal(section)
	if err != nil {
		return nil, merr.WrapErrServiceInternalMsg("failed to marshal summary section: " + err.Error())
	}
	offset := uint64(buf.Len())
	buf.Write(payload)
	return &streamingpb.VChannelSummarySectionRef{
		Offset:      offset,
		Length:      uint64(len(payload)),
		RecordCount: uint64(recordCount),
	}, nil
}

// unmarshalChunk decodes a whole chunk object back into per-vchannel records.
func unmarshalChunk(
	payload []byte,
) (map[string][]*streamingpb.VChannelSummaryTransformRecord, *streamingpb.PChannelSummaryChunkFooter, error) {
	footer, footerStart, err := unmarshalChunkTail(payload)
	if err != nil {
		return nil, nil, err
	}
	recordsByVChannel := make(map[string][]*streamingpb.VChannelSummaryTransformRecord, len(footer.GetChunks()))
	for _, index := range footer.GetChunks() {
		records, err := unmarshalTransformSection(payload, footerStart, index)
		if err != nil {
			return nil, nil, err
		}
		recordsByVChannel[index.GetVchannel()] = records
	}
	return recordsByVChannel, footer, nil
}

// unmarshalChunkTail decodes the object's trailer and footer and returns where
// the payload region ends. Every section offset is bounded by that position,
// which is what keeps a corrupt index from addressing the footer.
func unmarshalChunkTail(payload []byte) (*streamingpb.PChannelSummaryChunkFooter, uint64, error) {
	if len(payload) < chunkHeaderSize+chunkChecksumSize+len(chunkFooterMagic)+4 {
		return nil, 0, storeCorruptedf("summary chunk payload too short")
	}
	if !bytes.Equal(payload[:len(chunkHeaderMagic)], chunkHeaderMagic) {
		return nil, 0, storeCorruptedf("invalid summary chunk header magic")
	}
	if version := binary.BigEndian.Uint16(payload[8:10]); version != codecVersion {
		return nil, 0, storeCorruptedf("unsupported summary chunk version %d", version)
	}
	if headerSize := binary.BigEndian.Uint32(payload[12:16]); headerSize != chunkHeaderSize {
		return nil, 0, storeCorruptedf("invalid summary chunk header size %d", headerSize)
	}
	footerMagicStart := len(payload) - len(chunkFooterMagic)
	if !bytes.Equal(payload[footerMagicStart:], chunkFooterMagic) {
		return nil, 0, storeCorruptedf("invalid summary chunk footer magic")
	}
	footerLenStart := footerMagicStart - 4
	footerChecksumStart := footerLenStart - chunkChecksumSize
	if footerChecksumStart < chunkHeaderSize {
		return nil, 0, storeCorruptedf("invalid summary chunk footer length offset")
	}
	footerLen := int(binary.BigEndian.Uint32(payload[footerLenStart:footerMagicStart]))
	footerStart := footerChecksumStart - footerLen
	if footerLen <= 0 || footerStart < chunkHeaderSize {
		return nil, 0, storeCorruptedf("invalid summary chunk footer length")
	}
	footerPayload := payload[footerStart:footerChecksumStart]
	if actual := sha256.Sum256(footerPayload); !bytes.Equal(payload[footerChecksumStart:footerLenStart], actual[:]) {
		return nil, 0, storeCorruptedf("summary chunk footer checksum mismatch")
	}
	footer := &streamingpb.PChannelSummaryChunkFooter{}
	if err := proto.Unmarshal(footerPayload, footer); err != nil {
		return nil, 0, markStoreCorrupted(errors.Wrap(err, "failed to decode summary chunk footer"))
	}
	return footer, uint64(footerStart), nil
}

// unmarshalTransformSection decodes one vchannel's transform section.
//
// The section carries no checksum of its own. The object store already
// guarantees the bytes read are the bytes written, so the failure worth
// catching here is a mislocated section — an offset or length computed wrong —
// and that is caught without one: the bounds check below rejects a ref that
// leaves the payload region, a decode of the wrong bytes fails to parse, and
// the record count stored on the ref must match what actually decoded.
func unmarshalTransformSection(
	payload []byte,
	payloadEnd uint64,
	index *streamingpb.VChannelSummaryChunkIndex,
) ([]*streamingpb.VChannelSummaryTransformRecord, error) {
	vchannel := index.GetVchannel()
	ref := index.GetTransform()
	if ref == nil {
		return nil, storeCorruptedf("missing transform section for vchannel %s", vchannel)
	}
	end := ref.GetOffset() + ref.GetLength()
	if ref.GetOffset() < uint64(chunkHeaderSize) || end > payloadEnd || ref.GetOffset() > end {
		return nil, storeCorruptedf("invalid transform section range for vchannel %s", vchannel)
	}
	section := &streamingpb.VChannelSummaryTransformSection{}
	if err := proto.Unmarshal(payload[ref.GetOffset():end], section); err != nil {
		return nil, storeCorruptedf("failed to decode transform section for vchannel %s: %s", vchannel, err.Error())
	}
	if uint64(len(section.GetRecords())) != ref.GetRecordCount() {
		return nil, storeCorruptedf("transform section record count mismatch for vchannel %s", vchannel)
	}
	records := make([]*streamingpb.VChannelSummaryTransformRecord, 0, len(section.GetRecords()))
	for _, record := range section.GetRecords() {
		records = append(records, cloneTransformRecord(record))
	}
	return sortedTransformRecords(records), nil
}

func newChunkHeader() []byte {
	header := make([]byte, chunkHeaderSize)
	copy(header, chunkHeaderMagic)
	binary.BigEndian.PutUint16(header[8:10], codecVersion)
	binary.BigEndian.PutUint16(header[10:12], 0)
	binary.BigEndian.PutUint32(header[12:16], chunkHeaderSize)
	return header
}

func transformRecordTimetickRange(records []*streamingpb.VChannelSummaryTransformRecord) (uint64, uint64) {
	var start, end uint64
	for _, record := range records {
		tt := record.GetTimeTick()
		if start == 0 || tt < start {
			start = tt
		}
		if tt > end {
			end = tt
		}
	}
	return start, end
}

func extendFooterRange(footer *streamingpb.PChannelSummaryChunkFooter, start, end uint64) {
	if start > 0 && (footer.GetStartTimetick() == 0 || start < footer.GetStartTimetick()) {
		footer.StartTimetick = start
	}
	if end > footer.GetEndTimetick() {
		footer.EndTimetick = end
	}
}

func sortedTransformRecords(records []*streamingpb.VChannelSummaryTransformRecord) []*streamingpb.VChannelSummaryTransformRecord {
	if len(records) < 2 {
		return records
	}
	sorted := make([]*streamingpb.VChannelSummaryTransformRecord, len(records))
	copy(sorted, records)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].GetTimeTick() < sorted[j].GetTimeTick()
	})
	return sorted
}

func cloneTransformRecord(record *streamingpb.VChannelSummaryTransformRecord) *streamingpb.VChannelSummaryTransformRecord {
	if record == nil {
		return nil
	}
	return proto.Clone(record).(*streamingpb.VChannelSummaryTransformRecord)
}

// transformRecordsByVChannelEqual compares what two chunks carry rather than
// how they were encoded, so an identical rewrite is recognised as a retry even
// when the bytes differ.
func transformRecordsByVChannelEqual(
	left, right map[string][]*streamingpb.VChannelSummaryTransformRecord,
) bool {
	if len(left) != len(right) {
		return false
	}
	for vchannel, leftRecords := range left {
		rightRecords, ok := right[vchannel]
		if !ok {
			return false
		}
		leftSorted, rightSorted := sortedTransformRecords(leftRecords), sortedTransformRecords(rightRecords)
		if len(leftSorted) != len(rightSorted) {
			return false
		}
		for i := range leftSorted {
			if !transformRecordEqual(leftSorted[i], rightSorted[i]) {
				return false
			}
		}
	}
	return true
}

func transformRecordEqual(left, right *streamingpb.VChannelSummaryTransformRecord) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.GetTimeTick() == right.GetTimeTick() &&
		proto.Equal(left.GetDelete(), right.GetDelete())
}

// ---- manifest codec ----

// marshalManifest frames a manifest like a chunk: magic, then the proto, then a
// checksum over the exact stored bytes.
func marshalManifest(manifest *streamingpb.PChannelSummaryManifest) ([]byte, error) {
	payload, err := marshalOptions.Marshal(manifest)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal summary manifest")
	}
	buf := make([]byte, 0, manifestHeader+len(payload)+sha256.Size)
	header := make([]byte, manifestHeader)
	copy(header[0:8], manifestMagic)
	binary.BigEndian.PutUint16(header[8:10], manifestVersion)
	binary.BigEndian.PutUint32(header[10:14], uint32(len(payload)))
	buf = append(buf, header...)
	buf = append(buf, payload...)
	checksum := sha256.Sum256(payload)
	return append(buf, checksum[:]...), nil
}

func unmarshalManifest(payload []byte) (*streamingpb.PChannelSummaryManifest, error) {
	if len(payload) < manifestHeader+sha256.Size {
		return nil, storeCorruptedf("summary manifest too short: %d bytes", len(payload))
	}
	if string(payload[0:8]) != string(manifestMagic) {
		return nil, storeCorruptedf("summary manifest magic mismatch")
	}
	if version := binary.BigEndian.Uint16(payload[8:10]); version != manifestVersion {
		return nil, storeCorruptedf("unsupported summary manifest version %d", version)
	}
	size := int(binary.BigEndian.Uint32(payload[10:14]))
	if manifestHeader+size+sha256.Size != len(payload) {
		return nil, storeCorruptedf("summary manifest length mismatch")
	}
	body := payload[manifestHeader : manifestHeader+size]
	checksum := sha256.Sum256(body)
	if string(checksum[:]) != string(payload[manifestHeader+size:]) {
		return nil, storeCorruptedf("summary manifest checksum mismatch")
	}
	manifest := &streamingpb.PChannelSummaryManifest{}
	if err := proto.Unmarshal(body, manifest); err != nil {
		return nil, markStoreCorrupted(errors.Wrap(err, "failed to decode summary manifest"))
	}
	return manifest, nil
}

// inheritManifest produces the manifest a new owner publishes: everything the
// previous one knew, plus what this recovery just learned.
func inheritManifest(
	previous *streamingpb.PChannelSummaryManifest,
	discovered []*streamingpb.PChannelSummaryChunkIndexEntry,
) *streamingpb.PChannelSummaryManifest {
	manifest := &streamingpb.PChannelSummaryManifest{}
	if previous != nil {
		manifest = proto.Clone(previous).(*streamingpb.PChannelSummaryManifest)
	}
	for _, entry := range discovered {
		recordChunk(manifest, entry)
	}
	return manifest
}

// recordChunk adds a chunk to the set recovery reads, keeping the entries in
// generation order. It is the input to the retention computation — which
// therefore needs no reads at all.
func recordChunk(manifest *streamingpb.PChannelSummaryManifest, entry *streamingpb.PChannelSummaryChunkIndexEntry) {
	if entry == nil {
		return
	}
	for _, existing := range manifest.GetChunks() {
		if existing.GetGeneration() == entry.GetGeneration() {
			return
		}
	}
	manifest.Chunks = append(manifest.Chunks, entry)
	sort.Slice(manifest.Chunks, func(i, j int) bool {
		return manifest.Chunks[i].GetGeneration() < manifest.Chunks[j].GetGeneration()
	})
}

// chunkIndexEntryFromFooter mirrors a written chunk's footer into a manifest
// entry, so retention and lazy reads work off the manifest alone.
func chunkIndexEntryFromFooter(footer *streamingpb.PChannelSummaryChunkFooter, objectSize uint64) *streamingpb.PChannelSummaryChunkIndexEntry {
	if footer == nil {
		return nil
	}
	return &streamingpb.PChannelSummaryChunkIndexEntry{
		Generation:    footer.GetGeneration(),
		Term:          footer.GetTerm(),
		ObjectSize:    objectSize,
		StartTimetick: footer.GetStartTimetick(),
		EndTimetick:   footer.GetEndTimetick(),
		Vchannels:     footer.GetChunks(),
	}
}

// LogStoreState logs the store identity for diagnostics.
func (s *Store) LogStoreState(logger *mlog.Logger) {
	if logger == nil {
		return
	}
	logger.Info(context.TODO(), "walsummary store",
		mlog.String("pchannel", s.pchannel),
		mlog.Int64("term", s.term))
}
