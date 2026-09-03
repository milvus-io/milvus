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
// sections into chunk objects, so consumers (the idempotency and insert views
// today, others later) can recover their durable state without re-reading the
// whole WAL.
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
	"fmt"
	"io/fs"
	"math"
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

	chunkObjectDir    = "chunks"
	manifestObjectDir = "manifest"

	// walsummaryObjectDir is the object storage directory of the summary store,
	// mirrored by the etcd DirectorySummaryStore constant of the metastore.
	walsummaryObjectDir = "walsummary"
)

var (
	chunkHeaderMagic = []byte("PSCCH001")
	chunkFooterMagic = []byte("PSCFT001")
	manifestMagic    = []byte("PSMF0001")

	// marshalOptions pins deterministic output so that a rewrite of the same
	// generation is usually byte-identical and can be recognized as a retry
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
	sectionsByVChannel map[string]*ChunkSections,
) (*streamingpb.PChannelSummaryChunkFooter, uint64, error) {
	payload, footer, err := marshalChunk(s.pchannel, generation, s.term, sectionsByVChannel)
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
			chunkSectionsByVChannelEqual(existingRecords, sectionsByVChannel) {
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
) (map[string]*ChunkSections, *streamingpb.PChannelSummaryChunkFooter, error) {
	key := buildChunkKey(s.chunkManager, s.pchannel, generation, term)
	payload, err := s.chunkManager.Read(ctx, key)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to read summary chunk %s", key)
	}
	return unmarshalChunk(payload)
}

// ReadIdempotencySection decodes one vchannel's idempotency view of one chunk:
// the insert section rejoined with the idempotency section. The location comes
// from the manifest's per-vchannel index, so the whole chunk is never decoded.
func (s *Store) ReadIdempotencySection(
	ctx context.Context,
	generation uint64,
	term int64,
	vchannel string,
	index *streamingpb.VChannelSummaryChunkIndex,
) (*ChunkSections, error) {
	sections, err := s.ReadIdempotencySectionsOfChunk(ctx, generation, term,
		map[string]*streamingpb.VChannelSummaryChunkIndex{vchannel: index})
	if err != nil {
		return nil, err
	}
	if decoded, ok := sections[vchannel]; ok {
		return decoded, nil
	}
	return &ChunkSections{}, nil
}

// ReadIdempotencySectionsOfChunk decodes SEVERAL vchannels' idempotency views
// out of one chunk download.
//
// A chunk is a pchannel-wide object carrying every vchannel written in the same
// span, and the section refs only locate a slice inside a payload that is
// fetched whole -- object storage here has no range read. Recovery reads one
// vchannel at a time, so without this a chunk covering V vchannels would be
// downloaded V times: on a pchannel near its retention budget with a dozen
// vchannels that is gigabytes of transfer while the channel is unwritable.
func (s *Store) ReadIdempotencySectionsOfChunk(
	ctx context.Context,
	generation uint64,
	term int64,
	indexes map[string]*streamingpb.VChannelSummaryChunkIndex,
) (map[string]*ChunkSections, error) {
	if len(indexes) == 0 {
		return nil, nil
	}
	key := buildChunkKey(s.chunkManager, s.pchannel, generation, term)
	payload, err := s.chunkManager.Read(ctx, key)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read summary chunk %s", key)
	}
	_, footerStart, err := unmarshalChunkTail(payload)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*ChunkSections, len(indexes))
	for vchannel, index := range indexes {
		sections, err := unmarshalIdempotencySections(payload, footerStart, index)
		if err != nil {
			return nil, err
		}
		out[vchannel] = sections
	}
	return out, nil
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
// ListManifestTerms returns, in descending order, the terms that have a
// published manifest at or below the given term.
//
// It exists so recovery can locate the newest manifest with ONE list call
// instead of probing every term downwards. Terms are burned by assignment
// attempts that may never seal anything (TryAssignToServerID takes one per
// attempt), so the term counter can run far ahead of the terms that actually
// wrote, and a downward probe costs a read plus a full chunk-prefix list per
// empty term in between.
func (s *Store) ListManifestTerms(ctx context.Context, upTo int64) ([]int64, error) {
	prefix := buildManifestPrefix(s.chunkManager, s.pchannel)
	keys, _, err := storage.ListAllChunkWithPrefix(ctx, s.chunkManager, prefix, false)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "failed to list summary manifests under %s", prefix)
	}
	terms := make([]int64, 0, len(keys))
	for _, key := range keys {
		base := strings.TrimPrefix(key, prefix)
		term, err := strconv.ParseInt(base, 10, 64)
		if err != nil {
			// Not a manifest key this build writes; ignore rather than fail the
			// open on an unrelated object under the prefix.
			continue
		}
		if term <= upTo {
			terms = append(terms, term)
		}
	}
	sort.Slice(terms, func(i, j int) bool { return terms[i] > terms[j] })
	return terms, nil
}

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
	termSuffix := "_" + fmt.Sprintf("%020d", term)
	for _, key := range keys {
		base := strings.TrimPrefix(key, prefix)
		if !strings.HasSuffix(base, termSuffix) {
			continue
		}
		middle := strings.TrimSuffix(base, termSuffix)
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
//
// Keys are fixed-width, zero-padded decimals ("%020d"): lexicographic order
// equals numeric order, so a prefix list returns chunks in generation order
// without parsing. The width covers the full uint64 range (and every
// non-negative int64); a wider value would silently break ordering, so these
// formats must never shrink.

func buildChunkKey(cm storage.ChunkManager, pchannel string, generation uint64, term int64) string {
	return buildChunkPrefix(cm, pchannel) +
		fmt.Sprintf("%020d_%020d", generation, term)
}

func buildChunkPrefix(cm storage.ChunkManager, pchannel string) string {
	return path.Join(
		cm.RootPath(),
		walsummaryObjectDir,
		sanitizePathPart(pchannel),
		chunkObjectDir,
	) + "/"
}

func buildManifestPrefix(cm storage.ChunkManager, pchannel string) string {
	return path.Join(
		cm.RootPath(),
		walsummaryObjectDir,
		sanitizePathPart(pchannel),
		manifestObjectDir,
	) + "/"
}

func buildManifestKey(cm storage.ChunkManager, pchannel string, term int64) string {
	return path.Join(
		cm.RootPath(),
		walsummaryObjectDir,
		sanitizePathPart(pchannel),
		manifestObjectDir,
	) + "/" + fmt.Sprintf("%020d", term)
}

func buildStorePrefix(cm storage.ChunkManager, pchannel string) string {
	return path.Join(
		cm.RootPath(),
		walsummaryObjectDir,
		sanitizePathPart(pchannel),
	) + "/"
}

func sanitizePathPart(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	return replacer.Replace(value)
}

// ---- chunk codec ----

// ChunkSections is what one vchannel contributes to a chunk: one slice per
// consumer section. A chunk carries the sections side by side and the footer
// indexes each on its own, so a consumer range-reads only its own.
//
// Idempotency is stored split across two sections: the client key and the row
// offsets in the idempotency section, the write's identity and primary keys in
// the insert section. The insert section is self-sufficient -- a future
// primary-key index reads it without the idempotency section existing -- which
// is also what keeps the primary keys stored exactly once. The two are rejoined
// on read by position, which holds because a record without a key still takes
// its slot in the idempotency section.
type ChunkSections struct {
	// Idempotency and Inserts are the two halves of the idempotency consumer's
	// view, stored in separate sections and paired by position: Idempotency[i]
	// is the client key of the write Inserts[i] describes.
	//
	// They are kept apart rather than joined into one record because that is
	// what the sections are: Inserts is self-sufficient and is what a future
	// primary-key index reads on its own, which is also what keeps the primary
	// keys stored exactly once. Idempotency may be absent when no write of this
	// vchannel carries a key; when present it has exactly the same length,
	// because a write without a key still takes its slot.
	Idempotency []*streamingpb.VChannelSummaryIdempotencyRecord
	Inserts     []*streamingpb.VChannelSummaryInsertRecord
}

// empty reports whether the vchannel contributes nothing to the chunk, in which
// case it gets no footer entry at all.
func (c *ChunkSections) empty() bool {
	return c == nil || len(c.Inserts) == 0
}

// validateIdempotencyAlignment rejects a pairing that cannot be stored, before
// it reaches an object. The same check guards the read path; doing it here as
// well means a caller bug is caught at its own call site rather than surfacing
// as chunk corruption on the next recovery.
func (c *ChunkSections) validateIdempotencyAlignment(vchannel string) error {
	if len(c.Idempotency) != 0 && len(c.Idempotency) != len(c.Inserts) {
		return storeCorruptedf(
			"idempotency section is not aligned with the insert section for vchannel %s: %d keys, %d inserts",
			vchannel, len(c.Idempotency), len(c.Inserts),
		)
	}
	return nil
}

// carriesIdempotencyKey reports whether any write of this vchannel remembers a
// client key. The idempotency section is written only when one does.
func (c *ChunkSections) carriesIdempotencyKey() bool {
	for _, record := range c.Idempotency {
		if record.GetKey() != "" {
			return true
		}
	}
	return false
}

// marshalChunk frames one chunk object: fixed header, then each vchannel's
// sections (in vchannel order), then the indexed footer, then the footer
// checksum and trailer.
func marshalChunk(
	pchannel string,
	generation uint64,
	term int64,
	sectionsByVChannel map[string]*ChunkSections,
) ([]byte, *streamingpb.PChannelSummaryChunkFooter, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	buf.Write(newChunkHeader())

	vchannels := make([]string, 0, len(sectionsByVChannel))
	for vchannel := range sectionsByVChannel {
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
		sections := sectionsByVChannel[vchannel]
		if sections.empty() {
			continue
		}
		index := &streamingpb.VChannelSummaryChunkIndex{Vchannel: vchannel}
		start, end := uint64(math.MaxUint64), uint64(0)

		if len(sections.Inserts) > 0 {
			if err := sections.validateIdempotencyAlignment(vchannel); err != nil {
				return nil, nil, err
			}
			ordered := sortedByInsertTimetick(sections)
			if err := appendIdempotencySections(buf, index, ordered); err != nil {
				return nil, nil, err
			}
			insertStart, insertEnd := insertRecordTimetickRange(ordered.Inserts)
			start, end = minUint64(start, insertStart), maxUint64(end, insertEnd)
		}

		index.StartTimetick, index.EndTimetick = start, end
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

// appendIdempotencySections writes one vchannel's insert and idempotency
// sections and records both refs on the index.
//
// The insert section is always written; the idempotency section only when some
// write carries a client key. A write without one still takes its slot there,
// so the two sections stay index-aligned whatever the mix -- which is what lets
// them be paired by position on read.
func appendIdempotencySections(
	buf *bytes.Buffer,
	index *streamingpb.VChannelSummaryChunkIndex,
	sections *ChunkSections,
) error {
	inserts := &streamingpb.VChannelSummaryInsertSection{
		Records: make([]*streamingpb.VChannelSummaryInsertRecord, 0, len(sections.Inserts)),
	}
	for _, record := range sections.Inserts {
		inserts.Records = append(inserts.Records, proto.Clone(record).(*streamingpb.VChannelSummaryInsertRecord))
	}
	ref, err := appendSection(buf, inserts, len(inserts.Records))
	if err != nil {
		return err
	}
	index.Inserts = ref

	if !sections.carriesIdempotencyKey() {
		return nil
	}
	keys := &streamingpb.VChannelSummaryIdempotencySection{
		Records: make([]*streamingpb.VChannelSummaryIdempotencyRecord, 0, len(sections.Idempotency)),
	}
	for _, record := range sections.Idempotency {
		keys.Records = append(keys.Records, proto.Clone(record).(*streamingpb.VChannelSummaryIdempotencyRecord))
	}
	if ref, err = appendSection(buf, keys, len(keys.Records)); err != nil {
		return err
	}
	index.Idempotency = ref
	return nil
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

// unmarshalChunk decodes a whole chunk object back into per-vchannel sections.
func unmarshalChunk(
	payload []byte,
) (map[string]*ChunkSections, *streamingpb.PChannelSummaryChunkFooter, error) {
	footer, footerStart, err := unmarshalChunkTail(payload)
	if err != nil {
		return nil, nil, err
	}
	sectionsByVChannel := make(map[string]*ChunkSections, len(footer.GetChunks()))
	for _, index := range footer.GetChunks() {
		sections := &ChunkSections{}
		// A section absent from the index is not an error: a vchannel writes
		// only the sections it has records for.
		if index.GetInserts() != nil {
			idempotency, err := unmarshalIdempotencySections(payload, footerStart, index)
			if err != nil {
				return nil, nil, err
			}
			sections.Idempotency, sections.Inserts = idempotency.Idempotency, idempotency.Inserts
		}
		sectionsByVChannel[index.GetVchannel()] = sections
	}
	return sectionsByVChannel, footer, nil
}

// unmarshalIdempotencySections decodes one vchannel's insert section and, when
// present, its idempotency section. They are returned as they are stored -- two
// slices paired by position -- and joining them is the consumer's business.
//
// The insert section is authoritative for the record count; the idempotency
// section, when written, has exactly the same length because a write without a
// key still takes its slot. A length mismatch is corruption, not a mix to be
// tolerated: pairing the wrong key with the wrong primary keys would answer a
// duplicate with another write's rows.
//
// The sections carry no checksum of their own. The object store already
// guarantees the bytes read are the bytes written, so the failure worth
// catching here is a mislocated section, and that is caught without one: the
// bounds check rejects a ref that leaves the payload region, a decode of the
// wrong bytes fails to parse, and the record count on the ref must match what
// actually decoded.
func unmarshalIdempotencySections(
	payload []byte,
	payloadEnd uint64,
	index *streamingpb.VChannelSummaryChunkIndex,
) (*ChunkSections, error) {
	vchannel := index.GetVchannel()
	insertsPayload, err := sliceSection(payload, payloadEnd, vchannel, "inserts", index.GetInserts())
	if err != nil {
		return nil, err
	}
	inserts := &streamingpb.VChannelSummaryInsertSection{}
	if err := proto.Unmarshal(insertsPayload, inserts); err != nil {
		return nil, storeCorruptedf("failed to decode insert section for vchannel %s: %s", vchannel, err.Error())
	}
	if uint64(len(inserts.GetRecords())) != index.GetInserts().GetRecordCount() {
		return nil, storeCorruptedf("insert section record count mismatch for vchannel %s", vchannel)
	}
	sections := &ChunkSections{Inserts: inserts.GetRecords()}

	if ref := index.GetIdempotency(); ref != nil {
		keysPayload, err := sliceSection(payload, payloadEnd, vchannel, "idempotency", ref)
		if err != nil {
			return nil, err
		}
		keys := &streamingpb.VChannelSummaryIdempotencySection{}
		if err := proto.Unmarshal(keysPayload, keys); err != nil {
			return nil, storeCorruptedf("failed to decode idempotency section for vchannel %s: %s", vchannel, err.Error())
		}
		if uint64(len(keys.GetRecords())) != ref.GetRecordCount() {
			return nil, storeCorruptedf("idempotency section record count mismatch for vchannel %s", vchannel)
		}
		sections.Idempotency = keys.GetRecords()
	}
	if err := sections.validateIdempotencyAlignment(vchannel); err != nil {
		return nil, err
	}
	return sections, nil
}

// sliceSection bounds-checks one section ref against the payload region and
// returns the bytes it locates.
func sliceSection(
	payload []byte,
	payloadEnd uint64,
	vchannel string,
	name string,
	ref *streamingpb.VChannelSummarySectionRef,
) ([]byte, error) {
	if ref == nil {
		return nil, storeCorruptedf("missing %s section for vchannel %s", name, vchannel)
	}
	end := ref.GetOffset() + ref.GetLength()
	if ref.GetOffset() < uint64(chunkHeaderSize) || end > payloadEnd || ref.GetOffset() > end {
		return nil, storeCorruptedf("invalid %s section range for vchannel %s", name, vchannel)
	}
	return payload[ref.GetOffset():end], nil
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

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// sortedByInsertTimetick orders a vchannel's idempotency halves by the WAL
// timetick of the write, so a chunk's sections read back in WAL order.
//
// The permutation is computed once from the insert records and applied to both
// slices. Sorting them independently would be a silent corruption: the pairing
// is positional, so any ordering the two do not share pairs a client key with
// another write's primary keys.
func sortedByInsertTimetick(sections *ChunkSections) *ChunkSections {
	order := make([]int, len(sections.Inserts))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool {
		return sections.Inserts[order[i]].GetSourceTimetick() < sections.Inserts[order[j]].GetSourceTimetick()
	})
	out := &ChunkSections{Inserts: make([]*streamingpb.VChannelSummaryInsertRecord, 0, len(order))}
	if len(sections.Idempotency) == len(sections.Inserts) {
		out.Idempotency = make([]*streamingpb.VChannelSummaryIdempotencyRecord, 0, len(order))
	}
	for _, i := range order {
		out.Inserts = append(out.Inserts, sections.Inserts[i])
		if out.Idempotency != nil {
			out.Idempotency = append(out.Idempotency, sections.Idempotency[i])
		}
	}
	return out
}

func insertRecordTimetickRange(records []*streamingpb.VChannelSummaryInsertRecord) (uint64, uint64) {
	if len(records) == 0 {
		return 0, 0
	}
	start, end := records[0].GetSourceTimetick(), records[0].GetSourceTimetick()
	for _, record := range records[1:] {
		start = minUint64(start, record.GetSourceTimetick())
		end = maxUint64(end, record.GetSourceTimetick())
	}
	return start, end
}

func newChunkHeader() []byte {
	header := make([]byte, chunkHeaderSize)
	copy(header, chunkHeaderMagic)
	binary.BigEndian.PutUint16(header[8:10], codecVersion)
	binary.BigEndian.PutUint16(header[10:12], 0)
	binary.BigEndian.PutUint32(header[12:16], chunkHeaderSize)
	return header
}

func extendFooterRange(footer *streamingpb.PChannelSummaryChunkFooter, start, end uint64) {
	if start > 0 && (footer.GetStartTimetick() == 0 || start < footer.GetStartTimetick()) {
		footer.StartTimetick = start
	}
	if end > footer.GetEndTimetick() {
		footer.EndTimetick = end
	}
}

// chunkSectionsByVChannelEqual compares what two chunks actually contain, every
// section included. It is what decides whether a same-term rewrite of the same
// generation is an idempotent retry or corruption, so a difference confined to
// one section must not read as equal.
func chunkSectionsByVChannelEqual(left, right map[string]*ChunkSections) bool {
	if len(left) != len(right) {
		return false
	}
	for vchannel, leftSections := range left {
		rightSections, ok := right[vchannel]
		if !ok {
			return false
		}
		if !idempotencySectionsEqual(leftSections, rightSections) {
			return false
		}
	}
	return true
}

func idempotencySectionsEqual(left, right *ChunkSections) bool {
	if len(left.Inserts) != len(right.Inserts) || len(left.Idempotency) != len(right.Idempotency) {
		return false
	}
	leftOrdered, rightOrdered := sortedByInsertTimetick(left), sortedByInsertTimetick(right)
	for i := range leftOrdered.Inserts {
		if !proto.Equal(leftOrdered.Inserts[i], rightOrdered.Inserts[i]) {
			return false
		}
	}
	for i := range leftOrdered.Idempotency {
		if !proto.Equal(leftOrdered.Idempotency[i], rightOrdered.Idempotency[i]) {
			return false
		}
	}
	return true
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
