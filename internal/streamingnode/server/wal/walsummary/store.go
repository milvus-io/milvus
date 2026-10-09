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
// Manifests describe the retained chunk index and covered WAL position;
// immutable chunks hold per-vchannel consumer sections. LastAcked reports the
// continuous recoverable prefix independently of source message lifetimes.
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
	"github.com/milvus-io/milvus/pkg/v3/common"
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

	// walsummaryObjectDir is the object storage directory of the summary store.
	// The store keeps nothing in etcd, so this is its only root. It aliases the
	// registered segment so this top-level directory stays in
	// common.InternalStorageRootSegments, which import path validation denies.
	walsummaryObjectDir = common.WalSummaryRootPath
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

// TimeTickRange is complete WAL coverage [Start, End], including payload-free messages.
type TimeTickRange struct{ Start, End uint64 }

// WriteChunk writes one chunk object. It never overwrites a differing chunk at
// the same key: an object with identical content is a retry (idempotent
// no-op), and one with different content is corruption. The key is term-scoped,
// so a concurrent owner of another term writes a different object entirely.
func (s *Store) WriteChunk(
	ctx context.Context,
	generation uint64,
	sectionsByVChannel map[string]*ChunkSections,
	coverage TimeTickRange,
) (*streamingpb.PChannelSummaryChunkFooter, uint64, error) {
	payload, footer, err := marshalChunk(s.pchannel, generation, s.term, sectionsByVChannel, coverage)
	if err != nil {
		return nil, 0, err
	}
	key := s.ChunkKey(generation)
	exists, err := s.chunkManager.Exist(ctx, key)
	if err != nil {
		return nil, 0, merr.Wrapf(err, "failed to probe summary chunk %s", key)
	}
	if !exists {
		if err := s.chunkManager.Write(ctx, key, payload); err != nil {
			return nil, 0, merr.Wrapf(err, "failed to write summary chunk %s", key)
		}
		return footer, uint64(len(payload)), nil
	}
	existingPayload, err := s.chunkManager.Read(ctx, key)
	if err != nil {
		return nil, 0, merr.Wrapf(err, "failed to read existing summary chunk %s", key)
	}
	if bytes.Equal(existingPayload, payload) {
		return footer, uint64(len(payload)), nil
	}
	// Same key, different bytes. The key carries this store's term and the
	// footer is marshaled from that same term, so a split-brain owner writes a
	// different key and cannot land here: the only writer of this object is
	// this term, retrying. Byte inequality alone therefore does not prove a
	// conflict, because the payload encoding is not guaranteed to be
	// byte-stable across proto library versions. Compare what the chunk
	// actually contains instead, so an identical rewrite stays idempotent and
	// only genuinely different content is corruption.
	if existingRecords, existingFooter, decodeErr := unmarshalChunk(existingPayload); decodeErr == nil {
		if existingFooter.GetPchannel() == footer.GetPchannel() &&
			existingFooter.GetGeneration() == footer.GetGeneration() &&
			existingFooter.GetTerm() == footer.GetTerm() &&
			existingFooter.GetStartTimeTick() == footer.GetStartTimeTick() &&
			existingFooter.GetEndTimetick() == footer.GetEndTimetick() &&
			chunkSectionsByVChannelEqual(existingRecords, sectionsByVChannel) {
			// The STORED footer and size, not the ones just built. The records
			// match but the encodings do not, and the manifest carries the
			// footer's per-vchannel section offsets verbatim -- publishing the
			// new encoding's offsets against the old object's bytes would make
			// every later ranged read slice the wrong range. The object is not
			// rewritten, so what the manifest describes has to be the object
			// that is there.
			return existingFooter, uint64(len(existingPayload)), nil
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
		return nil, nil, merr.Wrapf(err, "failed to read summary chunk %s", key)
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
		return nil, merr.Wrapf(err, "failed to read summary chunk %s", key)
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
		return merr.Wrapf(err, "failed to delete summary chunk %s", key)
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
		return nil, merr.Wrapf(err, "failed to list summary manifests under %s", prefix)
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
		return nil, false, merr.Wrapf(err, "failed to probe summary manifest %s", key)
	}
	if !exists {
		return nil, false, nil
	}
	payload, err := s.chunkManager.Read(ctx, key)
	if err != nil {
		return nil, false, merr.Wrapf(err, "failed to read summary manifest %s", key)
	}
	manifest, err := unmarshalManifest(payload)
	if err != nil {
		return nil, false, err
	}
	return manifest, true, nil
}

// WriteManifest publishes the manifest. The manifest is always written as the
// previous manifest plus amendments: recovery reads the
// prior term's manifest on a term handoff and seals the inherited index into
// the new term's manifest, so the chain never grows beyond one hop.
func (s *Store) WriteManifest(ctx context.Context, manifest *streamingpb.PChannelSummaryManifest) error {
	payload, err := marshalManifest(manifest)
	if err != nil {
		return err
	}
	key := s.ManifestKey()
	if err := s.chunkManager.Write(ctx, key, payload); err != nil {
		return merr.Wrapf(err, "failed to write summary manifest %s", key)
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
	root := buildChunkPrefix(s.chunkManager, s.pchannel)
	expected := fromGeneration
	var entries []*streamingpb.PChannelSummaryChunkIndexEntry
	for {
		// Prefix covers exactly 100 generation values; the LIST page size is
		// unchanged and the walker follows all pages, including other terms.
		prefix := root + fmt.Sprintf("%020d", expected)[:18]
		keys, _, err := storage.ListAllChunkWithPrefix(ctx, s.chunkManager, prefix, false)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, merr.Wrapf(err, "failed to list summary prefix %s", prefix)
		}
		candidates := make(map[uint64]string)
		for _, key := range keys {
			generation, keyTerm, ok := parseChunkKey(strings.TrimPrefix(key, root))
			if ok && keyTerm == term && generation >= expected {
				candidates[generation] = key
			}
		}
		for {
			key, ok := candidates[expected]
			if !ok {
				return entries, nil
			}
			payload, err := s.chunkManager.Read(ctx, key)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) || errors.Is(err, merr.ErrIoKeyNotFound) {
					return entries, nil
				}
				return nil, merr.Wrapf(err, "failed to read summary tail %s", key)
			}
			_, footer, err := unmarshalChunk(payload)
			if err != nil {
				return nil, err
			}
			if footer.GetPchannel() != s.pchannel || footer.GetGeneration() != expected || footer.GetTerm() != term {
				return nil, storeCorruptedf("summary tail identity mismatch: %s", key)
			}
			entries = append(entries, chunkIndexEntryFromFooter(footer, uint64(len(payload))))
			if expected == math.MaxUint64 {
				return entries, nil
			}
			expected++
			if expected%100 == 0 {
				break
			}
		}
	}
}

// ChunkRef identifies one chunk object.
type ChunkRef struct {
	Generation uint64
	Term       int64
}

// DeleteManifestsBelowTerm deletes the manifest objects of terms strictly below
// belowTerm. Recovery adopts the highest term, even when empty, so once a manifest
// at or above belowTerm holds the whole retained set, no recovery can reach an
// older one.
func (s *Store) DeleteManifestsBelowTerm(ctx context.Context, belowTerm int64) error {
	terms, err := s.ListManifestTerms(ctx, belowTerm-1)
	if err != nil {
		return err
	}
	for _, term := range terms {
		key := buildManifestKey(s.chunkManager, s.pchannel, term)
		if err := s.chunkManager.Remove(ctx, key); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return merr.Wrapf(err, "failed to delete superseded summary manifest %s", key)
		}
	}
	return nil
}

// parseChunkKey splits a chunk object's name (the key without the prefix) into
// its generation and term. It reports false for anything this build did not
// write, which is ignored rather than failing the caller: an unrelated object
// under the prefix must not break recovery or gc.
func parseChunkKey(base string) (generation uint64, term int64, ok bool) {
	sep := strings.IndexByte(base, '_')
	if len(base) != 41 || sep != 20 {
		return 0, 0, false
	}
	generation, err := strconv.ParseUint(base[:sep], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	term, err = strconv.ParseInt(base[sep+1:], 10, 64)
	if err != nil || term < 0 {
		return 0, 0, false
	}
	return generation, term, true
}

// RemoveAllObjects deletes every object of the pchannel's summary store. It is
// only correct where no catalog meta references a chunk any more, such as
// dropping the store. A prefix removal over the whole store root reaps the
// per-term manifests, the chunks directory, and whatever an earlier partial
// removal left behind.
func (s *Store) RemoveAllObjects(ctx context.Context) error {
	prefix := buildStorePrefix(s.chunkManager, s.pchannel)
	if err := s.chunkManager.RemoveWithPrefix(ctx, prefix); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return merr.Wrapf(err, "failed to remove summary store with prefix %s", prefix)
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
	return buildManifestPrefix(cm, pchannel) + fmt.Sprintf("%020d", term)
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
	Transform []*streamingpb.VChannelSummaryTransformRecord
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
	return c == nil || (len(c.Inserts) == 0 && len(c.Transform) == 0)
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
	coverage TimeTickRange,
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
			start, end = min(start, insertStart), max(end, insertEnd)
		}

		if len(sections.Transform) > 0 {
			records := sortedTransformRecords(sections.Transform)
			section := &streamingpb.VChannelSummaryTransformSection{Records: records}
			ref, err := appendSection(buf, section, len(records))
			if err != nil {
				return nil, nil, err
			}
			var totalSize uint64
			for _, record := range records {
				_, size := transformEntrySize(&streamingpb.TransformLogEntry{TimeTick: record.GetTimeTick(), Entry: &streamingpb.TransformLogEntry_Delete{Delete: record.GetDelete()}})
				totalSize += size
			}
			transformStart, transformEnd := transformRecordTimetickRange(records)
			index.Transform = &streamingpb.VChannelSummaryTransformIndex{
				Ref: ref, StartTimeTick: transformStart, EndTimeTick: transformEnd, TotalSize: totalSize,
			}
			start, end = min(start, transformStart), max(end, transformEnd)
		}
		index.StartTimetick, index.EndTimetick = start, end
		footer.Chunks = append(footer.Chunks, index)
	}

	footer.StartTimeTick, footer.EndTimetick = coverage.Start, coverage.End
	if err := validateChunkIndex(chunkIndexEntryFromFooter(footer, 0)); err != nil {
		return nil, nil, err
	}
	footerPayload, err := marshalOptions.Marshal(footer)
	if err != nil {
		return nil, nil, merr.Wrap(err, "failed to marshal summary chunk footer")
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
		if index.GetTransform() != nil {
			records, err := unmarshalTransformSection(payload, footerStart, index)
			if err != nil {
				return nil, nil, err
			}
			sections.Transform = records
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
		return nil, 0, markStoreCorrupted(merr.Wrap(err, "failed to decode summary chunk footer"))
	}
	return footer, uint64(footerStart), nil
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
		start = min(start, record.GetSourceTimetick())
		end = max(end, record.GetSourceTimetick())
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
		leftTransforms := sortedTransformRecords(leftSections.Transform)
		rightTransforms := sortedTransformRecords(rightSections.Transform)
		if len(leftTransforms) != len(rightTransforms) {
			return false
		}
		for i := range leftTransforms {
			if !proto.Equal(leftTransforms[i], rightTransforms[i]) {
				return false
			}
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
		return nil, merr.Wrap(err, "failed to marshal summary manifest")
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
		return nil, markStoreCorrupted(merr.Wrap(err, "failed to decode summary manifest"))
	}
	return manifest, nil
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
	sort.Slice(manifest.Chunks, func(i, j int) bool { return manifest.Chunks[i].GetGeneration() < manifest.Chunks[j].GetGeneration() })
	if manifest.Coverage == nil {
		manifest.Coverage = &streamingpb.SummaryCoverage{
			StartTimeTick: entry.GetStartTimeTick(), Generation: entry.GetGeneration(),
			Term: entry.GetTerm(), EndTimeTick: entry.GetEndTimetick(),
		}
	} else if entry.GetGeneration() > manifest.Coverage.GetGeneration() {
		manifest.Coverage.Generation = entry.GetGeneration()
		manifest.Coverage.Term = entry.GetTerm()
		manifest.Coverage.EndTimeTick = entry.GetEndTimetick()
	}
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
		StartTimeTick: footer.GetStartTimeTick(),
		EndTimetick:   footer.GetEndTimetick(),
		Vchannels:     footer.GetChunks(),
	}
}

// ReadTransformSection decodes one vchannel's transform records from a chunk.
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
		return nil, merr.Wrapf(err, "failed to read summary chunk %s", key)
	}
	_, footerStart, err := unmarshalChunkTail(payload)
	if err != nil {
		return nil, err
	}
	return unmarshalTransformSection(payload, footerStart, index)
}

func unmarshalTransformSection(
	payload []byte,
	payloadEnd uint64,
	index *streamingpb.VChannelSummaryChunkIndex,
) ([]*streamingpb.VChannelSummaryTransformRecord, error) {
	vchannel := index.GetVchannel()
	ref := index.GetTransform().GetRef()
	if ref == nil {
		return nil, storeCorruptedf("missing transform section for vchannel %s", vchannel)
	}
	end := ref.GetOffset() + ref.GetLength()
	if ref.GetOffset() < uint64(chunkHeaderSize) || end > payloadEnd || ref.GetOffset() > end {
		return nil, storeCorruptedf("invalid transform section range for vchannel %s", vchannel)
	}
	section := &streamingpb.VChannelSummaryTransformSection{}
	if err := proto.Unmarshal(payload[ref.GetOffset():end], section); err != nil {
		return nil, markStoreCorrupted(merr.Wrapf(err, "failed to decode transform section for vchannel %s", vchannel))
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

// sweepGarbage rediscovers unreferenced objects after a committed manifest.
// It never touches later terms or the current term's recoverable upload tail.
func (s *Store) sweepGarbage(ctx context.Context, term int64, coverage *streamingpb.SummaryCoverage, referenced map[ChunkRef]struct{}, budget int) (int, bool, error) {
	prefix := buildChunkPrefix(s.chunkManager, s.pchannel)
	deleted, finished := 0, true
	var deleteErr error
	err := s.chunkManager.WalkWithPrefix(ctx, prefix, false, func(info *storage.ChunkObjectInfo) bool {
		generation, keyTerm, ok := parseChunkKey(strings.TrimPrefix(info.FilePath, prefix))
		if !ok || keyTerm > term || (keyTerm == term && (coverage == nil || generation > coverage.Generation)) {
			return true
		}
		if _, keep := referenced[ChunkRef{Generation: generation, Term: keyTerm}]; keep {
			return true
		}
		if err := s.DeleteChunk(ctx, generation, keyTerm); err != nil {
			deleteErr = err
			return false
		}
		deleted++
		if deleted >= budget {
			finished = false
			return false
		}
		return true
	})
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return deleted, false, merr.Wrap(err, "failed to sweep summary garbage")
	}
	return deleted, finished && deleteErr == nil, deleteErr
}
