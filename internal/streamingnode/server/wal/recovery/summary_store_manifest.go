package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// The manifest is the only description of the chunk set. One live version per
// pchannel, written under the owner's term, and always produced by inheriting
// the previous manifest and amending it.
//
// Inheritance is structural on purpose. Whatever a previous owner left
// unfinished -- an unsealed range, a range it wrote but never recorded, an
// entry still queued for deletion -- is carried forward because the new
// manifest is derived from the old one rather than assembled from scratch. A
// construct-from-scratch design would need a checklist of things not to forget,
// and forgetting any of them loses objects silently and permanently.
const (
	pchannelSummaryManifestObjectExt = ".manifest."
	// A manifest is framed like a chunk: magic, then the proto, then a checksum
	// over the exact stored bytes. Verification never re-marshals the parsed
	// message, because proto encoding is not guaranteed byte-stable across
	// library versions and re-deriving would flag a healthy manifest as corrupt
	// the day the encoding shifts.
	pchannelSummaryManifestVersion = 1
	pchannelSummaryManifestHeader  = 16
)

var pchannelSummaryManifestMagic = []byte("PSMF0001")

// buildPChannelSummaryManifestKey returns the object key of one term's manifest.
// The term is in the key so that a new owner publishes its own manifest without
// overwriting the one it recovered from, and so recovery can find the newest by
// probing terms downward.
func buildPChannelSummaryManifestKey(cm storage.ChunkManager, pchannel string, term int64) string {
	return path.Join(
		cm.RootPath(),
		"streamingnode",
		"summary-store",
		sanitizeSummaryStorePathPart(pchannel),
	) + "/" + sanitizeSummaryStorePathPart(pchannel) + pchannelSummaryManifestObjectExt + strconv.FormatInt(term, 10)
}

func marshalPChannelSummaryManifest(manifest *streamingpb.PChannelSummaryManifest) ([]byte, error) {
	payload, err := summaryChunkMarshalOptions.Marshal(manifest)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal pchannel summary manifest")
	}
	buf := make([]byte, 0, pchannelSummaryManifestHeader+len(payload)+sha256.Size)
	header := make([]byte, pchannelSummaryManifestHeader)
	copy(header[0:8], pchannelSummaryManifestMagic)
	binary.BigEndian.PutUint16(header[8:10], pchannelSummaryManifestVersion)
	binary.BigEndian.PutUint32(header[10:14], uint32(len(payload)))
	buf = append(buf, header...)
	buf = append(buf, payload...)
	checksum := sha256.Sum256(payload)
	return append(buf, checksum[:]...), nil
}

func unmarshalPChannelSummaryManifest(payload []byte) (*streamingpb.PChannelSummaryManifest, error) {
	if len(payload) < pchannelSummaryManifestHeader+sha256.Size {
		return nil, pchannelSummaryStoreCorruptedf("pchannel summary manifest too short: %d bytes", len(payload))
	}
	if string(payload[0:8]) != string(pchannelSummaryManifestMagic) {
		return nil, pchannelSummaryStoreCorruptedf("pchannel summary manifest magic mismatch")
	}
	if version := binary.BigEndian.Uint16(payload[8:10]); version != pchannelSummaryManifestVersion {
		return nil, pchannelSummaryStoreCorruptedf("unsupported pchannel summary manifest version %d", version)
	}
	size := int(binary.BigEndian.Uint32(payload[10:14]))
	if pchannelSummaryManifestHeader+size+sha256.Size != len(payload) {
		return nil, pchannelSummaryStoreCorruptedf("pchannel summary manifest length mismatch")
	}
	body := payload[pchannelSummaryManifestHeader : pchannelSummaryManifestHeader+size]
	checksum := sha256.Sum256(body)
	if string(checksum[:]) != string(payload[pchannelSummaryManifestHeader+size:]) {
		return nil, pchannelSummaryStoreCorruptedf("pchannel summary manifest checksum mismatch")
	}
	manifest := &streamingpb.PChannelSummaryManifest{}
	if err := proto.Unmarshal(body, manifest); err != nil {
		return nil, pchannelSummaryStoreCorruptedf("failed to decode pchannel summary manifest: %s", err.Error())
	}
	return manifest, nil
}

// readPChannelSummaryManifest reads one term's manifest. A missing object is
// reported as (nil, false, nil): that is the normal answer for a term that never
// wrote one. A corrupt object is an error and must fail the WAL open -- the
// manifest is the only index into the chunk set, so a damaged one means recovery
// cannot know what it is missing, and starting with an empty window would accept
// in-retention client retries as fresh writes.
func readPChannelSummaryManifest(ctx context.Context, pchannel string, term int64) (*streamingpb.PChannelSummaryManifest, bool, error) {
	cm := resource.Resource().ChunkManager()
	key := buildPChannelSummaryManifestKey(cm, pchannel, term)
	exists, err := cm.Exist(ctx, key)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to probe pchannel summary manifest %s", key)
	}
	if !exists {
		return nil, false, nil
	}
	payload, err := cm.Read(ctx, key)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to read pchannel summary manifest %s", key)
	}
	manifest, err := unmarshalPChannelSummaryManifest(payload)
	if err != nil {
		return nil, false, err
	}
	return manifest, true, nil
}

func writePChannelSummaryManifest(ctx context.Context, pchannel string, term int64, manifest *streamingpb.PChannelSummaryManifest) error {
	payload, err := marshalPChannelSummaryManifest(manifest)
	if err != nil {
		return err
	}
	cm := resource.Resource().ChunkManager()
	key := buildPChannelSummaryManifestKey(cm, pchannel, term)
	if err := cm.Write(ctx, key, payload); err != nil {
		return errors.Wrapf(err, "failed to write pchannel summary manifest %s", key)
	}
	return nil
}

// findNewestPChannelSummaryManifest probes terms downward from the current
// assignment and returns the newest manifest that exists.
//
// Probing a single dimension is sound because of the ordering a term must obey:
// it writes its manifest before it writes any chunk. So a manifest for term T
// exists implies every chunk of term T came after it, and no manifest for term T
// implies term T wrote no chunks. The highest manifest that exists therefore
// already covers every term that produced data.
func findNewestPChannelSummaryManifest(ctx context.Context, pchannel string, currentTerm int64, floorTerm int64) (*streamingpb.PChannelSummaryManifest, int64, error) {
	if floorTerm < 0 {
		floorTerm = 0
	}
	for term := currentTerm; term >= floorTerm; term-- {
		manifest, found, err := readPChannelSummaryManifest(ctx, pchannel, term)
		if err != nil {
			return nil, 0, err
		}
		if found {
			return manifest, term, nil
		}
	}
	return nil, 0, nil
}

// inheritPChannelSummaryManifest produces the manifest a new owner publishes:
// everything the previous one knew, plus what this recovery just learned.
//
// discovered are the chunks found by probing past what the previous manifest
// recorded, because chunk durability does not wait for the manifest. Folding
// them in is not an optimization: a later recovery probes forward only within
// its own term, so a tail left unrecorded here can never be found again.
func inheritPChannelSummaryManifest(
	previous *streamingpb.PChannelSummaryManifest,
	discovered []*streamingpb.PChannelSummaryChunkIndexEntry,
) *streamingpb.PChannelSummaryManifest {
	manifest := &streamingpb.PChannelSummaryManifest{}
	if previous != nil {
		manifest = proto.Clone(previous).(*streamingpb.PChannelSummaryManifest)
	}
	for _, entry := range discovered {
		recordPChannelSummaryChunk(manifest, entry)
	}
	return manifest
}

// recordPChannelSummaryChunk adds a chunk to the set recovery reads. This is
// what makes a lazy, ranged read of a single vchannel possible, and it is the
// input to the retention computation -- which therefore needs no reads at all.
func recordPChannelSummaryChunk(manifest *streamingpb.PChannelSummaryManifest, entry *streamingpb.PChannelSummaryChunkIndexEntry) {
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

// pchannelSummaryManifestNewest returns the newest chunk the manifest records.
func pchannelSummaryManifestNewest(manifest *streamingpb.PChannelSummaryManifest) (*streamingpb.PChannelSummaryChunkIndexEntry, bool) {
	chunks := manifest.GetChunks()
	if len(chunks) == 0 {
		return nil, false
	}
	return chunks[len(chunks)-1], true
}

// pchannelSummaryManifestOldest is the retention boundary: recovery reads from
// here upward and never below.
func pchannelSummaryManifestOldest(manifest *streamingpb.PChannelSummaryManifest) (*streamingpb.PChannelSummaryChunkIndexEntry, bool) {
	chunks := manifest.GetChunks()
	if len(chunks) == 0 {
		return nil, false
	}
	return chunks[0], true
}

// retentionBoundary returns the index of the oldest chunk retention keeps.
// Everything below it is released.
//
// Three bounds decide it, and they are not symmetric:
//
//   - minRetainedBytes is a FLOOR. A chunk inside it is kept regardless of age.
//   - ttlHorizonTimetick expires chunks by age, but only where the floor allows.
//   - maxRetainedChunks is a HARD CAP that overrides the floor.
//
// The floor beats the TTL because a horizon expressed in time is invalidated by
// time passing: a time-only rule empties the store after an outage, which is
// precisely when a resuming client needs to be deduplicated. A floor measured in
// bytes is invalidated only by new data arriving, which is the one condition
// under which forgetting old keys is safe.
//
// The cap then beats the floor, because the floor bounds bytes and recovery pays
// per CHUNK. A workload writing little per checkpoint fills the floor with a
// great many tiny chunks, and both the manifest and the replay would grow
// without limit. When the cap binds, the retained window is smaller than
// minRetainedBytes asks for -- a deliberate trade of dedup coverage for a
// bounded recovery.
//
// All three are accounted per OBJECT. A chunk is retained or released whole, and
// both the storage it costs and the read recovery pays for it are the object's,
// so a per-vchannel share of it would not describe either. The consequence is
// that retention spans a window of the PCHANNEL's write rate: on a busy pchannel
// every vchannel's history, including an idle one's, is displaced by the
// aggregate traffic rather than by its own.
func retentionBoundary(
	manifest *streamingpb.PChannelSummaryManifest,
	minRetainedBytes int,
	maxRetainedChunks int,
	ttlHorizonTimetick uint64,
) int {
	chunks := manifest.GetChunks()
	if len(chunks) == 0 {
		return 0
	}
	boundary := minInt(
		retentionFloorBoundary(chunks, minRetainedBytes),
		retentionTTLBoundary(chunks, ttlHorizonTimetick),
	)
	if maxRetainedChunks > 0 && len(chunks)-boundary > maxRetainedChunks {
		boundary = len(chunks) - maxRetainedChunks
	}
	return boundary
}

// retentionFloorBoundary is the oldest chunk index the byte floor still needs:
// walk back from the newest, accumulating object sizes, and stop once the floor
// is covered. Until it is covered nothing may be released.
func retentionFloorBoundary(chunks []*streamingpb.PChannelSummaryChunkIndexEntry, minRetainedBytes int) int {
	if minRetainedBytes <= 0 {
		return len(chunks)
	}
	accumulated := 0
	for i := len(chunks) - 1; i >= 0; i-- {
		accumulated += int(chunks[i].GetObjectSize())
		if accumulated >= minRetainedBytes {
			return i
		}
	}
	return 0
}

// retentionTTLBoundary is the length of the leading run of chunks older than the
// horizon. A chunk with no span (nothing dated it) is never expired by age.
func retentionTTLBoundary(chunks []*streamingpb.PChannelSummaryChunkIndexEntry, ttlHorizonTimetick uint64) int {
	if ttlHorizonTimetick == 0 {
		return 0
	}
	boundary := 0
	for boundary < len(chunks) {
		endTimetick := chunks[boundary].GetEndTimetick()
		if endTimetick == 0 || endTimetick >= ttlHorizonTimetick {
			break
		}
		boundary++
	}
	return boundary
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

// releaseBelowRetentionBoundary moves everything below the boundary out of the
// set recovery reads and into the gc queue, in one manifest value.
//
// The two halves of that move are what make the transition safe: dropping the
// chunk entry stops recovery from depending on the object, and adding the gc ref
// keeps the term needed to address it. Because both happen in the same object,
// there is no window in which a chunk is in neither place -- no crash can leave
// an object that recovery still expects, and none can leave one gc can no longer
// name. That is also why gc never re-checks this decision: it was already made
// atomically here.
func releaseBelowRetentionBoundary(manifest *streamingpb.PChannelSummaryManifest, boundary int) int {
	if manifest == nil || boundary <= 0 {
		return 0
	}
	chunks := manifest.GetChunks()
	if boundary > len(chunks) {
		boundary = len(chunks)
	}
	for _, entry := range chunks[:boundary] {
		manifest.PendingGc = append(manifest.PendingGc, &streamingpb.PChannelSummaryChunkRef{
			Generation: entry.GetGeneration(),
			Term:       entry.GetTerm(),
		})
	}
	manifest.Chunks = chunks[boundary:]
	return boundary
}

// dropCompletedPendingGC removes the refs gc reported done. Completion is
// tracked in memory and lands here at the next manifest write, so a crash in
// between only replays deletes that are already no-ops.
func dropCompletedPendingGC(manifest *streamingpb.PChannelSummaryManifest, completed map[pchannelSummaryGCRef]struct{}) {
	if manifest == nil || len(completed) == 0 {
		return
	}
	kept := manifest.GetPendingGc()[:0]
	for _, ref := range manifest.GetPendingGc() {
		if _, done := completed[pchannelSummaryGCRef{term: ref.GetTerm(), generation: ref.GetGeneration()}]; done {
			continue
		}
		kept = append(kept, ref)
	}
	manifest.PendingGc = kept
}

// pchannelSummaryGCRef identifies a queued deletion across the in-memory
// completion set and the persisted manifest.
type pchannelSummaryGCRef struct {
	term       int64
	generation uint64
}

// retentionTTLHorizon derives the TTL bound from the newest timetick the store
// has covered, not from the local wall clock, so the boundary is consistent with
// how the rest of the write path measures time.
func retentionTTLHorizon(latestTimetick uint64, ttl time.Duration) uint64 {
	if ttl <= 0 || latestTimetick == 0 {
		return 0
	}
	physical, logical := tsoutil.ParseHybridTs(latestTimetick)
	msecs := ttl.Milliseconds()
	if physical <= msecs {
		return 0
	}
	return tsoutil.ComposeTS(physical-msecs, logical)
}
