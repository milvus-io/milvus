package recovery

import (
	"bytes"
	"context"
	"io/fs"
	"path"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// persistPChannelSummary writes one chunk and then the manifest describing it.
//
// It is called synchronously from the WAL checkpoint's dirty persist and from
// nowhere else. There is no size trigger, no timer, and no background persist.
// Two things follow, and both are why this design is small:
//
//   - The chunk covering a range is durable BEFORE the WAL checkpoint covering
//     that range is persisted. So the WAL checkpoint is itself the boundary
//     between what is in a chunk and what is still in the WAL. Nothing needs to
//     record a second position, nothing needs to clamp the checkpoint, and
//     recovery needs no rewind.
//   - Batching is inherited from the WAL checkpoint, which already fires on
//     accumulated data. Chunks are therefore as large as the checkpoint's own
//     batch instead of being cut by a timer, which is what keeps small objects
//     out of the store.
//
// The chunk is written before the manifest, always. A crash between the two
// leaves a durable chunk the manifest does not name, which recovery repairs by
// probing forward; the reverse order would leave the manifest naming an object
// that does not exist, which it could not repair at all.
//
// The cost is deliberate: object-storage latency now sits on the checkpoint
// persist path. A failure here MUST fail the checkpoint persist -- a checkpoint
// that advanced past a chunk that was never written would lose that window with
// no way to notice.
func (m *summaryManager) persistPChannelSummary(
	ctx context.Context,
	logger *mlog.Logger,
	recordsByVChannel map[string][]*SummaryRecord,
) (uint64, error) {
	generation := m.nextGeneration
	chunkPayload, footer, err := marshalPChannelSummaryChunk(m.pchannel, generation, m.term, recordsByVChannel)
	if err != nil {
		return 0, err
	}
	chunkKey := buildPChannelSummaryChunkKey(resource.Resource().ChunkManager(), m.pchannel, generation, m.term)
	if err := retryOperationWithBackoff(ctx,
		logger.With(mlog.String("op", "persistPChannelSummaryChunk"), mlog.Uint64("generation", generation)),
		func(ctx context.Context) error {
			return writePChannelSummaryChunkIfAbsent(ctx, chunkKey, chunkPayload, footer, recordsByVChannel, m.term)
		}); err != nil {
		return 0, err
	}
	m.nextGeneration = generation + 1
	m.latestCoveredTT = footer.GetEndTimetick()

	recordPChannelSummaryChunk(m.manifest, chunkIndexEntryFromFooter(footer))
	if err := m.refreshPChannelSummaryManifest(ctx, logger); err != nil {
		return 0, err
	}
	return generation, nil
}

// refreshPChannelSummaryManifest recomputes retention and publishes the manifest.
//
// Releasing a chunk and queueing it for deletion happen in the same value, so a
// single PUT carries both: recovery stops depending on the object at the exact
// moment GC gains the term it needs to name it. No crash can land between the
// two.
func (m *summaryManager) refreshPChannelSummaryManifest(ctx context.Context, logger *mlog.Logger) error {
	dropCompletedPendingGC(m.manifest, m.completedGC)
	boundary := retentionBoundary(
		m.manifest,
		m.cfg.idempotencyMinRetainedBytes,
		m.cfg.idempotencyMaxRetainedChunks,
		retentionTTLHorizon(m.latestCoveredTT, m.cfg.idempotencyRetentionTTL),
	)
	releaseBelowRetentionBoundary(m.manifest, boundary)
	if err := retryOperationWithBackoff(ctx,
		logger.With(mlog.String("op", "persistPChannelSummaryManifest")),
		func(ctx context.Context) error {
			return writePChannelSummaryManifest(ctx, m.pchannel, m.term, m.manifest)
		}); err != nil {
		return err
	}
	m.completedGC = make(map[pchannelSummaryGCRef]struct{})
	return nil
}

func writePChannelSummaryChunkIfAbsent(
	ctx context.Context,
	chunkKey string,
	payload []byte,
	footer *streamingpb.PChannelSummaryChunkFooter,
	records map[string][]*SummaryRecord,
	term int64,
) error {
	chunkManager := resource.Resource().ChunkManager()
	if chunkManager == nil {
		return merr.WrapErrServiceInternalMsg("pchannel summary chunk manager is not initialized")
	}
	exists, err := chunkManager.Exist(ctx, chunkKey)
	if err != nil {
		return err
	}
	if !exists {
		return chunkManager.Write(ctx, chunkKey, payload)
	}
	existingPayload, err := chunkManager.Read(ctx, chunkKey)
	if err != nil {
		return err
	}
	if bytes.Equal(existingPayload, payload) {
		return nil
	}
	// Same generation, different bytes. Either this owner is retrying its own
	// write, or another writer produced this chunk. The Exist->Write above is not
	// atomic, so under split-brain both owners can pass the absence check and the
	// last write would silently win — replacing the other owner's summary records
	// with no error anywhere. Arbitrate on the decoded footer: the newer term is
	// the current owner and keeps/overwrites the chunk, the older term is fenced
	// and must stop persisting.
	if existingRecords, existingFooter, decodeErr := unmarshalPChannelSummaryChunk(existingPayload); decodeErr == nil {
		if existingFooter.GetTerm() > term {
			return pchannelSummaryStoreFencedf("pchannel summary chunk %s already written by term %d, own term %d", chunkKey, existingFooter.GetTerm(), term)
		}
		if existingFooter.GetTerm() < term {
			return chunkManager.Write(ctx, chunkKey, payload)
		}
		// Same term: this is our own chunk. Byte inequality alone does not prove a
		// conflict, because the payload encoding is not guaranteed to be
		// byte-stable across proto library versions — a retry that spans a binary
		// upgrade can re-encode the same records differently. Compare what the
		// chunk actually contains instead, so an identical rewrite stays
		// idempotent and only genuinely different content is corruption.
		if existingFooter.GetGeneration() == footer.GetGeneration() &&
			summaryRecordsByVChannelEqual(existingRecords, records) {
			return nil
		}
	}
	return pchannelSummaryStoreCorruptedf("pchannel summary chunk already exists with different payload: %s", chunkKey)
}

// summaryRecordsByVChannelEqual compares what two chunks carry rather than how
// they were encoded, so an identical rewrite is recognised as a retry even when
// the bytes differ.
func summaryRecordsByVChannelEqual(left, right map[string][]*SummaryRecord) bool {
	if len(left) != len(right) {
		return false
	}
	for vchannel, leftRecords := range left {
		rightRecords, ok := right[vchannel]
		if !ok {
			return false
		}
		leftSorted, rightSorted := sortedSummaryRecords(leftRecords), sortedSummaryRecords(rightRecords)
		if len(leftSorted) != len(rightSorted) {
			return false
		}
		for i := range leftSorted {
			if !summaryRecordEqual(leftSorted[i], rightSorted[i]) {
				return false
			}
		}
	}
	return true
}

func summaryRecordEqual(left, right *SummaryRecord) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.SourceTimeTick == right.SourceTimeTick &&
		left.IdempotencyKey == right.IdempotencyKey &&
		proto.Equal(left.SourceMessageID, right.SourceMessageID) &&
		proto.Equal(left.LastConfirmedMessageID, right.LastConfirmedMessageID) &&
		proto.Equal(left.InsertResult, right.InsertResult)
}

func buildPChannelSummaryChunkKey(cm storage.ChunkManager, pchannel string, generation uint64, term ...int64) string {
	chunkName := pchannelSummaryChunkObjectPrefix + strconv.FormatUint(generation, 10)
	if len(term) > 0 {
		chunkName += ".term" + strconv.FormatInt(term[0], 10)
	}
	return buildPChannelSummaryChunkPrefix(cm, pchannel) +
		chunkName + pchannelSummaryChunkObjectExt
}

// buildPChannelSummaryChunkPrefix returns the object prefix holding every chunk of
// the pchannel's summary store. The trailing separator keeps the prefix from
// matching a sibling whose name merely starts with "chunks".
//
// The prefix is rooted at the chunk manager that will store the object, which is
// what every object key in Milvus is built from: a remote chunk manager returns
// the configured minio.rootPath (a key prefix), a local one returns the local
// storage directory. Reading minio.rootPath directly would be wrong for
// storageType=local — LocalChunkManager writes the key verbatim, so the chunks
// would land under the process working directory instead of the configured
// local storage path. The manager is a parameter rather than a global lookup so
// a key can never be built against a different root than the one that writes it.
func buildPChannelSummaryChunkPrefix(cm storage.ChunkManager, pchannel string) string {
	return path.Join(
		cm.RootPath(),
		"streamingnode",
		"summary-store",
		sanitizeSummaryStorePathPart(pchannel),
		"chunks",
	) + "/"
}

// removeAllPChannelSummaryChunks deletes every chunk object of the pchannel's
// summary store. It is only correct where no catalog meta references a chunk any
// more, such as dropping the store while idempotency is disabled. A bootstrap
// path must not call this based on a stale no-meta read: another owner may have
// published a meta after the read, and a prefix delete would remove referenced
// chunks before the stale opener is fenced.
//
// A prefix removal (rather than a walk over [MinAvailableGeneration,
// LatestGeneration]) is what makes that guarantee hold: it also reaps orphans
// above LatestGeneration left by a persist that wrote the chunk but crashed
// before saving the meta, and any chunk left behind by an earlier partial
// removal.
func removeAllPChannelSummaryChunks(ctx context.Context, pchannel string) error {
	prefix := buildPChannelSummaryChunkPrefix(resource.Resource().ChunkManager(), pchannel)
	// A store that was never written has no chunk directory at all: object
	// storage lists nothing, local storage reports the missing directory.
	if err := resource.Resource().ChunkManager().RemoveWithPrefix(ctx, prefix); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return errors.Wrapf(err, "failed to remove pchannel summary chunks with prefix %s", prefix)
	}
	return nil
}

func sanitizeSummaryStorePathPart(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	return replacer.Replace(value)
}

// hasIdempotencyContent reports whether a replayed chunk carries anything a
// consumer can serve. A chunk of keyless records materializes no dedup entry.
func hasIdempotencyContent(records []*SummaryRecord) bool {
	for _, record := range records {
		if record.IdempotencyKey != "" {
			return true
		}
	}
	return false
}
