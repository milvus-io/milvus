package recovery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

type persistedPChannelSummaryChunk struct {
	footer     *streamingpb.PChannelSummaryChunkFooter
	generation uint64
}

const (
	pchannelSummaryChunkObjectExt    = ".psc"
	pchannelSummaryChunkObjectPrefix = "chunk."
	// probeLimit bounds a single forward probe. A term that wrote more than this
	// many chunks past its manifest is pathological; failing loudly beats
	// scanning object storage without end.
	pchannelSummaryChunkProbeLimit = 1 << 16
)

// recoverSummaries recovers the idempotency summary cache before WAL replay and
// returns the consume checkpoint to resume from (rewound when the durable summary
// store requires replaying earlier messages). The caller (recoveryStorageImpl)
// supplies the checkpoint and the active vchannel set so summaryManager does not
// reach into recoveryStorageImpl for them.
//
// Behavior by state:
//   - when idempotency is disabled it skips recovery/bootstrap; it only probes
//     the catalog to drop a store left behind by an earlier enabled run (a
//     pchannel that never used idempotency pays one catalog read, no writes);
//   - transient catalog/object-storage read errors are retried bounded and, if
//     persistent, abort the open with the transient cause;
//   - corruption of REFERENCED summary state FAILS the WAL open (the WAL may be
//     truncated past the store's coverage — see wrapSummaryRecoveryError), while
//     orphan-chunk corruption self-heals inline.
func (m *summaryManager) recoverSummaries(ctx context.Context, pchannel string, checkpoint *WALCheckpoint, vchannels map[string]*vchannelRecoveryInfo) (*WALCheckpoint, error) {
	if !m.cfg.idempotencyEnabled {
		// Any summary store persisted by an earlier enabled run is stale while the
		// feature is off (checkpoints advance and the WAL truncates past its
		// SourceCheckpoint); drop it so a later re-enable bootstraps from the
		// then-current checkpoint instead of rewinding to a truncated position.
		m.dropSummaryStoreForDisabledIdempotency(ctx, pchannel)
		return checkpoint, nil
	}
	rewound, err := m.recoverFromSummaryStore(ctx, pchannel, checkpoint, vchannels)
	if err != nil {
		return checkpoint, wrapSummaryRecoveryError(err)
	}
	return rewound, nil
}

// wrapSummaryRecoveryError decorates corruption of state the manifest still
// retains with the operator remediation, and FAILS the WAL open.
//
// A retained chunk is the only durable copy of the idempotency keys below the
// persist watermark once the WAL has been truncated past them -- and it is the
// summary store's own watermark that allowed that truncation. Silently starting
// with an empty window would accept in-retention client retries as fresh
// writes: duplicate data, no error anywhere. Failing the open is explicit and
// actionable instead.
//
// Chunks above the watermark are a different case and never escalate: they were
// never acknowledged, their data is still in the WAL, and probing simply stops
// short of them.
func wrapSummaryRecoveryError(err error) error {
	if errors.Is(err, ErrPChannelSummaryStoreFenced) {
		return errors.Wrap(err,
			"idempotency summary store is already owned by a newer WAL assignment term; "+
				"refusing the stale open instead of overwriting the current owner's summary state. "+
				"No remediation needed: the newer assignment is authoritative and this node should observe it shortly")
	}
	if errors.Is(err, ErrPChannelSummaryStoreCorrupted) {
		return errors.Wrap(err,
			"idempotency summary store is corrupted and the WAL may already be truncated past its coverage; "+
				"refusing to open the WAL rather than silently losing in-TTL idempotency keys. "+
				"Remediation: set streaming.idempotency.enabled=false and restart to drop the corrupted store, "+
				"then re-enable for a clean bootstrap (idempotency history is lost either way)")
	}
	return err
}

// recoverFromSummaryStore rebuilds the consumers from the durable store and
// returns the checkpoint WAL replay must resume from.
//
// The sequence is fixed, and every step exists to close a specific way data
// could otherwise be lost:
//
//  1. read the meta -- a pointer that may lag, never a source of truth about
//     which chunks exist;
//  2. find the newest manifest by probing terms downward;
//  3. probe chunks forward within that manifest's own term, stopping at the
//     first miss -- this recovers everything written after the last manifest;
//  4. publish this term's manifest, inheriting the previous one and sealing the
//     probed tail into it -- without this, the tail is invisible to the NEXT
//     recovery and is lost silently;
//  5. only now may this owner write chunks;
//  6. read the retained chunks and hand each vchannel's records to its consumer.
//
// WAL replay is never rewound. A chunk is durable before the WAL checkpoint that
// covers it, so the checkpoint handed in already separates what the chunks hold
// from what replay must redo.
func (m *summaryManager) recoverFromSummaryStore(
	ctx context.Context,
	pchannel string,
	checkpoint *WALCheckpoint,
	vchannels map[string]*vchannelRecoveryInfo,
) (*WALCheckpoint, error) {
	metaPB, err := m.loadPChannelSummaryMeta(ctx, pchannel)
	if err != nil {
		return checkpoint, err
	}
	meta := pchannelSummaryStoreMetaFromCatalog(metaPB)

	floorTerm := int64(0)
	if meta != nil {
		if meta.Term > m.term {
			return checkpoint, pchannelSummaryStoreFencedf(
				"pchannel summary meta of %s owned by newer term %d, own term %d", pchannel, meta.Term, m.term,
			)
		}
		floorTerm = meta.Term
	}

	previous, previousTerm, err := findNewestPChannelSummaryManifest(ctx, pchannel, m.term, floorTerm)
	if err != nil {
		return checkpoint, err
	}

	discovered, err := m.probeForwardFromManifest(ctx, pchannel, previous, previousTerm)
	if err != nil {
		return checkpoint, err
	}

	m.manifest = inheritPChannelSummaryManifest(previous, discovered)
	if newest, ok := pchannelSummaryManifestNewest(m.manifest); ok {
		m.nextGeneration = newest.GetGeneration() + 1
		m.latestCoveredTT = newest.GetEndTimetick()
	}

	// A term that cannot publish its manifest must not write a chunk: a chunk
	// written before its manifest is one recovery can never find. This is a hard
	// failure, not a best-effort ordering.
	if err := retryOperationWithBackoff(ctx,
		m.Logger().With(mlog.String("op", "publishPChannelSummaryManifest")),
		func(ctx context.Context) error {
			return writePChannelSummaryManifest(ctx, pchannel, m.term, m.manifest)
		}); err != nil {
		return checkpoint, errors.Wrap(err, "failed to publish the manifest for this term; refusing to write chunks without it")
	}

	// The WAL checkpoint is the boundary: everything below it is in a chunk,
	// everything above it is replayed. The store keeps no checkpoint of its own.
	m.ensureActiveSummaries(vchannels, checkpoint)
	if err := m.replayRetainedChunks(ctx, pchannel); err != nil {
		return checkpoint, err
	}
	m.markActiveViewsInitialized()

	m.Logger().Info(
		ctx, "recovered from pchannel summary store",
		mlog.Int("summaries", len(m.summaries())),
		mlog.Int64("manifestTerm", previousTerm),
		mlog.Uint64("nextGeneration", m.nextGeneration),
	)
	return checkpoint, nil
}

func (m *summaryManager) loadPChannelSummaryMeta(ctx context.Context, pchannel string) (*streamingpb.PChannelSummaryMeta, error) {
	var metaPB *streamingpb.PChannelSummaryMeta
	// Bounded retry: a transient catalog blip must not hard-fail the WAL open.
	if err := retry.Do(ctx, func() error {
		var getErr error
		metaPB, getErr = resource.Resource().StreamingNodeCatalog().GetPChannelSummaryMeta(ctx, pchannel)
		return getErr
	}, retry.Attempts(5)); err != nil {
		return nil, errors.Wrap(err, "failed to get pchannel summary meta")
	}
	return metaPB, nil
}

// probeForwardFromManifest discovers chunks written after the manifest was last
// written, because a chunk is made durable before the manifest naming it.
//
// It probes ONE term -- the manifest's own -- and stops at the first missing
// generation. Both are sound for the same reason: a term publishes its manifest
// before its first chunk, so a chunk beyond this manifest cannot belong to any
// other term; and generations are claimed in order, so a hole means the writer
// stopped there. Anything above a hole was never acknowledged and is still in
// the WAL.
//
// The probe starts at generation 0 when the manifest names no chunk at all. That
// is not a degenerate case: a term that published its manifest, wrote its first
// chunk and then died leaves exactly that state, and skipping it would drop the
// chunk permanently.
func (m *summaryManager) probeForwardFromManifest(
	ctx context.Context,
	pchannel string,
	manifest *streamingpb.PChannelSummaryManifest,
	manifestTerm int64,
) ([]*streamingpb.PChannelSummaryChunkIndexEntry, error) {
	if manifest == nil {
		return nil, nil
	}
	nextGeneration := uint64(0)
	if newest, ok := pchannelSummaryManifestNewest(manifest); ok {
		nextGeneration = newest.GetGeneration() + 1
	}

	discovered := make([]*streamingpb.PChannelSummaryChunkIndexEntry, 0)
	cm := resource.Resource().ChunkManager()
	for offset := 0; offset < pchannelSummaryChunkProbeLimit; offset++ {
		generation := nextGeneration + uint64(offset)
		exists, err := cm.Exist(ctx, buildPChannelSummaryChunkKey(cm, pchannel, generation, manifestTerm))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to probe pchannel summary chunk at generation %d", generation)
		}
		if !exists {
			break
		}
		_, footer, err := m.readPChannelSummaryChunk(ctx, pchannel, generation, manifestTerm)
		if errors.Is(err, ErrPChannelSummaryStoreCorrupted) {
			// A probed chunk is by construction one whose WAL checkpoint never
			// advanced: the persist that wrote it had to write the manifest next,
			// and failing that fails the whole checkpoint persist. So its writes are
			// still in the WAL and replay will redo them -- dropping it is safe, and
			// escalating would refuse the open over data that is not lost. Nothing
			// above it can be sound either, so the probe stops here.
			m.Logger().Warn(ctx, "dropping corrupt summary chunk found above the manifest; its writes are replayed from the WAL",
				mlog.Uint64("generation", generation),
				mlog.Int64("term", manifestTerm),
				mlog.Err(err))
			if removeErr := cm.Remove(ctx, buildPChannelSummaryChunkKey(cm, pchannel, generation, manifestTerm)); removeErr != nil {
				m.Logger().Warn(ctx, "failed to drop corrupt summary chunk; the object leaks", mlog.Err(removeErr))
			}
			break
		}
		if err != nil {
			return nil, err
		}
		discovered = append(discovered, chunkIndexEntryFromFooter(footer))
	}
	return discovered, nil
}

// replayRetainedChunks reads every chunk the manifest still retains and hands
// the decoded records to the per-vchannel summaries. The manifest carries each
// chunk's term, so nothing has to be looked up to address the object.
func (m *summaryManager) replayRetainedChunks(ctx context.Context, pchannel string) error {
	for _, entry := range m.manifest.GetChunks() {
		if err := m.recoverPChannelSummaryChunk(ctx, pchannel, entry.GetGeneration(), entry.GetTerm()); err != nil {
			return err
		}
	}
	return nil
}

func (m *summaryManager) recoverPChannelSummaryChunk(
	ctx context.Context,
	pchannel string,
	generation uint64,
	expectedTerm int64,
) error {
	recordsByVChannel, _, err := m.readPChannelSummaryChunk(ctx, pchannel, generation, expectedTerm)
	if err != nil {
		return err
	}
	for vchannel, records := range recordsByVChannel {
		if !hasIdempotencyContent(records) {
			continue
		}
		state := m.summaries()[vchannel]
		if state == nil {
			continue
		}
		state.applySummaryRecordsAtGeneration(records)
		state.capRecoveryBytes(m.evictionConfig.retainedBytes)
	}
	return nil
}

func (m *summaryManager) readPChannelSummaryChunk(
	ctx context.Context,
	pchannel string,
	generation uint64,
	expectedTerm int64,
) (map[string][]*SummaryRecord, *streamingpb.PChannelSummaryChunkFooter, error) {
	chunkKey := buildPChannelSummaryChunkKey(resource.Resource().ChunkManager(), pchannel, generation, expectedTerm)
	// Bounded retry on the raw read. Referenced-state corruption hard-fails the
	// WAL open, so only a VERIFIED decode/checksum failure below may be called
	// corruption; a transient object-storage blip is retried here and, if
	// persistent, aborts with the transient cause intact.
	var payload []byte
	if err := retry.Do(ctx, func() error {
		var readErr error
		payload, readErr = resource.Resource().ChunkManager().Read(ctx, chunkKey)
		return readErr
	}, retry.Attempts(5)); err != nil {
		return nil, nil, errors.Wrapf(err, "failed to read pchannel summary chunk %s", chunkKey)
	}
	recordsByVChannel, footer, err := unmarshalPChannelSummaryChunk(payload)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to unmarshal pchannel summary chunk %s", chunkKey)
	}
	if footer.Pchannel != "" && footer.Pchannel != pchannel {
		return nil, nil, pchannelSummaryStoreCorruptedf("pchannel summary chunk pchannel mismatch, meta %s, chunk %s", pchannel, footer.Pchannel)
	}
	if footer.Generation != generation {
		return nil, nil, pchannelSummaryStoreCorruptedf("pchannel summary chunk generation mismatch, expected %d, actual %d", generation, footer.Generation)
	}
	if footer.Term > m.term {
		return nil, nil, pchannelSummaryStoreFencedf("pchannel summary chunk %s written by newer term %d, recovering term %d stops", chunkKey, footer.Term, m.term)
	}
	if footer.Term != expectedTerm {
		return nil, nil, pchannelSummaryStoreCorruptedf("pchannel summary chunk %s term mismatch, manifest %d, footer %d", chunkKey, expectedTerm, footer.Term)
	}
	return recordsByVChannel, footer, nil
}
