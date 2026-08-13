package recovery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

type pchannelSummaryStoreMeta struct {
	PChannel               string
	LatestGeneration       uint64
	MinAvailableGeneration uint64
	MinInUseGeneration     uint64
	SourceCheckpoint       *WALCheckpoint
	ChunkManifest          *streamingpb.PChannelSummaryChunkManifest
	// Term is the WAL assignment term of the owner that last persisted the
	// meta; writers publish updates through catalog CAS and older terms are
	// fenced before they can advance the manifest.
	Term int64
}

type persistedPChannelSummaryChunk struct {
	footer                 *pchannelSummaryChunkFooter
	generation             uint64
	minAvailableGeneration uint64
}

type summaryStoreRecoveryInfo struct {
	summaryMetas []*streamingpb.VChannelSummaryMeta
	storeMeta    *pchannelSummaryStoreMeta
}

const (
	pchannelSummaryChunkObjectExt    = ".psc"
	pchannelSummaryChunkObjectPrefix = "chunk."
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
	summaryInfo, err := m.recoverSummaryInfoFromMeta(ctx, pchannel, checkpoint, vchannels)
	if err != nil {
		return checkpoint, wrapSummaryRecoveryError(err)
	}
	rewound, err := m.recoverSummaryStoreFromSnapshot(ctx, summaryInfo, checkpoint, vchannels)
	if err != nil {
		return checkpoint, wrapSummaryRecoveryError(err)
	}
	return rewound, nil
}

// wrapSummaryRecoveryError decorates referenced-state corruption with the
// operator remediation and FAILS the WAL open. Corruption reaching here always
// concerns state the meta references (chunks inside [MinInUse, Latest] or the
// meta's generation ranges) — orphan-chunk corruption never escalates, it is
// self-healed inline by the recovery probe. A referenced chunk is the only
// durable copy of the idempotency keys below the consume checkpoint once the
// WAL has been truncated past them (the summary snapshot checkpoint is what
// allowed that truncation), so silently resetting to an empty summary would
// accept in-TTL client retries as fresh writes — duplicate data with no error
// anywhere. Failing open is explicit and actionable instead.
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

func (m *summaryManager) recoverSummaryInfoFromMeta(ctx context.Context, pchannel string, checkpoint *WALCheckpoint, vchannels map[string]*vchannelRecoveryInfo) (*summaryStoreRecoveryInfo, error) {
	info, err := m.loadSummaryInfoFromMeta(ctx, pchannel, true, checkpoint)
	if err != nil {
		return nil, err
	}
	m.initializeSummariesFromMeta(vchannels, info.storeMeta.SourceCheckpoint, info.summaryMetas)
	m.Logger().Info(
		ctx, "recover idempotency summary meta done",
		mlog.Int("summaries", len(info.summaryMetas)),
		mlog.Bool("hasPChannelSummaryMeta", info.storeMeta != nil),
	)
	return info, nil
}

func (m *summaryManager) loadSummaryInfoFromMeta(ctx context.Context, pchannel string, allowBootstrap bool, checkpoint *WALCheckpoint) (*summaryStoreRecoveryInfo, error) {
	// Bounded retry: transient catalog blips must not hard-fail the WAL open.
	var summaryMetas []*streamingpb.VChannelSummaryMeta
	if err := retry.Do(ctx, func() error {
		var listErr error
		summaryMetas, listErr = resource.Resource().StreamingNodeCatalog().ListVChannelSummaryMetas(ctx, pchannel, common.VChannelSummaryViewTypeIdempotency)
		return listErr
	}, retry.Attempts(5)); err != nil {
		return nil, errors.Wrap(err, "failed to list idempotency summary meta")
	}
	var storeMetaPB *streamingpb.PChannelSummaryMeta
	if err := retry.Do(ctx, func() error {
		var getErr error
		storeMetaPB, getErr = resource.Resource().StreamingNodeCatalog().GetPChannelSummaryMeta(ctx, pchannel)
		return getErr
	}, retry.Attempts(5)); err != nil {
		return nil, errors.Wrap(err, "failed to get pchannel summary meta")
	}
	storeMeta := pchannelSummaryStoreMetaFromCatalog(storeMetaPB)
	if storeMeta == nil {
		if !allowBootstrap || len(summaryMetas) > 0 {
			return nil, merr.WrapErrServiceInternalMsg("pchannel summary meta missing for pchannel %s", pchannel)
		}
		bootstrapped, err := m.bootstrapPChannelSummaryStore(ctx, pchannel, checkpoint)
		if err != nil {
			return nil, err
		}
		storeMeta = bootstrapped
	}
	return &summaryStoreRecoveryInfo{
		summaryMetas: summaryMetas,
		storeMeta:    storeMeta,
	}, nil
}

func (m *summaryManager) recoverSummaryStoreFromSnapshot(ctx context.Context, info *summaryStoreRecoveryInfo, checkpoint *WALCheckpoint, vchannels map[string]*vchannelRecoveryInfo) (*WALCheckpoint, error) {
	if info == nil || info.storeMeta == nil {
		return checkpoint, nil
	}
	storeMeta := info.storeMeta
	recoveredStoreMeta, err := m.recoverSummariesFromStore(ctx, storeMeta.PChannel, storeMeta)
	if err != nil {
		return checkpoint, err
	}
	if recoveredStoreMeta != nil && recoveredStoreMeta.LatestGeneration > storeMeta.LatestGeneration {
		storeMeta = recoveredStoreMeta
		if err := m.repairPChannelSummaryMeta(ctx, storeMeta); err != nil {
			return checkpoint, err
		}
	}
	info.storeMeta = storeMeta
	m.setPChannelSummarySnapshotCheckpoint(storeMeta.SourceCheckpoint)
	m.ensureActiveSummaries(vchannels, storeMeta.SourceCheckpoint)
	m.markActiveViewsInitialized()
	rewound := m.rewindCheckpointForPChannelSummaryReplay(storeMeta.SourceCheckpoint, checkpoint, vchannels)
	m.Logger().Info(
		ctx, "recover idempotency summary info done",
		mlog.Int("summaries", len(m.summaries())),
		mlog.String("storage", "pchannel-summary-store"),
	)
	return rewound, nil
}

func (m *summaryManager) bootstrapPChannelSummaryStore(ctx context.Context, pchannel string, sourceCheckpoint *WALCheckpoint) (*pchannelSummaryStoreMeta, error) {
	if sourceCheckpoint == nil {
		return nil, merr.WrapErrServiceInternalMsg("cannot bootstrap pchannel summary store without source checkpoint for pchannel %s", pchannel)
	}
	chunkPayload, footer, _, err := marshalPChannelSummaryChunk(pchannel, 0, m.term, sourceCheckpoint, nil)
	if err != nil {
		return nil, err
	}
	chunkKey := buildPChannelSummaryChunkKey(pchannel, footer.Generation, m.term)
	logger := m.Logger().With(mlog.String("op", "bootstrapPChannelSummaryStore"), mlog.Uint64("generation", footer.Generation))
	// Do not remove chunks here based only on the earlier no-meta read: another
	// owner may have bootstrapped and published a meta after that read. A stale
	// opener can safely write an orphan term-suffixed generation-0 chunk and then
	// lose the pchannel-meta CAS, but a prefix delete would remove the new
	// owner's referenced chunk before the stale owner is fenced.
	if err := retryOperationWithBackoff(ctx, logger, func(ctx context.Context) error {
		return writePChannelSummaryChunkIfAbsent(ctx, chunkKey, chunkPayload, m.term)
	}); err != nil {
		return nil, err
	}
	if err := m.persistPChannelSummaryMeta(ctx, logger, &persistedPChannelSummaryChunk{
		footer:                 footer,
		generation:             footer.Generation,
		minAvailableGeneration: 0,
	}, 0); err != nil {
		return nil, err
	}
	m.Logger().Info(
		ctx, "bootstrap pchannel summary store done",
		mlog.String("pchannel", pchannel),
		mlog.String("chunkKey", chunkKey),
		mlog.Uint64("sourceTimeTick", sourceCheckpoint.TimeTick),
	)
	return newPChannelSummaryStoreMetaFromChunk(pchannel, footer, 0, 0), nil
}

func (m *summaryManager) recoverSummariesFromStore(ctx context.Context, pchannel string, meta *pchannelSummaryStoreMeta) (*pchannelSummaryStoreMeta, error) {
	if meta == nil {
		return nil, nil
	}
	if meta.Term > m.term {
		// The durable store was already taken over by a newer assignment term:
		// this open is stale (split-brain) and must not recover from — much
		// less later persist over — the current owner's state.
		return nil, pchannelSummaryStoreFencedf("pchannel summary store of %s already owned by term %d, recovering term %d stops", pchannel, meta.Term, m.term)
	}
	if meta.MinAvailableGeneration > meta.LatestGeneration {
		return nil, pchannelSummaryStoreCorruptedf("pchannel summary generation range mismatch, min available %d, latest %d", meta.MinAvailableGeneration, meta.LatestGeneration)
	}
	if meta.MinAvailableGeneration > meta.MinInUseGeneration || meta.MinInUseGeneration > meta.LatestGeneration {
		return nil, pchannelSummaryStoreCorruptedf("pchannel summary generation range mismatch, min available %d, min in use %d, latest %d", meta.MinAvailableGeneration, meta.MinInUseGeneration, meta.LatestGeneration)
	}
	if err := validatePChannelSummaryManifest(meta); err != nil {
		return nil, err
	}
	sealedMeta, err := m.sealPreviousPChannelSummaryTerm(ctx, pchannel, meta)
	if err != nil {
		return nil, err
	}
	if sealedMeta != nil {
		meta = sealedMeta
	}
	actualMeta := meta
	for generation := meta.MinInUseGeneration; generation <= meta.LatestGeneration; generation++ {
		termRange, ok := pchannelSummaryManifestRangeForGeneration(meta, generation)
		if !ok {
			return nil, pchannelSummaryStoreCorruptedf("pchannel summary chunk manifest misses generation %d", generation)
		}
		footer, err := m.recoverPChannelSummaryChunk(ctx, pchannel, generation, termRange.GetTerm())
		if err != nil {
			return nil, err
		}
		if generation == meta.LatestGeneration {
			actualMeta = newPChannelSummaryStoreMetaFromChunk(pchannel, footer, meta.MinAvailableGeneration, meta.MinInUseGeneration)
			actualMeta.ChunkManifest = clonePChannelSummaryChunkManifest(meta.ChunkManifest)
			break
		}
	}

	if meta.LatestGeneration == ^uint64(0) {
		return actualMeta, nil
	}
	for generation := meta.LatestGeneration + 1; ; generation++ {
		chunkKey := buildPChannelSummaryChunkKey(pchannel, generation, m.term)
		exists, err := resource.Resource().ChunkManager().Exist(ctx, chunkKey)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to probe pchannel summary chunk %s", chunkKey)
		}
		if !exists {
			break
		}
		footer, err := m.recoverPChannelSummaryChunk(ctx, pchannel, generation, m.term)
		if err != nil {
			if errors.Is(err, ErrPChannelSummaryStoreCorrupted) {
				// A chunk above the durable LatestGeneration is an orphan from a
				// persist that wrote the chunk but crashed before advancing the meta.
				// The meta never referenced it, so its source data is still in the WAL
				// and is rebuilt by replay. Drop a corrupt orphan and stop probing
				// rather than failing recovery -- and, crucially, rather than leaving
				// it to wedge the next persist, which would try to rewrite the same
				// generation and hit a permanent byte-mismatch.
				m.Logger().Warn(ctx, "dropping corrupt orphan pchannel summary chunk above latest generation",
					mlog.String("chunkKey", chunkKey), mlog.Uint64("generation", generation), mlog.Err(err))
				if removeErr := resource.Resource().ChunkManager().Remove(ctx, chunkKey); removeErr != nil {
					return nil, errors.Wrapf(removeErr, "failed to remove corrupt orphan pchannel summary chunk %s", chunkKey)
				}
				break
			}
			return nil, err
		}
		previousManifest := actualMeta.ChunkManifest
		actualMeta = newPChannelSummaryStoreMetaFromChunk(pchannel, footer, meta.MinAvailableGeneration, meta.MinInUseGeneration)
		manifest, err := pchannelSummaryManifestWithChunk(previousManifest, m.term, generation, footer.SourceCheckpointTimetick)
		if err != nil {
			return nil, err
		}
		actualMeta.ChunkManifest = manifest
		if generation == ^uint64(0) {
			break
		}
	}
	return actualMeta, nil
}

func (m *summaryManager) recoverPChannelSummaryChunk(
	ctx context.Context,
	pchannel string,
	generation uint64,
	expectedTerm int64,
) (*pchannelSummaryChunkFooter, error) {
	recordsByVChannel, footer, chunkKey, err := m.readPChannelSummaryChunk(ctx, pchannel, generation, expectedTerm)
	if err != nil {
		return nil, err
	}
	for vchannel, records := range recordsByVChannel {
		if !hasIdempotencyCommittedWriteRecords(records) {
			continue
		}
		state := m.summaries()[vchannel]
		if state == nil {
			continue
		}
		if err := state.applyCommittedWriteRecordsAtGeneration(records, generation); err != nil {
			return nil, errors.Wrapf(err, "failed to apply pchannel summary chunk %s for vchannel %s", chunkKey, vchannel)
		}
	}
	evictBeforeTT := evictBeforeTimetick(footer.SourceCheckpointTimetick, m.evictionConfig.entryTTL)
	for _, state := range m.summaries() {
		state.evictForRecovery(evictBeforeTT, m.evictionConfig.minEntries, m.evictionConfig.maxBytes)
	}
	return footer, nil
}

func (m *summaryManager) readPChannelSummaryChunk(
	ctx context.Context,
	pchannel string,
	generation uint64,
	expectedTerm int64,
) (map[string][]committedWriteRecord, *pchannelSummaryChunkFooter, string, error) {
	chunkKey := buildPChannelSummaryChunkKey(pchannel, generation, expectedTerm)
	// Bounded retry on the raw read: a transient object-storage blip must not
	// hard-fail the WAL open now that referenced-state corruption does — only a
	// VERIFIED decode/checksum failure below is corruption; IO errors here are
	// retried and, if persistent, abort with the transient cause intact.
	var payload []byte
	if err := retry.Do(ctx, func() error {
		var readErr error
		payload, readErr = resource.Resource().ChunkManager().Read(ctx, chunkKey)
		return readErr
	}, retry.Attempts(5)); err != nil {
		return nil, nil, chunkKey, errors.Wrapf(err, "failed to read pchannel summary chunk %s", chunkKey)
	}
	recordsByVChannel, footer, _, err := unmarshalPChannelSummaryChunk(payload)
	if err != nil {
		return nil, nil, chunkKey, errors.Wrapf(err, "failed to unmarshal pchannel summary chunk %s", chunkKey)
	}
	if footer.PChannel != "" && footer.PChannel != pchannel {
		return nil, nil, chunkKey, pchannelSummaryStoreCorruptedf("pchannel summary chunk pchannel mismatch, meta %s, chunk %s", pchannel, footer.PChannel)
	}
	if footer.Generation != generation {
		return nil, nil, chunkKey, pchannelSummaryStoreCorruptedf("pchannel summary chunk generation mismatch, expected %d, actual %d", generation, footer.Generation)
	}
	if footer.Term > m.term {
		return nil, nil, chunkKey, pchannelSummaryStoreFencedf("pchannel summary chunk %s written by newer term %d, recovering term %d stops", chunkKey, footer.Term, m.term)
	}
	if footer.Term != expectedTerm {
		return nil, nil, chunkKey, pchannelSummaryStoreCorruptedf("pchannel summary chunk %s term mismatch, manifest %d, footer %d", chunkKey, expectedTerm, footer.Term)
	}
	return recordsByVChannel, footer, chunkKey, nil
}

func (m *summaryManager) sealPreviousPChannelSummaryTerm(ctx context.Context, pchannel string, meta *pchannelSummaryStoreMeta) (*pchannelSummaryStoreMeta, error) {
	lastRange := pchannelSummaryManifestLastRange(meta)
	if lastRange == nil || lastRange.GetSealed() || lastRange.GetTerm() >= m.term {
		return meta, nil
	}
	actual := &pchannelSummaryStoreMeta{
		PChannel:               meta.PChannel,
		LatestGeneration:       meta.LatestGeneration,
		MinAvailableGeneration: meta.MinAvailableGeneration,
		MinInUseGeneration:     meta.MinInUseGeneration,
		SourceCheckpoint:       cloneWALCheckpoint(meta.SourceCheckpoint),
		Term:                   m.term,
		ChunkManifest:          clonePChannelSummaryChunkManifest(meta.ChunkManifest),
	}
	scanned := 0
	for generation := lastRange.GetEndGeneration() + 1; ; generation++ {
		chunkKey := buildPChannelSummaryChunkKey(pchannel, generation, lastRange.GetTerm())
		exists, err := resource.Resource().ChunkManager().Exist(ctx, chunkKey)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to probe previous-term pchannel summary chunk %s", chunkKey)
		}
		if !exists {
			break
		}
		_, footer, _, err := m.readPChannelSummaryChunk(ctx, pchannel, generation, lastRange.GetTerm())
		if err != nil {
			return nil, err
		}
		manifest, err := pchannelSummaryManifestWithChunk(actual.ChunkManifest, lastRange.GetTerm(), generation, footer.SourceCheckpointTimetick)
		if err != nil {
			return nil, err
		}
		actual.ChunkManifest = manifest
		actual.LatestGeneration = generation
		actual.SourceCheckpoint = pchannelSummarySourceCheckpointToWALCheckpoint(&pchannelSummarySourceCheckpoint{
			MessageID: cloneMessageIDProto(footer.SourceCheckpointMessageID),
			TimeTick:  footer.SourceCheckpointTimetick,
		})
		scanned++
		if scanned%pchannelSummaryTermSealProgressInterval == 0 {
			if err := m.repairPChannelSummaryMeta(ctx, actual); err != nil {
				return nil, err
			}
		}
		if generation == ^uint64(0) {
			break
		}
	}
	manifest, err := markPChannelSummaryRangeSealed(actual.ChunkManifest, lastRange.GetTerm())
	if err != nil {
		return nil, err
	}
	actual.ChunkManifest = manifest
	actual.Term = m.term
	if err := m.repairPChannelSummaryMeta(ctx, actual); err != nil {
		return nil, err
	}
	return actual, nil
}

func (m *summaryManager) repairPChannelSummaryMeta(ctx context.Context, storeMeta *pchannelSummaryStoreMeta) error {
	return updatePChannelSummaryMetaWithCAS(ctx,
		m.Logger().With(mlog.String("op", "repairPChannelSummaryMeta")),
		m.pchannel,
		func(currentPB *streamingpb.PChannelSummaryMeta, current *pchannelSummaryStoreMeta) (*streamingpb.PChannelSummaryMeta, error) {
			if current != nil && current.Term > m.term {
				return nil, pchannelSummaryStoreFencedf("pchannel summary meta of %s already owned by term %d, own term %d", m.pchannel, current.Term, m.term)
			}
			return storeMeta.intoCatalogMeta(), nil
		})
}
