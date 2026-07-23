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

type pchannelWindowStoreMeta struct {
	PChannel               string
	LatestGeneration       uint64
	MinAvailableGeneration uint64
	MinInUseGeneration     uint64
	SourceCheckpoint       *WALCheckpoint
	// Term is the WAL assignment term of the owner that last persisted the
	// meta; writers with an older term are fenced (best-effort, no CAS).
	Term int64
}

type persistedPChannelWindowChunk struct {
	footer                 *pchannelWindowChunkFooter
	generation             uint64
	minAvailableGeneration uint64
}

type windowStoreRecoveryInfo struct {
	windowMetas []*streamingpb.VChannelWindowMeta
	storeMeta   *pchannelWindowStoreMeta
}

const (
	pchannelWindowChunkObjectExt    = ".pwc"
	pchannelWindowChunkObjectPrefix = "chunk."
)

// recoverWindows recovers the idempotency window cache before WAL replay and
// returns the consume checkpoint to resume from (rewound when the durable window
// store requires replaying earlier messages). The caller (recoveryStorageImpl)
// supplies the checkpoint and the active vchannel set so windowManager does not
// reach into recoveryStorageImpl for them.
//
// Behavior by state:
//   - when idempotency is disabled it skips recovery/bootstrap; it only probes
//     the catalog to drop a store left behind by an earlier enabled run (a
//     pchannel that never used idempotency pays one catalog read, no writes);
//   - transient catalog/object-storage read errors are retried bounded and, if
//     persistent, abort the open with the transient cause;
//   - corruption of REFERENCED window state FAILS the WAL open (the WAL may be
//     truncated past the store's coverage — see wrapWindowRecoveryError), while
//     orphan-chunk corruption self-heals inline.
func (m *windowManager) recoverWindows(ctx context.Context, pchannel string, checkpoint *WALCheckpoint, vchannels map[string]*vchannelRecoveryInfo) (*WALCheckpoint, error) {
	if !m.cfg.idempotencyEnabled {
		// Any window store persisted by an earlier enabled run is stale while the
		// feature is off (checkpoints advance and the WAL truncates past its
		// SourceCheckpoint); drop it so a later re-enable bootstraps from the
		// then-current checkpoint instead of rewinding to a truncated position.
		m.dropWindowStoreForDisabledIdempotency(ctx, pchannel)
		return checkpoint, nil
	}
	windowInfo, err := m.recoverWindowInfoFromMeta(ctx, pchannel, checkpoint, vchannels)
	if err != nil {
		return checkpoint, wrapWindowRecoveryError(err)
	}
	rewound, err := m.recoverWindowStoreFromSnapshot(ctx, windowInfo, checkpoint, vchannels)
	if err != nil {
		return checkpoint, wrapWindowRecoveryError(err)
	}
	return rewound, nil
}

// wrapWindowRecoveryError decorates referenced-state corruption with the
// operator remediation and FAILS the WAL open. Corruption reaching here always
// concerns state the meta references (chunks inside [MinInUse, Latest] or the
// meta's generation ranges) — orphan-chunk corruption never escalates, it is
// self-healed inline by the recovery probe. A referenced chunk is the only
// durable copy of the idempotency keys below the consume checkpoint once the
// WAL has been truncated past them (the window snapshot checkpoint is what
// allowed that truncation), so silently resetting to an empty window would
// accept in-TTL client retries as fresh writes — duplicate data with no error
// anywhere. Failing open is explicit and actionable instead.
func wrapWindowRecoveryError(err error) error {
	if errors.Is(err, ErrPChannelWindowStoreFenced) {
		return errors.Wrap(err,
			"idempotency window store is already owned by a newer WAL assignment term; "+
				"refusing the stale open instead of overwriting the current owner's window state. "+
				"No remediation needed: the newer assignment is authoritative and this node should observe it shortly")
	}
	if errors.Is(err, ErrPChannelWindowStoreCorrupted) {
		return errors.Wrap(err,
			"idempotency window store is corrupted and the WAL may already be truncated past its coverage; "+
				"refusing to open the WAL rather than silently losing in-TTL idempotency keys. "+
				"Remediation: set streaming.idempotency.enabled=false and restart to drop the corrupted store, "+
				"then re-enable for a clean bootstrap (idempotency history is lost either way)")
	}
	return err
}

func (m *windowManager) recoverWindowInfoFromMeta(ctx context.Context, pchannel string, checkpoint *WALCheckpoint, vchannels map[string]*vchannelRecoveryInfo) (*windowStoreRecoveryInfo, error) {
	info, err := m.loadWindowInfoFromMeta(ctx, pchannel, true, checkpoint)
	if err != nil {
		return nil, err
	}
	m.initializeIdempotencyWindowsFromMeta(vchannels, info.storeMeta.SourceCheckpoint, info.windowMetas)
	m.Logger().Info(ctx, "recover idempotency window meta done",
		mlog.Int("windows", len(info.windowMetas)),
		mlog.Bool("hasPChannelWindowMeta", info.storeMeta != nil),
	)
	return info, nil
}

func (m *windowManager) loadWindowInfoFromMeta(ctx context.Context, pchannel string, allowBootstrap bool, checkpoint *WALCheckpoint) (*windowStoreRecoveryInfo, error) {
	// Bounded retry: transient catalog blips must not hard-fail the WAL open.
	var windowMetas []*streamingpb.VChannelWindowMeta
	if err := retry.Do(ctx, func() error {
		var listErr error
		windowMetas, listErr = resource.Resource().StreamingNodeCatalog().ListVChannelWindowMetas(ctx, pchannel, common.VChannelWindowViewTypeIdempotency)
		return listErr
	}, retry.Attempts(5)); err != nil {
		return nil, errors.Wrap(err, "failed to list idempotency window meta")
	}
	var storeMetaPB *streamingpb.PChannelWindowMeta
	if err := retry.Do(ctx, func() error {
		var getErr error
		storeMetaPB, getErr = resource.Resource().StreamingNodeCatalog().GetPChannelWindowMeta(ctx, pchannel)
		return getErr
	}, retry.Attempts(5)); err != nil {
		return nil, errors.Wrap(err, "failed to get pchannel window meta")
	}
	storeMeta := pchannelWindowStoreMetaFromCatalog(storeMetaPB)
	if storeMeta == nil {
		if !allowBootstrap || len(windowMetas) > 0 {
			return nil, merr.WrapErrServiceInternalMsg("pchannel window meta missing for pchannel %s", pchannel)
		}
		bootstrapped, err := m.bootstrapPChannelWindowStore(ctx, pchannel, checkpoint)
		if err != nil {
			return nil, err
		}
		storeMeta = bootstrapped
	}
	return &windowStoreRecoveryInfo{
		windowMetas: windowMetas,
		storeMeta:   storeMeta,
	}, nil
}

func (m *windowManager) recoverWindowStoreFromSnapshot(ctx context.Context, info *windowStoreRecoveryInfo, checkpoint *WALCheckpoint, vchannels map[string]*vchannelRecoveryInfo) (*WALCheckpoint, error) {
	if info == nil || info.storeMeta == nil {
		return checkpoint, nil
	}
	storeMeta := info.storeMeta
	recoveredStoreMeta, err := m.recoverIdempotencyWindowsFromPChannelWindowStore(ctx, storeMeta.PChannel, storeMeta)
	if err != nil {
		return checkpoint, err
	}
	if recoveredStoreMeta != nil && recoveredStoreMeta.LatestGeneration > storeMeta.LatestGeneration {
		storeMeta = recoveredStoreMeta
		if err := m.repairPChannelWindowMeta(ctx, storeMeta); err != nil {
			return checkpoint, err
		}
	}
	info.storeMeta = storeMeta
	m.setPChannelWindowSnapshotCheckpoint(storeMeta.SourceCheckpoint)
	m.ensureActiveIdempotencyWindows(vchannels, storeMeta.SourceCheckpoint)
	m.markActiveViewsInitialized()
	rewound := m.rewindCheckpointForPChannelWindowReplay(storeMeta.SourceCheckpoint, checkpoint, vchannels)
	m.Logger().Info(ctx, "recover idempotency window info done",
		mlog.Int("windows", len(m.idempotencyWindows())),
		mlog.String("storage", "pchannel-window-store"),
	)
	return rewound, nil
}

func (m *windowManager) bootstrapPChannelWindowStore(ctx context.Context, pchannel string, sourceCheckpoint *WALCheckpoint) (*pchannelWindowStoreMeta, error) {
	if sourceCheckpoint == nil {
		return nil, merr.WrapErrServiceInternalMsg("cannot bootstrap pchannel window store without source checkpoint for pchannel %s", pchannel)
	}
	chunkPayload, footer, _, err := marshalPChannelWindowChunk(pchannel, 0, m.term, sourceCheckpoint, nil)
	if err != nil {
		return nil, err
	}
	chunkKey := buildPChannelWindowChunkKey(pchannel, footer.Generation)
	logger := m.Logger().With(mlog.String("op", "bootstrapPChannelWindowStore"), mlog.Uint64("generation", footer.Generation))
	// Bootstrap only runs with no catalog meta and no vchannel window metas, so
	// nothing references any chunk of this pchannel: whatever is left under the
	// chunk prefix is garbage from a store dropped while idempotency was disabled
	// (its chunk removal is best-effort) or from an earlier bootstrap that wrote
	// the chunk and crashed before saving the meta. Reap it before writing
	// generation 0, otherwise a leftover chunk either fails the write-if-absent
	// below with a payload mismatch -- an unretriable error that
	// retryOperationWithBackoff retries forever, hanging the WAL open -- or gets
	// adopted by the orphan probe of
	// recoverIdempotencyWindowsFromPChannelWindowStore, rewinding recovery to an
	// already-truncated source checkpoint.
	if err := retryOperationWithBackoff(ctx, logger, func(ctx context.Context) error {
		return removeAllPChannelWindowChunks(ctx, pchannel)
	}); err != nil {
		return nil, err
	}
	if err := retryOperationWithBackoff(ctx, logger, func(ctx context.Context) error {
		return writePChannelWindowChunkIfAbsent(ctx, chunkKey, chunkPayload, m.term)
	}); err != nil {
		return nil, err
	}
	if err := m.persistPChannelWindowMeta(ctx, logger, &persistedPChannelWindowChunk{
		footer:                 footer,
		generation:             footer.Generation,
		minAvailableGeneration: 0,
	}, 0); err != nil {
		return nil, err
	}
	m.Logger().Info(ctx, "bootstrap pchannel window store done",
		mlog.String("pchannel", pchannel),
		mlog.String("chunkKey", chunkKey),
		mlog.Uint64("sourceTimeTick", sourceCheckpoint.TimeTick),
	)
	return newPChannelWindowStoreMetaFromChunk(pchannel, footer, 0, 0), nil
}

func (m *windowManager) recoverIdempotencyWindowsFromPChannelWindowStore(ctx context.Context, pchannel string, meta *pchannelWindowStoreMeta) (*pchannelWindowStoreMeta, error) {
	if meta == nil {
		return nil, nil
	}
	if meta.Term > m.term {
		// The durable store was already taken over by a newer assignment term:
		// this open is stale (split-brain) and must not recover from — much
		// less later persist over — the current owner's state.
		return nil, pchannelWindowStoreFencedf("pchannel window store of %s already owned by term %d, recovering term %d stops", pchannel, meta.Term, m.term)
	}
	if meta.MinAvailableGeneration > meta.LatestGeneration {
		return nil, pchannelWindowStoreCorruptedf("pchannel window generation range mismatch, min available %d, latest %d", meta.MinAvailableGeneration, meta.LatestGeneration)
	}
	if meta.MinAvailableGeneration > meta.MinInUseGeneration || meta.MinInUseGeneration > meta.LatestGeneration {
		return nil, pchannelWindowStoreCorruptedf("pchannel window generation range mismatch, min available %d, min in use %d, latest %d", meta.MinAvailableGeneration, meta.MinInUseGeneration, meta.LatestGeneration)
	}
	actualMeta := meta
	for generation := meta.MinInUseGeneration; generation <= meta.LatestGeneration; generation++ {
		footer, err := m.recoverPChannelWindowChunk(ctx, pchannel, generation)
		if err != nil {
			return nil, err
		}
		if generation == meta.LatestGeneration {
			actualMeta = newPChannelWindowStoreMetaFromChunk(pchannel, footer, meta.MinAvailableGeneration, meta.MinInUseGeneration)
			break
		}
	}

	if meta.LatestGeneration == ^uint64(0) {
		return actualMeta, nil
	}
	for generation := meta.LatestGeneration + 1; ; generation++ {
		chunkKey := buildPChannelWindowChunkKey(pchannel, generation)
		exists, err := resource.Resource().ChunkManager().Exist(ctx, chunkKey)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to probe pchannel window chunk %s", chunkKey)
		}
		if !exists {
			break
		}
		footer, err := m.recoverPChannelWindowChunk(ctx, pchannel, generation)
		if err != nil {
			if errors.Is(err, ErrPChannelWindowStoreCorrupted) {
				// A chunk above the durable LatestGeneration is an orphan from a
				// persist that wrote the chunk but crashed before advancing the meta.
				// The meta never referenced it, so its source data is still in the WAL
				// and is rebuilt by replay. Drop a corrupt orphan and stop probing
				// rather than failing recovery -- and, crucially, rather than leaving
				// it to wedge the next persist, which would try to rewrite the same
				// generation and hit a permanent byte-mismatch.
				m.Logger().Warn(ctx, "dropping corrupt orphan pchannel window chunk above latest generation",
					mlog.String("chunkKey", chunkKey), mlog.Uint64("generation", generation), mlog.Err(err))
				if removeErr := resource.Resource().ChunkManager().Remove(ctx, chunkKey); removeErr != nil {
					return nil, errors.Wrapf(removeErr, "failed to remove corrupt orphan pchannel window chunk %s", chunkKey)
				}
				break
			}
			return nil, err
		}
		actualMeta = newPChannelWindowStoreMetaFromChunk(pchannel, footer, meta.MinAvailableGeneration, meta.MinInUseGeneration)
		if generation == ^uint64(0) {
			break
		}
	}
	return actualMeta, nil
}

func (m *windowManager) recoverPChannelWindowChunk(
	ctx context.Context,
	pchannel string,
	generation uint64,
) (*pchannelWindowChunkFooter, error) {
	chunkKey := buildPChannelWindowChunkKey(pchannel, generation)
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
		return nil, errors.Wrapf(err, "failed to read pchannel window chunk %s", chunkKey)
	}
	recordsByVChannel, footer, _, err := unmarshalPChannelWindowChunk(payload)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal pchannel window chunk %s", chunkKey)
	}
	if footer.PChannel != "" && footer.PChannel != pchannel {
		return nil, pchannelWindowStoreCorruptedf("pchannel window chunk pchannel mismatch, meta %s, chunk %s", pchannel, footer.PChannel)
	}
	if footer.Generation != generation {
		return nil, pchannelWindowStoreCorruptedf("pchannel window chunk generation mismatch, expected %d, actual %d", generation, footer.Generation)
	}
	if footer.Term > m.term {
		return nil, pchannelWindowStoreFencedf("pchannel window chunk %s written by newer term %d, recovering term %d stops", chunkKey, footer.Term, m.term)
	}
	for vchannel, records := range recordsByVChannel {
		if !hasIdempotencyCommittedWriteRecords(records) {
			continue
		}
		state := m.idempotencyWindows()[vchannel]
		if state == nil {
			continue
		}
		if generation < state.minRequiredGeneration {
			continue
		}
		if err := state.applyCommittedWriteRecordsAtGeneration(records, generation); err != nil {
			return nil, errors.Wrapf(err, "failed to apply pchannel window chunk %s for vchannel %s", chunkKey, vchannel)
		}
	}
	evictBeforeTT := evictBeforeTimetick(footer.SourceCheckpointTimetick, m.evictionConfig.windowTTL)
	for _, state := range m.idempotencyWindows() {
		state.evictForRecovery(evictBeforeTT, m.evictionConfig.minEntries, m.evictionConfig.maxEntries, m.evictionConfig.maxBytes)
	}
	return footer, nil
}

func (m *windowManager) repairPChannelWindowMeta(ctx context.Context, storeMeta *pchannelWindowStoreMeta) error {
	return retryOperationWithBackoff(ctx,
		m.Logger().With(mlog.String("op", "repairPChannelWindowMeta")),
		func(ctx context.Context) error {
			return resource.Resource().StreamingNodeCatalog().SavePChannelWindowMeta(ctx, m.pchannel, storeMeta.intoCatalogMeta())
		})
}
