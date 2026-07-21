package recovery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

type pchannelWindowStoreMeta struct {
	PChannel               string
	LatestGeneration       uint64
	MinAvailableGeneration uint64
	MinInUseGeneration     uint64
	SourceCheckpoint       *WALCheckpoint
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
// The window store is a rebuildable cache, not a source of truth, so this must
// never block normal WAL recovery:
//   - when idempotency is disabled it skips recovery/bootstrap; it only probes
//     the catalog to drop a store left behind by an earlier enabled run (a
//     pchannel that never used idempotency pays one catalog read, no writes);
//   - when the durable window store is corrupted it logs, drops the corrupted
//     cache, and continues with an empty window set rather than failing.
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
		return checkpoint, m.handleWindowRecoveryError(err, checkpoint, vchannels)
	}
	rewound, err := m.recoverWindowStoreFromSnapshot(ctx, windowInfo, checkpoint, vchannels)
	if err != nil {
		return checkpoint, m.handleWindowRecoveryError(err, checkpoint, vchannels)
	}
	return rewound, nil
}

// handleWindowRecoveryError swallows window-store corruption (a rebuildable
// cache) by resetting to an empty window set, but lets every other error abort
// recovery.
func (m *windowManager) handleWindowRecoveryError(err error, checkpoint *WALCheckpoint, vchannels map[string]*vchannelRecoveryInfo) error {
	if !errors.Is(err, ErrPChannelWindowStoreCorrupted) {
		return err
	}
	m.Logger().Warn(context.TODO(), "idempotency window store corrupted; dropping window cache and continuing WAL recovery", mlog.Err(err))
	m.resetWindowCacheAfterCorruption(checkpoint, vchannels)
	return nil
}

// resetWindowCacheAfterCorruption discards any partially-recovered window state
// and re-establishes the same empty, initialized state a fresh (empty) store
// would yield at the given consume checkpoint. The cache repopulates from WAL
// replay and subsequent writes; persistence appends fresh generations on top of
// the existing meta.
func (m *windowManager) resetWindowCacheAfterCorruption(checkpoint *WALCheckpoint, vchannels map[string]*vchannelRecoveryInfo) {
	m.resetIdempotencyWindows()
	m.setPChannelWindowSnapshotCheckpoint(checkpoint)
	m.ensureActiveIdempotencyWindows(vchannels, checkpoint)
	m.markActiveViewsInitialized()
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
	windowMetas, err := resource.Resource().StreamingNodeCatalog().ListVChannelWindowMetas(ctx, pchannel, common.VChannelWindowViewTypeIdempotency)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list idempotency window meta")
	}
	storeMetaPB, err := resource.Resource().StreamingNodeCatalog().GetPChannelWindowMeta(ctx, pchannel)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get pchannel window meta")
	}
	storeMeta := pchannelWindowStoreMetaFromCatalog(storeMetaPB)
	if storeMeta == nil {
		if !allowBootstrap || len(windowMetas) > 0 {
			return nil, errors.Errorf("pchannel window meta missing for pchannel %s", pchannel)
		}
		storeMeta, err = m.bootstrapPChannelWindowStore(ctx, pchannel, checkpoint)
		if err != nil {
			return nil, err
		}
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
		return nil, errors.Errorf("cannot bootstrap pchannel window store without source checkpoint for pchannel %s", pchannel)
	}
	chunkPayload, footer, _, err := marshalPChannelWindowChunk(pchannel, 0, sourceCheckpoint, nil)
	if err != nil {
		return nil, err
	}
	chunkKey := buildPChannelWindowChunkKey(pchannel, footer.Generation)
	logger := m.Logger().With(mlog.String("op", "bootstrapPChannelWindowStore"), mlog.Uint64("generation", footer.Generation))
	if err := retryOperationWithBackoff(ctx, logger, func(ctx context.Context) error {
		return writePChannelWindowChunkIfAbsent(ctx, chunkKey, chunkPayload)
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
	payload, err := resource.Resource().ChunkManager().Read(ctx, chunkKey)
	if err != nil {
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
		state.evictForRecovery(evictBeforeTT, m.evictionConfig.minEntries, m.evictionConfig.maxEntries)
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
