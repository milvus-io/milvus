package recovery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// dropWindowStoreForDisabledIdempotency wipes the durable idempotency window
// store while the feature is disabled. With idempotency off, nothing is
// recorded into the store, the consume checkpoint advances freely and the WAL
// gets truncated past the stored SourceCheckpoint — so the persisted store is
// stale by definition and, worse, on re-enable its SourceCheckpoint would
// rewind recovery to a position that may no longer exist in the WAL. Dropping
// it makes a later re-enable bootstrap cleanly from the then-current
// checkpoint (losing window dedup state across the disabled period, which is
// inherent to disabling the feature).
//
// Deletion order is chosen for crash safety of the recover path: vchannel
// window metas first (recovery treats "vchannel metas without pchannel meta"
// as an error, so the pchannel meta must outlive them), then the pchannel
// meta, then the chunk objects. A crash before the meta removal is retried on
// the next disabled open; a crash after it only leaks unreferenced chunk
// objects, because bootstrap must not prefix-delete chunks based on a stale
// no-meta read. Best-effort: failures only log —
// recovery with idempotency disabled must never block on window-store IO — and
// the next open retries.
func (m *windowManager) dropWindowStoreForDisabledIdempotency(ctx context.Context, pchannel string) {
	logger := m.Logger().With(mlog.String("op", "dropWindowStoreForDisabledIdempotency"))
	catalog := resource.Resource().StreamingNodeCatalog()
	metaPB, err := catalog.GetPChannelWindowMeta(ctx, pchannel)
	if err != nil {
		logger.Warn(ctx, "failed to probe pchannel window meta; a stale idempotency window store (if any) is kept", mlog.Err(err))
		return
	}
	windowMetas, err := catalog.ListVChannelWindowMetas(ctx, pchannel, common.VChannelWindowViewTypeIdempotency)
	if err != nil {
		logger.Warn(ctx, "failed to list vchannel window metas; a stale idempotency window store (if any) is kept", mlog.Err(err))
		return
	}
	meta := pchannelWindowStoreMetaFromCatalog(metaPB)
	if meta == nil && len(windowMetas) == 0 {
		return
	}

	if len(windowMetas) > 0 {
		vchannels := make([]string, 0, len(windowMetas))
		for _, windowMeta := range windowMetas {
			vchannels = append(vchannels, windowMeta.GetVchannel())
		}
		if err := catalog.RemoveVChannelWindowMetas(ctx, pchannel, common.VChannelWindowViewTypeIdempotency, vchannels); err != nil {
			logger.Warn(ctx, "failed to remove stale vchannel window metas", mlog.Err(err))
			return
		}
	}
	if meta != nil {
		if err := catalog.RemovePChannelWindowMeta(ctx, pchannel); err != nil {
			logger.Warn(ctx, "failed to remove stale pchannel window meta", mlog.Err(err))
			return
		}
	}
	// Remove every chunk of the store, not just [MinAvailableGeneration,
	// LatestGeneration]: a persist writes the chunk before the meta, so a crash in
	// between leaves an orphan at LatestGeneration+1 that a generation-range walk
	// would keep. The enabled recover path reaps such orphans itself; the drop
	// path must too, otherwise the leftover chunk survives into a later re-enable
	// and permanently fails the write-if-absent of that generation.
	if err := removeAllPChannelWindowChunks(ctx, pchannel); err != nil {
		logger.Warn(ctx, "failed to delete stale pchannel window chunks; unreferenced chunk objects leak", mlog.Err(err))
		return
	}
	logger.Info(ctx, "dropped stale idempotency window store while idempotency is disabled",
		mlog.String("pchannel", pchannel),
		mlog.Int("vchannelWindowMetas", len(windowMetas)),
		mlog.Bool("hasPChannelWindowMeta", meta != nil),
	)
}

type pchannelWindowCleanBoundary struct {
	canClean                 bool
	hasActiveViewMinBoundary bool
	minInUseGeneration       uint64
}

func (m *windowManager) cleanPChannelWindow(ctx context.Context, logger *mlog.Logger) error {
	if logger == nil {
		logger = m.Logger()
	}
	if !m.canCleanPChannelWindow() {
		return nil
	}

	metaPB, err := resource.Resource().StreamingNodeCatalog().GetPChannelWindowMeta(ctx, m.pchannel)
	if err != nil {
		return err
	}
	meta := pchannelWindowStoreMetaFromCatalog(metaPB)
	if meta == nil {
		return nil
	}
	if meta.Term > m.term {
		// Another owner with a newer assignment term took over the store; this
		// stale cleaner must neither delete its chunks nor rewrite its meta.
		return pchannelWindowStoreFencedf("pchannel window store of %s already owned by term %d, cleaner term %d stops", m.pchannel, meta.Term, m.term)
	}
	if meta.MinAvailableGeneration > meta.LatestGeneration {
		return pchannelWindowStoreCorruptedf("pchannel window generation range mismatch, min available %d, latest %d", meta.MinAvailableGeneration, meta.LatestGeneration)
	}
	if err := validatePChannelWindowManifest(meta); err != nil {
		return err
	}

	boundary := m.pchannelWindowCleanBoundary(meta.LatestGeneration)
	if !boundary.canClean {
		return nil
	}
	targetMinInUse := boundary.minInUseGeneration
	if !boundary.hasActiveViewMinBoundary {
		targetMinInUse = meta.LatestGeneration
	}
	targetMinAvailable := targetMinInUse
	if targetMinAvailable > meta.LatestGeneration {
		targetMinAvailable = meta.LatestGeneration
	}
	if targetMinAvailable < meta.MinAvailableGeneration {
		targetMinAvailable = meta.MinAvailableGeneration
	}
	updatedMeta := *meta
	updatedMeta.MinInUseGeneration = targetMinInUse
	updatedMeta.MinAvailableGeneration = targetMinAvailable
	updatedMeta.Term = m.term

	if updatedMeta.MinAvailableGeneration == meta.MinAvailableGeneration &&
		updatedMeta.MinInUseGeneration == meta.MinInUseGeneration {
		return nil
	}

	// Persist the advanced MinInUseGeneration BEFORE deleting anything, keeping
	// MinAvailableGeneration at its old value for now. Recovery replays chunks
	// from the persisted MinInUseGeneration upward and hard-fails on a missing
	// chunk, so deletion must never run ahead of that durable boundary: an
	// interruption between a delete and the meta save — a crash, or this
	// background task's context being canceled on WAL close — would otherwise
	// leave the persisted MinInUseGeneration pointing into the deleted range and
	// permanently fail every subsequent WAL open. With the advance durable
	// first, an interruption only leaks chunks below the new boundary, and the
	// next cycle re-deletes them idempotently (Exist check in the delete loop).
	if updatedMeta.MinInUseGeneration != meta.MinInUseGeneration {
		intermediateMeta := updatedMeta
		intermediateMeta.MinAvailableGeneration = meta.MinAvailableGeneration
		if err := retryOperationWithBackoff(ctx,
			logger.With(
				mlog.String("op", "advancePChannelWindowMinInUseGeneration"),
				mlog.Uint64("minInUseGeneration", intermediateMeta.MinInUseGeneration),
				mlog.Bool("hasActiveViewMinBoundary", boundary.hasActiveViewMinBoundary),
			),
			func(ctx context.Context) error {
				return m.repairPChannelWindowMeta(ctx, &intermediateMeta)
			}); err != nil {
			return err
		}
	}

	// Reclaim [MinAvailableGeneration, MinInUseGeneration): these chunks sit
	// below the durably-persisted in-use boundary and are never read on recovery.
	// [MinInUseGeneration, LatestGeneration] is still in use. Starting from the
	// old MinAvailableGeneration also bounds each cycle's work to the newly
	// reclaimable range instead of re-probing every generation from 0.
	if err := m.deletePChannelWindowChunks(ctx, logger, meta, meta.MinAvailableGeneration, updatedMeta.MinAvailableGeneration, updatedMeta.LatestGeneration); err != nil {
		return err
	}

	if updatedMeta.MinAvailableGeneration == meta.MinAvailableGeneration {
		return nil
	}
	// Only after the deletions succeeded may the low-water advance: everything
	// below the persisted MinAvailableGeneration is promised to be gone.
	return retryOperationWithBackoff(ctx,
		logger.With(
			mlog.String("op", "cleanPChannelWindowMeta"),
			mlog.Uint64("oldMinAvailableGeneration", meta.MinAvailableGeneration),
			mlog.Uint64("newMinAvailableGeneration", updatedMeta.MinAvailableGeneration),
			mlog.Uint64("minInUseGeneration", updatedMeta.MinInUseGeneration),
			mlog.Bool("hasActiveViewMinBoundary", boundary.hasActiveViewMinBoundary),
		),
		func(ctx context.Context) error {
			return m.repairPChannelWindowMeta(ctx, &updatedMeta)
		})
}

func (m *windowManager) canCleanPChannelWindow() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeViewsInitialized
}

func (m *windowManager) pchannelWindowCleanBoundary(latestGeneration uint64) pchannelWindowCleanBoundary {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.activeViewsInitialized {
		return pchannelWindowCleanBoundary{}
	}
	minInUseGeneration, hasActiveViewMinBoundary := m.minRequiredGeneration(nil, latestGeneration)
	return pchannelWindowCleanBoundary{
		canClean:                 true,
		hasActiveViewMinBoundary: hasActiveViewMinBoundary,
		minInUseGeneration:       minInUseGeneration,
	}
}

func (m *windowManager) deletePChannelWindowChunks(ctx context.Context, logger *mlog.Logger, meta *pchannelWindowStoreMeta, startGeneration uint64, endGeneration uint64, latestGeneration uint64) error {
	if startGeneration >= endGeneration {
		return nil
	}
	chunkManager := resource.Resource().ChunkManager()
	for generation := startGeneration; generation < endGeneration; generation++ {
		if generation >= latestGeneration {
			break
		}
		termRange, ok := pchannelWindowManifestRangeForGeneration(meta, generation)
		if !ok {
			return pchannelWindowStoreCorruptedf("pchannel window chunk manifest misses generation %d before clean", generation)
		}
		chunkKey := buildPChannelWindowChunkKey(m.pchannel, generation, termRange.GetTerm())
		exists, err := chunkManager.Exist(ctx, chunkKey)
		if err != nil {
			return errors.Wrapf(err, "failed to check pchannel window chunk %s before clean", chunkKey)
		}
		if !exists {
			continue
		}
		if err := chunkManager.Remove(ctx, chunkKey); err != nil {
			return errors.Wrapf(err, "failed to remove pchannel window chunk %s", chunkKey)
		}
		logger.Debug(ctx, "clean pchannel window chunk",
			mlog.Uint64("generation", generation),
			mlog.String("chunkKey", chunkKey),
		)
	}
	return nil
}
