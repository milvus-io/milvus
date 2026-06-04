package recovery

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

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
	if meta.MinAvailableGeneration > meta.LatestGeneration {
		return pchannelWindowStoreCorruptedf("pchannel window generation range mismatch, min available %d, latest %d", meta.MinAvailableGeneration, meta.LatestGeneration)
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

	// Reclaim [MinAvailableGeneration, MinInUseGeneration): these chunks sit below
	// the in-use boundary and are never read on recovery (which replays from
	// MinInUseGeneration upward), so they can be deleted. [MinInUseGeneration,
	// LatestGeneration] is still in use. Delete BEFORE persisting the advanced
	// MinAvailableGeneration so the low-water never runs ahead of the actual
	// deletions: if we crash in between, the meta still points at the old
	// MinAvailableGeneration and the next cycle re-deletes the same range
	// (idempotent via the Exist check). This also bounds each cycle's work to the
	// newly reclaimable range instead of re-probing every generation from 0.
	if err := m.deletePChannelWindowChunks(ctx, logger, meta.MinAvailableGeneration, updatedMeta.MinAvailableGeneration, updatedMeta.LatestGeneration); err != nil {
		return err
	}

	if updatedMeta.MinAvailableGeneration == meta.MinAvailableGeneration &&
		updatedMeta.MinInUseGeneration == meta.MinInUseGeneration {
		return nil
	}
	return retryOperationWithBackoff(ctx,
		logger.With(
			mlog.String("op", "cleanPChannelWindowMeta"),
			mlog.Uint64("oldMinAvailableGeneration", meta.MinAvailableGeneration),
			mlog.Uint64("newMinAvailableGeneration", updatedMeta.MinAvailableGeneration),
			mlog.Uint64("minInUseGeneration", updatedMeta.MinInUseGeneration),
			mlog.Bool("hasActiveViewMinBoundary", boundary.hasActiveViewMinBoundary),
		),
		func(ctx context.Context) error {
			return resource.Resource().StreamingNodeCatalog().SavePChannelWindowMeta(ctx, m.pchannel, updatedMeta.intoCatalogMeta())
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

func (m *windowManager) deletePChannelWindowChunks(ctx context.Context, logger *mlog.Logger, startGeneration uint64, endGeneration uint64, latestGeneration uint64) error {
	if startGeneration >= endGeneration {
		return nil
	}
	chunkManager := resource.Resource().ChunkManager()
	for generation := startGeneration; generation < endGeneration; generation++ {
		if generation >= latestGeneration {
			break
		}
		chunkKey := buildPChannelWindowChunkKey(m.pchannel, generation)
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
