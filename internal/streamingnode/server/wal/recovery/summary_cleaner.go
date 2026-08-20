package recovery

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// dropSummaryStoreForDisabledIdempotency wipes the durable summary store while
// the feature is disabled. With idempotency off nothing is recorded, the
// consume checkpoint advances freely and the WAL is truncated past the stored
// watermark -- so the store is stale by definition and, on re-enable, would
// rewind recovery to a position that may no longer exist in the WAL. Dropping it
// makes a later re-enable bootstrap cleanly from the then-current checkpoint,
// losing the dedup state of the disabled period, which is inherent to disabling
// the feature.
//
// Deletion order is chosen for crash safety: the manifest first, so recovery
// immediately stops depending on the chunks; then the chunk objects; then the
// meta. A crash anywhere only leaks objects, and the next disabled open repeats
// the sweep.
//
// Best-effort throughout: recovery with idempotency disabled must never block on
// summary-store IO.
func (m *summaryManager) dropSummaryStoreForDisabledIdempotency(ctx context.Context, pchannel string) {
	logger := m.Logger().With(mlog.String("op", "dropSummaryStoreForDisabledIdempotency"))
	catalog := resource.Resource().StreamingNodeCatalog()
	metaPB, err := catalog.GetPChannelSummaryMeta(ctx, pchannel)
	if err != nil {
		logger.Warn(ctx, "failed to probe pchannel summary meta; a stale summary store (if any) is kept", mlog.Err(err))
		return
	}
	meta := pchannelSummaryStoreMetaFromCatalog(metaPB)
	if meta == nil {
		return
	}

	// Prefix removal rather than a walk over the recorded generations: a chunk is
	// written before the manifest records it, so a crash in between leaves an
	// object no manifest names. The enabled path reaps those when retention
	// passes them; the drop path has no later pass, so it must sweep everything.
	// This is the one place prefix deletion is used -- GC on the normal path
	// addresses objects exactly, because listing semantics differ across
	// object-storage backends.
	if err := removeAllPChannelSummaryChunks(ctx, pchannel); err != nil {
		logger.Warn(ctx, "failed to delete stale pchannel summary chunks; unreferenced objects leak", mlog.Err(err))
		return
	}
	if err := catalog.RemovePChannelSummaryMeta(ctx, pchannel); err != nil {
		logger.Warn(ctx, "failed to remove stale pchannel summary meta", mlog.Err(err))
		return
	}
	logger.Info(ctx, "dropped stale summary store while idempotency is disabled",
		mlog.String("pchannel", pchannel))
}

// runPendingGC deletes what the manifest has queued for deletion.
//
// The entire input is the manifest's pending_gc list, which the persist path
// filled in the same object write that removed those chunks from the set
// recovery reads. Three properties follow from that, and they are the reason
// this is the whole of GC:
//
//   - Clean. Every entry carries its own term, so the object key is constructed
//     exactly. No listing, no prefix scan, and no cross-check against the chunks
//     recovery reads: an entry only ever lands here in the same manifest write
//     that stopped recovery depending on it, so the decision was already made.
//   - Idempotent. Deleting an absent object succeeds. Completion is recorded in
//     memory and becomes durable at the next manifest write, so a crash at any
//     point replays a batch of no-ops rather than losing or double-applying
//     work.
//   - Efficient. One DELETE per object, no metadata round trips, no
//     cross-storage coordination, no locks. Completion rides along on a manifest
//     write the owner was going to do anyway.
func (m *summaryManager) runPendingGC(ctx context.Context, logger *mlog.Logger) error {
	pending := m.manifest.GetPendingGc()
	if len(pending) == 0 {
		return nil
	}
	cm := resource.Resource().ChunkManager()
	deleted := 0
	for _, entry := range pending {
		ref := pchannelSummaryGCRef{term: entry.GetTerm(), generation: entry.GetGeneration()}
		if _, done := m.completedGC[ref]; done {
			continue
		}
		chunkKey := buildPChannelSummaryChunkKey(cm, m.pchannel, ref.generation, ref.term)
		if err := cm.Remove(ctx, chunkKey); err != nil {
			// Leave the entry pending; the next cycle retries it. Nothing is lost by
			// stopping here: the objects are already unreferenced.
			logger.Warn(ctx, "failed to delete summary chunk; leaving it queued",
				mlog.String("chunkKey", chunkKey), mlog.Err(err))
			m.observePendingGC()
			return err
		}
		m.completedGC[ref] = struct{}{}
		deleted++
	}
	if deleted > 0 {
		logger.Info(ctx, "deleted summary chunks queued for gc", mlog.Int("objects", deleted))
	}
	m.observePendingGC()
	return nil
}

// observePendingGC publishes how much deletion is outstanding. It grows without
// bound when GC is stuck, which makes it the direct health signal.
func (m *summaryManager) observePendingGC() {
	if m.metrics == nil {
		return
	}
	m.metrics.ObserveIdempotencyPendingGC(len(m.manifest.GetPendingGc()) - len(m.completedGC))
}
