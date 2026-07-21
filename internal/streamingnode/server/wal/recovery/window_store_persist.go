package recovery

import (
	"bytes"
	"context"
	"io/fs"
	"path"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (m *windowManager) persistPChannelWindow(
	ctx context.Context,
	logger *mlog.Logger,
	recordsByVChannel map[string][]committedWriteRecord,
	windowMetaUpdates map[string]*idempotencyWindowMetaUpdate,
	sourceCheckpoint *WALCheckpoint,
) (map[string]*streamingpb.VChannelWindowMeta, uint64, error) {
	persistedChunk, err := m.persistPChannelWindowChunk(ctx, logger, recordsByVChannel, sourceCheckpoint)
	if err != nil || persistedChunk == nil {
		return nil, 0, err
	}
	windowMetas := materializeWindowMetaUpdates(windowMetaUpdates, persistedChunk.generation)
	if err := m.persistIdempotencyWindowMetas(ctx, logger, windowMetas); err != nil {
		return nil, 0, err
	}
	if err := m.persistPChannelWindowMeta(ctx, logger, persistedChunk, m.minRequiredGenerationForPChannelWindow(windowMetas, persistedChunk.generation)); err != nil {
		return nil, 0, err
	}
	return windowMetas, persistedChunk.generation, nil
}

func (m *windowManager) persistPChannelWindowChunk(
	ctx context.Context,
	logger *mlog.Logger,
	recordsByVChannel map[string][]committedWriteRecord,
	sourceCheckpoint *WALCheckpoint,
) (*persistedPChannelWindowChunk, error) {
	if sourceCheckpoint == nil {
		return nil, nil
	}

	// Wrap the meta load in retry like every other persist sub-operation below, so
	// a transient catalog error (e.g. an etcd blip) is retried until the context is
	// cancelled rather than propagated. The window background task treats any
	// persist error as fatal-and-exit on the assumption that errors only mean
	// shutdown; an unwrapped error here would break that assumption and silently
	// kill idempotency durability (windows then grow until OOM).
	var metaPB *streamingpb.PChannelWindowMeta
	if err := retryOperationWithBackoff(ctx,
		logger.With(mlog.String("op", "getPChannelWindowMeta")),
		func(ctx context.Context) error {
			var err error
			metaPB, err = resource.Resource().StreamingNodeCatalog().GetPChannelWindowMeta(ctx, m.pchannel)
			return err
		}); err != nil {
		return nil, err
	}
	meta := pchannelWindowStoreMetaFromCatalog(metaPB)
	nextGeneration := uint64(0)
	minAvailableGeneration := uint64(0)
	if meta != nil {
		if checkpointCovers(meta.SourceCheckpoint, sourceCheckpoint) {
			return &persistedPChannelWindowChunk{
				footer: &pchannelWindowChunkFooter{
					CodecVersion:              pchannelWindowCodecVersion,
					PChannel:                  m.pchannel,
					Generation:                meta.LatestGeneration,
					SourceCheckpointMessageID: cloneMessageIDProto(metaPB.GetSourceCheckpointMessageId()),
					SourceCheckpointTimetick:  metaPB.GetSourceCheckpointTimetick(),
				},
				generation:             meta.LatestGeneration,
				minAvailableGeneration: meta.MinAvailableGeneration,
			}, nil
		}
		nextGeneration = meta.LatestGeneration + 1
		minAvailableGeneration = meta.MinAvailableGeneration
	}

	chunkPayload, footer, _, err := marshalPChannelWindowChunk(m.pchannel, nextGeneration, sourceCheckpoint, recordsByVChannel)
	if err != nil {
		return nil, err
	}
	chunkKey := buildPChannelWindowChunkKey(m.pchannel, nextGeneration)
	if err := retryOperationWithBackoff(ctx,
		logger.With(mlog.String("op", "persistPChannelWindowChunk"), mlog.Uint64("generation", nextGeneration)),
		func(ctx context.Context) error {
			return writePChannelWindowChunkIfAbsent(ctx, chunkKey, chunkPayload)
		}); err != nil {
		return nil, err
	}

	return &persistedPChannelWindowChunk{
		footer:                 footer,
		generation:             nextGeneration,
		minAvailableGeneration: minAvailableGeneration,
	}, nil
}

func pchannelWindowStoreMetaFromCatalog(meta *streamingpb.PChannelWindowMeta) *pchannelWindowStoreMeta {
	if meta == nil {
		return nil
	}
	return &pchannelWindowStoreMeta{
		PChannel:               meta.GetPchannel(),
		LatestGeneration:       meta.GetLatestGeneration(),
		MinAvailableGeneration: meta.GetMinAvailableGeneration(),
		MinInUseGeneration:     meta.GetMinInUseGeneration(),
		SourceCheckpoint: pchannelWindowSourceCheckpointToWALCheckpoint(&pchannelWindowSourceCheckpoint{
			MessageID: cloneMessageIDProto(meta.GetSourceCheckpointMessageId()),
			TimeTick:  meta.GetSourceCheckpointTimetick(),
		}),
	}
}

func (meta *pchannelWindowStoreMeta) intoCatalogMeta() *streamingpb.PChannelWindowMeta {
	catalogMeta := &streamingpb.PChannelWindowMeta{
		Pchannel:                 meta.PChannel,
		SourceCheckpointTimetick: 0,
		LatestGeneration:         meta.LatestGeneration,
		MinAvailableGeneration:   meta.MinAvailableGeneration,
		MinInUseGeneration:       meta.MinInUseGeneration,
		CodecVersion:             uint32(pchannelWindowCodecVersion),
	}
	if meta.SourceCheckpoint != nil {
		catalogMeta.SourceCheckpointTimetick = meta.SourceCheckpoint.TimeTick
		if meta.SourceCheckpoint.MessageID != nil {
			catalogMeta.SourceCheckpointMessageId = meta.SourceCheckpoint.MessageID.IntoProto()
		}
	}
	return catalogMeta
}

func (m *windowManager) persistPChannelWindowMeta(
	ctx context.Context,
	logger *mlog.Logger,
	persistedChunk *persistedPChannelWindowChunk,
	minInUseGeneration uint64,
) error {
	if persistedChunk == nil {
		return nil
	}
	storeMeta := newPChannelWindowStoreMetaFromChunk(
		m.pchannel,
		persistedChunk.footer,
		persistedChunk.minAvailableGeneration,
		minInUseGeneration,
	)
	return retryOperationWithBackoff(ctx,
		logger.With(mlog.String("op", "persistPChannelWindowMeta")),
		func(ctx context.Context) error {
			return resource.Resource().StreamingNodeCatalog().SavePChannelWindowMeta(ctx, m.pchannel, storeMeta.intoCatalogMeta())
		})
}

func (m *windowManager) persistIdempotencyWindowMetas(ctx context.Context, logger *mlog.Logger, metas map[string]*streamingpb.VChannelWindowMeta) error {
	if len(metas) == 0 {
		return nil
	}
	return retryOperationWithBackoff(ctx,
		logger.With(mlog.String("op", "persistIdempotencyWindowMetas")),
		func(ctx context.Context) error {
			return resource.Resource().StreamingNodeCatalog().SaveVChannelWindowMetas(ctx, m.pchannel, common.VChannelWindowViewTypeIdempotency, metas)
		})
}

func writePChannelWindowChunkIfAbsent(ctx context.Context, chunkKey string, payload []byte) error {
	chunkManager := resource.Resource().ChunkManager()
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
	if !bytes.Equal(existingPayload, payload) {
		return pchannelWindowStoreCorruptedf("pchannel window chunk already exists with different payload: %s", chunkKey)
	}
	return nil
}

func buildPChannelWindowChunkKey(pchannel string, generation uint64) string {
	return buildPChannelWindowChunkPrefix(pchannel) +
		pchannelWindowChunkObjectPrefix + strconv.FormatUint(generation, 10) + pchannelWindowChunkObjectExt
}

// buildPChannelWindowChunkPrefix returns the object prefix holding every chunk of
// the pchannel's window store. The trailing separator keeps the prefix from
// matching a sibling whose name merely starts with "chunks".
func buildPChannelWindowChunkPrefix(pchannel string) string {
	root := paramtable.Get().MinioCfg.RootPath.GetValue()
	return path.Join(
		root,
		"streamingnode",
		"window-store",
		sanitizeWindowStorePathPart(pchannel),
		"chunks",
	) + "/"
}

// removeAllPChannelWindowChunks deletes every chunk object of the pchannel's
// window store. It is only correct where no catalog meta references a chunk any
// more: dropping the store while idempotency is disabled, and bootstrapping a
// store whose catalog meta is missing. There a leftover chunk is pure garbage
// that must not survive, because the store writes those generations again and
// writePChannelWindowChunkIfAbsent rejects a same-key-different-payload chunk
// with an unretriable corruption error that retryOperationWithBackoff then
// retries forever.
//
// A prefix removal (rather than a walk over [MinAvailableGeneration,
// LatestGeneration]) is what makes that guarantee hold: it also reaps orphans
// above LatestGeneration left by a persist that wrote the chunk but crashed
// before saving the meta, and any chunk left behind by an earlier partial
// removal.
func removeAllPChannelWindowChunks(ctx context.Context, pchannel string) error {
	prefix := buildPChannelWindowChunkPrefix(pchannel)
	// A store that was never written has no chunk directory at all: object
	// storage lists nothing, local storage reports the missing directory.
	if err := resource.Resource().ChunkManager().RemoveWithPrefix(ctx, prefix); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return errors.Wrapf(err, "failed to remove pchannel window chunks with prefix %s", prefix)
	}
	return nil
}

func sanitizeWindowStorePathPart(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	return replacer.Replace(value)
}

func (m *windowManager) markVChannelWindowsPersisted(recordsByVChannel map[string][]committedWriteRecord, metas map[string]*streamingpb.VChannelWindowMeta, generation uint64, sourceCheckpoint *WALCheckpoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.markIdempotencyWindowsPersisted(recordsByVChannel, metas, generation)
	m.markPChannelWindowSnapshotCheckpointPersisted(sourceCheckpoint)
	m.evictPersistedEntries()
}

func newPChannelWindowStoreMetaFromChunk(
	pchannel string,
	footer *pchannelWindowChunkFooter,
	minAvailableGeneration uint64,
	minInUseGeneration uint64,
) *pchannelWindowStoreMeta {
	return &pchannelWindowStoreMeta{
		PChannel:               pchannel,
		LatestGeneration:       footer.Generation,
		MinAvailableGeneration: minAvailableGeneration,
		MinInUseGeneration:     minInUseGeneration,
		SourceCheckpoint: pchannelWindowSourceCheckpointToWALCheckpoint(&pchannelWindowSourceCheckpoint{
			MessageID: cloneMessageIDProto(footer.SourceCheckpointMessageID),
			TimeTick:  footer.SourceCheckpointTimetick,
		}),
	}
}

func materializeWindowMetaUpdates(updates map[string]*idempotencyWindowMetaUpdate, generation uint64) map[string]*streamingpb.VChannelWindowMeta {
	if len(updates) == 0 {
		return nil
	}
	metas := make(map[string]*streamingpb.VChannelWindowMeta, len(updates))
	for vchannel, update := range updates {
		meta := update.WithPersistedGeneration(generation)
		if meta == nil {
			continue
		}
		if meta.GetVchannel() == "" {
			meta.Vchannel = vchannel
		}
		metas[meta.GetVchannel()] = meta
	}
	return metas
}

func (m *windowManager) minRequiredGenerationForPChannelWindow(windowMetas map[string]*streamingpb.VChannelWindowMeta, persistedGeneration uint64) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	minRequiredGeneration, hasActiveViewMinBoundary := m.minRequiredGeneration(windowMetas, persistedGeneration)
	if !hasActiveViewMinBoundary {
		return persistedGeneration
	}
	return minRequiredGeneration
}

func pchannelWindowSourceCheckpointToWALCheckpoint(checkpoint *pchannelWindowSourceCheckpoint) *WALCheckpoint {
	if checkpoint == nil || checkpoint.MessageID == nil {
		return nil
	}
	return utility.NewWALCheckpointFromProto(&streamingpb.WALCheckpoint{
		MessageId: cloneMessageIDProto(checkpoint.MessageID),
		TimeTick:  checkpoint.TimeTick,
	})
}

func hasIdempotencyCommittedWriteRecords(records []committedWriteRecord) bool {
	for _, record := range records {
		if record.Idempotency != nil {
			return true
		}
	}
	return false
}

// rewindCheckpointForPChannelWindowReplay returns the consume checkpoint to
// resume from, rewound to the earliest of the current checkpoint, the window
// source checkpoint, and the recovered flusher checkpoint so the window cache and
// flushed data can be rebuilt on restart. It does not mutate recoveryStorageImpl;
// the caller applies the returned checkpoint.
func (m *windowManager) rewindCheckpointForPChannelWindowReplay(sourceCheckpoint, checkpoint *WALCheckpoint, vchannels map[string]*vchannelRecoveryInfo) *WALCheckpoint {
	replayStart := minCheckpointByMessageID(checkpoint, sourceCheckpoint)
	if flusherCheckpoint := m.getRecoveredVChannelFlusherCheckpoint(vchannels); flusherCheckpoint != nil {
		replayStart = minCheckpointByMessageID(replayStart, flusherCheckpoint)
	}
	if replayStart == nil || sameWALCheckpoint(checkpoint, replayStart) {
		return checkpoint
	}
	m.Logger().Info(context.TODO(), "rewind recovery checkpoint for pchannel window replay",
		mlog.String("fromMessageID", checkpointMessageIDString(checkpoint)),
		mlog.Uint64("fromTimeTick", checkpointTimeTick(checkpoint)),
		mlog.String("toMessageID", checkpointMessageIDString(replayStart)),
		mlog.Uint64("toTimeTick", replayStart.TimeTick),
	)
	return replayStart.Clone()
}

func (m *windowManager) getRecoveredVChannelFlusherCheckpoint(vchannels map[string]*vchannelRecoveryInfo) *WALCheckpoint {
	if len(vchannels) == 0 {
		return nil
	}
	var minimum *WALCheckpoint
	for _, vchannel := range vchannels {
		if !vchannel.IsActive() {
			continue
		}
		checkpoint := vchannel.GetFlushCheckpoint()
		if checkpoint == nil {
			return nil
		}
		minimum = minCheckpointByMessageID(minimum, checkpoint)
	}
	return minimum
}
