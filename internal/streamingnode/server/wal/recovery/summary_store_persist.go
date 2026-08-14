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

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func (m *summaryManager) persistPChannelSummary(
	ctx context.Context,
	logger *mlog.Logger,
	recordsByVChannel map[string][]*streamingpb.SummaryEntry,
	summaryMetaUpdates map[string]*summaryMetaUpdate,
	sourceCheckpoint *WALCheckpoint,
) (map[string]*streamingpb.VChannelSummaryMeta, uint64, error) {
	persistedChunk, err := m.persistPChannelSummaryChunk(ctx, logger, recordsByVChannel, sourceCheckpoint)
	if err != nil || persistedChunk == nil {
		return nil, 0, err
	}
	summaryMetas := materializeSummaryMetaUpdates(summaryMetaUpdates, persistedChunk.generation)
	if err := m.persistPChannelSummaryMeta(ctx, logger, persistedChunk, m.minRequiredGenerationForPChannelSummary(summaryMetas, persistedChunk.generation)); err != nil {
		return nil, 0, err
	}
	if err := m.persistSummaryMetas(ctx, logger, summaryMetas); err != nil {
		return nil, 0, err
	}
	return summaryMetas, persistedChunk.generation, nil
}

func (m *summaryManager) persistPChannelSummaryChunk(
	ctx context.Context,
	logger *mlog.Logger,
	recordsByVChannel map[string][]*streamingpb.SummaryEntry,
	sourceCheckpoint *WALCheckpoint,
) (*persistedPChannelSummaryChunk, error) {
	if sourceCheckpoint == nil {
		return nil, nil
	}

	// Wrap the meta load in retry like every other persist sub-operation below, so
	// a transient catalog error (e.g. an etcd blip) is retried until the context is
	// canceled rather than propagated. The summary background task treats any
	// persist error as fatal-and-exit on the assumption that errors only mean
	// shutdown; an unwrapped error here would break that assumption and silently
	// kill idempotency durability (summaries then grow until OOM).
	var metaPB *streamingpb.PChannelSummaryMeta
	if err := retryOperationWithBackoff(ctx,
		logger.With(mlog.String("op", "getPChannelSummaryMeta")),
		func(ctx context.Context) error {
			var err error
			metaPB, err = resource.Resource().StreamingNodeCatalog().GetPChannelSummaryMeta(ctx, m.pchannel)
			return err
		}); err != nil {
		return nil, err
	}
	meta := pchannelSummaryStoreMetaFromCatalog(metaPB)
	nextGeneration := uint64(0)
	minAvailableGeneration := uint64(0)
	if meta != nil {
		if checkpointCovers(meta.SourceCheckpoint, sourceCheckpoint) {
			return &persistedPChannelSummaryChunk{
				footer: &streamingpb.PChannelSummaryChunkFooter{
					Pchannel:                  m.pchannel,
					Generation:                meta.LatestGeneration,
					SourceCheckpointMessageId: cloneMessageIDProto(metaPB.GetSourceCheckpointMessageId()),
					SourceCheckpointTimetick:  metaPB.GetSourceCheckpointTimetick(),
				},
				generation:             meta.LatestGeneration,
				minAvailableGeneration: meta.MinAvailableGeneration,
			}, nil
		}
		nextGeneration = meta.LatestGeneration + 1
		minAvailableGeneration = meta.MinAvailableGeneration
	}

	chunkPayload, footer, _, err := marshalPChannelSummaryChunk(m.pchannel, nextGeneration, m.term, sourceCheckpoint, recordsByVChannel)
	if err != nil {
		return nil, err
	}
	chunkKey := buildPChannelSummaryChunkKey(m.pchannel, nextGeneration, m.term)
	if err := retryOperationWithBackoff(ctx,
		logger.With(mlog.String("op", "persistPChannelSummaryChunk"), mlog.Uint64("generation", nextGeneration)),
		func(ctx context.Context) error {
			return writePChannelSummaryChunkIfAbsent(ctx, chunkKey, chunkPayload, footer, m.term)
		}); err != nil {
		return nil, err
	}

	return &persistedPChannelSummaryChunk{
		footer:                 footer,
		generation:             nextGeneration,
		minAvailableGeneration: minAvailableGeneration,
	}, nil
}

func pchannelSummaryStoreMetaFromCatalog(meta *streamingpb.PChannelSummaryMeta) *pchannelSummaryStoreMeta {
	if meta == nil {
		return nil
	}
	return &pchannelSummaryStoreMeta{
		PChannel:               meta.GetPchannel(),
		LatestGeneration:       meta.GetLatestGeneration(),
		MinAvailableGeneration: meta.GetMinAvailableGeneration(),
		MinInUseGeneration:     meta.GetMinInUseGeneration(),
		Term:                   meta.GetTerm(),
		ChunkManifest:          pchannelSummaryChunkManifestFromCatalog(meta),
		SourceCheckpoint: pchannelSummarySourceCheckpointToWALCheckpoint(&pchannelSummarySourceCheckpoint{
			MessageID: cloneMessageIDProto(meta.GetSourceCheckpointMessageId()),
			TimeTick:  meta.GetSourceCheckpointTimetick(),
		}),
	}
}

func (meta *pchannelSummaryStoreMeta) intoCatalogMeta() *streamingpb.PChannelSummaryMeta {
	catalogMeta := &streamingpb.PChannelSummaryMeta{
		Pchannel:                 meta.PChannel,
		SourceCheckpointTimetick: 0,
		LatestGeneration:         meta.LatestGeneration,
		MinAvailableGeneration:   meta.MinAvailableGeneration,
		MinInUseGeneration:       meta.MinInUseGeneration,
		Term:                     meta.Term,
		ChunkManifest:            clonePChannelSummaryChunkManifest(meta.ChunkManifest),
	}
	if meta.SourceCheckpoint != nil {
		catalogMeta.SourceCheckpointTimetick = meta.SourceCheckpoint.TimeTick
		if meta.SourceCheckpoint.MessageID != nil {
			catalogMeta.SourceCheckpointMessageId = meta.SourceCheckpoint.MessageID.IntoProto()
		}
	}
	return catalogMeta
}

func (m *summaryManager) persistPChannelSummaryMeta(
	ctx context.Context,
	logger *mlog.Logger,
	persistedChunk *persistedPChannelSummaryChunk,
	minInUseGeneration uint64,
) error {
	if persistedChunk == nil {
		return nil
	}
	return updatePChannelSummaryMetaWithCAS(ctx,
		logger.With(mlog.String("op", "persistPChannelSummaryMeta")),
		m.pchannel,
		func(currentPB *streamingpb.PChannelSummaryMeta, current *pchannelSummaryStoreMeta) (*streamingpb.PChannelSummaryMeta, error) {
			if current != nil {
				if current.Term > m.term {
					return nil, pchannelSummaryStoreFencedf("pchannel summary meta of %s already owned by term %d, own term %d", m.pchannel, current.Term, m.term)
				}
				if current.LatestGeneration >= persistedChunk.generation {
					if current.Term == m.term && checkpointCovers(current.SourceCheckpoint, pchannelSummarySourceCheckpointToWALCheckpoint(&pchannelSummarySourceCheckpoint{
						MessageID: cloneMessageIDProto(persistedChunk.footer.SourceCheckpointMessageId),
						TimeTick:  persistedChunk.footer.SourceCheckpointTimetick,
					})) {
						return nil, nil
					}
					return nil, merr.WrapErrServiceInternalMsg("pchannel summary meta advanced while persisting generation %d", persistedChunk.generation)
				}
				if current.LatestGeneration+1 != persistedChunk.generation {
					return nil, merr.WrapErrServiceInternalMsg("pchannel summary meta latest generation %d does not precede persisted generation %d", current.LatestGeneration, persistedChunk.generation)
				}
			}

			minAvailableGeneration := persistedChunk.minAvailableGeneration
			manifest := (*streamingpb.PChannelSummaryChunkManifest)(nil)
			if current != nil {
				minAvailableGeneration = current.MinAvailableGeneration
				manifest = current.ChunkManifest
			}
			manifest, err := pchannelSummaryManifestWithChunk(manifest, m.term, persistedChunk.footer.Generation, persistedChunk.footer.SourceCheckpointTimetick)
			if err != nil {
				return nil, err
			}
			storeMeta := newPChannelSummaryStoreMetaFromChunk(
				m.pchannel,
				persistedChunk.footer,
				minAvailableGeneration,
				minInUseGeneration,
			)
			storeMeta.Term = m.term
			storeMeta.ChunkManifest = manifest
			return storeMeta.intoCatalogMeta(), nil
		})
}

func (m *summaryManager) persistSummaryMetas(ctx context.Context, logger *mlog.Logger, metas map[string]*streamingpb.VChannelSummaryMeta) error {
	if len(metas) == 0 {
		return nil
	}
	return retryOperationWithBackoff(ctx,
		logger.With(mlog.String("op", "persistSummaryMetas")),
		func(ctx context.Context) error {
			return resource.Resource().StreamingNodeCatalog().SaveVChannelSummaryMetas(ctx, m.pchannel, common.VChannelSummaryViewTypeIdempotency, metas)
		})
}

func writePChannelSummaryChunkIfAbsent(ctx context.Context, chunkKey string, payload []byte, footer *streamingpb.PChannelSummaryChunkFooter, term int64) error {
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
	if _, existingFooter, _, decodeErr := unmarshalPChannelSummaryChunk(existingPayload); decodeErr == nil {
		if existingFooter.Term > term {
			return pchannelSummaryStoreFencedf("pchannel summary chunk %s already written by term %d, own term %d", chunkKey, existingFooter.Term, term)
		}
		if existingFooter.Term < term {
			return chunkManager.Write(ctx, chunkKey, payload)
		}
		// Same term: this is our own chunk. Byte inequality alone does not prove a
		// conflict, because the payload encoding is not guaranteed to be
		// byte-stable across proto library versions — a retry that spans a binary
		// upgrade can re-encode the same records differently. Compare what the
		// chunk actually contains instead, so an identical rewrite stays
		// idempotent and only genuinely different content is corruption.
		if pchannelSummaryChunkFooterSameContent(existingFooter, footer) {
			return nil
		}
	}
	return pchannelSummaryStoreCorruptedf("pchannel summary chunk already exists with different payload: %s", chunkKey)
}

// pchannelSummaryChunkFooterSameContent reports whether two footers describe the
// same chunk contents. It must cover everything a chunk durably carries, not
// just its records: a checkpoint-only generation has no vchannel chunks at all,
// and its source checkpoint is the whole of its content. The per-vchannel
// checksums cover the exact stored payload bytes of each vchannel chunk, so an
// equal (vchannel, record count, checksum) list means identical records even
// when the surrounding encodings differ.
func pchannelSummaryChunkFooterSameContent(left, right *streamingpb.PChannelSummaryChunkFooter) bool {
	if left == nil || right == nil {
		return false
	}
	if left.Generation != right.Generation || left.Term != right.Term {
		return false
	}
	if left.SourceCheckpointTimetick != right.SourceCheckpointTimetick {
		return false
	}
	if !proto.Equal(left.SourceCheckpointMessageId, right.SourceCheckpointMessageId) {
		return false
	}
	if len(left.Chunks) != len(right.Chunks) {
		return false
	}
	for i := range left.Chunks {
		l, r := left.Chunks[i], right.Chunks[i]
		if !bytes.Equal(l.Checksum, r.Checksum) {
			return false
		}
		if l.Vchannel != r.Vchannel || l.RecordCount != r.RecordCount {
			return false
		}
	}
	return true
}

func buildPChannelSummaryChunkKey(pchannel string, generation uint64, term ...int64) string {
	chunkName := pchannelSummaryChunkObjectPrefix + strconv.FormatUint(generation, 10)
	if len(term) > 0 {
		chunkName += ".term" + strconv.FormatInt(term[0], 10)
	}
	return buildPChannelSummaryChunkPrefix(pchannel) +
		chunkName + pchannelSummaryChunkObjectExt
}

// buildPChannelSummaryChunkPrefix returns the object prefix holding every chunk of
// the pchannel's summary store. The trailing separator keeps the prefix from
// matching a sibling whose name merely starts with "chunks".
func buildPChannelSummaryChunkPrefix(pchannel string) string {
	root := paramtable.Get().MinioCfg.RootPath.GetValue()
	return path.Join(
		root,
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
	prefix := buildPChannelSummaryChunkPrefix(pchannel)
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

func (m *summaryManager) markVChannelSummariesPersisted(recordsByVChannel map[string][]*streamingpb.SummaryEntry, metas map[string]*streamingpb.VChannelSummaryMeta, generation uint64, sourceCheckpoint *WALCheckpoint) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.markSummariesPersisted(recordsByVChannel, metas, generation)
	m.markPChannelSummarySnapshotCheckpointPersisted(sourceCheckpoint)
	m.evictPersistedEntries()
}

func newPChannelSummaryStoreMetaFromChunk(
	pchannel string,
	footer *streamingpb.PChannelSummaryChunkFooter,
	minAvailableGeneration uint64,
	minInUseGeneration uint64,
) *pchannelSummaryStoreMeta {
	manifest := &streamingpb.PChannelSummaryChunkManifest{
		Ranges: []*streamingpb.PChannelSummaryChunkTermRange{
			{
				Term:            footer.Term,
				StartGeneration: 0,
				EndGeneration:   footer.Generation,
				StartTimetick:   footer.SourceCheckpointTimetick,
				EndTimetick:     footer.SourceCheckpointTimetick,
			},
		},
	}
	return &pchannelSummaryStoreMeta{
		PChannel:               pchannel,
		LatestGeneration:       footer.Generation,
		MinAvailableGeneration: minAvailableGeneration,
		MinInUseGeneration:     minInUseGeneration,
		Term:                   footer.Term,
		ChunkManifest:          manifest,
		SourceCheckpoint: pchannelSummarySourceCheckpointToWALCheckpoint(&pchannelSummarySourceCheckpoint{
			MessageID: cloneMessageIDProto(footer.SourceCheckpointMessageId),
			TimeTick:  footer.SourceCheckpointTimetick,
		}),
	}
}

func materializeSummaryMetaUpdates(updates map[string]*summaryMetaUpdate, generation uint64) map[string]*streamingpb.VChannelSummaryMeta {
	if len(updates) == 0 {
		return nil
	}
	metas := make(map[string]*streamingpb.VChannelSummaryMeta, len(updates))
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

func (m *summaryManager) minRequiredGenerationForPChannelSummary(summaryMetas map[string]*streamingpb.VChannelSummaryMeta, persistedGeneration uint64) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	minRequiredGeneration, hasActiveViewMinBoundary := m.minRequiredGeneration(summaryMetas, persistedGeneration)
	if !hasActiveViewMinBoundary {
		return persistedGeneration
	}
	return minRequiredGeneration
}

func pchannelSummarySourceCheckpointToWALCheckpoint(checkpoint *pchannelSummarySourceCheckpoint) *WALCheckpoint {
	if checkpoint == nil || checkpoint.MessageID == nil {
		return nil
	}
	return utility.NewWALCheckpointFromProto(&streamingpb.WALCheckpoint{
		MessageId: cloneMessageIDProto(checkpoint.MessageID),
		TimeTick:  checkpoint.TimeTick,
	})
}

func hasIdempotencyContent(records []*streamingpb.SummaryEntry) bool {
	for _, record := range records {
		if record.GetIdempotency().GetKey() != "" {
			return true
		}
	}
	return false
}

// rewindCheckpointForPChannelSummaryReplay returns the consume checkpoint to
// resume from, rewound to the earliest of the current checkpoint, the summary
// source checkpoint, and the recovered flusher checkpoint so the summary cache and
// flushed data can be rebuilt on restart. It does not mutate recoveryStorageImpl;
// the caller applies the returned checkpoint.
func (m *summaryManager) rewindCheckpointForPChannelSummaryReplay(sourceCheckpoint, checkpoint *WALCheckpoint, vchannels map[string]*vchannelRecoveryInfo) *WALCheckpoint {
	replayStart := minCheckpointByMessageID(checkpoint, sourceCheckpoint)
	if flusherCheckpoint := m.getRecoveredVChannelFlusherCheckpoint(vchannels); flusherCheckpoint != nil {
		replayStart = minCheckpointByMessageID(replayStart, flusherCheckpoint)
	}
	if replayStart == nil || sameWALCheckpoint(checkpoint, replayStart) {
		return checkpoint
	}
	m.Logger().Info(
		context.TODO(), "rewind recovery checkpoint for pchannel summary replay",
		mlog.String("fromMessageID", checkpointMessageIDString(checkpoint)),
		mlog.Uint64("fromTimeTick", checkpointTimeTick(checkpoint)),
		mlog.String("toMessageID", checkpointMessageIDString(replayStart)),
		mlog.Uint64("toTimeTick", replayStart.TimeTick),
	)
	return replayStart.Clone()
}

func (m *summaryManager) getRecoveredVChannelFlusherCheckpoint(vchannels map[string]*vchannelRecoveryInfo) *WALCheckpoint {
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
