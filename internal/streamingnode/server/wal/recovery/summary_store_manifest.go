package recovery

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const pchannelSummaryTermSealProgressInterval = 10

type pchannelSummaryMetaCASCatalog interface {
	CompareAndSwapPChannelSummaryMeta(ctx context.Context, pchannelName string, expected *streamingpb.PChannelSummaryMeta, target *streamingpb.PChannelSummaryMeta) (bool, error)
}

func clonePChannelSummaryChunkManifest(manifest *streamingpb.PChannelSummaryChunkManifest) *streamingpb.PChannelSummaryChunkManifest {
	if manifest == nil {
		return nil
	}
	return proto.Clone(manifest).(*streamingpb.PChannelSummaryChunkManifest)
}

func pchannelSummaryChunkManifestFromCatalog(meta *streamingpb.PChannelSummaryMeta) *streamingpb.PChannelSummaryChunkManifest {
	if meta == nil {
		return nil
	}
	manifest := clonePChannelSummaryChunkManifest(meta.GetChunkManifest())
	if manifest != nil && len(manifest.GetRanges()) > 0 {
		return manifest
	}
	if meta.GetLatestGeneration() < meta.GetMinAvailableGeneration() {
		return manifest
	}
	return &streamingpb.PChannelSummaryChunkManifest{
		Ranges: []*streamingpb.PChannelSummaryChunkTermRange{
			{
				Term:            meta.GetTerm(),
				StartGeneration: 0,
				EndGeneration:   meta.GetLatestGeneration(),
				Sealed:          false,
				StartTimetick:   meta.GetSourceCheckpointTimetick(),
				EndTimetick:     meta.GetSourceCheckpointTimetick(),
			},
		},
	}
}

func pchannelSummaryManifestWithChunk(
	manifest *streamingpb.PChannelSummaryChunkManifest,
	term int64,
	generation uint64,
	sourceTimetick uint64,
) (*streamingpb.PChannelSummaryChunkManifest, error) {
	next := clonePChannelSummaryChunkManifest(manifest)
	if next == nil {
		next = &streamingpb.PChannelSummaryChunkManifest{}
	}
	ranges := next.GetRanges()
	if len(ranges) == 0 {
		next.Ranges = append(next.Ranges, newPChannelSummaryChunkTermRange(term, generation, sourceTimetick))
		return next, nil
	}
	last := ranges[len(ranges)-1]
	if last.GetTerm() > term {
		return nil, pchannelSummaryStoreFencedf("pchannel summary chunk manifest already owned by term %d, own term %d", last.GetTerm(), term)
	}
	if last.GetTerm() < term {
		if !last.GetSealed() {
			return nil, pchannelSummaryStoreCorruptedf("pchannel summary chunk manifest switches from unsealed term %d to term %d", last.GetTerm(), term)
		}
		if generation != last.GetEndGeneration()+1 {
			return nil, pchannelSummaryStoreCorruptedf("pchannel summary chunk manifest generation gap when switching from term %d to %d, previous end %d, append %d", last.GetTerm(), term, last.GetEndGeneration(), generation)
		}
		next.Ranges = append(next.Ranges, newPChannelSummaryChunkTermRange(term, generation, sourceTimetick))
		return next, nil
	}
	if last.GetSealed() && generation > last.GetEndGeneration() {
		return nil, pchannelSummaryStoreCorruptedf("pchannel summary chunk manifest appends generation %d to sealed term %d", generation, term)
	}
	if generation < last.GetStartGeneration() || generation > last.GetEndGeneration()+1 {
		return nil, pchannelSummaryStoreCorruptedf("pchannel summary chunk manifest generation gap, term %d range [%d,%d], append %d", term, last.GetStartGeneration(), last.GetEndGeneration(), generation)
	}
	extendPChannelSummaryChunkTermRange(last, generation, sourceTimetick)
	return next, nil
}

func newPChannelSummaryChunkTermRange(term int64, generation uint64, sourceTimetick uint64) *streamingpb.PChannelSummaryChunkTermRange {
	return &streamingpb.PChannelSummaryChunkTermRange{
		Term:            term,
		StartGeneration: generation,
		EndGeneration:   generation,
		StartTimetick:   sourceTimetick,
		EndTimetick:     sourceTimetick,
	}
}

func extendPChannelSummaryChunkTermRange(r *streamingpb.PChannelSummaryChunkTermRange, generation uint64, sourceTimetick uint64) {
	if r == nil {
		return
	}
	if generation < r.StartGeneration {
		r.StartGeneration = generation
	}
	if generation > r.EndGeneration {
		r.EndGeneration = generation
	}
	if sourceTimetick == 0 {
		return
	}
	if r.StartTimetick == 0 || sourceTimetick < r.StartTimetick {
		r.StartTimetick = sourceTimetick
	}
	if sourceTimetick > r.EndTimetick {
		r.EndTimetick = sourceTimetick
	}
}

func pchannelSummaryManifestRangeForGeneration(meta *pchannelSummaryStoreMeta, generation uint64) (*streamingpb.PChannelSummaryChunkTermRange, bool) {
	if meta == nil || meta.ChunkManifest == nil {
		return nil, false
	}
	for _, r := range meta.ChunkManifest.GetRanges() {
		if r == nil {
			continue
		}
		if generation >= r.GetStartGeneration() && generation <= r.GetEndGeneration() {
			return r, true
		}
	}
	return nil, false
}

func pchannelSummaryManifestLastRange(meta *pchannelSummaryStoreMeta) *streamingpb.PChannelSummaryChunkTermRange {
	if meta == nil || meta.ChunkManifest == nil || len(meta.ChunkManifest.GetRanges()) == 0 {
		return nil
	}
	return meta.ChunkManifest.GetRanges()[len(meta.ChunkManifest.GetRanges())-1]
}

func compareAndSwapPChannelSummaryMeta(
	ctx context.Context,
	logger *mlog.Logger,
	pchannel string,
	expected *streamingpb.PChannelSummaryMeta,
	target *streamingpb.PChannelSummaryMeta,
) (bool, error) {
	catalog := resource.Resource().StreamingNodeCatalog()
	if casCatalog, ok := catalog.(pchannelSummaryMetaCASCatalog); ok {
		return casCatalog.CompareAndSwapPChannelSummaryMeta(ctx, pchannel, expected, target)
	}
	if logger != nil {
		logger.Warn(ctx, "pchannel summary meta CAS is unavailable; falling back to plain save for test catalog",
			mlog.String("pchannel", pchannel))
	}
	if err := catalog.SavePChannelSummaryMeta(ctx, pchannel, target); err != nil {
		return false, err
	}
	return true, nil
}

func updatePChannelSummaryMetaWithCAS(
	ctx context.Context,
	logger *mlog.Logger,
	pchannel string,
	update func(currentPB *streamingpb.PChannelSummaryMeta, current *pchannelSummaryStoreMeta) (*streamingpb.PChannelSummaryMeta, error),
) error {
	return retryOperationWithBackoff(ctx, logger, func(ctx context.Context) error {
		currentPB, err := resource.Resource().StreamingNodeCatalog().GetPChannelSummaryMeta(ctx, pchannel)
		if err != nil {
			return err
		}
		current := pchannelSummaryStoreMetaFromCatalog(currentPB)
		targetPB, err := update(currentPB, current)
		if err != nil || targetPB == nil {
			return err
		}
		swapped, err := compareAndSwapPChannelSummaryMeta(ctx, logger, pchannel, currentPB, targetPB)
		if err != nil {
			return err
		}
		if !swapped {
			return merr.WrapErrServiceUnavailable("pchannel summary meta CAS conflict")
		}
		return nil
	})
}

func validatePChannelSummaryManifest(meta *pchannelSummaryStoreMeta) error {
	if meta == nil || meta.ChunkManifest == nil {
		return nil
	}
	var previous *streamingpb.PChannelSummaryChunkTermRange
	for _, r := range meta.ChunkManifest.GetRanges() {
		if r == nil {
			return pchannelSummaryStoreCorruptedf("pchannel summary chunk manifest contains nil range")
		}
		if r.GetEndGeneration() < r.GetStartGeneration() {
			return pchannelSummaryStoreCorruptedf("pchannel summary chunk manifest invalid range for term %d: [%d,%d]", r.GetTerm(), r.GetStartGeneration(), r.GetEndGeneration())
		}
		if previous != nil {
			if r.GetTerm() <= previous.GetTerm() {
				return pchannelSummaryStoreCorruptedf("pchannel summary chunk manifest term order mismatch, previous %d, current %d", previous.GetTerm(), r.GetTerm())
			}
			if r.GetStartGeneration() != previous.GetEndGeneration()+1 {
				return pchannelSummaryStoreCorruptedf("pchannel summary chunk manifest generation continuity mismatch, previous end %d, current start %d", previous.GetEndGeneration(), r.GetStartGeneration())
			}
			if !previous.GetSealed() {
				return pchannelSummaryStoreCorruptedf("pchannel summary chunk manifest has unsealed non-latest term %d", previous.GetTerm())
			}
		}
		previous = r
	}
	if _, ok := pchannelSummaryManifestRangeForGeneration(meta, meta.LatestGeneration); !ok {
		return pchannelSummaryStoreCorruptedf("pchannel summary chunk manifest misses latest generation %d", meta.LatestGeneration)
	}
	if meta.MinInUseGeneration <= meta.LatestGeneration {
		if _, ok := pchannelSummaryManifestRangeForGeneration(meta, meta.MinInUseGeneration); !ok {
			return pchannelSummaryStoreCorruptedf("pchannel summary chunk manifest misses min-in-use generation %d", meta.MinInUseGeneration)
		}
	}
	return nil
}

func markPChannelSummaryRangeSealed(manifest *streamingpb.PChannelSummaryChunkManifest, term int64) (*streamingpb.PChannelSummaryChunkManifest, error) {
	next := clonePChannelSummaryChunkManifest(manifest)
	if next == nil || len(next.GetRanges()) == 0 {
		return next, nil
	}
	last := next.GetRanges()[len(next.GetRanges())-1]
	if last.GetTerm() != term {
		return nil, errors.AssertionFailedf("latest pchannel summary chunk manifest range term %d does not match %d", last.GetTerm(), term)
	}
	last.Sealed = true
	return next, nil
}

func UpdatePChannelSummaryMetaSourceCheckpoint(ctx context.Context, pchannel string, checkpoint *WALCheckpoint) error {
	if checkpoint == nil {
		return nil
	}
	return updatePChannelSummaryMetaWithCAS(ctx,
		resource.Resource().Logger().With(mlog.String("op", "updatePChannelSummaryMetaSourceCheckpoint")),
		pchannel,
		func(currentPB *streamingpb.PChannelSummaryMeta, current *pchannelSummaryStoreMeta) (*streamingpb.PChannelSummaryMeta, error) {
			if currentPB == nil {
				return nil, nil
			}
			updated := proto.Clone(currentPB).(*streamingpb.PChannelSummaryMeta)
			updated.SourceCheckpointTimetick = checkpoint.TimeTick
			updated.SourceCheckpointMessageId = safeMessageIDProto(checkpoint.MessageID)
			return updated, nil
		})
}
