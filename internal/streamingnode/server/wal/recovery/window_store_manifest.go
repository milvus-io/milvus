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

const pchannelWindowTermSealProgressInterval = 10

type pchannelWindowMetaCASCatalog interface {
	CompareAndSwapPChannelWindowMeta(ctx context.Context, pchannelName string, expected *streamingpb.PChannelWindowMeta, target *streamingpb.PChannelWindowMeta) (bool, error)
}

func clonePChannelWindowChunkManifest(manifest *streamingpb.PChannelWindowChunkManifest) *streamingpb.PChannelWindowChunkManifest {
	if manifest == nil {
		return nil
	}
	return proto.Clone(manifest).(*streamingpb.PChannelWindowChunkManifest)
}

func pchannelWindowChunkManifestFromCatalog(meta *streamingpb.PChannelWindowMeta) *streamingpb.PChannelWindowChunkManifest {
	if meta == nil {
		return nil
	}
	manifest := clonePChannelWindowChunkManifest(meta.GetChunkManifest())
	if manifest != nil && len(manifest.GetRanges()) > 0 {
		return manifest
	}
	if meta.GetLatestGeneration() < meta.GetMinAvailableGeneration() {
		return manifest
	}
	return &streamingpb.PChannelWindowChunkManifest{
		Ranges: []*streamingpb.PChannelWindowChunkTermRange{
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

func pchannelWindowManifestWithChunk(
	manifest *streamingpb.PChannelWindowChunkManifest,
	term int64,
	generation uint64,
	sourceTimetick uint64,
) (*streamingpb.PChannelWindowChunkManifest, error) {
	next := clonePChannelWindowChunkManifest(manifest)
	if next == nil {
		next = &streamingpb.PChannelWindowChunkManifest{}
	}
	ranges := next.GetRanges()
	if len(ranges) == 0 {
		next.Ranges = append(next.Ranges, newPChannelWindowChunkTermRange(term, generation, sourceTimetick))
		return next, nil
	}
	last := ranges[len(ranges)-1]
	if last.GetTerm() > term {
		return nil, pchannelWindowStoreFencedf("pchannel window chunk manifest already owned by term %d, own term %d", last.GetTerm(), term)
	}
	if last.GetTerm() < term {
		if !last.GetSealed() {
			return nil, pchannelWindowStoreCorruptedf("pchannel window chunk manifest switches from unsealed term %d to term %d", last.GetTerm(), term)
		}
		if generation != last.GetEndGeneration()+1 {
			return nil, pchannelWindowStoreCorruptedf("pchannel window chunk manifest generation gap when switching from term %d to %d, previous end %d, append %d", last.GetTerm(), term, last.GetEndGeneration(), generation)
		}
		next.Ranges = append(next.Ranges, newPChannelWindowChunkTermRange(term, generation, sourceTimetick))
		return next, nil
	}
	if last.GetSealed() && generation > last.GetEndGeneration() {
		return nil, pchannelWindowStoreCorruptedf("pchannel window chunk manifest appends generation %d to sealed term %d", generation, term)
	}
	if generation < last.GetStartGeneration() || generation > last.GetEndGeneration()+1 {
		return nil, pchannelWindowStoreCorruptedf("pchannel window chunk manifest generation gap, term %d range [%d,%d], append %d", term, last.GetStartGeneration(), last.GetEndGeneration(), generation)
	}
	extendPChannelWindowChunkTermRange(last, generation, sourceTimetick)
	return next, nil
}

func newPChannelWindowChunkTermRange(term int64, generation uint64, sourceTimetick uint64) *streamingpb.PChannelWindowChunkTermRange {
	return &streamingpb.PChannelWindowChunkTermRange{
		Term:            term,
		StartGeneration: generation,
		EndGeneration:   generation,
		StartTimetick:   sourceTimetick,
		EndTimetick:     sourceTimetick,
	}
}

func extendPChannelWindowChunkTermRange(r *streamingpb.PChannelWindowChunkTermRange, generation uint64, sourceTimetick uint64) {
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

func pchannelWindowManifestRangeForGeneration(meta *pchannelWindowStoreMeta, generation uint64) (*streamingpb.PChannelWindowChunkTermRange, bool) {
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

func pchannelWindowManifestLastRange(meta *pchannelWindowStoreMeta) *streamingpb.PChannelWindowChunkTermRange {
	if meta == nil || meta.ChunkManifest == nil || len(meta.ChunkManifest.GetRanges()) == 0 {
		return nil
	}
	return meta.ChunkManifest.GetRanges()[len(meta.ChunkManifest.GetRanges())-1]
}

func compareAndSwapPChannelWindowMeta(
	ctx context.Context,
	logger *mlog.Logger,
	pchannel string,
	expected *streamingpb.PChannelWindowMeta,
	target *streamingpb.PChannelWindowMeta,
) (bool, error) {
	catalog := resource.Resource().StreamingNodeCatalog()
	if casCatalog, ok := catalog.(pchannelWindowMetaCASCatalog); ok {
		return casCatalog.CompareAndSwapPChannelWindowMeta(ctx, pchannel, expected, target)
	}
	if logger != nil {
		logger.Warn(ctx, "pchannel window meta CAS is unavailable; falling back to plain save for test catalog",
			mlog.String("pchannel", pchannel))
	}
	if err := catalog.SavePChannelWindowMeta(ctx, pchannel, target); err != nil {
		return false, err
	}
	return true, nil
}

func updatePChannelWindowMetaWithCAS(
	ctx context.Context,
	logger *mlog.Logger,
	pchannel string,
	update func(currentPB *streamingpb.PChannelWindowMeta, current *pchannelWindowStoreMeta) (*streamingpb.PChannelWindowMeta, error),
) error {
	return retryOperationWithBackoff(ctx, logger, func(ctx context.Context) error {
		currentPB, err := resource.Resource().StreamingNodeCatalog().GetPChannelWindowMeta(ctx, pchannel)
		if err != nil {
			return err
		}
		current := pchannelWindowStoreMetaFromCatalog(currentPB)
		targetPB, err := update(currentPB, current)
		if err != nil || targetPB == nil {
			return err
		}
		swapped, err := compareAndSwapPChannelWindowMeta(ctx, logger, pchannel, currentPB, targetPB)
		if err != nil {
			return err
		}
		if !swapped {
			return merr.WrapErrServiceUnavailable("pchannel window meta CAS conflict")
		}
		return nil
	})
}

func validatePChannelWindowManifest(meta *pchannelWindowStoreMeta) error {
	if meta == nil || meta.ChunkManifest == nil {
		return nil
	}
	var previous *streamingpb.PChannelWindowChunkTermRange
	for _, r := range meta.ChunkManifest.GetRanges() {
		if r == nil {
			return pchannelWindowStoreCorruptedf("pchannel window chunk manifest contains nil range")
		}
		if r.GetEndGeneration() < r.GetStartGeneration() {
			return pchannelWindowStoreCorruptedf("pchannel window chunk manifest invalid range for term %d: [%d,%d]", r.GetTerm(), r.GetStartGeneration(), r.GetEndGeneration())
		}
		if previous != nil {
			if r.GetTerm() <= previous.GetTerm() {
				return pchannelWindowStoreCorruptedf("pchannel window chunk manifest term order mismatch, previous %d, current %d", previous.GetTerm(), r.GetTerm())
			}
			if r.GetStartGeneration() != previous.GetEndGeneration()+1 {
				return pchannelWindowStoreCorruptedf("pchannel window chunk manifest generation continuity mismatch, previous end %d, current start %d", previous.GetEndGeneration(), r.GetStartGeneration())
			}
			if !previous.GetSealed() {
				return pchannelWindowStoreCorruptedf("pchannel window chunk manifest has unsealed non-latest term %d", previous.GetTerm())
			}
		}
		previous = r
	}
	if _, ok := pchannelWindowManifestRangeForGeneration(meta, meta.LatestGeneration); !ok {
		return pchannelWindowStoreCorruptedf("pchannel window chunk manifest misses latest generation %d", meta.LatestGeneration)
	}
	if meta.MinInUseGeneration <= meta.LatestGeneration {
		if _, ok := pchannelWindowManifestRangeForGeneration(meta, meta.MinInUseGeneration); !ok {
			return pchannelWindowStoreCorruptedf("pchannel window chunk manifest misses min-in-use generation %d", meta.MinInUseGeneration)
		}
	}
	return nil
}

func markPChannelWindowRangeSealed(manifest *streamingpb.PChannelWindowChunkManifest, term int64) (*streamingpb.PChannelWindowChunkManifest, error) {
	next := clonePChannelWindowChunkManifest(manifest)
	if next == nil || len(next.GetRanges()) == 0 {
		return next, nil
	}
	last := next.GetRanges()[len(next.GetRanges())-1]
	if last.GetTerm() != term {
		return nil, errors.AssertionFailedf("latest pchannel window chunk manifest range term %d does not match %d", last.GetTerm(), term)
	}
	last.Sealed = true
	return next, nil
}

func UpdatePChannelWindowMetaSourceCheckpoint(ctx context.Context, pchannel string, checkpoint *WALCheckpoint) error {
	if checkpoint == nil {
		return nil
	}
	return updatePChannelWindowMetaWithCAS(ctx,
		resource.Resource().Logger().With(mlog.String("op", "updatePChannelWindowMetaSourceCheckpoint")),
		pchannel,
		func(currentPB *streamingpb.PChannelWindowMeta, current *pchannelWindowStoreMeta) (*streamingpb.PChannelWindowMeta, error) {
			if currentPB == nil {
				return nil, nil
			}
			updated := proto.Clone(currentPB).(*streamingpb.PChannelWindowMeta)
			updated.SourceCheckpointTimetick = checkpoint.TimeTick
			updated.SourceCheckpointMessageId = safeMessageIDProto(checkpoint.MessageID)
			return updated, nil
		})
}
