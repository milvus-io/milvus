package policy

import (
	"context"
	"math"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type ForceMergeCompactionPolicy struct {
	meta            *meta.Meta
	IndexMeta       *index.IndexMeta
	allocator       allocator.Allocator
	handler         meta.Handler
	topologyQuerier view.CollectionTopologyQuerier
}

func NewForceMergeCompactionPolicy(mt *meta.Meta, indexMeta *index.IndexMeta, allocator allocator.Allocator, handler meta.Handler) *ForceMergeCompactionPolicy {
	return &ForceMergeCompactionPolicy{
		meta:            mt,
		IndexMeta:       indexMeta,
		allocator:       allocator,
		handler:         handler,
		topologyQuerier: nil,
	}
}

func (policy *ForceMergeCompactionPolicy) SetTopologyQuerier(querier view.CollectionTopologyQuerier) {
	policy.topologyQuerier = querier
}

func (policy *ForceMergeCompactionPolicy) TriggerOneCollection(
	ctx context.Context,
	collectionID int64,
	targetSize int64,
) ([]view.CompactionView, int64, error) {
	log := mlog.With(
		mlog.FieldCollectionID(collectionID),
		mlog.Int64("targetSize", targetSize))
	if policy.meta.IsCollectionCompactionBlocked(collectionID) {
		log.Info(ctx, "skip force merge compaction for collection due to unloaded protected snapshot RefIndex",
			mlog.FieldCollectionID(collectionID))
		return nil, 0, nil
	}
	collection, err := policy.handler.GetCollection(ctx, collectionID)
	if err != nil {
		return nil, 0, err
	}
	triggerID, err := policy.allocator.AllocID(ctx)
	if err != nil {
		return nil, 0, err
	}

	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		log.Warn(ctx, "failed to get collection ttl, use default", mlog.Err(err))
		collectionTTL = 0
	}

	// Convert targetSize from MB to bytes (per design doc: targetSize is in MB)
	// Handle overflow: when targetSize is very large (e.g., max_int64 for auto-calculate mode)
	var targetSizeBytes int64
	if targetSize > math.MaxInt64/(1024*1024) {
		targetSizeBytes = math.MaxInt64
	} else {
		targetSizeBytes = targetSize * 1024 * 1024
	}

	configMaxSize := GetExpectedSegmentSize(policy.IndexMeta, collectionID, collection.Schema)
	if targetSizeBytes < configMaxSize {
		return nil, 0, merr.WrapErrParameterInvalidMsg("targetSize %d MB should be greater than or equal to configMaxSize %d MB", targetSize, configMaxSize/(1024*1024))
	}

	segments := policy.meta.SelectSegments(ctx, meta.WithCollection(collectionID), meta.SegmentFilterFunc(func(segment *meta.SegmentInfo) bool {
		return IsNormalManualCompactionCandidate(policy.meta, segment)
	}))

	if len(segments) == 0 {
		log.Info(ctx, "no eligible segments for force merge")
		return nil, 0, nil
	}

	topology, err := policy.topologyQuerier.GetCollectionTopology(ctx, collectionID)
	if err != nil {
		return nil, 0, err
	}
	topology.NumShards = len(collection.VChannelNames)

	views := []view.CompactionView{}
	for label, groups := range groupByPartitionChannel(segments) {
		cv := &view.ForceMergeSegmentView{
			Label:         label,
			Segments:      groups,
			TriggerID:     triggerID,
			CollectionTTL: collectionTTL,

			ConfigMaxSize:      float64(configMaxSize),
			ExpectedTargetSize: float64(targetSizeBytes),
			Topology:           topology,
		}
		views = append(views, cv)
	}

	log.Info(ctx, "force merge triggered", mlog.Int("viewCount", len(views)))
	return views, triggerID, nil
}

func groupByPartitionChannel(segments []*meta.SegmentInfo) map[*view.CompactionGroupLabel][]*meta.SegmentInfo {
	groups := make(map[view.CompactionGroupLabel][]*meta.SegmentInfo)
	for _, seg := range segments {
		label := view.CompactionGroupLabel{
			CollectionID: seg.GetCollectionID(),
			PartitionID:  seg.GetPartitionID(),
			Channel:      seg.GetInsertChannel(),
		}
		groups[label] = append(groups[label], seg)
	}

	result := make(map[*view.CompactionGroupLabel][]*meta.SegmentInfo, len(groups))
	for label, group := range groups {
		label := label
		result[&label] = group
	}

	return result
}

var _ view.CollectionTopologyQuerier = (*view.MetricsNodeMemoryQuerier)(nil)
