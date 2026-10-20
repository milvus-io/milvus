package datacoord

import (
	"context"
	"math"

	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type forceMergeCompactionPolicy struct {
	meta            *meta
	indexMeta       *indexMeta
	allocator       allocator.Allocator
	handler         Handler
	topologyQuerier CollectionTopologyQuerier
}

func newForceMergeCompactionPolicy(meta *meta, indexMeta *indexMeta, allocator allocator.Allocator, handler Handler) *forceMergeCompactionPolicy {
	return &forceMergeCompactionPolicy{
		meta:            meta,
		indexMeta:       indexMeta,
		allocator:       allocator,
		handler:         handler,
		topologyQuerier: nil,
	}
}

func (policy *forceMergeCompactionPolicy) SetTopologyQuerier(querier CollectionTopologyQuerier) {
	policy.topologyQuerier = querier
}

func (policy *forceMergeCompactionPolicy) triggerOneCollection(
	ctx context.Context,
	collectionID int64,
	targetSize int64,
) ([]CompactionView, int64, error) {
	log := mlog.With(
		mlog.FieldCollectionID(collectionID),
		mlog.Int64("targetSize", targetSize))
	if policy.meta.isCollectionCompactionBlocked(collectionID) {
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

	configMaxSize := getExpectedSegmentSize(policy.indexMeta, collectionID, collection.Schema)
	if targetSizeBytes < configMaxSize {
		return nil, 0, merr.WrapErrParameterInvalidMsg("targetSize %d MB should be greater than or equal to configMaxSize %d MB", targetSize, configMaxSize/(1024*1024))
	}

	segments := policy.meta.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(segment *SegmentInfo) bool {
		return isNormalManualCompactionCandidate(policy.meta, segment)
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

	views := []CompactionView{}
	for label, groups := range groupByPartitionChannel(segments) {
		view := &ForceMergeSegmentView{
			label:         label,
			segments:      groups,
			triggerID:     triggerID,
			collectionTTL: collectionTTL,

			configMaxSize:      float64(configMaxSize),
			expectedTargetSize: float64(targetSizeBytes),
			topology:           topology,
		}
		views = append(views, view)
	}

	log.Info(ctx, "force merge triggered", mlog.Int("viewCount", len(views)))
	return views, triggerID, nil
}

func groupByPartitionChannel(segments []*SegmentInfo) map[*CompactionGroupLabel][]*SegmentInfo {
	groups := make(map[CompactionGroupLabel][]*SegmentInfo)
	for _, seg := range segments {
		label := CompactionGroupLabel{
			CollectionID: seg.GetCollectionID(),
			PartitionID:  seg.GetPartitionID(),
			Channel:      seg.GetInsertChannel(),
		}
		groups[label] = append(groups[label], seg)
	}

	result := make(map[*CompactionGroupLabel][]*SegmentInfo, len(groups))
	for label, group := range groups {
		label := label
		result[&label] = group
	}

	return result
}

var _ CollectionTopologyQuerier = (*metricsNodeMemoryQuerier)(nil)
