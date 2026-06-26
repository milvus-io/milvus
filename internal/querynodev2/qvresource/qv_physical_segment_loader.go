package qvresource

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (l *queryViewPhysicalSegmentLoader) Load(ctx context.Context, info *querypb.SegmentLoadInfo, collection qnview.CollectionRuntime) (qnview.TransformSegment, error) {
	if info == nil {
		return nil, fmt.Errorf("query view segment load info is nil")
	}
	if collection == nil {
		return nil, fmt.Errorf("query view collection runtime is nil")
	}

	loaded, err := l.loader.NewSegment(ctx, collection, info)
	if err != nil {
		return nil, err
	}
	releaseOnFailure := true
	defer func() {
		if releaseOnFailure {
			_ = loaded.Release(context.Background())
		}
	}()
	if err := l.loader.LoadSegment(ctx, loaded, info); err != nil {
		return nil, err
	}
	if err := l.loader.LoadDeltaLogs(ctx, loaded, info); err != nil {
		return nil, err
	}
	if err := l.loader.LoadPKCandidate(ctx, loaded, info); err != nil {
		return nil, err
	}
	releaseOnFailure = false
	return newQueryViewTransformSegment(loaded, l.segments, qvSegmentVChannel(info), qvSegmentTransformStartAfter(info)), nil
}

func qvSegmentVChannel(info *querypb.SegmentLoadInfo) string {
	if info != nil && info.GetInsertChannel() != "" {
		return info.GetInsertChannel()
	}
	return ""
}

func qvSegmentTransformStartAfter(info *querypb.SegmentLoadInfo) uint64 {
	if info == nil {
		return 0
	}
	if info.GetDeltaPosition() != nil {
		return info.GetDeltaPosition().GetTimestamp()
	}
	return info.GetStartPosition().GetTimestamp()
}

type realQVSegmentLoader struct {
	loader         segments.Loader
	collections    qvCollectionManager
	segmentManager segments.SegmentManager
}

func (l realQVSegmentLoader) NewSegment(ctx context.Context, collection qnview.CollectionRuntime, info *querypb.SegmentLoadInfo) (qvLoadedSegment, error) {
	if collection == nil {
		return nil, fmt.Errorf("query view collection runtime is nil")
	}
	localCollection := l.collections.Get(collection.CollectionID())
	if localCollection == nil {
		return nil, merr.WrapErrCollectionNotFound(collection.CollectionID())
	}
	segment, err := segments.NewSegment(ctx, localCollection, l.segmentManager, segments.SegmentTypeSealed, 0, info)
	if err != nil {
		return nil, err
	}
	return &qvLocalSegment{segment: segment, collection: localCollection}, nil
}

func (l realQVSegmentLoader) LoadSegment(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error {
	if info.GetLevel() == datapb.SegmentLevel_L0 {
		return nil
	}
	local, err := asQVLocalSegment(segment)
	if err != nil {
		return err
	}
	return l.loader.LoadSegment(ctx, local.segment, info)
}

func (l realQVSegmentLoader) LoadDeltaLogs(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error {
	local, err := asQVLocalSegment(segment)
	if err != nil {
		return err
	}
	if typeutil.IsExternalCollection(local.collection.Schema()) {
		return nil
	}
	return l.loader.LoadDeltaLogsWithoutResource(ctx, local.segment, info)
}

func (l realQVSegmentLoader) LoadPKCandidate(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error {
	local, err := asQVLocalSegment(segment)
	if err != nil {
		return err
	}
	if local.segment.PkCandidateExist() {
		return nil
	}
	if typeutil.IsExternalCollection(local.collection.Schema()) {
		local.segment.SetPKCandidate(pkoracle.NewExternalSegmentCandidate(
			info.GetSegmentID(),
			info.GetPartitionID(),
			local.segment.Type(),
		))
		return nil
	}
	if !paramtable.Get().CommonCfg.BloomFilterEnabled.GetAsBool() {
		return nil
	}
	bfs, err := l.loader.LoadBloomFilterSet(ctx, info.GetCollectionID(), info)
	if err != nil {
		return err
	}
	if len(bfs) != 1 {
		return fmt.Errorf("query view physical loader expected one bloom filter set, got %d", len(bfs))
	}
	local.segment.SetPKCandidate(bfs[0])
	return nil
}

type qvLocalSegment struct {
	segment    segments.Segment
	collection *segments.Collection
}

func (s *qvLocalSegment) ID() int64 {
	return s.segment.ID()
}

func (s *qvLocalSegment) Partition() int64 {
	return s.segment.Partition()
}

func (s *qvLocalSegment) Delete(ctx context.Context, primaryKeys storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
	return s.segment.Delete(ctx, primaryKeys, timestamps)
}

func (s *qvLocalSegment) Release(ctx context.Context) error {
	s.segment.Release(ctx)
	return nil
}

func (s *qvLocalSegment) PkCandidateExist() bool {
	return s.segment.PkCandidateExist()
}

func (s *qvLocalSegment) BatchPkExist(lc *storage.BatchLocationsCache) []bool {
	return s.segment.BatchPkExist(lc)
}

func asQVLocalSegment(segment qvLoadedSegment) (*qvLocalSegment, error) {
	local, ok := segment.(*qvLocalSegment)
	if !ok {
		return nil, merr.WrapErrParameterInvalid("*qvLocalSegment", fmt.Sprintf("%T", segment))
	}
	return local, nil
}
