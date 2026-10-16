package qvresource

import (
	"context"
	"sync"

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
		return nil, merr.WrapErrServiceInternalMsg("query view segment load info is nil")
	}
	if collection == nil {
		return nil, merr.WrapErrServiceInternalMsg("query view collection runtime is nil")
	}
	info, err := segments.PrepareSegmentLoadInfo(info, collection.Schema())
	if err != nil {
		return nil, err
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
	if err := l.loader.FinalizeLoad(loaded); err != nil {
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
	return info.GetTransformStartAfterTimetick()
}

type realQVSegmentLoader struct {
	loader         segments.Loader
	segmentManager segments.SegmentManager
}

func (l realQVSegmentLoader) NewSegment(ctx context.Context, collection qnview.CollectionRuntime, info *querypb.SegmentLoadInfo) (qvLoadedSegment, error) {
	if collection == nil {
		return nil, merr.WrapErrServiceInternalMsg("query view collection runtime is nil")
	}
	localCollection := collection.Collection()
	if localCollection == nil {
		return nil, merr.WrapErrCollectionNotFound(collection.CollectionID())
	}
	segment, err := segments.NewSegment(ctx, localCollection, l.segmentManager, segments.SegmentTypeSealed, 0, info)
	if err != nil {
		return nil, err
	}
	return &qvLocalSegment{
		segment: segment, collection: localCollection,
		collectionID: collection.CollectionID(),
	}, nil
}

func (l realQVSegmentLoader) LoadSegment(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error {
	if info.GetLevel() == datapb.SegmentLevel_L0 {
		return nil
	}
	local, err := asQVLocalSegment(segment)
	if err != nil {
		return err
	}
	if err := l.loader.LoadSegment(ctx, local.segment, info); err != nil {
		return err
	}
	return local.segment.(*segments.LocalSegment).ValidateQueryViewFields(local.collection)
}

func (l realQVSegmentLoader) LoadDeltaLogs(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error {
	local, err := asQVLocalSegment(segment)
	if err != nil {
		return err
	}
	// The shared loader handles milvus-table source deletes and skips only
	// external formats that do not carry historical deltalogs.
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
	collection := local.Collection()
	if collection == nil {
		return merr.WrapErrCollectionNotFound(local.collectionID)
	}
	schema := collection.Schema()
	isMilvusTableRealPK := typeutil.NewStorageColumnResolver(schema).IsMilvusTable() && segments.HasExternalPrimaryKey(schema)
	if typeutil.IsExternalCollection(schema) && !isMilvusTableRealPK {
		local.segment.SetPKCandidate(pkoracle.NewExternalSegmentCandidate(
			info.GetSegmentID(),
			info.GetPartitionID(),
			local.segment.Type(),
		))
		return nil
	}
	if !paramtable.Get().CommonCfg.BloomFilterEnabled.GetAsBool() && !isMilvusTableRealPK {
		return nil
	}
	bfs, err := l.loader.LoadBloomFilterSetWithSchema(ctx, collection.Schema(), info)
	if err != nil {
		return err
	}
	if len(bfs) != 1 {
		return merr.WrapErrServiceInternalMsg("query view physical loader expected one bloom filter set, got %d", len(bfs))
	}
	local.segment.SetPKCandidate(bfs[0])
	return nil
}

func (l realQVSegmentLoader) FinalizeLoad(segment qvLoadedSegment) error {
	local, err := asQVLocalSegment(segment)
	if err != nil {
		return err
	}
	segments.FinalizeLoad(local.segment)
	if err := local.segment.(*segments.LocalSegment).DetachPreparingCollection(); err != nil {
		return err
	}
	local.collection = nil
	return nil
}

type qvLocalSegment struct {
	collection   *segments.Collection
	releaseOnce  sync.Once
	segment      segments.Segment
	collectionID int64
}

func (s *qvLocalSegment) ID() int64 {
	return s.segment.ID()
}

func (s *qvLocalSegment) Partition() int64 {
	return s.segment.Partition()
}

func (s *qvLocalSegment) QuerySegment() segments.Segment {
	return s.segment
}

func (s *qvLocalSegment) Collection() *segments.Collection {
	return s.collection
}

func (s *qvLocalSegment) Delete(ctx context.Context, primaryKeys storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
	return s.segment.Delete(ctx, primaryKeys, timestamps)
}

func (s *qvLocalSegment) Release(ctx context.Context) error {
	s.releaseOnce.Do(func() {
		s.segment.Release(ctx)
	})
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
		return nil, merr.WrapErrServiceInternalMsg("expected *qvLocalSegment, got %T", segment)
	}
	return local, nil
}

// PrepareReadState stages a view's exact read schema and scope on shared columns.
func (l *queryViewPhysicalSegmentLoader) PrepareReadState(ctx context.Context, segment qnview.TransformSegment, reuse segments.ReadState, collection qnview.CollectionRuntime, snapshot qnview.SegmentLoadInfoSnapshot) (segments.ReadState, error) {
	transform := qnview.UnwrapTransformSegment(segment).(*queryViewTransformSegment)
	info, err := segments.PrepareSegmentLoadInfo(snapshot.LoadInfo, collection.Schema())
	if err != nil {
		return nil, err
	}
	// Historical deltas must be visible before publishing the new read state.
	if err := l.loader.LoadDeltaLogs(ctx, transform.segment, info); err != nil {
		return nil, err
	}
	return transform.QuerySegment().(*segments.LocalSegment).PrepareReadState(ctx, reuse, info, collection.Collection())
}

func (l *queryViewPhysicalSegmentLoader) AdoptReadState(segment qnview.TransformSegment, read segments.ReadState) error {
	transform := qnview.UnwrapTransformSegment(segment).(*queryViewTransformSegment)
	return transform.QuerySegment().(*segments.LocalSegment).AdoptReadState(read)
}
