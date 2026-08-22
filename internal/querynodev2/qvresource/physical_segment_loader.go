package qvresource

import (
	"context"

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

type queryViewLoadedSegment interface {
	ID() int64
	Partition() int64
	Delete(context.Context, storage.PrimaryKeys, []typeutil.Timestamp) error
	Release(context.Context) error
	PkCandidateExist() bool
	BatchPkExist(*storage.BatchLocationsCache) []bool
}

type queryViewSegmentLoader interface {
	NewSegment(context.Context, qnview.CollectionRuntime, *querypb.SegmentLoadInfo) (queryViewLoadedSegment, error)
	LoadSegment(context.Context, queryViewLoadedSegment, *querypb.SegmentLoadInfo) error
	ReopenSegment(context.Context, queryViewLoadedSegment, *querypb.SegmentLoadInfo) error
	LoadIndex(context.Context, queryViewLoadedSegment, *querypb.SegmentLoadInfo) error
	LoadDeltaLogs(context.Context, queryViewLoadedSegment, *querypb.SegmentLoadInfo, qnview.CollectionRuntime) error
	LoadPKCandidate(context.Context, queryViewLoadedSegment, *querypb.SegmentLoadInfo, qnview.CollectionRuntime) error
}

type queryViewPhysicalSegmentLoader struct {
	loader queryViewSegmentLoader
}

func NewQueryViewPhysicalSegmentLoader(
	loader segments.QueryViewLoader,
	collections segments.CollectionManager,
	segmentManager segments.SegmentManager,
) qnview.PhysicalSegmentLoader {
	return newQueryViewPhysicalSegmentLoader(realQueryViewSegmentLoader{
		loader: loader, collections: collections, segments: segmentManager,
	})
}

func newQueryViewPhysicalSegmentLoader(loader queryViewSegmentLoader) *queryViewPhysicalSegmentLoader {
	return &queryViewPhysicalSegmentLoader{loader: loader}
}

func (l *queryViewPhysicalSegmentLoader) Load(
	ctx context.Context,
	info *querypb.SegmentLoadInfo,
	collection qnview.CollectionRuntime,
) (qnview.TransformSegment, error) {
	if info == nil {
		return nil, merr.WrapErrServiceInternalMsg("query view segment load info is nil")
	}
	if collection == nil || collection.PinnedCollection() == nil {
		return nil, merr.WrapErrCollectionNotFound(info.GetCollectionID())
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
	if err := l.loader.LoadDeltaLogs(ctx, loaded, info, collection); err != nil {
		return nil, err
	}
	if err := l.loader.LoadPKCandidate(ctx, loaded, info, collection); err != nil {
		return nil, err
	}
	releaseOnFailure = false
	return newQueryViewTransformSegment(loaded, info.GetInsertChannel(), transformStartAfter(info)), nil
}

func (l *queryViewPhysicalSegmentLoader) Update(
	ctx context.Context,
	segment qnview.TransformSegment,
	collection qnview.CollectionRuntime,
	snapshot qnview.SegmentLoadInfoSnapshot,
	action qnview.SegmentUpdateAction,
) error {
	if segment == nil || snapshot.LoadInfo == nil {
		return merr.WrapErrServiceInternalMsg("query view segment update metadata is incomplete")
	}
	transform, ok := qnview.UnwrapTransformSegment(segment).(*queryViewTransformSegment)
	if !ok {
		return merr.WrapErrParameterInvalid("query view transform segment", "unexpected implementation")
	}
	if collection == nil || collection.CollectionID() != snapshot.CollectionID {
		return merr.WrapErrCollectionNotFound(snapshot.CollectionID)
	}
	if action.Has(qnview.SegmentUpdateReopen) {
		// Reopen consumes the complete load-info diff and therefore also applies
		// index replacement with the current segcore API. Do not invoke the
		// index-only entry point a second time when both action bits are set.
		return l.loader.ReopenSegment(ctx, transform.segment, snapshot.LoadInfo)
	}
	if action.Has(qnview.SegmentUpdateLoadIndex) {
		return l.loader.LoadIndex(ctx, transform.segment, snapshot.LoadInfo)
	}
	return nil
}

type realQueryViewSegmentLoader struct {
	loader      segments.QueryViewLoader
	collections segments.CollectionManager
	segments    segments.SegmentManager
}

func (l realQueryViewSegmentLoader) NewSegment(
	ctx context.Context,
	collection qnview.CollectionRuntime,
	info *querypb.SegmentLoadInfo,
) (queryViewLoadedSegment, error) {
	localCollection := collection.PinnedCollection()
	if localCollection == nil {
		return nil, merr.WrapErrCollectionNotFound(collection.CollectionID())
	}
	segment, err := segments.NewSegment(ctx, localCollection, l.segments, segments.SegmentTypeSealed, 0, info)
	if err != nil {
		return nil, err
	}
	return &queryViewLocalSegment{
		segment: segment, collections: l.collections, collectionID: collection.CollectionID(),
	}, nil
}

func (l realQueryViewSegmentLoader) LoadSegment(ctx context.Context, loaded queryViewLoadedSegment, info *querypb.SegmentLoadInfo) error {
	if info.GetLevel() == datapb.SegmentLevel_L0 {
		return nil
	}
	local, err := asQueryViewLocalSegment(loaded)
	if err != nil {
		return err
	}
	return l.loader.LoadSegment(ctx, local.segment, info)
}

func (realQueryViewSegmentLoader) ReopenSegment(ctx context.Context, loaded queryViewLoadedSegment, info *querypb.SegmentLoadInfo) error {
	local, err := asQueryViewLocalSegment(loaded)
	if err != nil {
		return err
	}
	return local.segment.Reopen(ctx, info)
}

func (l realQueryViewSegmentLoader) LoadIndex(ctx context.Context, loaded queryViewLoadedSegment, info *querypb.SegmentLoadInfo) error {
	local, err := asQueryViewLocalSegment(loaded)
	if err != nil {
		return err
	}
	return l.loader.LoadIndex(ctx, local.segment, info, 0)
}

func (l realQueryViewSegmentLoader) LoadDeltaLogs(
	ctx context.Context,
	loaded queryViewLoadedSegment,
	info *querypb.SegmentLoadInfo,
	collection qnview.CollectionRuntime,
) error {
	if typeutil.IsExternalCollection(collection.Schema()) {
		return nil
	}
	local, err := asQueryViewLocalSegment(loaded)
	if err != nil {
		return err
	}
	return l.loader.LoadDeltaLogsWithoutResource(ctx, local.segment, info)
}

func (l realQueryViewSegmentLoader) LoadPKCandidate(
	ctx context.Context,
	loaded queryViewLoadedSegment,
	info *querypb.SegmentLoadInfo,
	collection qnview.CollectionRuntime,
) error {
	local, err := asQueryViewLocalSegment(loaded)
	if err != nil {
		return err
	}
	if local.segment.PkCandidateExist() {
		return nil
	}
	if typeutil.IsExternalCollection(collection.Schema()) {
		local.segment.SetPKCandidate(pkoracle.NewExternalSegmentCandidate(
			info.GetSegmentID(), info.GetPartitionID(), local.segment.Type()))
		return nil
	}
	if !paramtable.Get().CommonCfg.BloomFilterEnabled.GetAsBool() {
		return nil
	}
	bloomFilters, err := l.loader.LoadBloomFilterSet(ctx, info.GetCollectionID(), info)
	if err != nil {
		return err
	}
	if len(bloomFilters) != 1 {
		return merr.WrapErrServiceInternalMsg(
			"query view physical loader expected one bloom filter set, got %d", len(bloomFilters))
	}
	local.segment.SetPKCandidate(bloomFilters[0])
	return nil
}

func asQueryViewLocalSegment(segment queryViewLoadedSegment) (*queryViewLocalSegment, error) {
	local, ok := segment.(*queryViewLocalSegment)
	if !ok {
		return nil, merr.WrapErrParameterInvalid("*queryViewLocalSegment", "unexpected implementation")
	}
	return local, nil
}

func transformStartAfter(info *querypb.SegmentLoadInfo) uint64 {
	if info.GetDeltaPosition() != nil {
		return info.GetDeltaPosition().GetTimestamp()
	}
	return info.GetStartPosition().GetTimestamp()
}
