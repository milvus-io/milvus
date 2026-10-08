package qvresource

import (
	"context"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (l *queryViewPhysicalSegmentLoader) Load(ctx context.Context, info *querypb.SegmentLoadInfo, collection qnview.CollectionRuntime) (qnview.TransformSegment, error) {
	if info == nil {
		return nil, merr.WrapErrServiceInternalMsg("query view segment load info is nil")
	}
	if collection == nil {
		return nil, merr.WrapErrServiceInternalMsg("query view collection runtime is nil")
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
	return newQueryViewTransformSegment(loaded, qvSegmentVChannel(info), qvSegmentTransformStartAfter(info)), nil
}

func (l *queryViewPhysicalSegmentLoader) Update(ctx context.Context, segment qnview.TransformSegment, collection qnview.CollectionRuntime, snapshot qnview.SegmentLoadInfoSnapshot, action qnview.SegmentUpdateAction) error {
	if segment == nil {
		return merr.WrapErrServiceInternalMsg("query view transform segment is nil")
	}
	if snapshot.LoadInfo == nil {
		return merr.WrapErrServiceInternalMsg("query view segment load info is nil")
	}
	segment = qnview.UnwrapTransformSegment(segment)
	transform, ok := segment.(*queryViewTransformSegment)
	if !ok {
		return merr.WrapErrServiceInternalMsg("unexpected query view transform segment type %T", segment)
	}
	if action.Has(qnview.SegmentUpdateReopen) || action.Has(qnview.SegmentUpdateLoadIndex) {
		if err := l.loader.ReopenSegment(ctx, transform.segment, collection, snapshot.LoadInfo); err != nil {
			return err
		}
	}

	return nil
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

// realQVSegmentLoader uses explicit native resources, never legacy registries.
type realQVSegmentLoader struct{ cm storage.ChunkManager }

func retainCollection(collection qnview.CollectionRuntime) (qnview.CollectionRuntimeGuard, error) {
	owner, ok := collection.(interface {
		Retain() (qnview.CollectionRuntimeGuard, error)
	})
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("collection runtime cannot be retained")
	}
	return owner.Retain()
}

func (l realQVSegmentLoader) NewSegment(ctx context.Context, collection qnview.CollectionRuntime, info *querypb.SegmentLoadInfo) (qvLoadedSegment, error) {
	guard, err := retainCollection(collection)
	if err != nil {
		return nil, err
	}
	owned := proto.Clone(info).(*querypb.SegmentLoadInfo)
	if err = segments.PrepareSegmentLoadInfo(collection.Schema(), owned); err != nil {
		guard.Release()
		return nil, err
	}
	native, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{Collection: guard.CCollection(), SegmentID: owned.GetSegmentID(), SegmentType: segcore.SegmentTypeSealed, IsSorted: owned.GetIsSorted(), LoadInfo: owned})
	if err != nil {
		guard.Release()
		return nil, err
	}
	return &qvLocalSegment{collection: guard, segment: native, info: owned}, nil
}

func (l realQVSegmentLoader) LoadSegment(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error {
	local, err := asQVLocalSegment(segment)
	if err != nil {
		return err
	}
	_, err = segments.GetLoadPool().Submit(func() (any, error) { return nil, local.segment.Load(ctx) }).Await()
	return err
}

func (l realQVSegmentLoader) ReopenSegment(ctx context.Context, segment qvLoadedSegment, collection qnview.CollectionRuntime, info *querypb.SegmentLoadInfo) error {
	local, err := asQVLocalSegment(segment)
	if err != nil {
		return err
	}
	next, err := retainCollection(collection)
	if err != nil {
		return err
	}
	owned := proto.Clone(info).(*querypb.SegmentLoadInfo)
	if err = segments.PrepareSegmentLoadInfo(collection.Schema(), owned); err != nil {
		next.Release()
		return err
	}
	if err = local.segment.Reopen(ctx, &segcore.ReopenRequest{LoadInfo: owned, Schema: next.Schema(), SchemaVersion: uint64(next.SchemaVersion())}); err != nil {
		next.Release()
		return err
	}
	local.mu.Lock()
	previous := local.collection
	local.collection = next
	local.info = owned
	local.mu.Unlock()
	previous.Release()
	return nil
}

func (l realQVSegmentLoader) LoadDeltaLogs(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error {
	local, err := asQVLocalSegment(segment)
	if err != nil {
		return err
	}
	return segments.LoadSegmentDeltaLogs(ctx, local.collection.Schema(), local.collection.CollectionID(), l.cm, local, info)
}

func (l realQVSegmentLoader) LoadPKCandidate(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error {
	local, err := asQVLocalSegment(segment)
	if err != nil {
		return err
	}
	schema := local.collection.Schema()
	if typeutil.IsExternalCollection(schema) && (!typeutil.NewStorageColumnResolver(schema).IsMilvusTable() || !segments.HasExternalPrimaryKey(schema)) {
		local.candidate = pkoracle.NewExternalSegmentCandidate(info.GetSegmentID(), info.GetPartitionID(), segcore.SegmentTypeSealed)
		return nil
	}
	bfs, err := segments.LoadSegmentBloomFilters(ctx, schema, local.collection.CollectionID(), l.cm, info)
	if err != nil {
		return err
	}
	local.candidate = bfs[0]
	return nil
}

// qvLocalSegment owns native resources. Queries borrow them under readiness handles.
type qvLocalSegment struct {
	releaseOnce sync.Once
	mu          sync.RWMutex
	collection  qnview.CollectionRuntimeGuard
	segment     segcore.CSegment
	info        *querypb.SegmentLoadInfo
	candidate   pkoracle.Candidate
	deltaMu     sync.Mutex
	lastDelta   atomic.Uint64
}

func (s *qvLocalSegment) ID() int64 { return s.segment.ID() }
func (s *qvLocalSegment) Partition() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.info.GetPartitionID()
}

func (s *qvLocalSegment) ReadView() qnview.SegmentReadView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return qnview.SegmentReadView{Collection: s.collection.CCollection(), Segment: s.segment, LoadInfo: s.info, DatabaseName: s.collection.DatabaseName()}
}

func (s *qvLocalSegment) Delete(ctx context.Context, pks storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
	s.deltaMu.Lock()
	defer s.deltaMu.Unlock()
	if pks.Len() == 0 {
		return nil
	}
	_, err := segments.GetMutatePool().Submit(func() (any, error) {
		return s.segment.Delete(ctx, &segcore.DeleteRequest{PrimaryKeys: pks, Timestamps: timestamps})
	}).Await()
	if err != nil {
		return err
	}
	s.advanceLastDeltaTimestamp(timestamps)
	return nil
}

func (s *qvLocalSegment) advanceLastDeltaTimestamp(timestamps []typeutil.Timestamp) {
	for _, ts := range timestamps {
		if ts > s.lastDelta.Load() {
			s.lastDelta.Store(ts)
		}
	}
}
func (s *qvLocalSegment) LastDeltaTimestamp() uint64 { return s.lastDelta.Load() }
func (s *qvLocalSegment) LoadDeltaData(ctx context.Context, data *storage.DeltaData) error {
	if data.DeleteRowCount() == 0 {
		return nil
	}
	s.deltaMu.Lock()
	defer s.deltaMu.Unlock()
	if err := segments.LoadSegmentDeletedRecords(ctx, s.segment, data); err != nil {
		return err
	}
	s.advanceLastDeltaTimestamp(data.DeleteTimestamps())
	return nil
}

func (s *qvLocalSegment) Release(ctx context.Context) error {
	s.releaseOnce.Do(func() {
		s.segment.Release()
		if s.candidate != nil {
			s.candidate.Refund()
		}
		s.collection.Release()
	})
	return nil
}

func (s *qvLocalSegment) PkCandidateExist() bool {
	return s.candidate != nil && s.candidate.PkCandidateExist()
}

func (s *qvLocalSegment) BatchPkExist(lc *storage.BatchLocationsCache) []bool {
	return s.candidate.BatchPkExist(lc)
}

func asQVLocalSegment(segment qvLoadedSegment) (*qvLocalSegment, error) {
	local, ok := segment.(*qvLocalSegment)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("unexpected native QueryView segment %T", segment)
	}
	return local, nil
}
