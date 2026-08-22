package qvresource

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type queryViewTransformSegment struct {
	segment    queryViewLoadedSegment
	vchannel   string
	startAfter uint64

	mu          sync.Mutex
	applied     uint64
	waiters     map[uint64][]chan struct{}
	releaseOnce sync.Once
	releaseErr  error
}

func newQueryViewTransformSegment(segment queryViewLoadedSegment, vchannel string, startAfter uint64) *queryViewTransformSegment {
	return &queryViewTransformSegment{
		segment:    segment,
		vchannel:   vchannel,
		startAfter: startAfter,
		applied:    startAfter,
		waiters:    make(map[uint64][]chan struct{}),
	}
}

func (s *queryViewTransformSegment) ID() int64                           { return s.segment.ID() }
func (s *queryViewTransformSegment) VChannel() string                    { return s.vchannel }
func (s *queryViewTransformSegment) PartitionID() int64                  { return s.segment.Partition() }
func (s *queryViewTransformSegment) TransformStartAfterTimeTick() uint64 { return s.startAfter }

func (s *queryViewTransformSegment) AppliedTransformTimeTick() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied
}

func (s *queryViewTransformSegment) WaitTransformApplied(ctx context.Context, timetick uint64) error {
	if timetick == 0 {
		return nil
	}
	waiter := make(chan struct{})
	s.mu.Lock()
	if s.applied >= timetick {
		s.mu.Unlock()
		return nil
	}
	s.waiters[timetick] = append(s.waiters[timetick], waiter)
	s.mu.Unlock()
	select {
	case <-waiter:
		return nil
	case <-ctx.Done():
		s.removeWaiter(timetick, waiter)
		return ctx.Err()
	}
}

func (s *queryViewTransformSegment) ApplyDelete(
	ctx context.Context,
	partitionID int64,
	primaryKeys storage.PrimaryKeys,
	timetick uint64,
) error {
	if partitionID != common.AllPartitionsID && partitionID != s.PartitionID() {
		return nil
	}
	primaryKeys = filterMaybeHitPrimaryKeys(s.segment, primaryKeys)
	if primaryKeys == nil || primaryKeys.Len() == 0 {
		return nil
	}
	timestamps := make([]typeutil.Timestamp, primaryKeys.Len())
	for i := range timestamps {
		timestamps[i] = timetick
	}
	return s.segment.Delete(ctx, primaryKeys, timestamps)
}

func (s *queryViewTransformSegment) MarkTransformApplied(timetick uint64) {
	ready := make([]chan struct{}, 0)
	s.mu.Lock()
	if timetick > s.applied {
		s.applied = timetick
	}
	for target, waiters := range s.waiters {
		if target <= s.applied {
			ready = append(ready, waiters...)
			delete(s.waiters, target)
		}
	}
	s.mu.Unlock()
	for _, waiter := range ready {
		close(waiter)
	}
}

func (s *queryViewTransformSegment) removeWaiter(timetick uint64, waiter chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	waiters := s.waiters[timetick]
	for i, candidate := range waiters {
		if candidate == waiter {
			waiters = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(waiters) == 0 {
		delete(s.waiters, timetick)
	} else {
		s.waiters[timetick] = waiters
	}
}

func (s *queryViewTransformSegment) Release(ctx context.Context) error {
	s.releaseOnce.Do(func() { s.releaseErr = s.segment.Release(ctx) })
	return s.releaseErr
}

type queryViewLocalSegment struct {
	segment      segments.Segment
	collections  segments.CollectionManager
	collectionID int64
}

func (s *queryViewLocalSegment) ID() int64        { return s.segment.ID() }
func (s *queryViewLocalSegment) Partition() int64 { return s.segment.Partition() }
func (s *queryViewLocalSegment) Collection() *segments.Collection {
	return s.collections.Get(s.collectionID)
}

func (s *queryViewLocalSegment) Delete(ctx context.Context, pks storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
	return s.segment.Delete(ctx, pks, timestamps)
}

func (s *queryViewLocalSegment) Release(ctx context.Context) error {
	s.segment.Release(ctx)
	return nil
}
func (s *queryViewLocalSegment) PkCandidateExist() bool { return s.segment.PkCandidateExist() }
func (s *queryViewLocalSegment) BatchPkExist(cache *storage.BatchLocationsCache) []bool {
	return s.segment.BatchPkExist(cache)
}

func filterMaybeHitPrimaryKeys(segment queryViewLoadedSegment, pks storage.PrimaryKeys) storage.PrimaryKeys {
	if !segment.PkCandidateExist() || pks == nil || pks.Len() == 0 {
		return pks
	}
	raw := make([]storage.PrimaryKey, 0, pks.Len())
	for i := 0; i < pks.Len(); i++ {
		raw = append(raw, pks.Get(i))
	}
	hits := segment.BatchPkExist(storage.NewBatchLocationsCache(raw))
	if len(hits) != len(raw) {
		return pks
	}
	filtered, ok := newPrimaryKeysLike(pks)
	if !ok {
		return pks
	}
	for i, hit := range hits {
		if hit {
			filtered.MustAppend(raw[i])
		}
	}
	return filtered
}

func newPrimaryKeysLike(pks storage.PrimaryKeys) (storage.PrimaryKeys, bool) {
	switch pks.Type() {
	case schemapb.DataType_Int64:
		return storage.NewInt64PrimaryKeys(int64(pks.Len())), true
	case schemapb.DataType_VarChar:
		return storage.NewVarcharPrimaryKeys(int64(pks.Len())), true
	default:
		return nil, false
	}
}

var _ qnview.TransformDeleteApplier = (*queryViewTransformSegment)(nil)
