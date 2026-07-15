package qvresource

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type queryViewTransformSegment struct {
	segment     qvLoadedSegment
	releaser    qvSegmentManager
	vchannel    string
	startAfter  uint64
	mu          sync.Mutex
	applied     uint64
	waiters     map[uint64][]chan struct{}
	releaseOnce sync.Once
	releaseErr  error
}

func newQueryViewTransformSegment(segment qvLoadedSegment, releaser qvSegmentManager, vchannel string, startAfter uint64) *queryViewTransformSegment {
	return &queryViewTransformSegment{
		segment:    segment,
		releaser:   releaser,
		vchannel:   vchannel,
		startAfter: startAfter,
		applied:    startAfter,
		waiters:    make(map[uint64][]chan struct{}),
	}
}

func (s *queryViewTransformSegment) ID() int64 {
	return s.segment.ID()
}

func (s *queryViewTransformSegment) VChannel() string {
	return s.vchannel
}

func (s *queryViewTransformSegment) PartitionID() int64 {
	return s.segment.Partition()
}

func (s *queryViewTransformSegment) QuerySegment() segments.Segment {
	readable, ok := s.segment.(qvReadableSegment)
	if !ok {
		return nil
	}
	return readable.QuerySegment()
}

func (s *queryViewTransformSegment) Collection() *segments.Collection {
	readable, ok := s.segment.(qvReadableSegment)
	if !ok {
		return nil
	}
	return readable.Collection()
}

func (s *queryViewTransformSegment) TransformStartAfterTimeTick() uint64 {
	return s.startAfter
}

func (s *queryViewTransformSegment) ApplyTransform(ctx context.Context, entry *streamingpb.TransformLogEntry) error {
	if entry == nil {
		return nil
	}
	deleteEntry := entry.GetDelete()
	if deleteEntry == nil {
		s.markTransformApplied(entry.GetTimeTick())
		return nil
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	for _, block := range deleteEntry.GetBlocks() {
		blockPartitionID := block.GetPartitionId()
		if blockPartitionID != common.AllPartitionsID && blockPartitionID != s.PartitionID() {
			mlog.Debug(ctx, "querynode transform segment skipped delete block by partition",
				mlog.FieldSegmentID(s.ID()),
				mlog.FieldPartitionID(s.PartitionID()),
				mlog.Int64("blockPartitionID", blockPartitionID),
				mlog.Uint64("timeTick", entry.GetTimeTick()),
			)
			continue
		}
		pks, err := parseTransformDeletePrimaryKeys(block.GetPrimaryKeys())
		if err != nil {
			return err
		}
		originalPKCount := pks.Len()
		timestamps := make([]typeutil.Timestamp, pks.Len())
		for i := range timestamps {
			timestamps[i] = entry.GetTimeTick()
		}
		pks, timestamps = filterMaybeHitPrimaryKeys(s.segment, pks, timestamps)
		if pks == nil || pks.Len() == 0 {
			mlog.Debug(ctx, "querynode transform segment delete block filtered out",
				mlog.FieldSegmentID(s.ID()),
				mlog.FieldPartitionID(s.PartitionID()),
				mlog.Uint64("timeTick", entry.GetTimeTick()),
				mlog.Int("originalPKCount", originalPKCount),
				mlog.Int("filteredPKCount", 0),
			)
			continue
		}
		mlog.Debug(ctx, "querynode transform segment applies delete block",
			mlog.FieldSegmentID(s.ID()),
			mlog.FieldPartitionID(s.PartitionID()),
			mlog.Uint64("timeTick", entry.GetTimeTick()),
			mlog.Int("originalPKCount", originalPKCount),
			mlog.Int("filteredPKCount", pks.Len()),
		)
		if err := s.segment.Delete(ctx, pks, timestamps); err != nil {
			return err
		}
	}
	s.markTransformApplied(entry.GetTimeTick())
	return nil
}

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
		s.removeTransformWaiter(timetick, waiter)
		return ctx.Err()
	}
}

func (s *queryViewTransformSegment) markTransformApplied(timetick uint64) {
	var ready []chan struct{}
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

func (s *queryViewTransformSegment) removeTransformWaiter(timetick uint64, waiter chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	waiters := s.waiters[timetick]
	for i, candidate := range waiters {
		if candidate != waiter {
			continue
		}
		waiters = append(waiters[:i], waiters[i+1:]...)
		break
	}
	if len(waiters) == 0 {
		delete(s.waiters, timetick)
		return
	}
	s.waiters[timetick] = waiters
}

func parseTransformDeletePrimaryKeys(ids *schemapb.IDs) (pks storage.PrimaryKeys, err error) {
	if ids == nil || ids.IdField == nil {
		return nil, fmt.Errorf("transform delete primary keys are empty")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			pks = nil
			err = fmt.Errorf("failed to parse transform delete primary keys: %v", recovered)
		}
	}()
	pks = storage.ParseIDs2PrimaryKeysBatch(ids)
	if pks == nil {
		return nil, fmt.Errorf("failed to parse transform delete primary keys")
	}
	return pks, nil
}

func (s *queryViewTransformSegment) Release(ctx context.Context) error {
	s.releaseOnce.Do(func() {
		s.releaseErr = s.segment.Release(ctx)
	})
	return s.releaseErr
}

func filterMaybeHitPrimaryKeys(segment qvLoadedSegment, pks storage.PrimaryKeys, timestamps []typeutil.Timestamp) (storage.PrimaryKeys, []typeutil.Timestamp) {
	candidate, ok := segment.(qvPKCandidateSegment)
	if !ok || !candidate.PkCandidateExist() || pks == nil || pks.Len() == 0 || len(timestamps) != pks.Len() {
		return pks, timestamps
	}

	raw := make([]storage.PrimaryKey, 0, pks.Len())
	for i := 0; i < pks.Len(); i++ {
		raw = append(raw, pks.Get(i))
	}
	hits := candidate.BatchPkExist(storage.NewBatchLocationsCache(raw))
	if len(hits) != len(raw) {
		return pks, timestamps
	}

	filtered, ok := newPrimaryKeysLike(pks)
	if !ok {
		return pks, timestamps
	}
	filteredTimestamps := make([]typeutil.Timestamp, 0, len(timestamps))
	for i, hit := range hits {
		if !hit {
			continue
		}
		filtered.MustAppend(raw[i])
		filteredTimestamps = append(filteredTimestamps, timestamps[i])
	}
	return filtered, filteredTimestamps
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
