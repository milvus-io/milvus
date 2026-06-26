package qvresource

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"go.uber.org/atomic"
)

type queryViewTransformSegment struct {
	segment     qvLoadedSegment
	releaser    qvSegmentManager
	vchannel    string
	startAfter  uint64
	applied     atomic.Uint64
	releaseOnce sync.Once
	releaseErr  error
}

func newQueryViewTransformSegment(segment qvLoadedSegment, releaser qvSegmentManager, vchannel string, startAfter uint64) *queryViewTransformSegment {
	return &queryViewTransformSegment{
		segment:    segment,
		releaser:   releaser,
		vchannel:   vchannel,
		startAfter: startAfter,
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

func (s *queryViewTransformSegment) TransformStartAfterTimeTick() uint64 {
	return s.startAfter
}

func (s *queryViewTransformSegment) ApplyTransform(ctx context.Context, entry *streamingpb.TransformLogEntry) error {
	if entry == nil {
		return nil
	}
	deleteEntry := entry.GetDelete()
	if deleteEntry == nil {
		s.applied.Store(entry.GetTimeTick())
		return nil
	}
	for _, block := range deleteEntry.GetBlocks() {
		if block.GetPartitionId() != s.PartitionID() {
			continue
		}
		pks, err := parseTransformDeletePrimaryKeys(block.GetPrimaryKeys())
		if err != nil {
			return err
		}
		timestamps := make([]typeutil.Timestamp, pks.Len())
		for i := range timestamps {
			timestamps[i] = entry.GetTimeTick()
		}
		pks, timestamps = filterMaybeHitPrimaryKeys(s.segment, pks, timestamps)
		if pks == nil || pks.Len() == 0 {
			continue
		}
		if err := s.segment.Delete(ctx, pks, timestamps); err != nil {
			return err
		}
	}
	s.applied.Store(entry.GetTimeTick())
	return nil
}

func (s *queryViewTransformSegment) AppliedTransformTimeTick() uint64 {
	return s.applied.Load()
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
