package qvresource

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type queryViewTransformSegment struct {
	segment     qvLoadedSegment
	vchannel    string
	startAfter  uint64
	releaseOnce sync.Once
	releaseErr  error
}

func newQueryViewTransformSegment(segment qvLoadedSegment, vchannel string, startAfter uint64) *queryViewTransformSegment {
	return &queryViewTransformSegment{
		segment:    segment,
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
	if ctx == nil {
		ctx = context.TODO()
	}
	return messageutil.VisitTransformMutations(entry, func(block *streamingpb.TransformDeleteBlock) error {
		blockPartitionID := block.GetPartitionId()
		if blockPartitionID != common.AllPartitionsID && blockPartitionID != s.PartitionID() {
			mlog.Debug(ctx, "querynode transform segment skipped delete block by partition",
				mlog.FieldSegmentID(s.ID()),
				mlog.FieldPartitionID(s.PartitionID()),
				mlog.Int64("blockPartitionID", blockPartitionID),
				mlog.Uint64("timeTick", entry.GetTimeTick()),
			)
			return nil
		}
		pks := storage.ParseIDs2PrimaryKeysBatch(block.GetPrimaryKeys())
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
			return nil
		}
		mlog.Debug(ctx, "querynode transform segment applies delete block",
			mlog.FieldSegmentID(s.ID()),
			mlog.FieldPartitionID(s.PartitionID()),
			mlog.Uint64("timeTick", entry.GetTimeTick()),
			mlog.Int("originalPKCount", originalPKCount),
			mlog.Int("filteredPKCount", pks.Len()),
		)
		return s.segment.Delete(ctx, pks, timestamps)
	}, func(ordinal uint32, body *messagespb.UpdateMessageBody) error {
		if body.GetPartitionId() != common.AllPartitionsID && body.GetPartitionId() != s.PartitionID() {
			return nil
		}
		pks := storage.ParseIDs2PrimaryKeysBatch(body.GetPrimaryKeys())
		candidates, _ := filterMaybeHitPrimaryKeys(s.segment, pks, nil)
		if candidates.Len() == 0 {
			return nil
		}
		if candidates.Len() == pks.Len() {
			return s.segment.ApplyUpdates(ctx, entry.GetTimeTick(), ordinal, body)
		}
		ids, err := storage.ParsePrimaryKeysBatch2IDs(candidates)
		if err != nil {
			return err
		}
		return s.segment.ApplyUpdates(ctx, entry.GetTimeTick(), ordinal, &messagespb.UpdateMessageBody{PartitionId: body.PartitionId, PrimaryKeys: ids, Assignments: body.Assignments})
	})
}

func (s *queryViewTransformSegment) Release(ctx context.Context) error {
	s.releaseOnce.Do(func() {
		s.releaseErr = s.segment.Release(ctx)
	})
	return s.releaseErr
}

func filterMaybeHitPrimaryKeys(segment qvLoadedSegment, pks storage.PrimaryKeys, timestamps []typeutil.Timestamp) (storage.PrimaryKeys, []typeutil.Timestamp) {
	candidate, ok := segment.(qvPKCandidateSegment)
	if !ok || !candidate.PkCandidateExist() || pks == nil || pks.Len() == 0 || (timestamps != nil && len(timestamps) != pks.Len()) {
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
		if timestamps != nil {
			filteredTimestamps = append(filteredTimestamps, timestamps[i])
		}
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

func (s *queryViewTransformSegment) GCUpdates(ctx context.Context, ts uint64) error {
	return s.segment.GCUpdates(ctx, ts)
}
