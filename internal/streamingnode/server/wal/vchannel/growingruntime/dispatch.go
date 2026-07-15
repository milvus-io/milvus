package growingruntime

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (r *Runtime) addSegment(segment *growingSegment) bool {
	if r == nil || segment == nil || segment.id() == 0 {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	if _, ok := r.segments[segment.id()]; ok {
		panic("duplicated growing segment")
	}
	r.segments[segment.id()] = segment
	r.segmentIDs = append(r.segmentIDs, segment.id())
	return true
}

func (r *Runtime) getOrCreateSegment(segmentID int64, partitionID int64) *growingSegment {
	if r == nil || segmentID == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	if segment := r.segments[segmentID]; segment != nil {
		return segment
	}
	segment := newGrowingSegment(r.collection, segmentID, partitionID)
	r.segments[segmentID] = segment
	r.segmentIDs = append(r.segmentIDs, segmentID)
	return segment
}

func (r *Runtime) segmentsSnapshot() []*growingSegment {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	segments := make([]*growingSegment, 0, len(r.segments))
	for _, segment := range r.segments {
		segments = append(segments, segment)
	}
	return segments
}

func (r *Runtime) dispatchMessage(ctx context.Context, msg message.ImmutableMessage) error {
	if r == nil || msg == nil {
		return nil
	}
	switch msg.MessageType() {
	case message.MessageTypeCreateSegment:
		created := message.MustAsImmutableCreateSegmentMessageV2(msg)
		segment := r.getOrCreateSegment(created.Header().GetSegmentId(), created.Header().GetPartitionId())
		if segment == nil {
			return nil
		}
		return segment.ensureCSegment()
	case message.MessageTypeInsert:
		return r.applyInsertMessage(ctx, msg)
	case message.MessageTypeTxn:
		if err := r.applyInsertMessage(ctx, msg); err != nil {
			return err
		}
		return r.applyLiveDeleteMessage(ctx, msg)
	case message.MessageTypeDelete:
		return r.applyLiveDeleteMessage(ctx, msg)
	case message.MessageTypeFlush:
		flushed := message.MustAsImmutableFlushMessageV2(msg)
		segment := r.getOrCreateSegment(flushed.Header().GetSegmentId(), flushed.Header().GetPartitionId())
		if segment == nil {
			return nil
		}
		segment.markFlushed()
		return nil
	default:
		return nil
	}
}

func (r *Runtime) applyInsertMessage(ctx context.Context, raw message.ImmutableMessage) error {
	return walview.ForEachSegmentInsertMessage(raw, 0, func(insert walview.SegmentInsertMessage) error {
		assignment := insert.Assignment
		if assignment == nil || assignment.GetSegmentAssignment() == nil {
			return errors.New("growing insert message has nil segment assignment")
		}
		segmentID := assignment.GetSegmentAssignment().GetSegmentId()
		segment := r.getOrCreateSegment(segmentID, assignment.GetPartitionId())
		if segment == nil {
			return nil
		}
		return segment.applyInsert(ctx, insert)
	})
}

func (r *Runtime) applyLiveDeleteMessage(ctx context.Context, msg message.ImmutableMessage) error {
	switch msg.MessageType() {
	case message.MessageTypeDelete:
		deleted := message.MustAsImmutableDeleteMessageV1(msg)
		return r.applyDeleteRequest(ctx, msg.TimeTick(), deleted.MustBody())
	case message.MessageTypeTxn:
		txn := message.AsImmutableTxnMessage(msg)
		if txn == nil {
			return errors.New("invalid txn WAL message")
		}
		return txn.RangeOver(func(inner message.ImmutableMessage) error {
			if inner.MessageType() != message.MessageTypeDelete {
				return nil
			}
			deleted := message.MustAsImmutableDeleteMessageV1(inner)
			return r.applyDeleteRequest(ctx, msg.TimeTick(), deleted.MustBody())
		})
	default:
		return nil
	}
}

func (r *Runtime) applyDeleteRequest(ctx context.Context, timeTick uint64, request *msgpb.DeleteRequest) error {
	if request == nil {
		return nil
	}
	return r.deleteFromAllSegments(ctx, storage.ParseIDs2PrimaryKeysBatch(request.GetPrimaryKeys()), deleteTimestampsFromRequest(timeTick, request))
}

func (r *Runtime) applyTransformLogEntry(ctx context.Context, entry *streamingpb.TransformLogEntry) error {
	if entry == nil || entry.GetDelete() == nil {
		return nil
	}
	for _, block := range entry.GetDelete().GetBlocks() {
		if err := r.deleteFromAllSegments(ctx, storage.ParseIDs2PrimaryKeysBatch(block.GetPrimaryKeys()), deleteTimestampsFromTransformLogBlock(entry.GetTimeTick(), block)); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) deleteFromAllSegments(ctx context.Context, primaryKeys storage.PrimaryKeys, timestamps []typeutil.Timestamp) error {
	if primaryKeys.Len() == 0 {
		return nil
	}
	for _, segment := range r.segmentsSnapshot() {
		if err := segment.applyDelete(ctx, primaryKeys, timestamps); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) markSegmentSealed(segmentID int64, sealedAt qviews.DataVersion) {
	segment := r.getOrCreateSegment(segmentID, 0)
	if segment == nil {
		return
	}
	segment.markFlushed()
	segment.markSealed(sealedAt)

	var release *growingSegment
	r.mu.Lock()
	if r.hasTruncateDataVersion && segment.shouldRelease(r.truncateDataVersion) {
		r.removeSegmentMetadataLocked(segmentID)
		release = segment
	}
	r.mu.Unlock()
	if release != nil {
		release.release()
	}
}
