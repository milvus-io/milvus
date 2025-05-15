package shard

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

const interceptorName = "shard"

var _ interceptors.InterceptorWithMetrics = (*shardInterceptor)(nil)

// shardInterceptor is the implementation of shard management interceptor.
type shardInterceptor struct {
	shardManager shards.ShardManager
	ops          map[message.MessageType]interceptors.AppendInterceptorCall
}

// initOpTable initializes the operation table for the segment interceptor.
func (impl *shardInterceptor) initOpTable() {
	impl.ops = map[message.MessageType]interceptors.AppendInterceptorCall{
		message.MessageTypeCreateCollection: impl.handleCreateCollection,
		message.MessageTypeDropCollection:   impl.handleDropCollection,
		message.MessageTypeCreatePartition:  impl.handleCreatePartition,
		message.MessageTypeDropPartition:    impl.handleDropPartition,
		message.MessageTypeInsert:           impl.handleInsertMessage,
		message.MessageTypeDelete:           impl.handleDeleteMessage,
		message.MessageTypeManualFlush:      impl.handleManualFlushMessage,
		message.MessageTypeSchemaChange:     impl.handleSchemaChange,
		message.MessageTypeCreateSegment:    impl.handleCreateSegment,
		message.MessageTypeFlush:            impl.handleFlushSegment,
	}
}

// Name returns the name of the interceptor.
func (impl *shardInterceptor) Name() string {
	return interceptorName
}

// DoAppend assigns segment for every partition in the message.
func (impl *shardInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (msgID message.MessageID, err error) {
	op, ok := impl.ops[msg.MessageType()]
	if ok {
		// If the message type is registered in the interceptor, use the registered operation.
		return op(ctx, msg, appendOp)
	}
	return appendOp(ctx, msg)
}

// handleCreateCollection handles the create collection message.
func (impl *shardInterceptor) handleCreateCollection(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	createCollectionMsg := message.MustAsMutableCreateCollectionMessageV1(msg)
	header := createCollectionMsg.Header()
	if err := impl.shardManager.CheckIfCollectionCanBeCreated(header.GetCollectionId()); err != nil {
		impl.shardManager.Logger().Warn("collection already exists when creating collection", zap.Int64("collectionID", header.GetCollectionId()))
		// The collection can not be created at current shard, ignored
		// TODO: idompotent for wal is required in future, but current milvus state is not recovered from wal.
		// return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	impl.shardManager.CreateCollection(message.MustAsImmutableCreateCollectionMessageV1(msg.IntoImmutableMessage(msgID)))
	return msgID, nil
}

// handleDropCollection handles the drop collection message.
func (impl *shardInterceptor) handleDropCollection(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	dropCollectionMessage := message.MustAsMutableDropCollectionMessageV1(msg)
	if err := impl.shardManager.CheckIfCollectionExists(dropCollectionMessage.Header().GetCollectionId()); err != nil {
		impl.shardManager.Logger().Warn("collection not found when dropping collection", zap.Int64("collectionID", dropCollectionMessage.Header().GetCollectionId()))
		// The collection can not be dropped at current shard, ignored
		// TODO: idompotent for wal is required in future, but current milvus state is not recovered from wal.
		// return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	impl.shardManager.DropCollection(message.MustAsImmutableDropCollectionMessageV1(msg.IntoImmutableMessage(msgID)))
	return msgID, nil
}

// handleCreatePartition handles the create partition message.
func (impl *shardInterceptor) handleCreatePartition(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	createPartitionMessage := message.MustAsMutableCreatePartitionMessageV1(msg)
	h := createPartitionMessage.Header()
	if err := impl.shardManager.CheckIfPartitionCanBeCreated(h.GetCollectionId(), h.GetPartitionId()); err != nil {
		impl.shardManager.Logger().Warn("partition already exists when creating partition", zap.Int64("collectionID", h.GetCollectionId()), zap.Int64("partitionID", h.GetPartitionId()))
		// TODO: idompotent for wal is required in future, but current milvus state is not recovered from wal.
		// return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return nil, err
	}
	impl.shardManager.CreatePartition(message.MustAsImmutableCreatePartitionMessageV1(msg.IntoImmutableMessage(msgID)))
	return msgID, nil
}

// handleDropPartition handles the drop partition message.
func (impl *shardInterceptor) handleDropPartition(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	dropPartitionMessage := message.MustAsMutableDropPartitionMessageV1(msg)
	h := dropPartitionMessage.Header()
	if err := impl.shardManager.CheckIfPartitionExists(h.GetCollectionId(), h.GetPartitionId()); err != nil {
		impl.shardManager.Logger().Warn("partition not found when dropping partition", zap.Int64("collectionID", h.GetCollectionId()), zap.Int64("partitionID", h.GetPartitionId()))
		// The partition can not be dropped at current shard, ignored
		// TODO: idompotent for wal is required in future, but current milvus state is not recovered from wal.
		// return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	impl.shardManager.DropPartition(message.MustAsImmutableDropPartitionMessageV1(msg.IntoImmutableMessage(msgID)))
	return msgID, nil
}

// handleInsertMessage handles the insert message.
func (impl *shardInterceptor) handleInsertMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	insertMsg := message.MustAsMutableInsertMessageV1(msg)
	// Assign segment for insert message.
	// !!! Current implementation a insert message only has one parition, but we need to merge the message for partition-key in future.
	header := insertMsg.Header()
	for _, partition := range header.GetPartitions() {
		if partition.BinarySize == 0 {
			// binary size should be set at proxy with estimate, but we don't implement it right now.
			// use payload size instead.
			partition.BinarySize = uint64(len(msg.Payload()))
		}
		req := &shards.AssignSegmentRequest{
			CollectionID: header.GetCollectionId(),
			PartitionID:  partition.GetPartitionId(),
			InsertMetrics: stats.InsertMetrics{
				Rows:       partition.GetRows(),
				BinarySize: partition.GetBinarySize(),
			},
			TimeTick: msg.TimeTick(),
		}
		if session := txn.GetTxnSessionFromContext(ctx); session != nil {
			// because the shard manager use the interface, txn is a struct,
			// so we need to check nil before the assignment.
			req.TxnSession = session
		}
		result, err := impl.shardManager.AssignSegment(req)
		if errors.IsAny(err, shards.ErrTimeTickTooOld, shards.ErrWaitForNewSegment, shards.ErrFencedAssign) {
			// 1. time tick is too old for segment assignment.
			// 2. partition is fenced.
			// 3. segment is not ready.
			// we just redo it to refresh a new latest timetick.
			if impl.shardManager.Logger().Level().Enabled(zap.DebugLevel) {
				impl.shardManager.Logger().Debug("segment assign interceptor redo insert message", zap.Object("message", msg), zap.Error(err))
			}
			return nil, redo.ErrRedo
		}
		if errors.IsAny(err, shards.ErrTooLargeInsert, shards.ErrPartitionNotFound, shards.ErrCollectionNotFound) {
			// Message is too large, so retry operation is unrecoverable, can't be retry at client side.
			impl.shardManager.Logger().Warn("unrecoverable insert operation", zap.Object("message", msg), zap.Error(err))
			return nil, status.NewUnrecoverableError("fail to assign segment, %s", err.Error())
		}
		if err != nil {
			return nil, err
		}
		// once the segment assignment is done, we need to ack the result,
		// if other partitions failed to assign segment or wal write failure,
		// the segment assignment will not rolled back for simple implementation.
		defer result.Ack()

		// Attach segment assignment to message.
		partition.SegmentAssignment = &message.SegmentAssignment{
			SegmentId: result.SegmentID,
		}
	}
	// Update the insert message headers.
	insertMsg.OverwriteHeader(header)
	return appendOp(ctx, msg)
}

// handleDeleteMessage handles the delete message.
func (impl *shardInterceptor) handleDeleteMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	deleteMessage := message.MustAsMutableDeleteMessageV1(msg)
	header := deleteMessage.Header()
	if err := impl.shardManager.CheckIfCollectionExists(header.GetCollectionId()); err != nil {
		// The collection can not be deleted at current shard, ignored
		return nil, status.NewUnrecoverableError(err.Error())
	}

	return appendOp(ctx, msg)
}

// handleManualFlushMessage handles the manual flush message.
func (impl *shardInterceptor) handleManualFlushMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	maunalFlushMsg := message.MustAsMutableManualFlushMessageV2(msg)
	header := maunalFlushMsg.Header()
	segmentIDs, err := impl.shardManager.FlushAndFenceSegmentAllocUntil(header.GetCollectionId(), msg.TimeTick())
	if err != nil {
		return nil, status.NewUnrecoverableError(err.Error())
	}

	// Modify the extra response for manual flush message.
	utility.ModifyAppendResultExtra(ctx, func(old *message.ManualFlushExtraResponse) *message.ManualFlushExtraResponse {
		return &messagespb.ManualFlushExtraResponse{SegmentIds: segmentIDs}
	})
	header.SegmentIds = segmentIDs
	maunalFlushMsg.OverwriteHeader(header)

	return appendOp(ctx, msg)
}

// handleSchemaChange handles the schema change message.
func (impl *shardInterceptor) handleSchemaChange(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	schemaChangeMsg := message.MustAsMutableCollectionSchemaChangeV2(msg)
	header := schemaChangeMsg.Header()
	segmentIDs, err := impl.shardManager.FlushAndFenceSegmentAllocUntil(header.GetCollectionId(), msg.TimeTick())
	if err != nil {
		return nil, status.NewUnrecoverableError(err.Error())
	}

	// Modify the header of schema change message, carry with the all flushed segment ids.
	header.FlushedSegmentIds = segmentIDs
	schemaChangeMsg.OverwriteHeader(header)

	return appendOp(ctx, msg)
}

// handleCreateSegment handles the create segment message.
func (impl *shardInterceptor) handleCreateSegment(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	createSegmentMsg := message.MustAsMutableCreateSegmentMessageV2(msg)
	h := createSegmentMsg.Header()
	if err := impl.shardManager.CheckIfSegmentCanBeCreated(h.GetCollectionId(), h.GetPartitionId(), h.GetSegmentId()); err != nil {
		// The segment can not be created at current shard, ignored
		return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return nil, err
	}
	impl.shardManager.CreateSegment(message.MustAsImmutableCreateSegmentMessageV2(msg.IntoImmutableMessage(msgID)))
	return msgID, nil
}

func (impl *shardInterceptor) handleFlushSegment(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	flushMsg := message.MustAsMutableFlushMessageV2(msg)
	h := flushMsg.Header()
	if err := impl.shardManager.CheckIfSegmentCanBeFlushed(h.GetCollectionId(), h.GetPartitionId(), h.GetSegmentId()); err != nil {
		// The segment can not be flushed at current shard, ignored
		return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return nil, err
	}
	impl.shardManager.FlushSegment(message.MustAsImmutableFlushMessageV2(msg.IntoImmutableMessage(msgID)))
	return msgID, nil
}

// Close closes the segment interceptor.
func (impl *shardInterceptor) Close() {}
