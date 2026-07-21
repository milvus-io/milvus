package shard

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/stats"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
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
		message.MessageTypeCreateCollection:   impl.handleCreateCollection,
		message.MessageTypeDropCollection:     impl.handleDropCollection,
		message.MessageTypeCreatePartition:    impl.handleCreatePartition,
		message.MessageTypeDropPartition:      impl.handleDropPartition,
		message.MessageTypeInsert:             impl.handleInsertMessage,
		message.MessageTypeDelete:             impl.handleDeleteMessage,
		message.MessageTypeManualFlush:        impl.handleManualFlushMessage,
		message.MessageTypeSchemaChange:       impl.handleSchemaChange,
		message.MessageTypeAlterCollection:    impl.handleAlterCollection,
		message.MessageTypeCreateSegment:      impl.handleCreateSegment,
		message.MessageTypeFlush:              impl.handleFlushSegment,
		message.MessageTypeFlushAll:           impl.handleFlushAllMessage,
		message.MessageTypeTruncateCollection: impl.handleTruncateCollectionMessage,
	}
}

// Name returns the name of the interceptor.
func (impl *shardInterceptor) Name() string {
	return interceptorName
}

// DoAppend assigns segment for every partition in the message.
func (impl *shardInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (msgID message.MessageID, err error) {
	op, ok := impl.ops[msg.MessageType()]
	if ok && (!funcutil.IsControlChannel(msg.VChannel()) || msg.IsPChannelLevel()) {
		// If the message type is registered in the interceptor, use the registered operation.
		// control channel message is only used to determine the DDL/DCL order,
		// perform no effect on the shard manager, so skip it.
		return op(ctx, msg, appendOp)
	}
	return appendOp(ctx, msg)
}

// handleCreateCollection handles the create collection message.
func (impl *shardInterceptor) handleCreateCollection(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	createCollectionMsg := message.MustAsMutableCreateCollectionMessageV1(msg)
	header := createCollectionMsg.Header()
	if err := impl.shardManager.CheckIfCollectionCanBeCreated(header.GetCollectionId()); err != nil {
		impl.shardManager.Logger().Warn(ctx, "collection already exists when creating collection", mlog.FieldCollectionID(header.GetCollectionId()))
		// The collection can not be created at current shard, ignored
		// TODO: idompotent for wal is required in future, but current milvus state is not recovered from wal.
		// return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	impl.shardManager.CreateCollection(message.MustAsImmutableCreateCollectionMessageV1(msg.IntoImmutableMessage(msgID)))
	if schema := createCollectionMsg.MustBody().GetCollectionSchema(); schema != nil {
		impl.allocFunctionRunners(header.GetCollectionId(), createCollectionMsg.VChannel(), schema)
	}
	return msgID, nil
}

// handleDropCollection handles the drop collection message.
func (impl *shardInterceptor) handleDropCollection(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	dropCollectionMessage := message.MustAsMutableDropCollectionMessageV1(msg)
	if err := impl.shardManager.CheckIfCollectionExists(dropCollectionMessage.Header().GetCollectionId()); err != nil {
		impl.shardManager.Logger().Warn(ctx, "collection not found when dropping collection", mlog.FieldCollectionID(dropCollectionMessage.Header().GetCollectionId()))
		// The collection can not be dropped at current shard, ignored
		// TODO: idompotent for wal is required in future, but current milvus state is not recovered from wal.
		// return nil, status.NewUnrecoverableError(err.Error())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	impl.shardManager.DropCollection(message.MustAsImmutableDropCollectionMessageV1(msg.IntoImmutableMessage(msgID)))
	function.GetManager().Release(dropCollectionMessage.Header().GetCollectionId(), walFunctionRunnerKey(dropCollectionMessage.VChannel()))
	return msgID, nil
}

// handleCreatePartition handles the create partition message.
func (impl *shardInterceptor) handleCreatePartition(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	createPartitionMessage := message.MustAsMutableCreatePartitionMessageV1(msg)
	h := createPartitionMessage.Header()
	if err := impl.shardManager.CheckIfPartitionCanBeCreated(shards.PartitionUniqueKey{CollectionID: h.GetCollectionId(), PartitionID: h.GetPartitionId()}); err != nil {
		impl.shardManager.Logger().Warn(ctx, "partition already exists when creating partition", mlog.FieldCollectionID(h.GetCollectionId()), mlog.FieldPartitionID(h.GetPartitionId()))
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
	if err := impl.shardManager.CheckIfPartitionExists(shards.PartitionUniqueKey{CollectionID: h.GetCollectionId(), PartitionID: h.GetPartitionId()}); err != nil {
		impl.shardManager.Logger().Warn(ctx, "partition not found when dropping partition", mlog.FieldCollectionID(h.GetCollectionId()), mlog.FieldPartitionID(h.GetPartitionId()))
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
	collectionID := header.GetCollectionId()
	schemaVersion := header.GetSchemaVersion()
	correctSchemaVersion, err := impl.shardManager.CheckIfCollectionSchemaVersionMatch(header)
	if err != nil {
		if errors.Is(err, shards.ErrCollectionNotFound) {
			return nil, status.NewUnrecoverableError("collection %d not found", collectionID)
		}
		if errors.Is(err, shards.ErrCollectionSchemaNotFound) {
			return nil, status.NewUnrecoverableError("collection %d schema not provided by create collection message", collectionID)
		}
		if errors.Is(err, shards.ErrCollectionSchemaVersionNotMatch) {
			impl.shardManager.Logger().Warn(ctx, "insertMessage schema version mismatch",
				mlog.FieldCollectionID(collectionID),
				mlog.Bool("schemaVersionProvided", header.SchemaVersion != nil),
				mlog.Int32("schemaVersion", schemaVersion),
				mlog.Int32("collectionSchemaVersion", correctSchemaVersion),
				mlog.Err(err))
			return nil, status.NewSchemaVersionMismatch("schema version mismatch, input schema version: %d, collection schema version: %d",
				schemaVersion, correctSchemaVersion)
		}
		impl.shardManager.Logger().Error(ctx, "unexpected error from CheckIfCollectionSchemaVersionMatch",
			mlog.FieldCollectionID(collectionID),
			mlog.Bool("schemaVersionProvided", header.SchemaVersion != nil),
			mlog.Int32("schemaVersion", schemaVersion),
			mlog.Err(err))
		return nil, status.NewUnrecoverableError("unexpected error from CheckIfCollectionSchemaVersionMatch: %s", err.Error())
	}
	schemaVersion = correctSchemaVersion
	if err := impl.materializeFunctionFields(ctx, insertMsg, header.GetCollectionId(), schemaVersion); err != nil {
		impl.shardManager.Logger().Warn(ctx, "failed to materialize function fields before WAL append",
			mlog.Int64("collectionID", header.GetCollectionId()),
			mlog.Int32("schemaVersion", schemaVersion),
			mlog.Err(err))
		return nil, status.NewUnrecoverableError("failed to materialize function fields before WAL append: %s", err.Error())
	}
	for _, partition := range header.GetPartitions() {
		if partition.BinarySize == 0 {
			// Proxy does not estimate binary size today. Use payload size after
			// write-before materialization when the estimate is absent.
			partition.BinarySize = uint64(msg.EstimateSize())
		}
		req := &shards.AssignSegmentRequest{
			CollectionID: header.GetCollectionId(),
			PartitionID:  partition.GetPartitionId(),
			ModifiedMetrics: stats.ModifiedMetrics{
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
			return nil, redo.ErrRedo
		}
		if errors.IsAny(err, shards.ErrTooLargeInsert, shards.ErrPartitionNotFound, shards.ErrCollectionNotFound) {
			// Message is too large, so retry operation is unrecoverable, can't be retry at client side.
			impl.shardManager.Logger().Warn(ctx, "unrecoverable insert operation", mlog.Object("message", msg), mlog.Err(err))
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

	impl.shardManager.ApplyDelete(deleteMessage)
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
	schemaChangeMsg := message.MustAsMutableSchemaChangeMessageV2(msg)
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

// handleAlterCollection handles the alter collection message.
func (impl *shardInterceptor) handleAlterCollection(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	putCollectionMsg := message.MustAsMutableAlterCollectionMessageV2(msg)
	header := putCollectionMsg.Header()

	// AlterCollection atomically flushes+fences segments (if schema change) and updates
	// in-memory schema — all within one critical region of the shard manager.
	segmentIDs, err := impl.shardManager.AlterCollection(putCollectionMsg)
	if err != nil {
		return nil, status.NewUnrecoverableError(err.Error())
	}

	// Embed flushed segment IDs into the WAL message header before appending.
	if len(segmentIDs) > 0 {
		header.FlushedSegmentIds = segmentIDs
		putCollectionMsg.OverwriteHeader(header)
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	if messageutil.IsSchemaChange(header) {
		if schema := putCollectionMsg.MustBody().GetUpdates().GetSchema(); schema != nil {
			impl.updateFunctionRunners(header.GetCollectionId(), putCollectionMsg.VChannel(), schema)
		}
	}
	return msgID, nil
}

// handleCreateSegment handles the create segment message.
func (impl *shardInterceptor) handleCreateSegment(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	createSegmentMsg := message.MustAsMutableCreateSegmentMessageV2(msg)
	h := createSegmentMsg.Header()
	if err := impl.shardManager.CheckIfSegmentCanBeCreated(shards.PartitionUniqueKey{CollectionID: h.GetCollectionId(), PartitionID: h.GetPartitionId()}, h.GetSegmentId()); err != nil {
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
	if utility.GetFlushFromOldArch(ctx) {
		// The flush message come from old arch, so it's not managed by shard manager.
		// We need to flush it into wal directly.
		impl.shardManager.Logger().Info(ctx, "flush segment from old arch, skip checking of shard manager", mlog.FieldMessage(msg))
		return appendOp(ctx, msg)
	}

	if err := impl.shardManager.CheckIfSegmentCanBeFlushed(shards.PartitionUniqueKey{CollectionID: h.GetCollectionId(), PartitionID: h.GetPartitionId()}, h.GetSegmentId()); err != nil {
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

// handleFlushAllMessage handles the flush all message.
func (impl *shardInterceptor) handleFlushAllMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	_, err := impl.shardManager.FlushAllAndFenceSegmentAllocUntil(msg.TimeTick())
	if err != nil {
		return nil, status.NewUnrecoverableError(err.Error())
	}
	return appendOp(ctx, msg)
}

// handleTruncateCollectionMessage handles the truncate collection message.
func (impl *shardInterceptor) handleTruncateCollectionMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	truncateCollectionMsg := message.MustAsMutableTruncateCollectionMessageV2(msg)
	header := truncateCollectionMsg.Header()
	segmentIDs, err := impl.shardManager.FlushAndFenceSegmentAllocUntil(header.GetCollectionId(), msg.TimeTick())
	if err != nil {
		return nil, status.NewUnrecoverableError(err.Error())
	}

	header.SegmentIds = segmentIDs
	truncateCollectionMsg.OverwriteHeader(header)

	return appendOp(ctx, msg)
}

// Close closes the segment interceptor.
func (impl *shardInterceptor) Close() {
	if schemaProvider, ok := impl.shardManager.(collectionSchemaProvider); ok {
		for collectionID, schemaInfo := range schemaProvider.GetAllCollectionSchemaInfos() {
			function.GetManager().Release(collectionID, walFunctionRunnerKey(schemaInfo.VChannel))
		}
	}
}
