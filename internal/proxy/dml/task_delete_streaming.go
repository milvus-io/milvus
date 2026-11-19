package dml

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Execute is a function to delete task by streaming service
// we only overwrite the Execute function
func (dt *DeleteTask) Execute(ctx context.Context) (err error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Delete-Execute")
	defer sp.End()

	if len(dt.req.GetExpr()) == 0 {
		return merr.WrapErrParameterInvalid("valid expr", "empty expr", "invalid expression")
	}

	dt.tr = timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute delete %d", dt.ID()))

	var collectionSchema *schemapb.CollectionSchema
	if dt.req.Namespace != nil || hookutil.IsClusterEncryptionEnabled() {
		collectionSchema = dt.schema.CollectionSchema
	}

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		ez = hookutil.GetEzByCollProperties(collectionSchema.GetProperties(), dt.collectionID).AsMessageConfig()
	}

	// Route, repack and append until every tombstone is durable. A shard split
	// fences its source, which refuses an append with SHARD_FENCED; the
	// tombstones of the refused messages are routed again against a fresh
	// describe of the collection, while those of every message that committed
	// are never sent again. A re-sent tombstone would take a later tick and
	// delete whatever was inserted in between (see shard_fenced_retry.go). A
	// delete by expression reaches here once per batch of primary keys its
	// query streams back, so each batch settles per message the same way.
	fence := newSplitFence()
	pending := newPendingRows(typeutil.GetSizeOfIDs(dt.primaryKeys))
	attempt := 0
	// prepareFailed ends the request on a failure met before the first append,
	// and leaves a later one to retryPreparation.
	prepareFailed := func(ctx context.Context, err error) (bool, error) {
		if attempt > 1 {
			return fence.retryPreparation(ctx, dt.GetMetaCache(), dt.collectionID, err)
		}
		return false, err
	}
	err = fence.run(ctx, func(ctx context.Context) (bool, error) {
		route, err := resolveWriteRoute(ctx, dt.GetMetaCache(), dt.collectionID)
		attempt++
		if err != nil {
			return prepareFailed(ctx, err)
		}
		dt.vChannels = route.vchannels

		result, offsets, _, err := repackPendingDeleteMsgs(
			ctx, route.table, dt.primaryKeys, pending.pendingSet(),
			dt.vChannels, dt.idAllocator,
			dt.ts, dt.collectionID,
			dt.req.GetCollectionName(),
			dt.partitionID, dt.req.GetPartitionName(),
			dt.req.GetDbName(),
			dt.req.Namespace,
			collectionSchema,
		)
		if err != nil {
			return prepareFailed(ctx, err)
		}
		msgs, msgOffsets, err := dt.buildDeleteMessages(result, offsets, ez)
		if err != nil {
			return prepareFailed(ctx, err)
		}

		mlog.Debug(ctx, "send delete request to virtual channels",
			mlog.String("collectionName", dt.req.GetCollectionName()),
			mlog.Int64("collectionID", dt.collectionID),
			mlog.Strings("virtual_channels", dt.vChannels),
			mlog.Int64("taskID", dt.ID()),
			mlog.Int("attempt", attempt),
			mlog.Duration("prepare duration", dt.tr.RecordSpan()))

		fence.routedBy(route)
		resp := streaming.WAL().AppendMessages(ctx, msgs...)
		err = fence.settle(resp, msgs, msgOffsets, pending)
		if err != nil {
			return false, err
		}
		if pending.done() {
			return false, nil
		}
		return fence.refresh(ctx, dt.GetMetaCache(), dt.collectionID, nil)
	})
	if err != nil {
		mlog.Warn(ctx, "append messages to wal failed", mlog.Err(err))
		return fence.finalError(ctx, dt.collectionID, err, writtenPart{what: "deletions", pending: pending})
	}
	dt.sessionTS = fence.maxTimeTick
	dt.count += int64(typeutil.GetSizeOfIDs(dt.primaryKeys))
	return nil
}

// buildDeleteMessages turns the repacked tombstones into WAL messages, one per
// repacked delete, addressed to the vchannel its hash key names, and returns
// with each message the primary key offsets it carries.
func (dt *DeleteTask) buildDeleteMessages(
	result map[uint32][]*msgstream.DeleteMsg,
	offsets map[*msgstream.DeleteMsg][]int,
	ez *message.CipherConfig,
) ([]message.MutableMessage, [][]int, error) {
	var msgs []message.MutableMessage
	var msgOffsets [][]int
	for hashKey, deleteMsgs := range result {
		vchannel := dt.vChannels[hashKey]
		for _, deleteMsg := range deleteMsgs {
			msg, err := message.NewDeleteMessageBuilderV1().
				WithHeader(&message.DeleteMessageHeader{
					CollectionId: dt.collectionID,
					Rows:         uint64(deleteMsg.NumRows),
				}).
				WithBody(deleteMsg.DeleteRequest).
				WithVChannel(vchannel).
				WithCipher(ez).
				BuildMutable()
			if err != nil {
				return nil, nil, err
			}
			msgs = append(msgs, msg)
			msgOffsets = append(msgOffsets, offsets[deleteMsg])
		}
	}
	return msgs, msgOffsets, nil
}
