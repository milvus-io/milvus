package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Execute is a function to delete task by streaming service
// we only overwrite the Execute function
func (dt *deleteTask) Execute(ctx context.Context) (err error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Delete-Execute")
	defer sp.End()

	if len(dt.req.GetExpr()) == 0 {
		return merr.WrapErrParameterInvalid("valid expr", "empty expr", "invalid expression")
	}

	dt.tr = timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute delete %d", dt.ID()))

	var collectionSchema *schemapb.CollectionSchema
	if dt.req.Namespace != nil || hookutil.IsClusterEncryptionEnabled() {
		schema, err := globalMetaCache.GetCollectionSchema(ctx, dt.req.GetDbName(), dt.req.GetCollectionName())
		if err != nil {
			mlog.Warn(ctx, "get collection schema from global meta cache failed", mlog.String("collectionName", dt.req.GetCollectionName()), mlog.Err(err))
			return err
		}
		collectionSchema = schema.CollectionSchema
	}

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		ez = hookutil.GetEzByCollProperties(collectionSchema.GetProperties(), dt.collectionID).AsMessageConfig()
	}

	// Repack and append in a bounded loop. For a range-routed (namespace) collection the
	// delete targets the single shard owning the request's namespace; if that shard was
	// fenced by a concurrent shard split the streamingnode rejects the append with
	// ShardFenced. The proxy drops its stale routing cache and re-resolves the channel
	// against the post-split topology. Because the delete lands wholly on one shard, a
	// rejected append wrote nothing, so the retry cannot double-write.
	var resp streaming.AppendResponses
	var numRows int64
	appendErr := retry.Handle(ctx, func() (bool, error) {
		// Re-resolve the channel set each attempt so a refreshed topology is picked up.
		vChannels := dt.vChannels
		collInfo, err := globalMetaCache.GetCollectionInfo(ctx, dt.req.GetDbName(), dt.req.GetCollectionName(), dt.collectionID)
		if err != nil {
			return false, err
		}
		vChannels, err = resolveRangeRoutingChannels(collInfo, dt.req.GetNamespace(), vChannels)
		if err != nil {
			return false, err
		}

		result, rows, err := repackDeleteMsgByHash(
			ctx, dt.primaryKeys,
			vChannels, dt.idAllocator,
			dt.ts, dt.collectionID,
			dt.req.GetCollectionName(),
			dt.partitionID, dt.req.GetPartitionName(),
			dt.req.GetDbName(),
			dt.req.Namespace,
			collectionSchema,
		)
		if err != nil {
			return false, err
		}

		var msgs []message.MutableMessage
		for hashKey, deleteMsgs := range result {
			vchannel := vChannels[hashKey]
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
					return false, err
				}
				msgs = append(msgs, msg)
			}
		}

		mlog.Debug(ctx, "send delete request to virtual channels",
			mlog.String("collectionName", dt.req.GetCollectionName()),
			mlog.Int64("collectionID", dt.collectionID),
			mlog.Strings("virtual_channels", vChannels),
			mlog.Int64("taskID", dt.ID()),
			mlog.Duration("prepare duration", dt.tr.RecordSpan()))

		resp = streaming.WAL().AppendMessages(ctx, msgs...)
		if err := resp.UnwrapFirstError(); err != nil {
			if status.AsStreamingError(err).IsShardFenced() {
				// The target shard was fenced by a shard split; drop the stale routing
				// cache so the next attempt routes against the post-split topology.
				globalMetaCache.RemoveCollection(ctx, dt.req.GetDbName(), dt.req.GetCollectionName(), 0)
				return true, err
			}
			return false, err
		}
		numRows = rows
		return false, nil
	}, retry.Attempts(shardFencedRetryAttempts))
	if appendErr != nil {
		mlog.Warn(ctx, "append messages to wal failed", mlog.Err(appendErr))
		return appendErr
	}
	dt.sessionTS = resp.MaxTimeTick()
	dt.count += numRows
	return nil
}
