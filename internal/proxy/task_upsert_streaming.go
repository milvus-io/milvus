package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (ut *upsertTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-Execute")
	defer sp.End()
	log := log.Ctx(ctx).With(zap.String("collectionName", ut.req.CollectionName))

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		ez = hookutil.GetEzByCollProperties(ut.schema.GetProperties(), ut.collectionID).AsMessageConfig()
	}

	// Pack and append in a bounded loop. For a range-routed (namespace) collection the
	// insert and delete both target the single shard owning the request's namespace; if
	// that shard was fenced by a concurrent shard split the streamingnode rejects the
	// append with ShardFenced. The proxy drops its stale routing cache and re-packs
	// against the post-split topology. Because the request lands wholly on one shard, a
	// rejected append wrote nothing, so the retry cannot double-write.
	var resp streaming.AppendResponses
	appendErr := retry.Handle(ctx, func() (bool, error) {
		insertMsgs, err := ut.packInsertMessage(ctx, ez)
		if err != nil {
			log.Warn("pack insert message failed", zap.Error(err))
			return false, err
		}
		deleteMsgs, err := ut.packDeleteMessage(ctx, ez)
		if err != nil {
			log.Warn("pack delete message failed", zap.Error(err))
			return false, err
		}

		messages := append(insertMsgs, deleteMsgs...)
		resp = streaming.WAL().AppendMessages(ctx, messages...)
		if err := resp.UnwrapFirstError(); err != nil {
			if status.AsStreamingError(err).IsShardFenced() {
				// The target shard was fenced by a shard split; drop the stale routing
				// cache so the next attempt routes against the post-split topology.
				globalMetaCache.RemoveCollection(ctx, ut.req.GetDbName(), ut.req.GetCollectionName(), 0)
				return true, err
			}
			return false, err
		}
		return false, nil
	}, retry.Attempts(shardFencedRetryAttempts))
	if appendErr != nil {
		log.Warn("append messages to wal failed", zap.Error(appendErr))
		if status.AsStreamingError(appendErr).IsSchemaVersionMismatch() {
			return merr.ErrCollectionSchemaMismatch
		}
		return appendErr
	}
	// Update result.Timestamp for session consistency.
	ut.result.Timestamp = resp.MaxTimeTick()
	return nil
}

func (ut *upsertTask) packInsertMessage(ctx context.Context, ez *message.CipherConfig) ([]message.MutableMessage, error) {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy insertExecute upsert %d", ut.ID()))
	defer tr.Elapse("insert execute done when insertExecute")

	collectionName := ut.upsertMsg.InsertMsg.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, ut.req.GetDbName(), collectionName)
	if err != nil {
		return nil, err
	}
	ut.upsertMsg.InsertMsg.CollectionID = collID
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collID))
	getCacheDur := tr.RecordSpan()

	getMsgStreamDur := tr.RecordSpan()
	channelNames, err := ut.chMgr.getVChannels(collID)
	if err != nil {
		log.Warn("get vChannels failed when insertExecute",
			zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}

	// For a range-routed (namespace) collection, narrow the channel set to the single
	// shard owning this request's namespace; the request then lands wholly on that shard.
	collInfo, err := globalMetaCache.GetCollectionInfo(ctx, ut.req.GetDbName(), collectionName, collID)
	if err != nil {
		log.Warn("get collection info failed when insertExecute", zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}
	channelNames, err = resolveRangeRoutingChannels(collInfo, ut.req.GetNamespace(), channelNames)
	if err != nil {
		log.Warn("resolve range routing channel failed when insertExecute", zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}

	log.Debug("send insert request to virtual channels when insertExecute",
		zap.String("collection", ut.req.GetCollectionName()),
		zap.String("partition", ut.req.GetPartitionName()),
		zap.Int64("collection_id", collID),
		zap.Strings("virtual_channels", channelNames),
		zap.Int64("task_id", ut.ID()),
		zap.Duration("get cache duration", getCacheDur),
		zap.Duration("get msgStream duration", getMsgStreamDur))

	// start to repack insert data
	var msgs []message.MutableMessage
	if ut.partitionKeys == nil {
		msgs, err = repackInsertDataForStreamingService(ut.TraceCtx(), channelNames, ut.upsertMsg.InsertMsg, ut.result, ez, ut.schemaVersion)
	} else {
		msgs, err = repackInsertDataWithPartitionKeyForStreamingService(ut.TraceCtx(), channelNames, ut.upsertMsg.InsertMsg, ut.result, ut.partitionKeys, ez, ut.schemaVersion)
	}
	if err != nil {
		log.Warn("assign segmentID and repack insert data failed", zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}
	return msgs, nil
}

func (ut *upsertTask) packDeleteMessage(ctx context.Context, ez *message.CipherConfig) ([]message.MutableMessage, error) {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy deleteExecute upsert %d", ut.ID()))
	collID := ut.upsertMsg.DeleteMsg.CollectionID
	if ut.upsertMsg.DeleteMsg.PrimaryKeys == nil {
		// if primary keys are not set by queryPreExecute, use oldIDs to delete all given records
		ut.upsertMsg.DeleteMsg.PrimaryKeys = ut.oldIDs
	}
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collID))
	// hash primary keys to channels
	vChannels, err := ut.chMgr.getVChannels(collID)
	if err != nil {
		log.Warn("get vChannels failed when deleteExecute", zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}
	// For a range-routed (namespace) collection, narrow the channel set to the single
	// shard owning this request's namespace; the delete then lands wholly on that shard.
	collInfo, err := globalMetaCache.GetCollectionInfo(ctx, ut.req.GetDbName(), ut.upsertMsg.DeleteMsg.CollectionName, collID)
	if err != nil {
		log.Warn("get collection info failed when deleteExecute", zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}
	vChannels, err = resolveRangeRoutingChannels(collInfo, ut.req.GetNamespace(), vChannels)
	if err != nil {
		log.Warn("resolve range routing channel failed when deleteExecute", zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}
	result, numRows, err := repackDeleteMsgByHash(
		ctx,
		ut.upsertMsg.DeleteMsg.PrimaryKeys,
		vChannels, ut.idAllocator,
		ut.BeginTs(),
		ut.upsertMsg.DeleteMsg.CollectionID, ut.upsertMsg.DeleteMsg.CollectionName,
		ut.upsertMsg.DeleteMsg.PartitionID, ut.upsertMsg.DeleteMsg.PartitionName,
		ut.req.GetDbName(),
	)
	if err != nil {
		return nil, err
	}

	var msgs []message.MutableMessage
	for hashKey, deleteMsgs := range result {
		vchannel := vChannels[hashKey]
		for _, deleteMsg := range deleteMsgs {
			msg, err := message.NewDeleteMessageBuilderV1().
				WithHeader(&message.DeleteMessageHeader{
					CollectionId: ut.upsertMsg.DeleteMsg.CollectionID,
					Rows:         uint64(deleteMsg.NumRows),
				}).
				WithBody(deleteMsg.DeleteRequest).
				WithVChannel(vchannel).
				BuildMutable()
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, msg)
		}
	}

	log.Debug("Proxy Upsert deleteExecute done",
		zap.Int64("collection_id", collID),
		zap.Strings("virtual_channels", vChannels),
		zap.Int64("taskID", ut.ID()),
		zap.Int64("numRows", numRows),
		zap.Duration("prepare duration", tr.ElapseSpan()))

	return msgs, nil
}
