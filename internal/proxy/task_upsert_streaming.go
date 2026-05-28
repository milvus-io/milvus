package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (ut *upsertTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-Execute")
	defer sp.End()
	log := mlog.With(mlog.FieldCollectionName(ut.req.CollectionName))

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		ez = hookutil.GetEzByCollProperties(ut.schema.GetProperties(), ut.collectionID).AsMessageConfig()
	}

	insertMsgs, err := ut.packInsertMessage(ctx, ez)
	if err != nil {
		log.Warn(ctx, "pack insert message failed", mlog.Err(err))
		return err
	}
	deleteMsgs, err := ut.packDeleteMessage(ctx, ez)
	if err != nil {
		log.Warn(ctx, "pack delete message failed", mlog.Err(err))
		return err
	}

	messages := append(insertMsgs, deleteMsgs...)
	resp := streaming.WAL().AppendMessages(ctx, messages...)
	if err := resp.UnwrapFirstError(); err != nil {
		log.Warn(ctx, "append messages to wal failed", mlog.Err(err))
		if status.AsStreamingError(err).IsSchemaVersionMismatch() {
			return merr.ErrCollectionSchemaMismatch
		}
		return err
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
	log := mlog.With(
		mlog.FieldCollectionID(collID))
	getCacheDur := tr.RecordSpan()

	getMsgStreamDur := tr.RecordSpan()
	channelNames, err := ut.chMgr.getVChannels(collID)
	if err != nil {
		log.Warn(ctx, "get vChannels failed when insertExecute",
			mlog.Err(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}

	log.Debug(ctx, "send insert request to virtual channels when insertExecute",
		mlog.String("collection", ut.req.GetCollectionName()),
		mlog.String("partition", ut.req.GetPartitionName()),
		mlog.FieldCollectionID(collID),
		mlog.Strings("virtual_channels", channelNames),
		mlog.FieldTaskID(ut.ID()),
		mlog.Duration("get cache duration", getCacheDur),
		mlog.Duration("get msgStream duration", getMsgStreamDur))

	// start to repack insert data
	var msgs []message.MutableMessage
	if ut.partitionKeys == nil {
		msgs, err = repackInsertDataForStreamingService(ut.TraceCtx(), channelNames, ut.upsertMsg.InsertMsg, ut.result, ez, ut.schemaVersion)
	} else {
		msgs, err = repackInsertDataWithPartitionKeyForStreamingService(ut.TraceCtx(), channelNames, ut.upsertMsg.InsertMsg, ut.result, ut.partitionKeys, ez, ut.schema.CollectionSchema, ut.schemaVersion)
	}
	if err != nil {
		log.Warn(ctx, "assign segmentID and repack insert data failed", mlog.Err(err))
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
	log := mlog.With(
		mlog.FieldCollectionID(collID))
	// hash primary keys to channels
	vChannels, err := ut.chMgr.getVChannels(collID)
	if err != nil {
		log.Warn(ctx, "get vChannels failed when deleteExecute", mlog.Err(err))
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
		ut.req.Namespace,
		ut.schema.CollectionSchema,
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

	log.Debug(ctx, "Proxy Upsert deleteExecute done",
		mlog.FieldCollectionID(collID),
		mlog.Strings("virtual_channels", vChannels),
		mlog.FieldTaskID(ut.ID()),
		mlog.Int64("numRows", numRows),
		mlog.Duration("prepare duration", tr.ElapseSpan()))

	return msgs, nil
}
