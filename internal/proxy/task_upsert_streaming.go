package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type upsertTaskByStreamingService struct {
	*upsertTask
}

func (ut *upsertTaskByStreamingService) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-Execute")
	defer sp.End()
	log := log.Ctx(ctx).With(zap.String("collectionName", ut.req.CollectionName))

	insertMsgs, err := ut.packInsertMessage(ctx)
	if err != nil {
		log.Warn("pack insert message failed", zap.Error(err))
		return err
	}
	deleteMsgs, err := ut.packDeleteMessage(ctx)
	if err != nil {
		log.Warn("pack delete message failed", zap.Error(err))
		return err
	}

	messages := append(insertMsgs, deleteMsgs...)
	resp := streaming.WAL().AppendMessages(ctx, messages...)
	if err := resp.UnwrapFirstError(); err != nil {
		log.Warn("append messages to wal failed", zap.Error(err))
		return err
	}
	// Update result.Timestamp for session consistency.
	ut.result.Timestamp = resp.MaxTimeTick()
	return nil
}

func (ut *upsertTaskByStreamingService) packInsertMessage(ctx context.Context) ([]message.MutableMessage, error) {
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
		msgs, err = repackInsertDataForStreamingService(ut.TraceCtx(), channelNames, ut.upsertMsg.InsertMsg, ut.result)
	} else {
		msgs, err = repackInsertDataWithPartitionKeyForStreamingService(ut.TraceCtx(), channelNames, ut.upsertMsg.InsertMsg, ut.result, ut.partitionKeys)
	}
	if err != nil {
		log.Warn("assign segmentID and repack insert data failed", zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}
	return msgs, nil
}

func (it *upsertTaskByStreamingService) packDeleteMessage(ctx context.Context) ([]message.MutableMessage, error) {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy deleteExecute upsert %d", it.ID()))
	collID := it.upsertMsg.DeleteMsg.CollectionID
	it.upsertMsg.DeleteMsg.PrimaryKeys = it.oldIds
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collID))
	// hash primary keys to channels
	vChannels, err := it.chMgr.getVChannels(collID)
	if err != nil {
		log.Warn("get vChannels failed when deleteExecute", zap.Error(err))
		it.result.Status = merr.Status(err)
		return nil, err
	}
	result, numRows, err := repackDeleteMsgByHash(
		ctx,
		it.upsertMsg.DeleteMsg.PrimaryKeys,
		vChannels,
		it.idAllocator,
		it.BeginTs(),
		it.upsertMsg.DeleteMsg.CollectionID,
		it.upsertMsg.DeleteMsg.CollectionName,
		it.upsertMsg.DeleteMsg.PartitionID,
		it.upsertMsg.DeleteMsg.PartitionName,
	)
	if err != nil {
		return nil, err
	}

	var msgs []message.MutableMessage
	for hashKey, deleteMsg := range result {
		vchannel := vChannels[hashKey]
		msg, err := message.NewDeleteMessageBuilderV1().
			WithHeader(&message.DeleteMessageHeader{
				CollectionId: it.upsertMsg.DeleteMsg.CollectionID,
			}).
			WithBody(deleteMsg.DeleteRequest).
			WithVChannel(vchannel).
			BuildMutable()
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}

	log.Debug("Proxy Upsert deleteExecute done",
		zap.Int64("collection_id", collID),
		zap.Strings("virtual_channels", vChannels),
		zap.Int64("taskID", it.ID()),
		zap.Int64("numRows", numRows),
		zap.Duration("prepare duration", tr.ElapseSpan()))

	return msgs, nil
}
