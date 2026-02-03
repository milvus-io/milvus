package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (ut *upsertTask) Execute(ctx context.Context) error {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Upsert-Execute")
	defer sp.End()
	log := log.Ctx(ctx).With(zap.String("collectionName", ut.req.CollectionName))

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		ez = hookutil.GetEzByCollProperties(ut.schema.GetProperties(), ut.collectionID).AsMessageConfig()
	}

	// Pack upsert messages combining insert and delete
	upsertMsgs, err := ut.packUpsertMessage(ctx, ez)
	if err != nil {
		log.Warn("pack upsert message failed", zap.Error(err))
		return err
	}

	resp := streaming.WAL().AppendMessages(ctx, upsertMsgs...)
	if err := resp.UnwrapFirstError(); err != nil {
		log.Warn("append messages to wal failed", zap.Error(err))
		return err
	}
	// Update result.Timestamp for session consistency.
	ut.result.Timestamp = resp.MaxTimeTick()
	return nil
}

func (ut *upsertTask) packUpsertMessage(ctx context.Context, ez *message.CipherConfig) ([]message.MutableMessage, error) {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy upsertExecute %d", ut.ID()))
	defer tr.Elapse("upsert execute done")

	collectionName := ut.upsertMsg.InsertMsg.CollectionName
	collID, err := globalMetaCache.GetCollectionID(ctx, ut.req.GetDbName(), collectionName)
	if err != nil {
		return nil, err
	}
	ut.upsertMsg.InsertMsg.CollectionID = collID
	ut.upsertMsg.DeleteMsg.CollectionID = collID

	log := log.Ctx(ctx).With(zap.Int64("collectionID", collID))

	channelNames, err := ut.chMgr.getVChannels(collID)
	if err != nil {
		log.Warn("get vChannels failed", zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}

	if ut.upsertMsg.DeleteMsg.PrimaryKeys == nil {
		ut.upsertMsg.DeleteMsg.PrimaryKeys = ut.oldIDs
	}

	// Repack upsert data by hash
	var msgs []message.MutableMessage
	if ut.partitionKeys == nil {
		msgs, err = repackUpsertDataForStreamingService(
			ut.TraceCtx(), channelNames,
			ut.upsertMsg.InsertMsg, ut.upsertMsg.DeleteMsg,
			ut.result, ut.idAllocator, ut.BeginTs(),
			ut.req.GetDbName(), ez)
	} else {
		msgs, err = repackUpsertDataWithPartitionKeyForStreamingService(
			ut.TraceCtx(), channelNames,
			ut.upsertMsg.InsertMsg, ut.upsertMsg.DeleteMsg,
			ut.result, ut.partitionKeys, ut.idAllocator, ut.BeginTs(),
			ut.req.GetDbName(), ez)
	}
	if err != nil {
		log.Warn("repack upsert data failed", zap.Error(err))
		ut.result.Status = merr.Status(err)
		return nil, err
	}

	log.Debug("Proxy Upsert execute done",
		zap.Int64("collection_id", collID),
		zap.Strings("virtual_channels", channelNames),
		zap.Int64("taskID", ut.ID()),
		zap.Duration("prepare duration", tr.ElapseSpan()))

	return msgs, nil
}
