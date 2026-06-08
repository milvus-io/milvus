package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// appendPredicateDeleteMessages appends one predicate-delete WAL message to each target vchannel.
// It does not reuse the PK-delete repack path because predicate delete has no primary-key hash.
func appendPredicateDeleteMessages(
	ctx context.Context,
	collectionID int64,
	collectionName string,
	partitionID int64,
	partitionName string,
	dbName string,
	vChannels []string,
	idAllocator allocator.Interface,
	deleteTs uint64,
	serializedPlan []byte,
) (uint64, error) {
	startMsgID, _, err := idAllocator.Alloc(uint32(len(vChannels)))
	if err != nil {
		return 0, err
	}

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		schema, err := globalMetaCache.GetCollectionSchema(ctx, dbName, collectionName)
		if err != nil {
			log.Ctx(ctx).Warn("get collection schema from global meta cache failed", zap.String("collectionName", collectionName), zap.Error(err))
			return 0, merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
		}

		ez = hookutil.GetEzByCollProperties(schema.GetProperties(), collectionID).AsMessageConfig()
	}

	msgs := make([]message.MutableMessage, 0, len(vChannels))
	for idx, vchannel := range vChannels {
		deleteReq := &msgpb.DeleteRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_Delete),
				commonpbutil.WithMsgID(startMsgID+int64(idx)),
				commonpbutil.WithTimeStamp(deleteTs),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			ShardName:          vchannel,
			DbName:             dbName,
			CollectionName:     collectionName,
			PartitionName:      partitionName,
			CollectionID:       collectionID,
			PartitionID:        partitionID,
			Timestamps:         []uint64{deleteTs},
			SerializedExprPlan: serializedPlan,
		}

		msg, err := message.NewDeleteMessageBuilderV1().
			WithHeader(&message.DeleteMessageHeader{
				CollectionId: collectionID,
				Rows:         0,
			}).
			WithBody(deleteReq).
			WithVChannel(vchannel).
			WithCipher(ez).
			BuildMutable()
		if err != nil {
			return 0, err
		}
		msgs = append(msgs, msg)
	}

	resp := streaming.WAL().AppendMessages(ctx, msgs...)
	if err := resp.UnwrapFirstError(); err != nil {
		log.Ctx(ctx).Warn("append predicate delete messages to wal failed", zap.Error(err))
		return 0, err
	}
	return resp.MaxTimeTick(), nil
}

// Execute is a function to delete task by streaming service
// we only overwrite the Execute function
func (dt *deleteTask) Execute(ctx context.Context) (err error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Delete-Execute")
	defer sp.End()

	if len(dt.req.GetExpr()) == 0 {
		return merr.WrapErrParameterInvalid("valid expr", "empty expr", "invalid expression")
	}

	dt.tr = timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute delete %d", dt.ID()))
	result, numRows, err := repackDeleteMsgByHash(
		ctx, dt.primaryKeys,
		dt.vChannels, dt.idAllocator,
		dt.ts, dt.collectionID,
		dt.req.GetCollectionName(),
		dt.partitionID, dt.req.GetPartitionName(),
		dt.req.GetDbName(),
	)
	if err != nil {
		return err
	}

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		schema, err := globalMetaCache.GetCollectionSchema(ctx, dt.req.GetDbName(), dt.req.GetCollectionName())
		if err != nil {
			log.Ctx(ctx).Warn("get collection schema from global meta cache failed", zap.String("collectionName", dt.req.GetCollectionName()), zap.Error(err))
			return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
		}

		ez = hookutil.GetEzByCollProperties(schema.GetProperties(), dt.collectionID).AsMessageConfig()
	}

	var msgs []message.MutableMessage
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
				return err
			}
			msgs = append(msgs, msg)
		}
	}

	log.Ctx(ctx).Debug("send delete request to virtual channels",
		zap.String("collectionName", dt.req.GetCollectionName()),
		zap.Int64("collectionID", dt.collectionID),
		zap.Strings("virtual_channels", dt.vChannels),
		zap.Int64("taskID", dt.ID()),
		zap.Duration("prepare duration", dt.tr.RecordSpan()))

	resp := streaming.WAL().AppendMessages(ctx, msgs...)
	if err := resp.UnwrapFirstError(); err != nil {
		log.Ctx(ctx).Warn("append messages to wal failed", zap.Error(err))
		return err
	}
	dt.sessionTS = resp.MaxTimeTick()
	dt.count += numRows
	return nil
}
