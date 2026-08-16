package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

	result, numRows, err := repackDeleteMsgByHash(
		ctx, dt.primaryKeys,
		dt.vChannels, dt.idAllocator,
		dt.ts, dt.collectionID,
		dt.req.GetCollectionName(),
		dt.partitionID, dt.req.GetPartitionName(),
		dt.req.GetDbName(),
		dt.req.Namespace,
		collectionSchema,
	)
	if err != nil {
		return err
	}

	var ez *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		ez = hookutil.GetEzByCollProperties(collectionSchema.GetProperties(), dt.collectionID).AsMessageConfig()
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

	mlog.Debug(ctx, "send delete request to virtual channels",
		mlog.String("collectionName", dt.req.GetCollectionName()),
		mlog.Int64("collectionID", dt.collectionID),
		mlog.Strings("virtual_channels", dt.vChannels),
		mlog.Int64("taskID", dt.ID()),
		mlog.Duration("prepare duration", dt.tr.RecordSpan()))

	resp := streaming.WAL().AppendMessages(ctx, msgs...)
	if err := resp.UnwrapFirstError(); err != nil {
		mlog.Warn(ctx, "append messages to wal failed", mlog.Err(err))
		return err
	}
	dt.sessionTS = resp.MaxTimeTick()
	dt.count += numRows
	return nil
}
