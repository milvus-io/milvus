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

type deleteTaskByStreamingService struct {
	*deleteTask
}

// Execute is a function to delete task by streaming service
// we only overwrite the Execute function
func (dt *deleteTaskByStreamingService) Execute(ctx context.Context) (err error) {
	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, "Proxy-Delete-Execute")
	defer sp.End()

	if len(dt.req.GetExpr()) == 0 {
		return merr.WrapErrParameterInvalid("valid expr", "empty expr", "invalid expression")
	}

	dt.tr = timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute delete %d", dt.ID()))
	result, numRows, err := repackDeleteMsgByHash(
		ctx,
		dt.primaryKeys,
		dt.vChannels,
		dt.idAllocator,
		dt.ts,
		dt.collectionID,
		dt.req.GetCollectionName(),
		dt.partitionID,
		dt.req.GetPartitionName(),
	)
	if err != nil {
		return err
	}

	var msgs []message.MutableMessage
	for hashKey, deleteMsg := range result {
		vchannel := dt.vChannels[hashKey]
		msg, err := message.NewDeleteMessageBuilderV1().
			WithHeader(&message.DeleteMessageHeader{
				CollectionId: dt.collectionID,
			}).
			WithBody(deleteMsg.DeleteRequest).
			WithVChannel(vchannel).
			BuildMutable()
		if err != nil {
			return err
		}
		msgs = append(msgs, msg)
	}

	log.Debug("send delete request to virtual channels",
		zap.String("collectionName", dt.req.GetCollectionName()),
		zap.Int64("collectionID", dt.collectionID),
		zap.Strings("virtual_channels", dt.vChannels),
		zap.Int64("taskID", dt.ID()),
		zap.Duration("prepare duration", dt.tr.RecordSpan()))

	resp := streaming.WAL().AppendMessages(ctx, msgs...)
	if resp.UnwrapFirstError(); err != nil {
		log.Warn("append messages to wal failed", zap.Error(err))
		return err
	}
	dt.sessionTS = resp.MaxTimeTick()
	dt.count += numRows
	return nil
}
