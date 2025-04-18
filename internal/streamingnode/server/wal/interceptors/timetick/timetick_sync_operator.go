package timetick

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/inspector"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// timeTickSyncOperator is a time tick sync operator.
var _ inspector.TimeTickSyncOperator = &timeTickSyncOperator{}

// NewTimeTickSyncOperator creates a new time tick sync operator.
func newTimeTickSyncOperator(param *interceptors.InterceptorBuildParam) *timeTickSyncOperator {
	metrics := metricsutil.NewTimeTickMetrics(param.ChannelInfo.Name)
	return &timeTickSyncOperator{
		logger: resource.Resource().Logger().With(
			log.FieldComponent("timetick-sync"),
			zap.Any("pchannel", param.ChannelInfo),
		),
		interceptorBuildParam: param,
		ackManager:            ack.NewAckManager(param.LastTimeTickMessage.TimeTick(), param.LastTimeTickMessage.LastConfirmedMessageID(), metrics),
		ackDetails:            ack.NewAckDetails(),
		sourceID:              paramtable.GetNodeID(),
		metrics:               metrics,
	}
}

// timeTickSyncOperator is a time tick sync operator.
type timeTickSyncOperator struct {
	logger                *log.MLogger
	interceptorBuildParam *interceptors.InterceptorBuildParam // interceptor build param.
	ackManager            *ack.AckManager                     // ack manager.
	ackDetails            *ack.AckDetails                     // all acknowledged details, all acked messages but not sent to wal will be kept here.
	sourceID              int64                               // source id of the time tick sync operator.
	metrics               *metricsutil.TimeTickMetrics
}

// Channel returns the pchannel info.
func (impl *timeTickSyncOperator) Channel() types.PChannelInfo {
	return impl.interceptorBuildParam.ChannelInfo
}

// WriteAheadBuffer returns the write ahead buffer.
func (impl *timeTickSyncOperator) WriteAheadBuffer() wab.ROWriteAheadBuffer {
	return impl.interceptorBuildParam.WriteAheadBuffer
}

// MVCCManager returns the mvcc manager.
func (impl *timeTickSyncOperator) MVCCManager() *mvcc.MVCCManager {
	return impl.interceptorBuildParam.MVCCManager
}

// Sync trigger a sync operation.
// Sync operation is not thread safe, so call it in a single goroutine.
func (impl *timeTickSyncOperator) Sync(ctx context.Context, persisted bool) {
	// Sync operation cannot trigger until isReady.
	wal, err := impl.interceptorBuildParam.WAL.GetWithContext(ctx)
	if err != nil {
		impl.logger.Warn("unreachable: get wal failed", zap.Error(err))
		return
	}

	err = impl.sendTsMsg(ctx, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendResult, err := wal.Append(ctx, msg)
		if err != nil {
			return nil, err
		}
		return appendResult.MessageID, nil
	}, persisted)
	if err != nil {
		impl.logger.Warn("send time tick sync message failed", zap.Error(err))
	}
}

// AckManager returns the ack manager.
func (impl *timeTickSyncOperator) AckManager() *ack.AckManager {
	return impl.ackManager
}

// Close close the time tick sync operator.
func (impl *timeTickSyncOperator) Close() {
	impl.metrics.Close()
}

// sendTsMsg sends first timestamp message to wal.
// TODO: TT lag warning.
func (impl *timeTickSyncOperator) sendTsMsg(ctx context.Context, appender func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error), forcePersisted bool) error {
	// Sync the timestamp acknowledged details.
	impl.syncAcknowledgedDetails(ctx)

	if impl.ackDetails.Empty() {
		// No acknowledged info can be sent.
		// Some message sent operation is blocked, new TT cannot be pushed forward.
		return nil
	}

	// Construct time tick message.
	ts := impl.ackDetails.LastAllAcknowledgedTimestamp()
	lastConfirmedMessageID := impl.ackDetails.EarliestLastConfirmedMessageID()
	persist := (!impl.ackDetails.IsNoPersistedMessage() || forcePersisted)

	return impl.sendTsMsgToWAL(ctx, ts, lastConfirmedMessageID, persist, appender)
}

// sendPersistentTsMsg sends persistent time tick message to wal.
func (impl *timeTickSyncOperator) sendTsMsgToWAL(ctx context.Context,
	ts uint64,
	lastConfirmedMessageID message.MessageID,
	persist bool,
	appender func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error),
) error {
	msg, err := NewTimeTickMsg(ts, lastConfirmedMessageID, impl.sourceID)
	if err != nil {
		return errors.Wrap(err, "at build time tick msg")
	}

	if !persist {
		// there's no persisted message, so no need to send persistent time tick message.
		// With the hint of not persisted message, the underlying wal will not persist it.
		// but the interceptors will still be triggered.
		ctx = utility.WithNotPersisted(ctx, &utility.NotPersistedHint{
			MessageID: lastConfirmedMessageID,
		})
		// setup a hint for the message.
		msg.WithNotPersisted()
	}

	// Append it to wal.
	msgID, err := appender(ctx, msg)
	if err != nil {
		return errors.Wrapf(err,
			"append time tick msg to wal failed, timestamp: %d, previous message counter: %d",
			impl.ackDetails.LastAllAcknowledgedTimestamp(),
			impl.ackDetails.Len(),
		)
	}

	// metrics updates
	impl.metrics.CountTimeTickSync(ts, persist)
	msgs := make([]message.ImmutableMessage, 0, impl.ackDetails.Len())
	impl.ackDetails.Range(func(detail *ack.AckDetail) bool {
		impl.metrics.CountSyncTimeTick(detail.IsSync)
		if !detail.IsSync && detail.Err == nil {
			msgs = append(msgs, detail.Message)
		}
		return true
	})
	// Ack details has been committed to wal, clear it.
	impl.ackDetails.Clear()
	tsMsg := msg.IntoImmutableMessage(msgID)
	// Add it into write ahead buffer.
	impl.interceptorBuildParam.WriteAheadBuffer.Append(msgs, tsMsg)
	return nil
}

// syncAcknowledgedDetails syncs the timestamp acknowledged details.
func (impl *timeTickSyncOperator) syncAcknowledgedDetails(ctx context.Context) {
	// Sync up and get last confirmed timestamp.
	ackDetails, err := impl.ackManager.SyncAndGetAcknowledged(ctx)
	if err != nil {
		impl.logger.Warn("sync timestamp ack manager failed", zap.Error(err))
	}

	// Add ack details to ackDetails.
	impl.ackDetails.AddDetails(ackDetails)
}
