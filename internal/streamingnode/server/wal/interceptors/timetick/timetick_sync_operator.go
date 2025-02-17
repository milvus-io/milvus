package timetick

import (
	"context"
	"time"

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
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// timeTickSyncOperator is a time tick sync operator.
var _ inspector.TimeTickSyncOperator = &timeTickSyncOperator{}

// NewTimeTickSyncOperator creates a new time tick sync operator.
func newTimeTickSyncOperator(param interceptors.InterceptorBuildParam) *timeTickSyncOperator {
	ctx, cancel := context.WithCancel(context.Background())
	return &timeTickSyncOperator{
		ctx:    ctx,
		cancel: cancel,
		logger: resource.Resource().Logger().With(
			log.FieldComponent("timetick-sync"),
			zap.Any("pchannel", param.WALImpls.Channel()),
		),
		pchannel:              param.WALImpls.Channel(),
		ready:                 make(chan struct{}),
		interceptorBuildParam: param,
		ackManager:            nil,
		ackDetails:            ack.NewAckDetails(),
		sourceID:              paramtable.GetNodeID(),
		metrics:               metricsutil.NewTimeTickMetrics(param.WALImpls.Channel().Name),
	}
}

// timeTickSyncOperator is a time tick sync operator.
type timeTickSyncOperator struct {
	ctx    context.Context
	cancel context.CancelFunc

	logger                *log.MLogger
	pchannel              types.PChannelInfo                 // pchannel info belong to.
	ready                 chan struct{}                      // hint the time tick operator is ready to use.
	interceptorBuildParam interceptors.InterceptorBuildParam // interceptor build param.
	ackManager            *ack.AckManager                    // ack manager.
	ackDetails            *ack.AckDetails                    // all acknowledged details, all acked messages but not sent to wal will be kept here.
	sourceID              int64                              // the current node id.
	writeAheadBuffer      *wab.WriteAheadBuffer              // write ahead buffer.
	mvccManager           *mvcc.MVCCManager                  // cursor manager is used to record the maximum presisted timetick of vchannel.
	metrics               *metricsutil.TimeTickMetrics
}

// WriteAheadBuffer returns the write ahead buffer.
func (impl *timeTickSyncOperator) WriteAheadBuffer(ctx context.Context) (wab.ROWriteAheadBuffer, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-impl.ready:
	}
	if impl.writeAheadBuffer == nil {
		panic("unreachable: write ahead buffer is not ready")
	}
	return impl.writeAheadBuffer, nil
}

// MVCCManager returns the mvcc manager.
func (impl *timeTickSyncOperator) MVCCManager(ctx context.Context) (*mvcc.MVCCManager, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-impl.ready:
	}
	if impl.mvccManager == nil {
		panic("unreachable: mvcc manager is not ready")
	}
	return impl.mvccManager, nil
}

// Channel returns the pchannel info.
func (impl *timeTickSyncOperator) Channel() types.PChannelInfo {
	return impl.pchannel
}

// Sync trigger a sync operation.
// Sync operation is not thread safe, so call it in a single goroutine.
func (impl *timeTickSyncOperator) Sync(ctx context.Context) {
	// Sync operation cannot trigger until isReady.
	if !impl.isReady() {
		return
	}

	wal := impl.interceptorBuildParam.WAL.Get()
	err := impl.sendTsMsg(ctx, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		appendResult, err := wal.Append(ctx, msg)
		if err != nil {
			return nil, err
		}
		return appendResult.MessageID, nil
	})
	if err != nil {
		impl.logger.Warn("send time tick sync message failed", zap.Error(err))
	}
}

// initialize initializes the time tick sync operator.
func (impl *timeTickSyncOperator) initialize() {
	impl.blockUntilSyncTimeTickReady()
}

// blockUntilSyncTimeTickReady blocks until the first time tick message is sent.
func (impl *timeTickSyncOperator) blockUntilSyncTimeTickReady() error {
	underlyingWALImpls := impl.interceptorBuildParam.WALImpls

	impl.logger.Info("start to sync first time tick")
	defer impl.logger.Info("sync first time tick done")

	backoffTimer := typeutil.NewBackoffTimer(typeutil.BackoffTimerConfig{
		Default: 5 * time.Second,
		Backoff: typeutil.BackoffConfig{
			InitialInterval: 20 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     5 * time.Second,
		},
	})
	backoffTimer.EnableBackoff()

	var lastErr error
	// Send first timetick message to wal before interceptor is ready.
	for count := 0; ; count++ {
		if count > 0 {
			nextTimer, nextBalanceInterval := backoffTimer.NextTimer()
			impl.logger.Warn(
				"send first time tick failed",
				zap.Duration("nextBalanceInterval", nextBalanceInterval),
				zap.Int("retryCount", count),
				zap.Error(lastErr),
			)
			select {
			case <-impl.ctx.Done():
				return impl.ctx.Err()
			case <-nextTimer:
			}
		}

		// Sent first timetick message to wal before ready.
		// New TT is always greater than all tt on previous streamingnode.
		// A fencing operation of underlying WAL is needed to make exclusive produce of topic.
		// Otherwise, the TT principle may be violated.
		// And sendTsMsg must be done, to help ackManager to get first LastConfirmedMessageID
		// !!! Send a timetick message into walimpls directly is safe.
		resource.Resource().TSOAllocator().Sync()
		ts, err := resource.Resource().TSOAllocator().Allocate(impl.ctx)
		if err != nil {
			lastErr = errors.Wrap(err, "allocate timestamp failed")
			continue
		}
		msg, err := NewTimeTickMsg(ts, nil, impl.sourceID)
		if err != nil {
			lastErr = errors.Wrap(err, "at build time tick msg")
			continue
		}
		msgID, err := underlyingWALImpls.Append(impl.ctx, msg)
		if err != nil {
			lastErr = errors.Wrap(err, "send first timestamp message failed")
			continue
		}
		// initialize ack manager.
		impl.ackManager = ack.NewAckManager(ts, msgID, impl.metrics)
		impl.logger.Info(
			"send first time tick success",
			zap.Uint64("timestamp", ts),
			zap.Stringer("messageID", msgID))
		capacity := int(paramtable.Get().StreamingCfg.WALWriteAheadBufferCapacity.GetAsSize())
		keepalive := paramtable.Get().StreamingCfg.WALWriteAheadBufferKeepalive.GetAsDurationByParse()
		impl.writeAheadBuffer = wab.NewWirteAheadBuffer(
			impl.logger,
			capacity,
			keepalive,
			msg.IntoImmutableMessage(msgID),
		)
		impl.mvccManager = mvcc.NewMVCCManager(ts)
		break
	}
	// interceptor is ready now.
	close(impl.ready)
	return nil
}

// Ready implements AppendInterceptor.
func (impl *timeTickSyncOperator) Ready() <-chan struct{} {
	return impl.ready
}

// isReady returns true if the operator is ready.
func (impl *timeTickSyncOperator) isReady() bool {
	select {
	case <-impl.ready:
		return true
	default:
		return false
	}
}

// AckManager returns the ack manager.
func (impl *timeTickSyncOperator) AckManager() *ack.AckManager {
	return impl.ackManager
}

// Close close the time tick sync operator.
func (impl *timeTickSyncOperator) Close() {
	impl.cancel()
	impl.metrics.Close()
}

// sendTsMsg sends first timestamp message to wal.
// TODO: TT lag warning.
func (impl *timeTickSyncOperator) sendTsMsg(ctx context.Context, appender func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)) error {
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
	persist := !impl.ackDetails.IsNoPersistedMessage()

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
	impl.writeAheadBuffer.Append(msgs, tsMsg)
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
