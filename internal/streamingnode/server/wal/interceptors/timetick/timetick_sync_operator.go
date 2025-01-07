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
		timeTickNotifier:      inspector.NewTimeTickNotifier(),
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
	timeTickNotifier      *inspector.TimeTickNotifier        // used to notify the time tick change.
	metrics               *metricsutil.TimeTickMetrics
}

// Channel returns the pchannel info.
func (impl *timeTickSyncOperator) Channel() types.PChannelInfo {
	return impl.pchannel
}

// TimeTickNotifier returns the time tick notifier.
func (impl *timeTickSyncOperator) TimeTickNotifier() *inspector.TimeTickNotifier {
	return impl.timeTickNotifier
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
		msgID, err := impl.sendPersistentTsMsg(impl.ctx, ts, nil, underlyingWALImpls.Append)
		if err != nil {
			lastErr = errors.Wrap(err, "send first timestamp message failed")
			continue
		}
		// initialize ack manager.
		impl.ackManager = ack.NewAckManager(ts, msgID, impl.metrics)
		impl.logger.Info(
			"send first time tick success",
			zap.Uint64("timestamp", ts),
			zap.String("messageID", msgID.String()))
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

	if impl.ackDetails.IsNoPersistedMessage() {
		// there's no persisted message, so no need to send persistent time tick message.
		return impl.sendNoPersistentTsMsg(ctx, ts, appender)
	}
	// otherwise, send persistent time tick message.
	_, err := impl.sendPersistentTsMsg(ctx, ts, lastConfirmedMessageID, appender)
	return err
}

// sendPersistentTsMsg sends persistent time tick message to wal.
func (impl *timeTickSyncOperator) sendPersistentTsMsg(ctx context.Context,
	ts uint64,
	lastConfirmedMessageID message.MessageID,
	appender func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error),
) (message.MessageID, error) {
	msg, err := NewTimeTickMsg(ts, lastConfirmedMessageID, impl.sourceID)
	if err != nil {
		return nil, errors.Wrap(err, "at build time tick msg")
	}

	// Append it to wal.
	msgID, err := appender(ctx, msg)
	if err != nil {
		return nil, errors.Wrapf(err,
			"append time tick msg to wal failed, timestamp: %d, previous message counter: %d",
			impl.ackDetails.LastAllAcknowledgedTimestamp(),
			impl.ackDetails.Len(),
		)
	}

	// metrics updates
	impl.metrics.CountPersistentTimeTickSync(ts)
	impl.ackDetails.Range(func(detail *ack.AckDetail) bool {
		impl.metrics.CountSyncTimeTick(detail.IsSync)
		return true
	})
	// Ack details has been committed to wal, clear it.
	impl.ackDetails.Clear()
	// Update last time tick message id and time tick.
	impl.timeTickNotifier.Update(inspector.TimeTickInfo{
		MessageID: msgID,
		TimeTick:  ts,
	})
	return msgID, nil
}

// sendNoPersistentTsMsg sends no persistent time tick message to wal.
func (impl *timeTickSyncOperator) sendNoPersistentTsMsg(ctx context.Context, ts uint64, appender func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)) error {
	msg, err := NewTimeTickMsg(ts, nil, impl.sourceID)
	if err != nil {
		return errors.Wrap(err, "at build time tick msg when send no persist msg")
	}

	// with the hint of not persisted message, the underlying wal will not persist it.
	// but the interceptors will still be triggered.
	ctx = utility.WithNotPersisted(ctx, &utility.NotPersistedHint{
		MessageID: impl.timeTickNotifier.Get().MessageID,
	})

	// Append it to wal.
	_, err = appender(ctx, msg)
	if err != nil {
		return errors.Wrapf(err,
			"append no persist time tick msg to wal failed, timestamp: %d, previous message counter: %d",
			impl.ackDetails.LastAllAcknowledgedTimestamp(),
			impl.ackDetails.Len(),
		)
	}

	// metrics updates.
	impl.metrics.CountMemoryTimeTickSync(ts)
	impl.ackDetails.Range(func(detail *ack.AckDetail) bool {
		impl.metrics.CountSyncTimeTick(detail.IsSync)
		return true
	})
	// Ack details has been committed to wal, clear it.
	impl.ackDetails.Clear()
	// Only update time tick.
	impl.timeTickNotifier.OnlyUpdateTs(ts)
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
