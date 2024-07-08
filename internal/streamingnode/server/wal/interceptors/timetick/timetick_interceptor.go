package timetick

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
)

var _ interceptors.AppendInterceptor = (*timeTickAppendInterceptor)(nil)

// timeTickAppendInterceptor is a append interceptor.
type timeTickAppendInterceptor struct {
	ctx    context.Context
	cancel context.CancelFunc
	ready  chan struct{}

	ackManager *ack.AckManager
	ackDetails *ackDetails
	sourceID   int64
}

// Ready implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) Ready() <-chan struct{} {
	return impl.ready
}

// Do implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (msgID message.MessageID, err error) {
	if msg.MessageType() != message.MessageTypeTimeTick {
		// Allocate new acker for message.
		var acker *ack.Acker
		if acker, err = impl.ackManager.Allocate(ctx); err != nil {
			return nil, errors.Wrap(err, "allocate timestamp failed")
		}
		defer func() {
			acker.Ack(ack.OptError(err))
			impl.ackManager.AdvanceLastConfirmedMessageID(msgID)
		}()

		// Assign timestamp to message and call append method.
		msg = msg.
			WithTimeTick(acker.Timestamp()).                  // message assigned with these timetick.
			WithLastConfirmed(acker.LastConfirmedMessageID()) // start consuming from these message id, the message which timetick greater than current timetick will never be lost.
	}
	return append(ctx, msg)
}

// Close implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) Close() {
	impl.cancel()
}

// execute start a background task.
func (impl *timeTickAppendInterceptor) executeSyncTimeTick(interval time.Duration, param interceptors.InterceptorBuildParam) {
	underlyingWALImpls := param.WALImpls

	logger := log.With(zap.Any("channel", underlyingWALImpls.Channel()))
	logger.Info("start to sync time tick...")
	defer logger.Info("sync time tick stopped")

	if err := impl.blockUntilSyncTimeTickReady(underlyingWALImpls); err != nil {
		logger.Warn("sync first time tick failed", zap.Error(err))
		return
	}

	// interceptor is ready, wait for the final wal object is ready to use.
	wal := param.WAL.Get()

	// TODO: sync time tick message to wal periodically.
	// Add a trigger on `AckManager` to sync time tick message without periodically.
	// `AckManager` gather detail information, time tick sync can check it and make the message between tt more smaller.
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-impl.ctx.Done():
			return
		case <-ticker.C:
			if err := impl.sendTsMsg(impl.ctx, wal.Append); err != nil {
				log.Warn("send time tick sync message failed", zap.Error(err))
			}
		}
	}
}

// blockUntilSyncTimeTickReady blocks until the first time tick message is sent.
func (impl *timeTickAppendInterceptor) blockUntilSyncTimeTickReady(underlyingWALImpls walimpls.WALImpls) error {
	logger := log.With(zap.Any("channel", underlyingWALImpls.Channel()))
	logger.Info("start to sync first time tick")
	defer logger.Info("sync first time tick done")

	// Send first timetick message to wal before interceptor is ready.
	for count := 0; ; count++ {
		// Sent first timetick message to wal before ready.
		// New TT is always greater than all tt on previous streamingnode.
		// A fencing operation of underlying WAL is needed to make exclusive produce of topic.
		// Otherwise, the TT principle may be violated.
		// And sendTsMsg must be done, to help ackManager to get first LastConfirmedMessageID
		// !!! Send a timetick message into walimpls directly is safe.
		select {
		case <-impl.ctx.Done():
			return impl.ctx.Err()
		default:
		}
		if err := impl.sendTsMsg(impl.ctx, underlyingWALImpls.Append); err != nil {
			logger.Warn("send first timestamp message failed", zap.Error(err), zap.Int("retryCount", count))
			// TODO: exponential backoff.
			time.Sleep(50 * time.Millisecond)
			continue
		}
		break
	}
	// interceptor is ready now.
	close(impl.ready)
	return nil
}

// syncAcknowledgedDetails syncs the timestamp acknowledged details.
func (impl *timeTickAppendInterceptor) syncAcknowledgedDetails() {
	// Sync up and get last confirmed timestamp.
	ackDetails, err := impl.ackManager.SyncAndGetAcknowledged(impl.ctx)
	if err != nil {
		log.Warn("sync timestamp ack manager failed", zap.Error(err))
	}

	// Add ack details to ackDetails.
	impl.ackDetails.AddDetails(ackDetails)
}

// sendTsMsg sends first timestamp message to wal.
// TODO: TT lag warning.
func (impl *timeTickAppendInterceptor) sendTsMsg(_ context.Context, appender func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)) error {
	// Sync the timestamp acknowledged details.
	impl.syncAcknowledgedDetails()

	if impl.ackDetails.Empty() {
		// No acknowledged info can be sent.
		// Some message sent operation is blocked, new TT cannot be pushed forward.
		return nil
	}

	// Construct time tick message.
	msg, err := newTimeTickMsg(impl.ackDetails.LastAllAcknowledgedTimestamp(), impl.sourceID)
	if err != nil {
		return errors.Wrap(err, "at build time tick msg")
	}

	// Append it to wal.
	msgID, err := appender(impl.ctx, msg)
	if err != nil {
		return errors.Wrapf(err,
			"append time tick msg to wal failed, timestamp: %d, previous message counter: %d",
			impl.ackDetails.LastAllAcknowledgedTimestamp(),
			impl.ackDetails.Len(),
		)
	}

	// Ack details has been committed to wal, clear it.
	impl.ackDetails.Clear()
	impl.ackManager.AdvanceLastConfirmedMessageID(msgID)
	return nil
}
