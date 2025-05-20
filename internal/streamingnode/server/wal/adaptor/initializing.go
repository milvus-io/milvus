package adaptor

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// buildInterceptorParams builds the interceptor params for the walimpls.
func buildInterceptorParams(ctx context.Context, underlyingWALImpls walimpls.WALImpls) (*interceptors.InterceptorBuildParam, error) {
	msg, err := sendFirstTimeTick(ctx, underlyingWALImpls)
	if err != nil {
		return nil, err
	}

	capacity := int(paramtable.Get().StreamingCfg.WALWriteAheadBufferCapacity.GetAsSize())
	keepalive := paramtable.Get().StreamingCfg.WALWriteAheadBufferKeepalive.GetAsDurationByParse()
	writeAheadBuffer := wab.NewWriteAheadBuffer(
		underlyingWALImpls.Channel().Name,
		resource.Resource().Logger().With(),
		capacity,
		keepalive,
		msg,
	)
	mvccManager := mvcc.NewMVCCManager(msg.TimeTick())
	return &interceptors.InterceptorBuildParam{
		ChannelInfo:         underlyingWALImpls.Channel(),
		WAL:                 syncutil.NewFuture[wal.WAL](),
		LastTimeTickMessage: msg,
		WriteAheadBuffer:    writeAheadBuffer,
		MVCCManager:         mvccManager,
	}, nil
}

// sendFirstTimeTick sends the first timetick message to walimpls.
// It is used to make a fence operation with the underlying walimpls and get the timetick and last message id to recover the wal state.
func sendFirstTimeTick(ctx context.Context, underlyingWALImpls walimpls.WALImpls) (msg message.ImmutableMessage, err error) {
	logger := resource.Resource().Logger().With(zap.String("channel", underlyingWALImpls.Channel().String()))
	logger.Info("start to sync first time tick")
	defer func() {
		if err != nil {
			logger.Error("sync first time tick failed", zap.Error(err))
			return
		}
		logger.Info("sync first time tick done", zap.String("msgID", msg.MessageID().String()), zap.Uint64("timetick", msg.TimeTick()))
	}()

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
	sourceID := paramtable.GetNodeID()
	// Send first timetick message to wal before interceptor is ready.
	for count := 0; ; count++ {
		if count > 0 {
			nextTimer, nextBalanceInterval := backoffTimer.NextTimer()
			logger.Warn(
				"send first time tick failed",
				zap.Duration("nextBalanceInterval", nextBalanceInterval),
				zap.Int("retryCount", count),
				zap.Error(lastErr),
			)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
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
		ts, err := resource.Resource().TSOAllocator().Allocate(ctx)
		if err != nil {
			lastErr = errors.Wrap(err, "allocate timestamp failed")
			continue
		}
		msg := timetick.NewTimeTickMsg(ts, nil, sourceID, true)
		msgID, err := underlyingWALImpls.Append(ctx, msg)
		if err != nil {
			lastErr = errors.Wrap(err, "send first timestamp message failed")
			continue
		}
		return msg.IntoImmutableMessage(msgID), nil
	}
}
