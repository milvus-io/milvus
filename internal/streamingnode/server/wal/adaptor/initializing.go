package adaptor

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// buildInterceptorParams builds the interceptor params for the walimpls.
func buildInterceptorParams(ctx context.Context, underlyingWALImpls walimpls.WALImpls, cp *utility.WALCheckpoint) (*interceptors.InterceptorBuildParam, error) {
	var lastConfirmedMessageID message.MessageID
	if cp != nil {
		// lastConfirmedMessageID is used to promise we can use it to read all messages which timetick is greater than timetick of current message.
		// When we send the recovery barrier message, its timetick is always greater than the timetick of the previous message.
		// But for the uncommitted message, its timetick is undetermined, and wal support to recover the uncommitted-txn.
		// For protecting the `LastConfirmedMessageID` promise,
		// we use the checkpoint (checkpoint is always see the committed message) to promise we can see the uncommitted message
		// when using the recovery barrier message as the position to read.
		lastConfirmedMessageID = cp.MessageID
	}
	msg, err := sendRecoveryBarrier(ctx, underlyingWALImpls, lastConfirmedMessageID)
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

// sendRecoveryBarrier sends the first recovery barrier message to walimpls.
// It is used to
// 1. make a fence operation with the underlying walimpls
// 2. get position of wal to determine the end of current wal.
// 3. establish a recovered query-resource baseline for live vchannels after replay reaches the barrier.
func sendRecoveryBarrier(ctx context.Context, underlyingWALImpls walimpls.WALImpls, lastConfirmedMessageID message.MessageID) (msg message.ImmutableMessage, err error) {
	logger := resource.Resource().Logger().With(mlog.String("channel", underlyingWALImpls.Channel().String()))
	if lastConfirmedMessageID != nil {
		logger = logger.With(mlog.Stringer("lastConfirmedMessageID", lastConfirmedMessageID))
	}

	logger.Info(ctx, "start to append recovery barrier")
	defer func() {
		if err != nil {
			logger.Error(ctx, "append recovery barrier failed", mlog.Err(err))
			return
		}
		logger.Info(ctx, "append recovery barrier done", mlog.String("msgID", msg.MessageID().String()), mlog.Uint64("timetick", msg.TimeTick()))
	}()

	// Send recovery barrier message to wal before interceptor is ready.
	// New TT is always greater than all tt on previous streamingnode.
	// A fencing operation of underlying WAL is needed to make exclusive produce of topic.
	// Otherwise, the TT principle may be violated.
	// !!! Sending a recovery barrier into walimpls directly is safe because interceptors
	// are not ready until bounded recovery consumes it.
	resource.Resource().TSOAllocator().Sync()
	ts, err := resource.Resource().TSOAllocator().Allocate(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "allocate timestamp failed")
	}
	mutableMsg := newRecoveryBarrierMsg(ts, lastConfirmedMessageID)
	msgID, err := underlyingWALImpls.Append(ctx, mutableMsg)
	if err != nil {
		return nil, errors.Wrap(err, "append recovery barrier message failed")
	}
	return mutableMsg.IntoImmutableMessage(msgID), nil
}

func newRecoveryBarrierMsg(ts uint64, lastConfirmedMessageID message.MessageID) message.MutableMessage {
	msg := message.NewRecoveryBarrierMessageBuilderV2().
		WithHeader(&message.RecoveryBarrierMessageHeader{}).
		WithBody(&message.RecoveryBarrierMessageBody{}).
		WithAllVChannel().
		MustBuildMutable()
	if lastConfirmedMessageID != nil {
		return msg.WithTimeTick(ts).WithLastConfirmed(lastConfirmedMessageID)
	}
	return msg.WithTimeTick(ts).WithLastConfirmedUseMessageID()
}
