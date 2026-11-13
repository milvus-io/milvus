package timetick

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// appendAlterWAL runs under TimeTickSyncMu and the exclusive PChannel lock.
func (impl *timeTickAppendInterceptor) appendAlterWAL(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	op := impl.operator
	if storage, ok := op.interceptorBuildParam.RecoveryStorage.(interface{ EnableWALSwitch(context.Context) error }); ok {
		if err := storage.EnableWALSwitch(ctx); err != nil {
			return nil, err
		}
	}
	manager := op.AckManager()
	boundary, err := manager.AllocateForAppend(ctx, msg.BarrierTimeTick())
	if err != nil {
		return nil, err
	}
	msg = msg.WithTimeTick(boundary.Timestamp()).WithLastConfirmed(boundary.LastConfirmedMessageID())
	op.ackDetails.AddDetails(manager.TakeAcknowledgedForSwitch())
	batch := make([]message.ImmutableMessage, 0, op.ackDetails.Len())
	op.ackDetails.Range(func(d *ack.AckDetail) bool {
		if !d.IsSync && d.Err == nil {
			batch = append(batch, d.Message)
		}
		return true
	})
	// Query targets precede physical visibility, as in the ordinary Sync path.
	op.MVCCManager().QueryMVCC().Publish(func(yield func(message.ImmutableMessage, bool) bool) {
		op.ackDetails.Range(func(d *ack.AckDetail) bool {
			if d.IsSync || d.Err != nil {
				return true
			}
			insertOnly := d.Message.MessageType() == message.MessageTypeCommitTxn && d.TxnSession != nil && d.TxnSession.IsInsertOnly()
			return yield(d.Message, insertOnly)
		})
		yield(msg.IntoImmutableMessage(boundary.LastConfirmedMessageID()), false)
	}, msg.TimeTick(), nil)
	immutable, err := impl.appendMsg(ctx, msg, appendOp)
	if err != nil {
		boundary.Ack(ack.OptError(err))
		return nil, err
	}
	start, err := message.WALSwitchStart(immutable)
	if err != nil {
		panic(err)
	} // the wrapper wrote this property under this lock
	if start == nil {
		// Legacy/test backends retain the existing fenced-recovery protocol.
		boundary.Ack(ack.OptImmutableMessage(immutable))
		return immutable.MessageID(), nil
	}
	impl.txnManager.FailTxnAtVChannel("")
	op.interceptorBuildParam.WriteAheadBuffer.Append(batch, immutable)
	op.ackDetails.Clear()
	manager.CompleteWALSwitch(boundary, start)
	op.lastPublishedTick = msg.TimeTick()
	return immutable.MessageID(), nil
}
