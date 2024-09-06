package timetick

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var _ interceptors.InterceptorWithReady = (*timeTickAppendInterceptor)(nil)

// timeTickAppendInterceptor is a append interceptor.
type timeTickAppendInterceptor struct {
	operator   *timeTickSyncOperator
	txnManager *txn.TxnManager
}

// Ready implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) Ready() <-chan struct{} {
	return impl.operator.Ready()
}

// Do implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (msgID message.MessageID, err error) {
	var txnSession *txn.TxnSession
	if msg.MessageType() != message.MessageTypeTimeTick {
		// Allocate new timestamp acker for message.
		var acker *ack.Acker
		if msg.BarrierTimeTick() == 0 {
			if acker, err = impl.operator.AckManager().Allocate(ctx); err != nil {
				return nil, errors.Wrap(err, "allocate timestamp failed")
			}
		} else {
			if acker, err = impl.operator.AckManager().AllocateWithBarrier(ctx, msg.BarrierTimeTick()); err != nil {
				return nil, errors.Wrap(err, "allocate timestamp with barrier failed")
			}
		}

		// Assign timestamp to message and call the append method.
		msg = msg.
			WithTimeTick(acker.Timestamp()).                  // message assigned with these timetick.
			WithLastConfirmed(acker.LastConfirmedMessageID()) // start consuming from these message id, the message which timetick greater than current timetick will never be lost.

		defer func() {
			if err != nil {
				acker.Ack(ack.OptError(err))
				return
			}
			acker.Ack(
				ack.OptMessageID(msgID),
				ack.OptTxnSession(txnSession),
			)
		}()
	}

	switch msg.MessageType() {
	case message.MessageTypeBeginTxn:
		if txnSession, msg, err = impl.handleBegin(ctx, msg); err != nil {
			return nil, err
		}
	case message.MessageTypeCommitTxn:
		if txnSession, err = impl.handleCommit(ctx, msg); err != nil {
			return nil, err
		}
		defer txnSession.CommitDone()
	case message.MessageTypeRollbackTxn:
		if txnSession, err = impl.handleRollback(ctx, msg); err != nil {
			return nil, err
		}
		defer txnSession.RollbackDone()
	case message.MessageTypeTimeTick:
		// cleanup the expired transaction sessions and the already done transaction.
		impl.txnManager.CleanupTxnUntil(msg.TimeTick())
	default:
		// handle the transaction body message.
		if msg.TxnContext() != nil {
			if txnSession, err = impl.handleTxnMessage(ctx, msg); err != nil {
				return nil, err
			}
			defer func() {
				if err != nil {
					txnSession.AddNewMessageFail()
				}
				// perform keepalive for the transaction session if append success.
				txnSession.AddNewMessageDoneAndKeepalive(msg.TimeTick())
			}()
		}
	}

	// Attach the txn session to the context.
	// So the all interceptors of append operation can see it.
	if txnSession != nil {
		ctx = txn.WithTxnSession(ctx, txnSession)
	}
	msgID, err = impl.appendMsg(ctx, msg, append)
	return
}

// GracefulClose implements InterceptorWithGracefulClose.
func (impl *timeTickAppendInterceptor) GracefulClose() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	logger := log.With(zap.Any("pchannel", impl.operator.pchannel))
	logger.Info("timeTickAppendInterceptor is closing, try to perform a txn manager graceful shutdown")
	if err := impl.txnManager.GracefulClose(ctx); err != nil {
		logger.Warn("timeTickAppendInterceptor is closed", zap.Error(err))
		return
	}
	logger.Info("txnManager of timeTickAppendInterceptor is graceful closed")
}

// Close implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) Close() {
	resource.Resource().TimeTickInspector().UnregisterSyncOperator(impl.operator)
	impl.operator.Close()
}

// handleBegin handle the begin transaction message.
func (impl *timeTickAppendInterceptor) handleBegin(ctx context.Context, msg message.MutableMessage) (*txn.TxnSession, message.MutableMessage, error) {
	beginTxnMsg, err := message.AsMutableBeginTxnMessageV2(msg)
	if err != nil {
		return nil, nil, err
	}
	// Begin transaction will generate a txn context.
	session, err := impl.txnManager.BeginNewTxn(ctx, msg.TimeTick(), time.Duration(beginTxnMsg.Header().KeepaliveMilliseconds)*time.Millisecond)
	if err != nil {
		session.BeginRollback()
		return nil, nil, err
	}
	session.BeginDone()
	return session, msg.WithTxnContext(session.TxnContext()), nil
}

// handleCommit handle the commit transaction message.
func (impl *timeTickAppendInterceptor) handleCommit(ctx context.Context, msg message.MutableMessage) (*txn.TxnSession, error) {
	commitTxnMsg, err := message.AsMutableCommitTxnMessageV2(msg)
	if err != nil {
		return nil, err
	}
	session, err := impl.txnManager.GetSessionOfTxn(commitTxnMsg.TxnContext().TxnID)
	if err != nil {
		return nil, err
	}

	// Start commit the message.
	if err = session.RequestCommitAndWait(ctx, msg.TimeTick()); err != nil {
		return nil, err
	}
	return session, nil
}

// handleRollback handle the rollback transaction message.
func (impl *timeTickAppendInterceptor) handleRollback(ctx context.Context, msg message.MutableMessage) (session *txn.TxnSession, err error) {
	rollbackTxnMsg, err := message.AsMutableRollbackTxnMessageV2(msg)
	if err != nil {
		return nil, err
	}
	session, err = impl.txnManager.GetSessionOfTxn(rollbackTxnMsg.TxnContext().TxnID)
	if err != nil {
		return nil, err
	}

	// Start commit the message.
	if err = session.RequestRollback(ctx, msg.TimeTick()); err != nil {
		return nil, err
	}
	return session, nil
}

// handleTxnMessage handle the transaction body message.
func (impl *timeTickAppendInterceptor) handleTxnMessage(ctx context.Context, msg message.MutableMessage) (session *txn.TxnSession, err error) {
	txnContext := msg.TxnContext()
	session, err = impl.txnManager.GetSessionOfTxn(txnContext.TxnID)
	if err != nil {
		return nil, err
	}
	// Add the message to the transaction.
	if err = session.AddNewMessage(ctx, msg.TimeTick()); err != nil {
		return nil, err
	}
	return session, nil
}

// appendMsg is a helper function to append message.
func (impl *timeTickAppendInterceptor) appendMsg(
	ctx context.Context,
	msg message.MutableMessage,
	append func(context.Context, message.MutableMessage) (message.MessageID, error),
) (message.MessageID, error) {
	msgID, err := append(ctx, msg)
	if err != nil {
		return nil, err
	}

	utility.AttachAppendResultTimeTick(ctx, msg.TimeTick())
	utility.AttachAppendResultTxnContext(ctx, msg.TxnContext())
	return msgID, nil
}
