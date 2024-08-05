package timetick

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/txn"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

var (
	_ interceptors.InterceptorWithReady           = (*timeTickAppendInterceptor)(nil)
	_ interceptors.InterceptorWithUnwrapMessageID = (*timeTickAppendInterceptor)(nil)
)

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
		if acker, err = impl.operator.AckManager().Allocate(ctx); err != nil {
			return nil, errors.Wrap(err, "allocate timestamp failed")
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
				ack.OptMessageID(msgID.(wrappedMessageID).MessageID),
				ack.OptTxnSession(txnSession),
			)
		}()
	}

	switch msg.MessageType() {
	case message.MessageTypeBeginTxn:
		txnSession, msg, err = impl.handleBegin(ctx, msg)
	case message.MessageTypeCommitTxn:
		txnSession, err = impl.handleCommit(ctx, msg)
	case message.MessageTypeRollbackTxn:
		txnSession, err = impl.handleRollback(msg)
	case message.MessageTypeTimeTick:
		// cleanup the expired transaction sessions and the already done transaction.
		impl.txnManager.CleanupTxnUntil(msg.TimeTick())
	default:
		// handle the transaction body message.
		if msg.TxnContext() != nil {
			txnSession, err = impl.handleTxnMessage(ctx, msg)
			if err != nil {
				return nil, err
			}
			defer txnSession.AddNewMessageDone()
		}
	}
	msgID, err = impl.appendMsg(ctx, msg, append)
	return
}

// GracefulClose implements InterceptorWithGracefulClose.
func (impl *timeTickAppendInterceptor) GracefulClose() {
	log.Warn("timeTickAppendInterceptor is closing")
	impl.txnManager.GracefulClose()
}

// Close implements AppendInterceptor.
func (impl *timeTickAppendInterceptor) Close() {
	resource.Resource().TimeTickInspector().UnregisterSyncOperator(impl.operator)
	impl.operator.Close()
}

func (impl *timeTickAppendInterceptor) UnwrapMessageID(r *wal.AppendResult) {
	m := r.MessageID.(wrappedMessageID)
	r.MessageID = m.MessageID
	r.TimeTick = m.timetick
	r.TxnCtx = m.txnContext
}

// handleBegin handle the begin transaction message.
func (impl *timeTickAppendInterceptor) handleBegin(ctx context.Context, msg message.MutableMessage) (*txn.TxnSession, message.MutableMessage, error) {
	beginTxnMsg, err := message.AsMutableBeginTxnMessageV2(msg)
	if err != nil {
		return nil, nil, err
	}
	// Begin transaction will generate a txn context.
	session, err := impl.txnManager.BeginNewTxn(ctx, msg.TimeTick(), time.Duration(beginTxnMsg.Header().TtlMilliseconds)*time.Millisecond)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			// mark the session rollback if sent begin message failed.
			session.BeginRollback()
		}
		session.BeginDone()
	}()
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
func (impl *timeTickAppendInterceptor) handleRollback(msg message.MutableMessage) (session *txn.TxnSession, err error) {
	rollbackTxnMsg, err := message.AsMutableRollbackTxnMessageV2(msg)
	if err != nil {
		return nil, err
	}
	session, err = impl.txnManager.GetSessionOfTxn(rollbackTxnMsg.TxnContext().TxnID)
	if err != nil {
		return nil, err
	}

	// Start commit the message.
	if err = session.RequestRollback(msg.TimeTick()); err != nil {
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
	return wrappedMessageID{
		MessageID:  msgID,
		timetick:   msg.TimeTick(),
		txnContext: msg.TxnContext(),
	}, nil
}

type wrappedMessageID struct {
	message.MessageID
	timetick   uint64
	txnContext *message.TxnContext
}
