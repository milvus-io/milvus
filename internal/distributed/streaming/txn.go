package streaming

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

var _ Txn = (*txnImpl)(nil)

// txnImpl is the implementation of Txn.
type txnImpl struct {
	mu            sync.Mutex
	inFlightCount int
	state         message.TxnState
	opts          TxnOption
	txnCtx        *message.TxnContext
	*walAccesserImpl
}

// Append writes records to the log.
func (t *txnImpl) Append(ctx context.Context, msg message.MutableMessage, opts ...AppendOption) error {
	assertValidMessage(msg)
	assertIsDmlMessage(msg)

	t.mu.Lock()
	if t.state != message.TxnStateInFlight {
		t.mu.Unlock()
		return status.NewInvalidTransactionState("Append", message.TxnStateInFlight, t.state)
	}
	t.inFlightCount++
	t.mu.Unlock()

	defer func() {
		t.mu.Lock()
		t.inFlightCount--
		t.mu.Unlock()
	}()

	// assert if vchannel is equal.
	if msg.VChannel() != t.opts.VChannel {
		panic("vchannel not match when using transaction")
	}

	// setup txn context and add to wal.
	applyOpt(msg, opts...)
	_, err := t.appendToWAL(ctx, msg.WithTxnContext(*t.txnCtx))
	return err
}

// Commit commits the transaction.
func (t *txnImpl) Commit(ctx context.Context) (*types.AppendResult, error) {
	t.mu.Lock()
	if t.state != message.TxnStateInFlight {
		t.mu.Unlock()
		return nil, status.NewInvalidTransactionState("Commit", message.TxnStateInFlight, t.state)
	}
	t.state = message.TxnStateCommitted
	if t.inFlightCount != 0 {
		panic("in flight count not zero when commit")
	}
	t.mu.Unlock()
	defer t.walAccesserImpl.lifetime.Done()

	commit, err := message.NewCommitTxnMessageBuilderV2().
		WithVChannel(t.opts.VChannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		BuildMutable()
	if err != nil {
		return nil, err
	}
	return t.appendToWAL(ctx, commit.WithTxnContext(*t.txnCtx))
}

// Rollback rollbacks the transaction.
func (t *txnImpl) Rollback(ctx context.Context) error {
	t.mu.Lock()
	if t.state != message.TxnStateInFlight {
		t.mu.Unlock()
		return status.NewInvalidTransactionState("Rollback", message.TxnStateInFlight, t.state)
	}
	t.state = message.TxnStateRollbacked
	if t.inFlightCount != 0 {
		panic("in flight count not zero when rollback")
	}
	t.mu.Unlock()
	defer t.walAccesserImpl.lifetime.Done()

	rollback, err := message.NewRollbackTxnMessageBuilderV2().
		WithVChannel(t.opts.VChannel).
		WithHeader(&message.RollbackTxnMessageHeader{}).
		WithBody(&message.RollbackTxnMessageBody{}).
		BuildMutable()
	if err != nil {
		return err
	}
	_, err = t.appendToWAL(ctx, rollback.WithTxnContext(*t.txnCtx))
	return err
}
