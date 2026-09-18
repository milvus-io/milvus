package txn

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// AppendOp appends one message through the rest of the interceptor chain.
// It has the same shape as interceptors.Append. The interceptors package imports
// this package, so the type cannot be referenced from here.
type AppendOp = func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error)

// InTxnResult is the outcome of AppendInTxn.
type InTxnResult struct {
	// Main is the appended form of main. For an autocommit main it is the copy that
	// was written as a transaction body, carrying the time tick, the txn context
	// and any header rewritten downstream. For a CommitTxn main it equals Commit.
	Main message.ImmutableMessage
	// Commit is the appended CommitTxn. Its message id and time tick are what the
	// client of the original append should observe.
	Commit message.ImmutableMessage
}

// AppendInTxn appends main together with extras as one transaction, so that the
// consumer sees all of them at the time tick of the CommitTxn.
//
// It must be called from an interceptor that sits above the timetick interceptor
// and below the lock interceptor: every message needs its own time tick, and the
// whole group runs under the single lock acquisition of the original append.
//
//   - main is an autocommit DML message: BeginTxn, a copy of main, extras and
//     CommitTxn are appended. main itself is never modified, so the caller can
//     append it again after a redo. If any step after BeginTxn fails, a RollbackTxn
//     is appended and the error is returned.
//   - main is a CommitTxn: extras are appended as bodies of that transaction, then
//     main. If an extra fails, main is not appended and nothing is rolled back,
//     because the transaction belongs to the client.
//
// main must be the raw mutable message of the append chain and not a specialized
// wrapper, because the copy of main is built from the raw message.
//
// extras must be autocommit DML messages of the same vchannel. They are modified
// in place, so the caller must build a fresh extra for every attempt.
//
// ctx must be the context of the append chain. The txn context of the group is
// removed from the append result of an autocommit main, so that the client of the
// original append does not see a transaction it did not ask for.
//
// TODO: every redo of an upgraded insert writes one BeginTxn/RollbackTxn pair.
// TODO: rolled-back transactions written here are not yet covered by recovery and replication tests.
// TODO: when the client retries a CommitTxn, extras are appended again.
func AppendInTxn(ctx context.Context, appendOp AppendOp, main message.MutableMessage, extras []message.MutableMessage) (*InTxnResult, error) {
	if err := validateInTxnInput(main, extras); err != nil {
		return nil, err
	}
	if main.MessageType() == message.MessageTypeCommitTxn {
		return appendExtrasThenCommit(ctx, appendOp, main, extras)
	}
	return appendAsNewTxn(ctx, appendOp, main, extras)
}

func validateInTxnInput(main message.MutableMessage, extras []message.MutableMessage) error {
	if main == nil {
		return status.NewInner("append in txn: main message is nil")
	}
	if main.BroadcastHeader() != nil {
		return status.NewInner("append in txn: broadcast message %s is not supported", main.MessageType())
	}
	if main.ReplicateHeader() != nil {
		return status.NewInner("append in txn: replicated message %s is not supported", main.MessageType())
	}
	if main.MessageType() == message.MessageTypeCommitTxn {
		if main.TxnContext() == nil {
			return status.NewInner("append in txn: commit message has no txn context")
		}
	} else {
		if !main.MessageType().IsDMLMessageType() {
			return status.NewInner("append in txn: message type %s is not supported", main.MessageType())
		}
		if main.TxnContext() != nil {
			return status.NewInner("append in txn: a transaction body can not carry extra messages")
		}
	}
	for _, extra := range extras {
		if extra == nil {
			return status.NewInner("append in txn: extra message is nil")
		}
		if !extra.MessageType().IsDMLMessageType() {
			return status.NewInner("append in txn: extra message type %s is not supported", extra.MessageType())
		}
		if extra.BroadcastHeader() != nil {
			return status.NewInner("append in txn: broadcast extra message %s is not supported", extra.MessageType())
		}
		if extra.ReplicateHeader() != nil {
			return status.NewInner("append in txn: replicated extra message %s is not supported", extra.MessageType())
		}
		if extra.TxnContext() != nil {
			return status.NewInner("append in txn: extra message %s already belongs to a transaction", extra.MessageType())
		}
		if extra.VChannel() != main.VChannel() {
			return status.NewInner("append in txn: extra message vchannel %s differs from %s", extra.VChannel(), main.VChannel())
		}
	}
	return nil
}

func appendAsNewTxn(ctx context.Context, appendOp AppendOp, main message.MutableMessage, extras []message.MutableMessage) (*InTxnResult, error) {
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(main.VChannel()).
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable()
	if barrierTimeTick := main.BarrierTimeTick(); barrierTimeTick != 0 {
		// main waits for the barrier, so the whole group must wait for it. Otherwise
		// BeginTxn takes a time tick below the barrier.
		begin = begin.WithBarrierTimeTick(barrierTimeTick)
	}
	message.InjectTraceContext(ctx, begin)
	if _, err := appendOp(ctx, begin); err != nil {
		// the timetick interceptor creates the session before it writes the begin
		// message, so a session may exist even though the append failed.
		if txnCtx := begin.TxnContext(); txnCtx != nil {
			rollbackTxn(ctx, appendOp, main.VChannel(), *txnCtx)
		}
		// The txn manager refuses a new transaction with a transaction error while it closes.
		return nil, selfBuiltTxnError(err)
	}
	// the timetick interceptor writes the allocated txn context into the begin message in place.
	txnCtx := begin.TxnContext()
	if txnCtx == nil {
		return nil, status.NewInner("append in txn: begin message carries no txn context after append")
	}

	body := message.CloneMutableMessage(main).WithTxnContext(*txnCtx)
	mainID, err := appendOp(ctx, body)
	if err != nil {
		rollbackTxn(ctx, appendOp, main.VChannel(), *txnCtx)
		return nil, selfBuiltTxnError(err)
	}
	for _, extra := range extras {
		message.InjectTraceContext(ctx, extra)
		if _, err := appendOp(ctx, extra.WithTxnContext(*txnCtx)); err != nil {
			rollbackTxn(ctx, appendOp, main.VChannel(), *txnCtx)
			return nil, selfBuiltTxnError(err)
		}
	}

	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel(main.VChannel()).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		// The durable idempotency window is rebuilt from the wal. For an assembled
		// transaction it reads the key of the CommitTxn only, so a commit without
		// the key makes a retry after a wal reopen write the rows a second time.
		// The copy of main keeps its own key, like every body of a client
		// transaction does.
		WithIdempotencyKey(message.IdempotencyKeyOf(main)).
		MustBuildMutable().
		WithTxnContext(*txnCtx)
	message.InjectTraceContext(ctx, commit)
	commitID, err := appendOp(ctx, commit)
	if err != nil {
		// A failed CommitTxn may still have been persisted, and the session may
		// still be in flight. A rollback selects the second case by itself, because
		// it is only persisted while the session is in flight. Without it such a
		// session holds the last confirmed message id of the pchannel for the whole
		// keepalive.
		rollbackTxn(ctx, appendOp, main.VChannel(), *txnCtx)
		return nil, selfBuiltTxnError(err)
	}
	// the client of the original append asked for one autocommit message, so it must
	// not see the txn context of the transaction built here.
	if utility.GetExtraAppendResult(ctx) != nil {
		utility.ReplaceAppendResultTxnContext(ctx, nil)
	}
	return &InTxnResult{
		Main:   body.IntoImmutableMessage(mainID),
		Commit: commit.IntoImmutableMessage(commitID),
	}, nil
}

func appendExtrasThenCommit(ctx context.Context, appendOp AppendOp, commit message.MutableMessage, extras []message.MutableMessage) (*InTxnResult, error) {
	txnCtx := *commit.TxnContext()
	for _, extra := range extras {
		message.InjectTraceContext(ctx, extra)
		if _, err := appendOp(ctx, extra.WithTxnContext(txnCtx)); err != nil {
			return nil, err
		}
	}
	commitID, err := appendOp(ctx, commit)
	if err != nil {
		return nil, err
	}
	appended := commit.IntoImmutableMessage(commitID)
	return &InTxnResult{Main: appended, Commit: appended}, nil
}

// selfBuiltTxnError replaces an error that reports the refusal or the end of the
// transaction built by appendAsNewTxn. err must not be nil. The client appended one
// autocommit message and knows nothing about that transaction, so a transaction
// error would stop it from retrying. Every other error is returned unchanged.
func selfBuiltTxnError(err error) error {
	if status.AsStreamingError(err).IsTxnUnavilable() {
		return status.NewInner("the transaction started inside the wal did not reach its commit: %v", err)
	}
	return err
}

// rollbackTxn ends a transaction started by appendAsNewTxn. A failure is only
// logged: the transaction then ends by keepalive expiration.
func rollbackTxn(ctx context.Context, appendOp AppendOp, vchannel string, txnCtx message.TxnContext) {
	rollback := message.NewRollbackTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.RollbackTxnMessageHeader{}).
		WithBody(&message.RollbackTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx)
	message.InjectTraceContext(ctx, rollback)
	if _, err := appendOp(ctx, rollback); err != nil {
		if status.AsStreamingError(err).IsTxnUnavilable() {
			// The rollback is rejected before anything is written when the session is
			// no longer in flight or already gone. The commit took effect or the
			// session ended, so there is nothing left to roll back.
			mlog.Debug(ctx, "the transaction started inside the wal ended before the rollback",
				mlog.FieldVChannel(vchannel),
				mlog.Int64("txnID", int64(txnCtx.TxnID)),
				mlog.Err(err))
			return
		}
		mlog.Warn(ctx, "failed to roll back the transaction started inside the wal, it will expire by keepalive",
			mlog.FieldVChannel(vchannel),
			mlog.Int64("txnID", int64(txnCtx.TxnID)),
			mlog.Err(err))
	}
}
