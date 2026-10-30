package partialupdate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// The tests in this file pin the transaction ordering contract for local
// transactions: BeginTxn takes effect before any body, every body takes effect
// before CommitTxn, a CommitTxn that arrives while a body is in flight is
// rejected, and no body is accepted once CommitTxn has arrived.

// A body sent before its BeginTxn has no session: TimeTick rejects it before
// the WAL append, and partial-update admission creates no record for it.
func TestPartialUpdateChainBodyBeforeBeginIsRejected(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	readTS := env.allocateReadTS(t)
	txnContext := message.TxnContext{TxnID: 4242, Keepalive: time.Minute}
	for name, msg := range map[string]message.MutableMessage{
		"ordinary body": newGuardTestDelete(10).WithTxnContext(txnContext),
		"cas body":      newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10)}, validCASMeta(readTS, 1)).WithTxnContext(txnContext),
		"commit":        newCommitTxnMessage("v1", txnContext.TxnID, 0),
	} {
		appended := false
		_, err := env.append(msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			appended = true
			return env.nextMessageID(), nil
		})
		requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE, name+" before BeginTxn")
		require.ErrorContains(t, err, "not found")
		require.False(t, appended, "%s before BeginTxn reached the WAL", name)
		require.Nil(t, env.partial.state.getTxn(txnContext.TxnID), "%s before BeginTxn created a record", name)
	}
	require.False(t, env.hasPKVersionAfter("v1", 10, 0))
}

// A producer must send CommitTxn only after every body has returned. A commit
// that arrives while a body is still in flight is rejected with
// TRANSACTION_EXPIRED before TimeTick: it never reaches the WAL append, takes
// no row lock and leaves the session InFlight until its lease ends. It still
// freezes the write set, so every later body is rejected. Bodies are counted:
// one returned body does not admit the commit while another is in flight.
func TestPartialUpdateChainCommitWithBodyInFlightIsRejected(t *testing.T) {
	for _, cas := range []bool{true, false} {
		t.Run(fmt.Sprintf("cas_%t", cas), func(t *testing.T) {
			env := newPartialUpdateChainTestEnv(t)
			readTS := env.allocateReadTS(t)
			txnContext := beginOrderTestTxn(t, env)
			newBody := func(pks ...int64) message.MutableMessage {
				return newOrderTestBody(t, txnContext, readTS, cas, pks...)
			}
			_, err := env.append(newBody(10), nil)
			require.NoError(t, err, "returned body")
			first := startStalledAppend(t, env, newBody(20))
			second := startStalledAppend(t, env, newBody(30))
			first.release()
			require.NoError(t, receiveCommitValue(t, first.done), "first in-flight body")

			// An earlier CAS holds every PK of the transaction, so a commit
			// that reached the row-lock queue would block instead of returning.
			holder, err := env.partial.state.rowLocks.acquire(context.Background(), "v1", int64TestKeys(10, 20, 30), true)
			require.NoError(t, err)
			err = appendRejectedBeforeWAL(t, env, newOrderTestCommit(t, txnContext, cas))
			requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, "commit with a body in flight")
			require.Equal(t, 1, testRowLockRefs(env.partial.state.rowLocks, "v1"), "the rejected commit took a row lock")
			holder()
			session, err := env.txnManager.GetSessionOfTxn(txnContext.TxnID)
			require.NoError(t, err)
			require.Equal(t, message.TxnStateInFlight, session.State(), "the commit must be rejected before TimeTick")

			err = appendRejectedBeforeWAL(t, env, newBody(40))
			requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, "body after the rejected CommitTxn")

			second.release()
			require.NoError(t, receiveCommitValue(t, second.done), "second in-flight body")
			// The rejection response may be lost and the producer may resend the
			// same CommitTxn; with no body in flight any more it stays rejected.
			err = appendRejectedBeforeWAL(t, env, newOrderTestCommit(t, txnContext, cas))
			requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, "resent commit after the rejection")
			for _, pk := range []int64{10, 20, 30, 40} {
				require.False(t, env.hasPKVersionAfter("v1", pk, 0), "PK %d of the rejected transaction was published", pk)
			}
			// TimeTick runs this cleanup for every TimeTick message; the
			// session ends with its lease.
			env.txnManager.CleanupTxnUntil(tsoutil.AddPhysicalDurationOnTs(env.allocateReadTS(t), time.Hour))
			require.Nil(t, env.partial.state.getTxn(txnContext.TxnID))
			requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10, 20, 30, 40)
		})
	}
}

// After a produce stream breaks, the producer resends a body whose first
// attempt keeps running in the WAL, because the WAL detaches the append from
// the client. The resent copy returns, and the producer sends CommitTxn while
// the first attempt is still in flight. The commit is rejected with
// TRANSACTION_EXPIRED, a local CAS commit included: the producer replays an
// ordinary transaction in full and reports a CAS as PartialUpdateRetryable.
// This rare false conflict is the accepted cost of not waiting for bodies.
func TestPartialUpdateChainCommitWithStaleBodyAttemptIsRejected(t *testing.T) {
	for _, cas := range []bool{true, false} {
		t.Run(fmt.Sprintf("cas_%t", cas), func(t *testing.T) {
			env := newPartialUpdateChainTestEnv(t)
			readTS := env.allocateReadTS(t)
			txnContext := beginOrderTestTxn(t, env)
			newBody := func(pks ...int64) message.MutableMessage {
				return newOrderTestBody(t, txnContext, readTS, cas, pks...)
			}
			stale := startStalledAppend(t, env, newBody(10, 20))
			_, err := env.append(newBody(10, 20), nil)
			require.NoError(t, err, "resent body")

			err = appendRejectedBeforeWAL(t, env, newOrderTestCommit(t, txnContext, cas))
			requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, "commit with a stale body attempt in flight")
			session, err := env.txnManager.GetSessionOfTxn(txnContext.TxnID)
			require.NoError(t, err)
			require.Equal(t, message.TxnStateInFlight, session.State(), "the commit must be rejected before TimeTick")

			err = appendRejectedBeforeWAL(t, env, newBody(30))
			requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, "body after CommitTxn")

			stale.release()
			require.NoError(t, receiveCommitValue(t, stale.done), "stale body attempt")
			for _, pk := range []int64{10, 20, 30} {
				require.False(t, env.hasPKVersionAfter("v1", pk, 0), "PK %d of the rejected transaction was published", pk)
			}
			require.Equal(t, message.TxnStateInFlight, session.State())
			requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10, 20, 30)
		})
	}
}

// The lock interceptor releases a body's count on every outcome of its append.
// Once a stale first attempt has failed in the WAL and returned, the commit is
// admitted as after one that succeeds, and publishes the resent body's PKs.
func TestPartialUpdateChainCommitAfterFailedStaleBodyAttemptCommits(t *testing.T) {
	for _, cas := range []bool{true, false} {
		t.Run(fmt.Sprintf("cas_%t", cas), func(t *testing.T) {
			env := newPartialUpdateChainTestEnv(t)
			readTS := env.allocateReadTS(t)
			txnContext := beginOrderTestTxn(t, env)
			walErr := errors.New("stale body attempt failed in the WAL")
			stale := startStalledAppendWithResult(t, env, newOrderTestBody(t, txnContext, readTS, cas, 10, 20), walErr)
			_, err := env.append(newOrderTestBody(t, txnContext, readTS, cas, 10, 20), nil)
			require.NoError(t, err, "resent body")
			stale.release()
			require.ErrorIs(t, receiveCommitValue(t, stale.done), walErr, "stale body attempt")

			_, err = env.append(newOrderTestCommit(t, txnContext, cas), nil)
			require.NoError(t, err, "commit")
			require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
			require.True(t, env.hasPKVersionAfter("v1", 20, readTS))
			require.Nil(t, env.partial.state.getTxn(txnContext.TxnID))
			requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10, 20)
		})
	}
}

// The commit freezes its write set on arrival, before it waits in the row-lock
// queue. A body arriving while the commit waits there must be rejected and its
// PK never published, or the commit would publish a PK it never locked.
func TestPartialUpdateChainBodyDuringCommitRowLockWaitIsRejected(t *testing.T) {
	for _, cas := range []bool{true, false} {
		t.Run(fmt.Sprintf("cas_%t", cas), func(t *testing.T) {
			env := newPartialUpdateChainTestEnv(t)
			readTS := env.allocateReadTS(t)
			var txnContext message.TxnContext
			if cas {
				txnContext = env.prepareCASTxn(t, readTS, 10)
			} else {
				txnContext = beginOrderTestTxn(t, env)
				_, err := env.append(newGuardTestDelete(10).WithTxnContext(txnContext), nil)
				require.NoError(t, err)
			}
			// An earlier CAS on PK 10 makes both CAS and ordinary commits wait.
			holder, err := env.partial.state.rowLocks.acquire(context.Background(), "v1", int64TestKeys(10), true)
			require.NoError(t, err)

			commitDone := appendAsync(env, newOrderTestCommit(t, txnContext, cas))
			// The commit pins the table in rowLocks.acquire, after admission.
			waitForTestRowLockRefs(t, env.partial.state.rowLocks, "v1", 2)
			requireCommitPending(t, commitDone)
			_, err = env.append(newGuardTestDelete(20).WithTxnContext(txnContext), nil)
			requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, "body during commit row-lock wait")

			holder()
			require.NoError(t, receiveCommitValue(t, commitDone))
			require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
			require.False(t, env.hasPKVersionAfter("v1", 20, 0), "the rejected body was published")
			requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10, 20)
		})
	}
}

func beginOrderTestTxn(t *testing.T, env *partialUpdateChainTestEnv) message.TxnContext {
	t.Helper()
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: time.Minute.Milliseconds()}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable()
	result, err := env.append(begin, nil)
	require.NoError(t, err)
	require.NotNil(t, result.TxnCtx)
	return *result.TxnCtx
}

func newOrderTestBody(t *testing.T, txnContext message.TxnContext, readTS uint64, cas bool, pks ...int64) message.MutableMessage {
	t.Helper()
	if cas {
		return newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(pks...)}, validCASMeta(readTS, 1)).
			WithTxnContext(txnContext)
	}
	return newGuardTestDelete(pks...).WithTxnContext(txnContext)
}

func newOrderTestCommit(t *testing.T, txnContext message.TxnContext, cas bool) message.MutableMessage {
	t.Helper()
	if cas {
		return newChainTestCASCommit(t, txnContext)
	}
	return newCommitTxnMessage("v1", txnContext.TxnID, 0)
}

func appendAsync(env *partialUpdateChainTestEnv, msg message.MutableMessage) <-chan error {
	done := make(chan error, 1)
	go func() {
		ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
		ctx = utility.WithAppendMetricsContext(ctx, env.writeMetrics.StartAppend(msg))
		_, err := env.chain.DoAppend(ctx, msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return env.nextMessageID(), nil
		})
		done <- err
	}()
	return done
}

// appendRejectedBeforeWAL appends msg, which must be rejected before the WAL
// append, and returns its error. It fails if msg reaches the WAL append or
// blocks.
func appendRejectedBeforeWAL(t *testing.T, env *partialUpdateChainTestEnv, msg message.MutableMessage) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := env.append(msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			t.Errorf("%s reached the WAL append", msg.MessageType())
			return env.nextMessageID(), nil
		})
		done <- err
	}()
	return receiveCommitValue(t, done)
}

// heldAppend is an append blocked in the WAL append until release, like the
// first attempt of a body whose produce stream broke.
// Tests release blockers only explicitly: a failed test leaves its appends
// blocked rather than letting them run into the environment teardown.
type heldAppend struct {
	release func()
	done    chan error
}

func newHeldAppend() (*heldAppend, chan struct{}) {
	unblock := make(chan struct{})
	var once sync.Once
	held := &heldAppend{release: func() { once.Do(func() { close(unblock) }) }, done: make(chan error, 1)}
	return held, unblock
}

// startStalledAppend appends msg and returns once it is inside the WAL append.
func startStalledAppend(t *testing.T, env *partialUpdateChainTestEnv, msg message.MutableMessage) *heldAppend {
	t.Helper()
	return startStalledAppendWithResult(t, env, msg, nil)
}

// startStalledAppendWithResult is startStalledAppend whose WAL append fails
// with walErr after release when walErr is not nil.
func startStalledAppendWithResult(t *testing.T, env *partialUpdateChainTestEnv, msg message.MutableMessage, walErr error) *heldAppend {
	t.Helper()
	held, unblock := newHeldAppend()
	atWAL := make(chan struct{})
	go func() {
		_, err := env.append(msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(atWAL)
			<-unblock
			if walErr != nil {
				return nil, walErr
			}
			return env.nextMessageID(), nil
		})
		held.done <- err
	}()
	receiveCommitValue(t, atWAL)
	return held
}

func requireOrderTestCode(t *testing.T, err error, code streamingpb.StreamingCode, what string) {
	t.Helper()
	require.Error(t, err, what)
	streamingErr := status.AsStreamingError(err)
	require.NotNil(t, streamingErr, what)
	require.Equal(t, code, streamingErr.Code, "%s: %v", what, err)
}

// requireCommitPending asserts that an append observed to be blocked stays
// blocked.
func requireCommitPending(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("append bypassed its blocker: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}
