package partialupdate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/idempotency"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// The tests in this file pin the transaction ordering contract for local
// transactions: BeginTxn takes effect before any body, every body takes effect
// before CommitTxn, no body is accepted once CommitTxn has arrived, and a
// CommitTxn arriving while a body is still in flight is rejected.

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
		requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, name+" before BeginTxn")
		require.False(t, appended, "%s before BeginTxn reached the WAL", name)
		require.Nil(t, env.partial.state.getTxn(txnContext.TxnID), "%s before BeginTxn created a record", name)
	}
	require.False(t, env.hasPKVersionAfter("v1", 10, 0))
}

// A producer sends CommitTxn only after every body has returned. A commit
// arriving while a body is still in flight is rejected with
// TRANSACTION_EXPIRED before TimeTick: it never reaches the WAL and takes no
// row lock, even on a PK an earlier CAS holds. It still freezes the write set,
// so every later body of the transaction is rejected, and the session stays in
// flight until its lease ends.
func TestPartialUpdateChainCommitWithBodyInFlightIsRejected(t *testing.T) {
	for _, cas := range []bool{true, false} {
		t.Run(fmt.Sprintf("cas_%t", cas), func(t *testing.T) {
			env := newPartialUpdateChainTestEnv(t)
			readTS := env.allocateReadTS(t)
			txnContext := beginOrderTestTxn(t, env)
			_, err := env.append(newOrderTestBody(t, txnContext, readTS, cas, 10), nil)
			require.NoError(t, err)
			inFlight := startStalledAppend(t, env, newOrderTestBody(t, txnContext, readTS, cas, 20))

			// An earlier CAS holds PK 10, so a commit that reached the row-lock
			// queue would block instead of returning.
			holder, err := env.partial.state.rowLocks.acquire(context.Background(), "v1", int64TestKeys(10), true)
			require.NoError(t, err)
			appended := false
			_, err = env.append(newOrderTestCommit(t, txnContext, cas), func(context.Context, message.MutableMessage) (message.MessageID, error) {
				appended = true
				return env.nextMessageID(), nil
			})
			requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, "commit with a body in flight")
			require.False(t, appended, "the rejected commit reached the WAL")
			require.Equal(t, 1, testRowLockRefs(env.partial.state.rowLocks, "v1"), "the rejected commit pinned the row-lock table")
			holder()
			session, err := env.txnManager.GetSessionOfTxn(txnContext.TxnID)
			require.NoError(t, err)
			require.Equal(t, message.TxnStateInFlight, session.State(), "the commit must be rejected before TimeTick")

			inFlight.release()
			require.NoError(t, receiveCommitValue(t, inFlight.done), "in-flight body")
			_, err = env.append(newOrderTestBody(t, txnContext, readTS, cas, 30), nil)
			requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, "body after the rejected commit")
			// The rejection response may be lost and the producer may resend the
			// same CommitTxn; with no body in flight any more it stays rejected.
			_, err = env.append(newOrderTestCommit(t, txnContext, cas), func(context.Context, message.MutableMessage) (message.MessageID, error) {
				appended = true
				return env.nextMessageID(), nil
			})
			requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, "resent commit after the rejection")
			require.False(t, appended, "the resent commit reached the WAL")
			for _, pk := range []int64{10, 20, 30} {
				require.False(t, env.hasPKVersionAfter("v1", pk, 0), "PK %d of the rejected transaction was published", pk)
			}
			requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10, 20, 30)

			// TimeTick runs this cleanup for every TimeTick message: lease expiry
			// ends the session and removes the record.
			env.txnManager.CleanupTxnUntil(tsoutil.AddPhysicalDurationOnTs(env.allocateReadTS(t), time.Hour))
			require.Nil(t, env.partial.state.getTxn(txnContext.TxnID))
		})
	}
}

// After a produce stream breaks, the producer resends a body whose first
// attempt keeps running in the WAL, because the WAL detaches the append from
// the client. The resent copy returns, and the producer sends CommitTxn while
// the first attempt is still in flight. The commit is rejected with
// TRANSACTION_EXPIRED, a local CAS commit included: the producer replays an
// ordinary transaction in full and turns the error into
// PartialUpdateRetryable for CAS. The stale attempt still completes, and
// nothing of the transaction is published.
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

			_, err = env.append(newOrderTestCommit(t, txnContext, cas), nil)
			requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, "commit with a stale body attempt in flight")
			session, err := env.txnManager.GetSessionOfTxn(txnContext.TxnID)
			require.NoError(t, err)
			require.Equal(t, message.TxnStateInFlight, session.State(), "the commit must be rejected before TimeTick")

			stale.release()
			require.NoError(t, receiveCommitValue(t, stale.done), "stale body attempt")
			_, err = env.append(newBody(30), nil)
			requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, "body after the rejected commit")
			for _, pk := range []int64{10, 20, 30} {
				require.False(t, env.hasPKVersionAfter("v1", pk, 0), "PK %d of the rejected transaction was published", pk)
			}
			require.Equal(t, message.TxnStateInFlight, session.State())
			requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10, 20, 30)
		})
	}
}

// The lock interceptor releases a body's count on every outcome of its append.
// A stale first attempt that fails in the WAL before CommitTxn is sent must
// not leave its count behind, or the commit would be rejected as if a body
// were still in flight.
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

// The idempotency interceptor builds the insert result of a keyed commit from
// the body results buffered before the commit, and completes the key only
// after the commit append succeeds. A keyed commit rejected because a stale
// body attempt is still in flight fails the key instead, so nothing is cached
// as a success: the full replay the producer sends next under a new TxnID owns
// the key, commits instead of being rolled back as a duplicate, and stores the
// complete insert result.
func TestPartialUpdateChainRejectedIdempotentCommitIsNotCached(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	env.chain = interceptors.NewChainedInterceptor(idempotency.NewInterceptorBuilder().Build(env.buildParam), env.chain)
	newCommit := func(txnContext message.TxnContext) message.MutableMessage {
		return message.NewCommitTxnMessageBuilderV2().
			WithVChannel("v1").
			WithHeader(&message.CommitTxnMessageHeader{}).
			WithBody(&message.CommitTxnMessageBody{}).
			WithIdempotencyKey("txn-key").
			MustBuildMutable().
			WithTxnContext(txnContext)
	}

	first := beginOrderTestTxn(t, env)
	stale := startStalledAppend(t, env, newOrderTestIdempotentInsert(first, []uint32{0}, 1))
	_, err := env.append(newOrderTestIdempotentInsert(first, []uint32{0}, 1), nil)
	require.NoError(t, err, "resent body")
	_, err = env.append(newOrderTestIdempotentInsert(first, []uint32{1, 2}, 2, 3), nil)
	require.NoError(t, err)
	_, err = env.append(newCommit(first), nil)
	requireOrderTestCode(t, err, streamingpb.StreamingCode_STREAMING_CODE_TRANSACTION_EXPIRED, "keyed commit with a stale body attempt in flight")
	stale.release()
	require.NoError(t, receiveCommitValue(t, stale.done), "stale body attempt")

	// The producer replays the full transaction under a new TxnID.
	replay := beginOrderTestTxn(t, env)
	_, err = env.append(newOrderTestIdempotentInsert(replay, []uint32{0}, 1), nil)
	require.NoError(t, err)
	_, err = env.append(newOrderTestIdempotentInsert(replay, []uint32{1, 2}, 2, 3), nil)
	require.NoError(t, err)
	session, err := env.txnManager.GetSessionOfTxn(replay.TxnID)
	require.NoError(t, err)
	_, err = env.append(newCommit(replay), nil)
	require.NoError(t, err, "replayed commit")
	// A duplicate hit would roll the replay back instead.
	require.Equal(t, message.TxnStateCommitted, session.State(), "the rejected commit was cached as a success")

	extra, err := env.append(newCommit(replay), nil)
	require.NoError(t, err, "duplicate commit")
	result, ok := extra.Extra.(*messagespb.IdempotentInsertResult)
	require.True(t, ok, "duplicate commit carries no insert result: %v", extra.Extra)
	require.Equal(t, []uint32{0, 1, 2}, result.GetRowOffsets(), "the idempotency record misses a body")
	require.Equal(t, []int64{1, 2, 3}, result.GetIds().GetIntId().GetData())
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

func newOrderTestIdempotentInsert(txnContext message.TxnContext, offsets []uint32, pks ...int64) message.MutableMessage {
	schemaVersion := int32(1)
	header := &message.InsertMessageHeader{CollectionId: 10, SchemaVersion: &schemaVersion}
	message.SetInsertHeaderIdempotentInsertResult(header, message.NewIdempotentInsertResult(offsets, &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}},
	}))
	return message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(header).
		WithBody(&msgpb.InsertRequest{FieldsData: []*schemapb.FieldData{int64PKFieldData(pks...)}}).
		MustBuildMutable().
		WithTxnContext(txnContext)
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
