package partialupdate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// Producers send CommitTxn only after every body append has returned. A
// CommitTxn that arrives while a body is still in flight would lock an
// incomplete write set, so it is rejected and can be retried once the body
// finishes.
func TestPartialUpdateChainCommitRejectsInFlightBody(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	readTS := env.allocateReadTS(t)
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: time.Second.Milliseconds()}).
		WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable()
	result, err := env.append(begin, nil)
	require.NoError(t, err)
	require.NotNil(t, result.TxnCtx)
	body := newCASInsertMessage(t, []*schemapb.FieldData{int64PKFieldData(10, 20)}, validCASMeta(readTS, 1)).
		WithTxnContext(*result.TxnCtx)
	atWAL := make(chan struct{})
	releaseBody := make(chan struct{})
	bodyDone := make(chan error, 1)
	go func() {
		_, err := env.append(body, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(atWAL)
			<-releaseBody
			return env.nextMessageID(), nil
		})
		bodyDone <- err
	}()
	<-atWAL
	_, err = env.append(newChainTestCASCommit(t, *result.TxnCtx), nil)
	requirePartialUpdateRetryable(t, err)
	session, err := env.txnManager.GetSessionOfTxn(result.TxnCtx.TxnID)
	require.NoError(t, err)
	require.Equal(t, message.TxnStateInFlight, session.State(), "rejected commit must not freeze the session")
	close(releaseBody)
	require.NoError(t, <-bodyDone)

	_, err = env.append(newChainTestCASCommit(t, *result.TxnCtx), nil)
	require.NoError(t, err)
	require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
	require.True(t, env.hasPKVersionAfter("v1", 20, readTS))
}

// Once CommitTxn holds its row locks, the locked write set must not grow.
func TestPartialUpdateChainBodyAfterCommitAdmissionIsRejected(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	cas := env.prepareCASTxn(t, env.allocateReadTS(t), 10)
	atWAL, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	commitDone := make(chan error, 1)
	go func() {
		_, err := env.append(newChainTestCASCommit(t, cas), func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(atWAL)
			<-release
			return env.nextMessageID(), nil
		})
		commitDone <- err
	}()
	<-atWAL
	_, err := env.append(newGuardTestDelete(20).WithTxnContext(cas), nil)
	require.True(t, status.AsStreamingError(err).IsTxnExpired(), "late body returned: %v", err)
	once.Do(func() { close(release) })
	require.NoError(t, <-commitDone)
	require.False(t, env.hasPKVersionAfter("v1", 20, 0))
}

// A late body of a transaction that is already gone must not recreate partial
// update state: TimeTick rejects the body and nothing would clean the record.
func TestPartialUpdateBodyOfUnknownTxnDoesNotCreateState(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	body := newGuardTestDelete(10).WithTxnContext(message.TxnContext{TxnID: 999, Keepalive: time.Second})
	_, release, err := env.partial.Acquire(context.Background(), body)
	require.NoError(t, err)
	release()
	require.Nil(t, env.partial.state.getTxn(999))
}

func TestPartialUpdateChainRecoveredCommitPublishesPKBeforeCAS(t *testing.T) {
	env := newPartialUpdateChainTestEnvWithRecoveredTxn(t, false)
	// The mock allocator starts at 1, which is already owned by the recovered
	// fixture. Production allocators never reuse a recovered transaction ID.
	_, err := resource.Resource().IDAllocator().Allocate(context.Background())
	require.NoError(t, err)
	readTS := env.allocateReadTS(t)
	cas := env.prepareCASTxn(t, readTS, 10)
	atWAL := make(chan struct{})
	release := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		_, err := env.append(newCommitTxnMessage("v1", 1, 0), func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(atWAL)
			<-release
			return env.nextMessageID(), nil
		})
		writerDone <- err
	}()
	<-atWAL
	casDone := make(chan error, 1)
	go func() {
		_, err := env.append(newChainTestCASCommit(t, cas), nil)
		casDone <- err
	}()
	// The CAS queues on the stripe in the lock interceptor, before TimeTick.
	waitForTestPKWriter(t, env.partial.state.rowLocks, "v1", 10)
	select {
	case err := <-casDone:
		close(release)
		t.Fatalf("CAS bypassed the recovered ordinary transaction PK lock: %v", err)
	default:
	}
	close(release)
	require.NoError(t, <-writerDone)
	requirePartialUpdateRetryable(t, <-casDone)
}

func TestPartialUpdateChainDisjointPKsProceedDuringCASCommit(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	readTS := env.allocateReadTS(t)
	first := env.prepareCASTxn(t, readTS, 10)
	second := env.prepareCASTxn(t, readTS, 20)
	atWAL := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := env.append(newChainTestCASCommit(t, first), func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(atWAL)
			<-release
			return env.nextMessageID(), nil
		})
		firstDone <- err
	}()
	<-atWAL
	// Construct both writes while the first CAS remains inside its WAL append.
	// These PKs use different stripes, so neither write may wait.
	secondDone := make(chan error, 1)
	go func() {
		_, err := env.append(newChainTestCASCommit(t, second), nil)
		secondDone <- err
	}()
	writerDone := make(chan error, 1)
	go func() {
		_, err := env.append(newGuardTestDelete(30), nil)
		writerDone <- err
	}()
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("different-PK CAS was serialized behind the first CAS")
	}
	select {
	case err := <-writerDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("different-PK ordinary write was serialized behind CAS")
	}
	close(release)
	require.NoError(t, <-firstDone)
}

func TestPartialUpdateChainDisjointOrdinaryWriterDoesNotBlockCAS(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	readTS := env.allocateReadTS(t)
	cas := env.prepareCASTxn(t, readTS, 10)
	atWAL := make(chan struct{})
	release := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		_, err := env.append(newGuardTestDelete(20), func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(atWAL)
			<-release
			return env.nextMessageID(), nil
		})
		writerDone <- err
	}()
	<-atWAL
	casDone := make(chan error, 1)
	go func() {
		_, err := env.append(newChainTestCASCommit(t, cas), nil)
		casDone <- err
	}()
	select {
	case err := <-casDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("CAS waited for an unrelated ordinary PK write")
	}
	close(release)
	require.NoError(t, <-writerDone)
}

func TestPartialUpdateChainOrdinaryTxnCommitParticipatesInPKLock(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	readTS := env.allocateReadTS(t)
	cas := env.prepareCASTxn(t, readTS, 10)
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: time.Second.Milliseconds()}).
		WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable()
	result, err := env.append(begin, nil)
	require.NoError(t, err)
	require.NotNil(t, result.TxnCtx)
	// The PK only appears in a body message, not the CommitTxn envelope.
	_, err = env.append(newGuardTestDelete(10, 20).WithTxnContext(*result.TxnCtx), nil)
	require.NoError(t, err)
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*result.TxnCtx)
	atWAL := make(chan struct{})
	release := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		_, err := env.append(commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(atWAL)
			<-release
			return env.nextMessageID(), nil
		})
		writerDone <- err
	}()
	<-atWAL
	casDone := make(chan error, 1)
	go func() {
		_, err := env.append(newChainTestCASCommit(t, cas), nil)
		casDone <- err
	}()
	waitForTestPKWriter(t, env.partial.state.rowLocks, "v1", 10)
	select {
	case err := <-casDone:
		close(release)
		t.Fatalf("CAS completed before ordinary txn write-set publication: %v", err)
	default:
	}
	close(release)
	require.NoError(t, <-writerDone)
	requirePartialUpdateRetryable(t, <-casDone)
}

func TestPartialUpdateChainMultiPKCASDeduplicatesWriteSet(t *testing.T) {
	env := newPartialUpdateChainTestEnv(t)
	readTS := env.allocateReadTS(t)
	// Duplicate PKs occur naturally when an upsert contains both Delete/Insert.
	cas := env.prepareCASTxn(t, readTS, 20, 10, 20, 10)
	done := make(chan error, 1)
	go func() {
		_, err := env.append(newChainTestCASCommit(t, cas), nil)
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("duplicate PKs deadlocked CAS admission")
	}
	require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
	require.True(t, env.hasPKVersionAfter("v1", 20, readTS))
}

func TestRowLocksEmptyWriteSetDoesNotBlockExactPKWrites(t *testing.T) {
	g := newRowLockManager()
	emptyRelease := acquireTestRowLock(t, g, "v1", primaryKeys{}, false)
	defer emptyRelease()
	release := acquireTestRowLock(t, g, "v1", primaryKeys{int64Values: []int64{10}}, true)
	release()
	requireTestRowLocksAvailable(t, g, "v1", 10)
}

func TestRowLocksOrderAndSeparateTypedPKs(t *testing.T) {
	g := newRowLockManager()
	intPKs := primaryKeys{int64Values: []int64{20, 10, 20}}
	release := acquireTestRowLock(t, g, "v1", intPKs, true)
	// These typed values hash to different stripes in the same vchannel.
	require.NotEqual(t, int64RowLockStripe(10), stringRowLockStripe("10"))
	require.NotEqual(t, int64RowLockStripe(20), stringRowLockStripe("10"))
	stringRelease := acquireTestRowLock(t, g, "v1", primaryKeys{stringValues: []string{"10", "10"}}, true)
	stringRelease()
	done := make(chan struct{})
	go func() {
		secondRelease := acquireTestRowLock(t, g, "v1", primaryKeys{int64Values: []int64{10, 20}}, true)
		secondRelease()
		close(done)
	}()
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("oppositely ordered overlapping PK sets deadlocked")
	}
}

func TestRowLockManagerOrdinaryWritesTakeSharedPKLocks(t *testing.T) {
	g := newRowLockManager()
	pks := primaryKeys{int64Values: []int64{20, 10, 20}, stringValues: []string{"20", "10", "20"}}
	first := acquireTestRowLock(t, g, "v1", pks, false)
	second := acquireTestRowLock(t, g, "v1", pks, false)
	for _, key := range []pkLockKey{
		{vchannel: "v1", kind: primaryKeyKindInt64, int64Value: 10},
		{vchannel: "v1", kind: primaryKeyKindString, stringValue: "10"},
	} {
		require.False(t, testPKMutex(t, g, key).TryLock())
		require.True(t, testPKMutex(t, g, key).TryRLock())
		testPKMutex(t, g, key).RUnlock()
	}
	require.Equal(t, []int64{20, 10, 20}, pks.int64Values)
	require.Equal(t, []string{"20", "10", "20"}, pks.stringValues)
	first()
	second()
	requireTestRowLocksAvailable(t, g, "v1", 10, 20)
}

func TestRowLockManagerCASWaitsForReadersAndBlocksLaterReaders(t *testing.T) {
	g := newRowLockManager()
	pks := primaryKeys{int64Values: []int64{10, 20}}
	first := acquireTestRowLock(t, g, "v1", pks, false)
	second := acquireTestRowLock(t, g, "v1", pks, false)
	casResult := startTestRowLock(context.Background(), g, "v1", primaryKeys{int64Values: []int64{10, 10}}, true)
	waitForTestPKWriter(t, g, "v1", 10)
	laterResult := startTestRowLock(context.Background(), g, "v1", primaryKeys{int64Values: []int64{5, 10}}, false)
	waitForTestPKHeld(t, g, "v1", 5)
	first()
	select {
	case result := <-casResult:
		if result.release != nil {
			result.release()
		}
		t.Fatalf("CAS bypassed the remaining shared holder: %v", result.err)
	default:
	}
	disjoint := acquireTestRowLock(t, g, "v1", primaryKeys{int64Values: []int64{30}, stringValues: []string{"10"}}, false)
	disjoint()
	otherChannel := acquireTestRowLock(t, g, "v2", primaryKeys{int64Values: []int64{10}}, true)
	otherChannel()
	second()
	cas := awaitTestRowLock(t, casResult)
	require.NoError(t, cas.err)
	require.NotNil(t, cas.release)
	select {
	case result := <-laterResult:
		if result.release != nil {
			result.release()
		}
		cas.release()
		t.Fatalf("ordinary write bypassed the queued CAS: %v", result.err)
	default:
	}
	cas.release()
	later := awaitTestRowLock(t, laterResult)
	require.NoError(t, later.err)
	later.release()
	requireTestRowLocksAvailable(t, g, "v1", 5, 10, 20, 30)
}

func TestRowLockManagerCancellationReleasesPartiallyAcquiredPKs(t *testing.T) {
	for _, exclusive := range []bool{false, true} {
		name := "shared"
		if exclusive {
			name = "exclusive"
		}
		t.Run(name, func(t *testing.T) {
			g := newRowLockManager()
			blocker := acquireTestRowLock(t, g, "v1", primaryKeys{int64Values: []int64{10}}, true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := startTestRowLock(ctx, g, "v1", primaryKeys{int64Values: []int64{10, 20, 10}}, exclusive)
			// Stripe(20) precedes stripe(10); cancellation must release the
			// already acquired stripe even though the input starts at PK 10.
			require.Less(t, int64RowLockStripe(20), int64RowLockStripe(10), "test relies on stripe order")
			waitForTestPKHeld(t, g, "v1", 20)
			cancel()
			// RWMutex itself is not cancellable. Release its blocker so acquire
			// can observe cancellation and unwind all acquired PKs.
			blocker()
			acquired := awaitTestRowLock(t, result)
			require.ErrorIs(t, acquired.err, context.Canceled)
			require.Nil(t, acquired.release)
			requireTestRowLocksAvailable(t, g, "v1", 10, 20)
		})
	}
}

func TestRowLockManagerDuplicateSharedPKDoesNotDeadlockQueuedCAS(t *testing.T) {
	g := newRowLockManager()
	ctx := &pauseAfterFirstPKContext{
		Context: context.Background(), manager: g, t: t,
		key:    pkLockKey{vchannel: "v1", kind: primaryKeyKindInt64, int64Value: 10},
		locked: make(chan struct{}), resume: make(chan struct{}),
	}
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(ctx.resume) }) }
	t.Cleanup(resume)
	ordinaryResult := startTestRowLock(ctx, g, "v1", primaryKeys{int64Values: []int64{20, 10, 10}}, false)
	select {
	case <-ctx.locked:
	case <-time.After(time.Second):
		t.Fatal("ordinary write did not acquire its first PK")
	}
	casResult := startTestRowLock(context.Background(), g, "v1", primaryKeys{int64Values: []int64{10}}, true)
	waitForTestPKWriter(t, g, "v1", 10)
	resume()
	// A second RLock(10) would wait behind CAS while still holding the first
	// RLock(10), so this cannot finish unless the shared set is deduplicated.
	ordinary := awaitTestRowLock(t, ordinaryResult)
	require.NoError(t, ordinary.err)
	ordinary.release()
	cas := awaitTestRowLock(t, casResult)
	require.NoError(t, cas.err)
	cas.release()
	requireTestRowLocksAvailable(t, g, "v1", 10, 20)
}

type pauseAfterFirstPKContext struct {
	context.Context
	manager *rowLockManager
	t       *testing.T
	key     pkLockKey
	once    sync.Once
	locked  chan struct{}
	resume  chan struct{}
}

func (ctx *pauseAfterFirstPKContext) Err() error {
	if err := ctx.Context.Err(); err != nil {
		return err
	}
	if testPKMutex(ctx.t, ctx.manager, ctx.key).TryLock() {
		testPKMutex(ctx.t, ctx.manager, ctx.key).Unlock()
		return nil
	}
	ctx.once.Do(func() {
		close(ctx.locked)
		<-ctx.resume
	})
	return ctx.Context.Err()
}

func acquireTestRowLock(t *testing.T, g *rowLockManager, vchannel string, pks primaryKeys, exclusive bool) func() {
	t.Helper()
	release, err := g.acquire(context.Background(), vchannel, pks, exclusive)
	require.NoError(t, err)
	var once sync.Once
	done := func() { once.Do(release) }
	t.Cleanup(done)
	return done
}

type testRowLockResult struct {
	release func()
	err     error
}

func startTestRowLock(ctx context.Context, g *rowLockManager, vchannel string, pks primaryKeys, exclusive bool) <-chan testRowLockResult {
	result := make(chan testRowLockResult, 1)
	go func() {
		release, err := g.acquire(ctx, vchannel, pks, exclusive)
		result <- testRowLockResult{release: release, err: err}
	}()
	return result
}

func awaitTestRowLock(t *testing.T, result <-chan testRowLockResult) testRowLockResult {
	t.Helper()
	select {
	case acquired := <-result:
		return acquired
	case <-time.After(time.Second):
		t.Fatal("row lock did not finish after its blocker completed")
		return testRowLockResult{}
	}
}

func waitForTestPKWriter(t *testing.T, g *rowLockManager, vchannel string, pk int64) {
	t.Helper()
	key := pkLockKey{vchannel: vchannel, kind: primaryKeyKindInt64, int64Value: pk}
	mutex := testPKMutex(t, g, key)
	require.Eventually(t, func() bool {
		if mutex.TryRLock() {
			mutex.RUnlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
}

func waitForTestPKHeld(t *testing.T, g *rowLockManager, vchannel string, pk int64) {
	t.Helper()
	key := pkLockKey{vchannel: vchannel, kind: primaryKeyKindInt64, int64Value: pk}
	mutex := testPKMutex(t, g, key)
	require.Eventually(t, func() bool {
		if mutex.TryLock() {
			mutex.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
}

func requireTestRowLocksAvailable(t *testing.T, g *rowLockManager, vchannel string, pks ...int64) {
	t.Helper()
	for _, pk := range pks {
		key := pkLockKey{vchannel: vchannel, kind: primaryKeyKindInt64, int64Value: pk}
		require.True(t, testPKMutex(t, g, key).TryLock())
		testPKMutex(t, g, key).Unlock()
	}
}

func newGuardTestDelete(pks ...int64) message.MutableMessage {
	return newDeleteMessage(&schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}},
	})
}
