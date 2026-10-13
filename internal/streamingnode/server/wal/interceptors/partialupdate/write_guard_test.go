package partialupdate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestPartialUpdateChainCommitDrainsBodyBeforeTakingPKGuard(t *testing.T) {
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
	commitDone := make(chan error, 1)
	go func() {
		_, err := env.append(newChainTestCASCommit(t, *result.TxnCtx), nil)
		commitDone <- err
	}()
	session, err := env.txnManager.GetSessionOfTxn(result.TxnCtx.TxnID)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return session.State() == message.TxnStateOnCommit }, time.Second, time.Millisecond)
	select {
	case err := <-commitDone:
		close(releaseBody)
		t.Fatalf("commit did not wait for its complete body write set: %v", err)
	default:
	}
	close(releaseBody)
	require.NoError(t, <-bodyDone)
	require.NoError(t, <-commitDone)
	require.True(t, env.hasPKVersionAfter("v1", 10, readTS))
	require.True(t, env.hasPKVersionAfter("v1", 20, readTS))
}

func TestPartialUpdateChainRecoveredCommitPublishesFenceBeforeCAS(t *testing.T) {
	env := newPartialUpdateChainTestEnvWithRecoveredTxn(t, false)
	// The mock allocator starts at 1, which is already owned by the recovered
	// fixture. Production allocators never reuse a recovered transaction ID.
	_, err := resource.Resource().IDAllocator().Allocate(context.Background())
	require.NoError(t, err)
	readTS := env.allocateReadTS(t)
	cas := env.prepareCASTxn(t, readTS, 20)
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
	session, err := env.txnManager.GetSessionOfTxn(cas.TxnID)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return session.State() == message.TxnStateOnCommit }, time.Second, time.Millisecond)
	select {
	case err := <-casDone:
		close(release)
		t.Fatalf("CAS bypassed an incomplete recovered write set: %v", err)
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
	// Neither a different-PK CAS nor an ordinary different-PK write may wait.
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

func TestPartialUpdateChainOrdinaryTxnCommitParticipatesInPKGuard(t *testing.T) {
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
	waitForTestCASReservation(t, env.partial.state.writeGuards, "v1", 10)
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

func TestWriteGuardsUnknownWriteSetExcludesExactPKAdmission(t *testing.T) {
	g := newWriteGuards()
	unknownRelease := acquireTestWriteGuard(t, g, "v1", primaryKeys{}, false, true)
	entered := make(chan struct{})
	done := make(chan struct{})
	go func() {
		release := acquireTestWriteGuard(t, g, "v1", primaryKeys{int64Values: []int64{10}}, true, false)
		close(entered)
		release()
		close(done)
	}()
	// The admission barrier itself must reject exact-PK admission while the
	// unknown writer holds it. This checks the blocking condition directly.
	if g.admission.TryRLock("v1") {
		g.admission.RUnlock("v1")
		t.Fatal("unknown write did not hold the exact-PK admission barrier")
	}
	select {
	case <-entered:
		unknownRelease()
		t.Fatal("CAS bypassed an incomplete write-set fence")
	default:
	}
	// A fence on v1 must not block an unrelated vchannel.
	other := acquireTestWriteGuard(t, g, "v2", primaryKeys{int64Values: []int64{10}}, true, false)
	other()
	unknownRelease()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("CAS did not resume after the unknown writer published")
	}
}

func TestWriteGuardsOrderAndSeparateTypedPKs(t *testing.T) {
	g := newWriteGuards()
	intPKs := primaryKeys{int64Values: []int64{20, 10, 20}}
	release := acquireTestWriteGuard(t, g, "v1", intPKs, true, false)
	// String "10" is distinct from integer 10 even within the same vchannel.
	stringRelease := acquireTestWriteGuard(t, g, "v1", primaryKeys{stringValues: []string{"10", "10"}}, true, false)
	stringRelease()
	done := make(chan struct{})
	go func() {
		secondRelease := acquireTestWriteGuard(t, g, "v1", primaryKeys{int64Values: []int64{10, 20}}, true, false)
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

func TestWriteGuardsOrdinaryBatchesDoNotAcquirePKLocks(t *testing.T) {
	g := newWriteGuards()
	pks := primaryKeys{int64Values: []int64{20, 10, 20}, stringValues: []string{"20", "10", "20"}}
	first := acquireTestWriteGuard(t, g, "v1", pks, false, false)
	second := acquireTestWriteGuard(t, g, "v1", pks, false, false)
	// Even overlapping ordinary batches must not create PK read locks.
	for _, key := range []pkLockKey{
		{vchannel: "v1", kind: primaryKeyKindInt64, int64Value: 10},
		{vchannel: "v1", kind: primaryKeyKindString, stringValue: "10"},
	} {
		require.True(t, g.pks.TryLock(key))
		g.pks.Unlock(key)
	}
	require.Equal(t, []int64{20, 10, 20}, pks.int64Values)
	require.Equal(t, []string{"20", "10", "20"}, pks.stringValues)
	first()
	second()
	requireWriteGuardRegistriesEmpty(t, g)
}

func TestWriteGuardsCASReservationDrainsEarlierAndBlocksLaterOrdinaryBatches(t *testing.T) {
	g := newWriteGuards()
	earlier := acquireTestWriteGuard(t, g, "v1", primaryKeys{int64Values: []int64{10, 20}}, false, false)
	casCtx := newWriteGuardWaitContext(context.Background())
	casResult := startTestWriteGuard(g, casCtx, "v1", primaryKeys{int64Values: []int64{10, 10}}, true)
	waitForTestWriteGuardWait(t, casCtx)
	// The earlier ordinary batch is still active, and CAS has atomically
	// reserved its PK before waiting for that batch's completion.
	select {
	case result := <-casResult:
		if result.release != nil {
			result.release()
		}
		t.Fatalf("CAS bypassed its earlier ordinary batch: %v", result.err)
	default:
	}
	laterCtx := newWriteGuardWaitContext(context.Background())
	laterResult := startTestWriteGuard(g, laterCtx, "v1", primaryKeys{int64Values: []int64{10}}, false)
	waitForTestWriteGuardWait(t, laterCtx)
	// Other PKs and typed keys are unaffected by the reservation.
	disjoint := acquireTestWriteGuard(t, g, "v1", primaryKeys{int64Values: []int64{30}, stringValues: []string{"10"}}, false, false)
	disjoint()
	earlier()
	cas := awaitTestWriteGuard(t, casResult)
	require.NoError(t, cas.err)
	require.NotNil(t, cas.release)
	select {
	case result := <-laterResult:
		if result.release != nil {
			result.release()
		}
		cas.release()
		t.Fatalf("ordinary batch bypassed the active CAS: %v", result.err)
	default:
	}
	cas.release()
	later := awaitTestWriteGuard(t, laterResult)
	require.NoError(t, later.err)
	later.release()
	requireWriteGuardRegistriesEmpty(t, g)
}

func TestWriteGuardsCancelCASReservationReleasesPendingOrdinaryBatch(t *testing.T) {
	g := newWriteGuards()
	earlier := acquireTestWriteGuard(t, g, "v1", primaryKeys{int64Values: []int64{10}}, false, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	casCtx := newWriteGuardWaitContext(ctx)
	casResult := startTestWriteGuard(g, casCtx, "v1", primaryKeys{int64Values: []int64{10}}, true)
	waitForTestWriteGuardWait(t, casCtx)
	laterCtx := newWriteGuardWaitContext(context.Background())
	laterResult := startTestWriteGuard(g, laterCtx, "v1", primaryKeys{int64Values: []int64{10}}, false)
	waitForTestWriteGuardWait(t, laterCtx)
	cancel()
	cas := awaitTestWriteGuard(t, casResult)
	require.ErrorIs(t, cas.err, context.Canceled)
	require.Nil(t, cas.release)
	// Cancellation must release the reservation even though the earlier
	// ordinary batch has not completed; ordinary batches remain concurrent.
	later := awaitTestWriteGuard(t, laterResult)
	require.NoError(t, later.err)
	later.release()
	key := pkLockKey{vchannel: "v1", kind: primaryKeyKindInt64, int64Value: 10}
	require.True(t, g.pks.TryLock(key))
	g.pks.Unlock(key)
	earlier()
	requireWriteGuardRegistriesEmpty(t, g)
	require.True(t, g.admission.TryLock("v1"))
	g.admission.Unlock("v1")
}

func TestWriteGuardsCancelOrdinaryReservationWaitCleansUpAdmission(t *testing.T) {
	g := newWriteGuards()
	cas := acquireTestWriteGuard(t, g, "v1", primaryKeys{int64Values: []int64{10}}, true, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ordinaryCtx := newWriteGuardWaitContext(ctx)
	ordinaryResult := startTestWriteGuard(g, ordinaryCtx, "v1", primaryKeys{int64Values: []int64{10}}, false)
	waitForTestWriteGuardWait(t, ordinaryCtx)
	cancel()
	ordinary := awaitTestWriteGuard(t, ordinaryResult)
	require.ErrorIs(t, ordinary.err, context.Canceled)
	require.Nil(t, ordinary.release)
	cas()
	requireWriteGuardRegistriesEmpty(t, g)
	require.True(t, g.admission.TryLock("v1"))
	g.admission.Unlock("v1")
}

func acquireTestWriteGuard(t *testing.T, g *writeGuards, vchannel string, pks primaryKeys, exclusive, unknown bool) func() {
	t.Helper()
	release, err := g.acquire(context.Background(), vchannel, pks, exclusive, unknown)
	require.NoError(t, err)
	var once sync.Once
	done := func() { once.Do(release) }
	t.Cleanup(done)
	return done
}

// Calling Done identifies the actual reservation/batch wait, rather than
// guessing whether another goroutine reached it after a short sleep.
type writeGuardWaitContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func newWriteGuardWaitContext(ctx context.Context) *writeGuardWaitContext {
	return &writeGuardWaitContext{Context: ctx, waiting: make(chan struct{})}
}

func (ctx *writeGuardWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

type testWriteGuardResult struct {
	release func()
	err     error
}

func startTestWriteGuard(g *writeGuards, ctx context.Context, vchannel string, pks primaryKeys, exclusive bool) <-chan testWriteGuardResult {
	result := make(chan testWriteGuardResult, 1)
	go func() {
		release, err := g.acquire(ctx, vchannel, pks, exclusive, false)
		result <- testWriteGuardResult{release: release, err: err}
	}()
	return result
}

func awaitTestWriteGuard(t *testing.T, result <-chan testWriteGuardResult) testWriteGuardResult {
	t.Helper()
	select {
	case acquired := <-result:
		return acquired
	case <-time.After(time.Second):
		t.Fatal("write guard did not finish after its blocker completed")
		return testWriteGuardResult{}
	}
}

func waitForTestWriteGuardWait(t *testing.T, ctx *writeGuardWaitContext) {
	t.Helper()
	select {
	case <-ctx.waiting:
	case <-time.After(time.Second):
		t.Fatal("write guard did not reach the expected reservation wait")
	}
}

func requireWriteGuardRegistriesEmpty(t *testing.T, g *writeGuards) {
	t.Helper()
	g.registryMu.Lock()
	defer g.registryMu.Unlock()
	require.Empty(t, g.registries)
}

func waitForTestCASReservation(t *testing.T, g *writeGuards, vchannel string, pk int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		g.registryMu.Lock()
		defer g.registryMu.Unlock()
		registry := g.registries[vchannel]
		if registry == nil {
			return false
		}
		registry.mu.Lock()
		defer registry.mu.Unlock()
		return registry.cas[pkLockKey{vchannel: vchannel, kind: primaryKeyKindInt64, int64Value: pk}] != nil
	}, time.Second, time.Millisecond)
}

func newGuardTestDelete(pks ...int64) message.MutableMessage {
	return newDeleteMessage(&schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}},
	})
}
