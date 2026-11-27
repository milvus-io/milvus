package partialupdate

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// No body is accepted once its CommitTxn has arrived, here while the commit
// is in its WAL append. commit_order_test.go covers the earlier windows, when
// the commit is rejected with a body in flight or waits in the row-lock queue.
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
	receiveCommitValue(t, atWAL)
	_, err := env.append(newGuardTestDelete(20).WithTxnContext(cas), nil)
	require.True(t, status.AsStreamingError(err).IsTxnExpired(), "late body returned: %v", err)
	once.Do(func() { close(release) })
	require.NoError(t, receiveCommitValue(t, commitDone))
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
	receiveCommitValue(t, atWAL)
	casDone := make(chan error, 1)
	go func() {
		_, err := env.append(newChainTestCASCommit(t, cas), nil)
		casDone <- err
	}()
	// The CAS takes its place for PK 10 in the lock interceptor, before
	// TimeTick, and waits there for the registered ordinary commit.
	waitForTestPKAwaited(t, env.partial.state.rowLocks, "v1", 10)
	select {
	case err := <-casDone:
		close(release)
		t.Fatalf("CAS bypassed the recovered ordinary transaction PK lock: %v", err)
	default:
	}
	close(release)
	require.NoError(t, receiveCommitValue(t, writerDone))
	requirePartialUpdateRetryable(t, receiveCommitValue(t, casDone))
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
	receiveCommitValue(t, atWAL)
	// Construct both writes while the first CAS remains inside its WAL append.
	// Their PKs are disjoint from the first CAS, so neither write may wait.
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
	require.NoError(t, receiveCommitValue(t, firstDone))
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
	receiveCommitValue(t, atWAL)
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
	require.NoError(t, receiveCommitValue(t, writerDone))
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
	receiveCommitValue(t, atWAL)
	casDone := make(chan error, 1)
	go func() {
		_, err := env.append(newChainTestCASCommit(t, cas), nil)
		casDone <- err
	}()
	waitForTestPKAwaited(t, env.partial.state.rowLocks, "v1", 10)
	select {
	case err := <-casDone:
		close(release)
		t.Fatalf("CAS completed before ordinary txn write-set publication: %v", err)
	default:
	}
	close(release)
	require.NoError(t, receiveCommitValue(t, writerDone))
	requirePartialUpdateRetryable(t, receiveCommitValue(t, casDone))
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

func TestRowLocksDuplicatePKsAndSeparateTypedPKs(t *testing.T) {
	g := newRowLockManager()
	intPKs := primaryKeys{int64Values: []int64{20, 10, 20}}
	release := acquireTestRowLock(t, g, "v1", intPKs, true)
	// VarChar "10" is a different row from Int64 10.
	stringRelease := acquireTestRowLock(t, g, "v1", primaryKeys{stringValues: []string{"10", "10"}}, true)
	stringRelease()
	// The Int64 CAS keeps the table pinned. The released VarChar CAS leaves no
	// entry behind, and its emptied map is dropped.
	require.Equal(t, testPKLock{}, inspectTestPK(g, "v1", "10"))
	int64Map, stringMap := testCASMapsAllocated(g, "v1")
	require.True(t, int64Map)
	require.False(t, stringMap, "the emptied VarChar CAS map was not dropped")
	second := startTestRowLock(context.Background(), g, "v1", primaryKeys{int64Values: []int64{10, 20}}, true)
	waitForTestRowLockRefs(t, g, "v1", 2)
	requireTestRowLockPending(t, second)
	release()
	acquired := awaitTestRowLock(t, second)
	require.NoError(t, acquired.err)
	acquired.release()
	require.Equal(t, []int64{20, 10, 20}, intPKs.int64Values, "CAS must not reorder the caller's write set")
	requireTestRowLockTablesEmpty(t, g)
}

func TestRowLockManagerOrdinaryWritesTakeSharedPKLocks(t *testing.T) {
	g := newRowLockManager()
	pks := primaryKeys{int64Values: []int64{20, 10, 20}, stringValues: []string{"20", "10", "20"}}
	first := acquireTestRowLock(t, g, "v1", pks, false)
	second := acquireTestRowLock(t, g, "v1", pks, false)
	for _, state := range []testPKLock{inspectTestPK(g, "v1", int64(10)), inspectTestPK(g, "v1", "10")} {
		require.Equal(t, testPKLock{ordinary: 2}, state)
	}
	require.Equal(t, []int64{20, 10, 20}, pks.int64Values)
	require.Equal(t, []string{"20", "10", "20"}, pks.stringValues)
	first()
	second()
	requireTestRowLocksAvailable(t, g, "v1", 10, 20)
	requireTestRowLockTablesEmpty(t, g)
}

func TestRowLockManagerCASWaitsForEarlierWritesAndBlocksLaterWrites(t *testing.T) {
	// The ordinary writes are listed newest first, so release them in both
	// orders: the CAS must wait for every earlier write of PK 10.
	for _, olderFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("older_first_%t", olderFirst), func(t *testing.T) {
			g := newRowLockManager()
			pks := primaryKeys{int64Values: []int64{10, 20}}
			older := acquireTestRowLock(t, g, "v1", pks, false)
			newer := acquireTestRowLock(t, g, "v1", pks, false)
			casResult := startTestRowLock(context.Background(), g, "v1", primaryKeys{int64Values: []int64{10, 10}}, true)
			waitForTestPKAwaited(t, g, "v1", 10)
			require.Equal(t, testPKLock{queued: true, ordinary: 2, awaited: 1}, inspectTestPK(g, "v1", int64(10)))
			laterResult := startTestRowLock(context.Background(), g, "v1", primaryKeys{int64Values: []int64{5, 10}}, false)
			// The later write registers at once and then waits for the CAS.
			waitForTestPKOrdinary(t, g, "v1", 5, 1)
			require.Equal(t, testPKLock{ordinary: 1}, inspectTestPK(g, "v1", int64(5)))
			first, second := older, newer
			if !olderFirst {
				first, second = newer, older
			}
			first()
			requireTestRowLockPending(t, casResult)
			disjoint := acquireTestRowLock(t, g, "v1", primaryKeys{int64Values: []int64{30}, stringValues: []string{"10"}}, false)
			disjoint()
			otherChannel := acquireTestRowLock(t, g, "v2", primaryKeys{int64Values: []int64{10}}, true)
			otherChannel()
			second()
			cas := awaitTestRowLock(t, casResult)
			require.NoError(t, cas.err)
			require.NotNil(t, cas.release)
			requireTestRowLockPending(t, laterResult)
			cas.release()
			later := awaitTestRowLock(t, laterResult)
			require.NoError(t, later.err)
			later.release()
			requireTestRowLocksAvailable(t, g, "v1", 5, 10, 20, 30)
			requireTestRowLockTablesEmpty(t, g)
		})
	}
}

// acquireTestRowLock fails instead of hanging when the lock is not granted.
func acquireTestRowLock(t *testing.T, g *rowLockManager, vchannel string, pks primaryKeys, exclusive bool) func() {
	t.Helper()
	acquired := awaitTestRowLock(t, startTestRowLock(context.Background(), g, vchannel, pks, exclusive))
	require.NoError(t, acquired.err)
	var once sync.Once
	done := func() { once.Do(acquired.release) }
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
		t.Fatal("row lock was not granted within a second")
		return testRowLockResult{}
	}
}

// requireTestRowLockPending asserts that a waiter stays blocked. Callers first
// observe that the waiter is inside acquire, so a broken lock returns at once.
func requireTestRowLockPending(t *testing.T, result <-chan testRowLockResult) {
	t.Helper()
	select {
	case acquired := <-result:
		if acquired.release != nil {
			acquired.release()
		}
		t.Fatalf("row lock bypassed its blocker: %v", acquired.err)
	case <-time.After(50 * time.Millisecond):
	}
}

// testPKLock describes one exact PK of a row-lock table.
type testPKLock struct {
	// queued means a CAS has taken its place in line for the PK: it holds the
	// PK, waits for it, or, if canceled, has not yet given up its place.
	queued bool
	// ordinary counts registered, unreleased ordinary writes containing the
	// PK, whether they hold it or still wait for a CAS.
	ordinary int
	// awaited counts those ordinary writes that a CAS has started waiting for.
	awaited int
}

// inspectTestPK reads a table without pinning it, so probes never change
// reference counts or keep a table alive. It uses its own exact comparison
// rather than the manager's lookup helpers.
func inspectTestPK[K int64 | string](g *rowLockManager, vchannel string, pk K) testPKLock {
	g.mu.Lock()
	defer g.mu.Unlock()
	table := g.channels[vchannel]
	if table == nil {
		return testPKLock{}
	}
	table.keys.RLock()
	defer table.keys.RUnlock()
	table.ordMu.Lock()
	defer table.ordMu.Unlock()
	var state testPKLock
	contains := func(pks primaryKeys) bool { return false }
	switch pk := any(pk).(type) {
	case int64:
		state.queued = table.lastCASInt64[pk] != nil
		contains = func(pks primaryKeys) bool { return slices.Contains(pks.int64Values, pk) }
	case string:
		state.queued = table.lastCASStr[pk] != nil
		contains = func(pks primaryKeys) bool { return slices.Contains(pks.stringValues, pk) }
	}
	for write := table.ordinary; write != nil; write = write.next {
		if contains(write.pks) {
			state.ordinary++
			if write.done != nil {
				state.awaited++
			}
		}
	}
	return state
}

// waitForTestPK waits until the state of pk satisfies ok.
func waitForTestPK[K int64 | string](t *testing.T, g *rowLockManager, vchannel string, pk K, ok func(testPKLock) bool, what string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return ok(inspectTestPK(g, vchannel, pk))
	}, time.Second, time.Millisecond, "PK %v: %s", pk, what)
}

// waitForTestPKQueued waits until a CAS has taken its place in line for pk.
// Tests give a CAS a PK no other CAS uses to observe that it is in line.
func waitForTestPKQueued(t *testing.T, g *rowLockManager, vchannel string, pk int64) {
	t.Helper()
	waitForTestPK(t, g, vchannel, pk, func(s testPKLock) bool { return s.queued }, "no CAS is in line")
}

// waitForTestPKOrdinary waits until n ordinary writes of pk have registered.
func waitForTestPKOrdinary(t *testing.T, g *rowLockManager, vchannel string, pk int64, n int) {
	t.Helper()
	waitForTestPK(t, g, vchannel, pk, func(s testPKLock) bool { return s.ordinary == n },
		fmt.Sprintf("%d ordinary writes did not register", n))
}

// waitForTestPKAwaited waits until a CAS is in line for pk and waits for a
// registered ordinary write of it. The CAS creates the wait channel right
// before it blocks, so this observes the wait without sleeping.
func waitForTestPKAwaited(t *testing.T, g *rowLockManager, vchannel string, pk int64) {
	t.Helper()
	waitForTestPK(t, g, vchannel, pk, func(s testPKLock) bool { return s.queued && s.awaited > 0 },
		"no CAS waits for an ordinary write")
}

// testPKSharedError returns nil if registered ordinary writes hold pk and no
// CAS is in line for it. WAL append callbacks return this error instead of
// failing inside the interceptor chain, where FailNow would leave TimeTick
// with a half-finished append and crash the test binary.
func testPKSharedError(g *rowLockManager, vchannel string, pk int64) error {
	state := inspectTestPK(g, vchannel, pk)
	if state.ordinary == 0 {
		return errors.Errorf("PK %d is not held", pk)
	}
	if state.queued {
		return errors.Errorf("a CAS is in line for PK %d", pk)
	}
	return nil
}

// requireTestPKShared asserts that registered ordinary writes hold pk and no
// CAS is in line for it.
func requireTestPKShared(t *testing.T, g *rowLockManager, vchannel string, pk int64, msg string) {
	t.Helper()
	require.NoError(t, testPKSharedError(g, vchannel, pk), msg)
}

// testCASMapsAllocated reports which CAS maps of a table exist. A map exists
// only while a CAS is in line for a PK of its type.
func testCASMapsAllocated(g *rowLockManager, vchannel string) (int64Map, stringMap bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	table := g.channels[vchannel]
	if table == nil {
		return false, false
	}
	table.keys.RLock()
	defer table.keys.RUnlock()
	return table.lastCASInt64 != nil, table.lastCASStr != nil
}

func requireTestRowLocksAvailable(t *testing.T, g *rowLockManager, vchannel string, pks ...int64) {
	t.Helper()
	for _, pk := range pks {
		require.Equal(t, testPKLock{}, inspectTestPK(g, vchannel, pk), "PK %d is still locked", pk)
	}
}

func testRowLockRefs(g *rowLockManager, vchannel string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if table := g.channels[vchannel]; table != nil {
		return table.refs
	}
	return 0
}

// waitForTestRowLockRefs waits until refs holders and waiters pin the table.
func waitForTestRowLockRefs(t *testing.T, g *rowLockManager, vchannel string, refs int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return testRowLockRefs(g, vchannel) == refs
	}, time.Second, time.Millisecond, "row-lock table of %s never reached %d references", vchannel, refs)
}

// requireTestRowLockTablesEmpty waits until every table is gone. A canceled CAS
// gives up its place in line from another goroutine once the CAS commits
// before it have released, so the last unpin may come shortly after.
func requireTestRowLockTablesEmpty(t *testing.T, g *rowLockManager) {
	t.Helper()
	require.Eventually(t, func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return len(g.channels) == 0
	}, time.Second, time.Millisecond, "row-lock tables are still in use")
}

func newGuardTestDelete(pks ...int64) message.MutableMessage {
	return newDeleteMessage(&schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}},
	})
}
