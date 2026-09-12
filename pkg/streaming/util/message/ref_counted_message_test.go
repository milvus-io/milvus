package message

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
)

func TestOwnedImmutableMessageCloneAndFinalize(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	var finalizerCalls atomic.Int32
	owner := NewOwnedImmutableMessage(raw, func() {
		finalizerCalls.Add(1)
	})

	assert.Same(t, raw, owner.Message())
	first := owner.Clone()
	second := first.Clone()
	require.NotSame(t, first, second)
	assert.Same(t, raw, first.Message())
	assert.Same(t, raw, second.Message())

	owner.Release()
	assert.Zero(t, finalizerCalls.Load())
	first.Release()
	assert.Zero(t, finalizerCalls.Load())
	second.Release()
	assert.Equal(t, int32(1), finalizerCalls.Load())

	assert.Panics(t, func() { _ = owner.Message() })
	assert.Panics(t, func() { _ = first.Message() })
	assert.Panics(t, func() { _ = second.Message() })
	first.Release()
}

func TestOwnedImmutableMessageRegistersExclusiveCallbackImmediately(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, nil)
	var calls atomic.Int32

	owner.RegisterExclusiveCallback(func() {
		calls.Add(1)
	})

	assert.Equal(t, int32(1), calls.Load())
	owner.Release()
}

func TestOwnedImmutableMessageInvokesExclusiveCallbackAfterRetainedRelease(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, nil)
	retained := owner.Clone()
	var calls atomic.Int32

	owner.RegisterExclusiveCallback(func() {
		calls.Add(1)
	})
	assert.Zero(t, calls.Load())

	retained.Release()
	assert.Equal(t, int32(1), calls.Load())

	retained = owner.Clone()
	retained.Release()
	assert.Equal(t, int32(1), calls.Load())
	owner.Release()
}

func TestOwnedImmutableMessageRejectsDuplicateExclusiveCallback(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, nil)
	retained := owner.Clone()
	owner.RegisterExclusiveCallback(func() {})

	assert.Panics(t, func() {
		owner.RegisterExclusiveCallback(func() {})
	})

	retained.Release()
	owner.Release()
}

func TestOwnedImmutableMessageReleaseDoesNotInvalidateClones(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, nil)
	clone := owner.Clone()

	owner.Release()
	assert.Panics(t, func() { _ = owner.Message() })
	assert.Equal(t, uint64(20), clone.Message().TimeTick())
	clone.Release()
}

func TestOwnedImmutableMessageUntypedSharesLifetime(t *testing.T) {
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, nil)
	typed := MustAsOwnedImmutableInsertMessageV1(owner)
	untyped := typed.Untyped()
	retained := untyped.Clone()

	owner.Release()
	assert.Panics(t, func() { _ = untyped.Message() })
	assert.Equal(t, raw.MessageID(), retained.Message().MessageID())
	retained.Release()
}

func TestOwnedImmutableMessageWithoutConsumers(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	var finalized atomic.Bool
	owner := NewOwnedImmutableMessage(raw, func() {
		finalized.Store(true)
	})

	owner.Release()
	assert.True(t, finalized.Load())
}

func TestRetainedImmutableMessageConcurrentReleaseFinalizesOnce(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	var finalizerCalls atomic.Int32
	owner := NewOwnedImmutableMessage(raw, func() {
		finalizerCalls.Add(1)
	})
	handles := make([]RetainedImmutableMessage, 64)
	for i := range handles {
		handles[i] = owner.Clone()
	}
	owner.Release()

	var wg sync.WaitGroup
	for _, handle := range handles {
		wg.Go(handle.Release)
	}
	wg.Wait()

	assert.Equal(t, int32(1), finalizerCalls.Load())
}

func TestRetainedImmutableMessageCloneIsIndependent(t *testing.T) {
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, nil)
	first := owner.Clone()
	second := first.Clone()

	first.Release()
	assert.Equal(t, uint64(20), second.Message().TimeTick())
	second.Release()
	owner.Release()
}

func TestRetainedImmutableDoesNotExposeMessageAfterRelease(t *testing.T) {
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, nil)
	typed := MustAsOwnedImmutableInsertMessageV1(owner).Clone()

	typed.Release()
	assert.Panics(t, func() { _ = typed.Message() })
	owner.Release()
}

func TestRetainedTxnKeepsWholeTransactionAlive(t *testing.T) {
	txn := buildRefCountedTestTxn(t)
	owner := NewOwnedImmutableMessage(txn, nil)
	retained := owner.Clone()

	retainedTxn := AsImmutableTxnMessage(retained.Message())
	require.NotNil(t, retainedTxn)
	require.Equal(t, 1, retainedTxn.Size())
	require.NoError(t, retainedTxn.RangeOver(func(inner ImmutableMessage) error {
		assert.Equal(t, MessageTypeInsert, inner.MessageType())
		return nil
	}))

	retained.Release()
	owner.Release()
}

func TestMustAsOwnedImmutableInsertMessageV1(t *testing.T) {
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, nil)
	owned := MustAsOwnedImmutableInsertMessageV1(owner)

	assert.Equal(t, raw.MessageID(), owned.Message().MessageID())
	retained := owned.Clone()
	assert.Equal(t, raw.MessageID(), retained.Message().MessageID())
	retained.Release()
	owner.Release()
}

func TestMustAsOwnedImmutableInsertMessageV1RejectsMismatchedOwner(t *testing.T) {
	raw := CreateTestTimeTickSyncMessage(t, 1, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, nil)

	assert.Panics(t, func() { MustAsOwnedImmutableInsertMessageV1(owner) })
	owner.Release()
}

func TestMustAsOwnedImmutableTxnMessage(t *testing.T) {
	txn := buildRefCountedTestTxn(t)
	owner := NewOwnedImmutableMessage(txn, nil)
	owned := MustAsOwnedImmutableTxnMessage(owner)

	assert.Same(t, txn, owned.Message())
	retained := owned.Clone()
	assert.Same(t, txn, retained.Message())
	retained.Release()
	owner.Release()
}

func TestImmutableMessageCanOutliveOwner(t *testing.T) {
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, nil)
	borrowed := owner.Message()
	owner.Release()

	assert.Same(t, raw, borrowed)
	assert.Equal(t, uint64(20), borrowed.TimeTick())
}

// TestPoisonedReleaseMarksAndReleases covers the poison-then-release semantics:
// PoisonedRelease marks the message poisoned (observable through another handle
// to the same message) and releases this reference, so releasing is otherwise
// identical to Release — the refcount is dropped and the shared message is
// finalized once the last reference goes away.
func TestPoisonedReleaseMarksAndReleases(t *testing.T) {
	var finalizerCalls atomic.Int32
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, func() {
		finalizerCalls.Add(1)
	})
	retained := owner.Clone()
	probe := owner.Clone()

	retained.PoisonedRelease()
	require.True(t, probe.IsPoisoned(), "poison is message-level and visible through another handle")

	probe.Release()
	owner.Release()
	assert.Equal(t, int32(1), finalizerCalls.Load(), "finalization still happens once after the last reference is released")
}

// TestIntoPoisonedMarksWithoutReleasing covers the mark-only semantics:
// IntoPoisoned marks the message poisoned without touching the reference count,
// so the caller still owns its reference and must release it later; every
// handle to the same message observes the poison.
func TestIntoPoisonedMarksWithoutReleasing(t *testing.T) {
	var finalizerCalls atomic.Int32
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, func() {
		finalizerCalls.Add(1)
	})
	retained := owner.Clone()
	probe := owner.Clone()

	retained.IntoPoisoned()
	require.True(t, retained.IsPoisoned())
	require.True(t, probe.IsPoisoned(), "poison is message-level and visible through another handle")

	// IntoPoisoned must not have released anything: both handles are still
	// alive and the message is not finalized yet.
	probe.Release()
	require.Equal(t, int32(0), finalizerCalls.Load(), "IntoPoisoned must not release the reference")
	retained.Release()
	owner.Release()
	assert.Equal(t, int32(1), finalizerCalls.Load())
}

// TestPoisonedReleaseFinalizesOnlyAtLastHandle covers that PoisonedRelease does
// not finalize the message while other handles remain: it is poison + Release,
// not poison + destroy.
func TestPoisonedReleaseFinalizesOnlyAtLastHandle(t *testing.T) {
	var finalizerCalls atomic.Int32
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, func() {
		finalizerCalls.Add(1)
	})
	first := owner.Clone()
	second := owner.Clone()

	first.PoisonedRelease()
	require.True(t, second.IsPoisoned())
	require.Equal(t, int32(0), finalizerCalls.Load(), "a surviving handle keeps the message alive after PoisonedRelease")

	second.Release()
	owner.Release()
	assert.Equal(t, int32(1), finalizerCalls.Load())
}

// TestPoisonIsNotInheritedByOtherMessages covers that poison is per-message:
// marking one message poisoned never leaks onto another message.
func TestPoisonIsNotInheritedByOtherMessages(t *testing.T) {
	raw1 := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	raw2 := CreateTestInsertMessage(t, 100, 2, 30, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner1 := NewOwnedImmutableMessage(raw1, nil)
	owner2 := NewOwnedImmutableMessage(raw2, nil)
	h1 := owner1.Clone()
	h2 := owner2.Clone()

	h1.IntoPoisoned()
	require.True(t, h1.IsPoisoned())
	require.False(t, h2.IsPoisoned(), "poison must not leak across different messages")

	h1.Release()
	h2.Release()
	owner1.Release()
	owner2.Release()
}

// TestSpecializedRetainedPoisonForwarding covers that the specialized wrapper
// (used by ObserveCreateSegmentMessageV2) forwards the poison semantics to the
// underlying retained message, so an Observe entry can poison a specialized
// incoming handle.
func TestSpecializedRetainedPoisonForwarding(t *testing.T) {
	var finalizerCalls atomic.Int32
	raw := CreateTestInsertMessage(t, 100, 2, 20, testMessageID("10")).
		IntoImmutableMessage(testMessageID("11"))
	owner := NewOwnedImmutableMessage(raw, func() {
		finalizerCalls.Add(1)
	})
	specialized := MustAsOwnedImmutableInsertMessageV1(owner).Clone()

	require.False(t, specialized.IsPoisoned())
	specialized.IntoPoisoned()
	require.True(t, specialized.IsPoisoned(), "specialized wrapper forwards IntoPoisoned")
	require.Equal(t, int32(0), finalizerCalls.Load(), "IntoPoisoned on the wrapper must not release the underlying message")

	specialized.PoisonedRelease()
	require.Equal(t, int32(0), finalizerCalls.Load(), "one of two refs released does not finalize")
	owner.Release()
	assert.Equal(t, int32(1), finalizerCalls.Load())
}

// TestRetainedTxnPoisonForwarding covers that the retained txn wrapper forwards
// the poison semantics to the underlying retained message.
func TestRetainedTxnPoisonForwarding(t *testing.T) {
	txn := buildRefCountedTestTxn(t)
	owner := NewOwnedImmutableMessage(txn, nil)
	retainedTxn := MustAsRetainedImmutableTxnMessage(owner.Clone())

	require.False(t, retainedTxn.IsPoisoned())
	retainedTxn.IntoPoisoned()
	require.True(t, retainedTxn.IsPoisoned(), "txn wrapper forwards IntoPoisoned")
	retainedTxn.PoisonedRelease()

	owner.Release()
}

func buildRefCountedTestTxn(t *testing.T) ImmutableTxnMessage {
	t.Helper()
	txnCtx := TxnContext{TxnID: 1, Keepalive: time.Second}
	begin, err := NewBeginTxnMessageBuilderV2().
		WithVChannel("vchan").
		WithHeader(&BeginTxnMessageHeader{}).
		WithBody(&BeginTxnMessageBody{}).
		BuildMutable()
	require.NoError(t, err)
	immutableBegin := begin.WithTxnContext(txnCtx).
		WithTimeTick(1).
		WithLastConfirmed(testMessageID("1")).
		IntoImmutableMessage(testMessageID("1"))
	beginMessage := MustAsImmutableBeginTxnMessageV2(immutableBegin)

	insert, err := NewInsertMessageBuilderV1().
		WithVChannel("vchan").
		WithHeader(&InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		BuildMutable()
	require.NoError(t, err)

	commit, err := NewCommitTxnMessageBuilderV2().
		WithVChannel("vchan").
		WithHeader(&CommitTxnMessageHeader{}).
		WithBody(&CommitTxnMessageBody{}).
		BuildMutable()
	require.NoError(t, err)
	immutableCommit := commit.WithTxnContext(txnCtx).
		WithTimeTick(3).
		WithLastConfirmed(testMessageID("3")).
		IntoImmutableMessage(testMessageID("4"))
	commitMessage := MustAsImmutableCommitTxnMessageV2(immutableCommit)

	txn, err := NewImmutableTxnMessageBuilder(beginMessage).
		Add(insert.WithTimeTick(2).WithTxnContext(txnCtx).IntoImmutableMessage(testMessageID("2"))).
		Build(commitMessage)
	require.NoError(t, err)
	return txn
}
