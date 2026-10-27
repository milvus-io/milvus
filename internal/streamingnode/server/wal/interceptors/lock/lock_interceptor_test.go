package lock

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
)

const (
	testPChannelName = "test-pchannel"
	testVChannel     = "test-pchannel_v0"
)

func newTestInterceptor() *lockAppendInterceptor {
	return &lockAppendInterceptor{
		channel:        types.PChannelInfo{Name: testPChannelName},
		vchannelLocker: lock.NewKeyLock[string](),
		txnManager:     new(txn.TxnManager),
	}
}

func TestAcquireLockGuard(t *testing.T) {
	t.Run("PChannelLevelOnControlChannel", func(t *testing.T) {
		interceptor := newTestInterceptor()
		controlChannel := funcutil.GetControlChannel(testPChannelName)
		broadcast := message.NewFlushAllMessageBuilderV2().
			WithHeader(&message.FlushAllMessageHeader{}).
			WithBody(&message.FlushAllMessageBody{}).
			WithClusterLevelBroadcast(message.ClusterChannels{
				Channels:       []string{testPChannelName},
				ControlChannel: controlChannel,
			}).
			MustBuildBroadcast()
		broadcast.WithBroadcastID(1)
		msg := broadcast.SplitIntoMutableMessage()[0]

		guard := interceptor.acquireLockGuard(msg)
		assert.False(t, interceptor.glock.TryRLock(), "pchannel-level message should hold the global write lock")
		guard()
		assert.True(t, interceptor.glock.TryRLock())
		interceptor.glock.RUnlock()
	})

	// Test: Exclusive message with pchannel name as vchannel should acquire global write lock (existing behavior).
	t.Run("ExclusiveWithPChannelName", func(t *testing.T) {
		interceptor := newTestInterceptor()
		msg := message.NewManualFlushMessageBuilderV2().
			WithVChannel(testPChannelName).
			WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
			WithBody(&message.ManualFlushMessageBody{}).
			MustBuildMutable()

		guard := interceptor.acquireLockGuard(msg)
		assert.False(t, interceptor.glock.TryRLock(), "glock should be write-locked for exclusive message with pchannel name")
		guard()
		assert.True(t, interceptor.glock.TryRLock())
		interceptor.glock.RUnlock()
	})

	// Test: Exclusive message on regular vchannel should acquire the global
	// read lock before the per-vchannel write lock.
	t.Run("ExclusiveOnRegularVChannel", func(t *testing.T) {
		interceptor := newTestInterceptor()
		msg := message.NewManualFlushMessageBuilderV2().
			WithVChannel(testVChannel).
			WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
			WithBody(&message.ManualFlushMessageBody{}).
			MustBuildMutable()

		guard := interceptor.acquireLockGuard(msg)
		assert.False(t, interceptor.glock.TryLock(), "glock write lock should fail while the shared hierarchy lock is held")
		assert.True(t, interceptor.glock.TryRLock(), "glock read lock should remain shareable")
		interceptor.glock.RUnlock()
		// Per-vchannel write lock should be held.
		assert.False(t, interceptor.vchannelLocker.TryLock(testVChannel), "vchannel lock should be held")
		// Other vchannels should not be blocked.
		assert.True(t, interceptor.vchannelLocker.TryLock("other-vchannel"), "other vchannels should not be blocked")
		interceptor.vchannelLocker.Unlock("other-vchannel")
		guard()
	})

	t.Run("ExclusiveOnControlChannel", func(t *testing.T) {
		interceptor := newTestInterceptor()
		controlChannel := funcutil.GetControlChannel(testPChannelName)
		msg := message.NewCreateCollectionMessageBuilderV1().
			WithVChannel(controlChannel).
			WithHeader(&messagespb.CreateCollectionMessageHeader{CollectionId: 1}).
			WithBody(&msgpb.CreateCollectionRequest{}).
			MustBuildMutable()

		guard := interceptor.acquireLockGuard(msg)
		// A control-channel copy of collection DDL uses shared locks so another
		// non-conflicting DDL can append concurrently.
		assert.False(t, interceptor.glock.TryLock(), "global read lock should be held")
		assert.True(t, interceptor.glock.TryRLock(), "another global read lock should succeed")
		interceptor.glock.RUnlock()
		assert.False(t, interceptor.vchannelLocker.TryLock(controlChannel), "control channel read lock should be held")
		assert.True(t, interceptor.vchannelLocker.TryRLock(controlChannel), "another control channel read lock should succeed")
		interceptor.vchannelLocker.RUnlock(controlChannel)
		guard()
	})

	t.Run("PartialUpdateCASCommit", func(t *testing.T) {
		interceptor := newTestInterceptor()
		msg := message.NewCommitTxnMessageBuilderV2().
			WithVChannel(testVChannel).
			WithHeader(&message.CommitTxnMessageHeader{}).
			WithBody(&message.CommitTxnMessageBody{}).
			MustBuildMutable()
		assert.NoError(t, message.MarkPartialUpdateCASCommit(msg))

		guard := interceptor.acquireLockGuard(msg)
		assert.False(t, interceptor.glock.TryLock())
		assert.True(t, interceptor.glock.TryRLock())
		interceptor.glock.RUnlock()
		assert.False(t, interceptor.vchannelLocker.TryLock(testVChannel))
		assert.True(t, interceptor.vchannelLocker.TryRLock(testVChannel), "CAS commit must keep the shard lock shareable")
		interceptor.vchannelLocker.RUnlock(testVChannel)
		assert.True(t, interceptor.vchannelLocker.TryLock("other-vchannel"))
		interceptor.vchannelLocker.Unlock("other-vchannel")
		guard()
	})

	// Test: Non-exclusive message on regular vchannel should acquire read locks on both glock and vchannel.
	t.Run("NonExclusiveOnRegularVChannel", func(t *testing.T) {
		interceptor := newTestInterceptor()
		msg := message.NewInsertMessageBuilderV1().
			WithVChannel(testVChannel).
			WithHeader(&messagespb.InsertMessageHeader{
				CollectionId: 1,
				Partitions: []*messagespb.PartitionSegmentAssignment{
					{PartitionId: 1, Rows: 1, BinarySize: 100},
				},
			}).
			WithBody(&msgpb.InsertRequest{}).
			MustBuildMutable()

		guard := interceptor.acquireLockGuard(msg)
		// glock should be read-locked: write TryLock fails, read TryRLock succeeds.
		assert.False(t, interceptor.glock.TryLock(), "glock write lock should fail when read-locked")
		assert.True(t, interceptor.glock.TryRLock(), "glock read lock should succeed when read-locked")
		interceptor.glock.RUnlock()
		// vchannel should be read-locked: write TryLock fails, read TryRLock succeeds.
		assert.False(t, interceptor.vchannelLocker.TryLock(testVChannel), "vchannel write lock should fail when read-locked")
		assert.True(t, interceptor.vchannelLocker.TryRLock(testVChannel), "vchannel read lock should succeed when read-locked")
		interceptor.vchannelLocker.RUnlock(testVChannel)
		guard()
	})
}

func TestPartialUpdateCASCommitSharesShardLockWithWriter(t *testing.T) {
	interceptor := newTestInterceptor()
	writer := newLockTestInsertMessage(testVChannel)
	commit := newLockTestCASCommit(t, testVChannel)
	writerStarted := make(chan struct{})
	releaseWriter := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		_, err := interceptor.DoAppend(context.Background(), writer, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(writerStarted)
			<-releaseWriter
			return nil, nil
		})
		writerDone <- err
	}()
	<-writerStarted

	commitEntered := make(chan struct{})
	commitDone := make(chan error, 1)
	go func() {
		_, err := interceptor.DoAppend(context.Background(), commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(commitEntered)
			return nil, nil
		})
		commitDone <- err
	}()

	select {
	case <-commitEntered:
	case <-time.After(time.Second):
		close(releaseWriter)
		t.Fatal("CAS commit still acquired an exclusive shard lock")
	}
	close(releaseWriter)
	require.NoError(t, <-writerDone)
	require.NoError(t, <-commitDone)
}

func TestPartialUpdateCASCommitsOnDifferentVChannelsProceedConcurrently(t *testing.T) {
	interceptor := newTestInterceptor()
	first := newLockTestCASCommit(t, testVChannel)
	second := newLockTestCASCommit(t, "test-pchannel_other-v0")
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := interceptor.DoAppend(context.Background(), first, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(firstStarted)
			<-releaseFirst
			return nil, nil
		})
		firstDone <- err
	}()
	<-firstStarted

	secondEntered := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		_, err := interceptor.DoAppend(context.Background(), second, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(secondEntered)
			return nil, nil
		})
		secondDone <- err
	}()

	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		close(releaseFirst)
		require.NoError(t, <-firstDone)
		t.Fatal("different vchannels shared one CAS commit lock")
	}
	require.NoError(t, <-secondDone)
	close(releaseFirst)
	require.NoError(t, <-firstDone)
}

func TestPChannelExclusiveWaitsForSharedWriter(t *testing.T) {
	interceptor := newTestInterceptor()
	writerStarted := make(chan struct{})
	releaseWriter := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		_, err := interceptor.DoAppend(context.Background(), newLockTestInsertMessage(testVChannel), func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(writerStarted)
			<-releaseWriter
			return nil, nil
		})
		writerDone <- err
	}()
	<-writerStarted

	exclusive := message.NewManualFlushMessageBuilderV2().
		WithVChannel(testPChannelName).
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
		WithBody(&message.ManualFlushMessageBody{}).
		MustBuildMutable()
	exclusiveEntered := make(chan struct{})
	exclusiveDone := make(chan error, 1)
	go func() {
		_, err := interceptor.DoAppend(context.Background(), exclusive, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(exclusiveEntered)
			return nil, nil
		})
		exclusiveDone <- err
	}()

	select {
	case <-exclusiveEntered:
		close(releaseWriter)
		require.NoError(t, <-writerDone)
		t.Fatal("PChannel-exclusive message bypassed an active shared writer")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseWriter)
	require.NoError(t, <-writerDone)
	select {
	case <-exclusiveEntered:
	case <-time.After(time.Second):
		t.Fatal("PChannel-exclusive message did not resume")
	}
	require.NoError(t, <-exclusiveDone)
}

func newLockTestInsertMessage(vchannel string) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{PartitionId: 1, Rows: 1, BinarySize: 100},
			},
		}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable()
}

func newLockTestCASCommit(t *testing.T, vchannel string) message.MutableMessage {
	t.Helper()
	msg := message.NewCommitTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable()
	require.NoError(t, message.MarkPartialUpdateCASCommit(msg))
	return msg
}

type testRowLock func(context.Context, message.MutableMessage) (context.Context, func(), error)

func (f testRowLock) Acquire(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
	return f(ctx, msg)
}

type rowLockTestKey struct{}

func newRowLockTestInterceptor(rowLock interceptors.MessageRowLock) *lockAppendInterceptor {
	interceptor := newTestInterceptor()
	interceptor.buildParam = &interceptors.InterceptorBuildParam{RowLock: rowLock}
	return interceptor
}

func TestRowLockIsHeldUnderShardLockAcrossAppend(t *testing.T) {
	var interceptor *lockAppendInterceptor
	acquired, released := false, false
	interceptor = newRowLockTestInterceptor(testRowLock(func(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
		// The shard lifecycle lock is already held in shared mode.
		require.False(t, interceptor.vchannelLocker.TryLock(testVChannel))
		acquired = true
		return context.WithValue(ctx, rowLockTestKey{}, true), func() {
			released = true
			require.False(t, interceptor.vchannelLocker.TryLock(testVChannel), "row lock released after the shard lock")
		}, nil
	}))
	_, err := interceptor.DoAppend(context.Background(), newLockTestInsertMessage(testVChannel), func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		require.True(t, acquired)
		require.False(t, released, "row lock released before the downstream append returned")
		require.Equal(t, true, ctx.Value(rowLockTestKey{}), "row-lock context did not reach downstream interceptors")
		return nil, nil
	})
	require.NoError(t, err)
	require.True(t, released)
	require.True(t, interceptor.vchannelLocker.TryLock(testVChannel))
	interceptor.vchannelLocker.Unlock(testVChannel)
}

func TestRowLockFailureSkipsAppendAndReleasesShardLock(t *testing.T) {
	expected := errors.New("row lock admission failed")
	interceptor := newRowLockTestInterceptor(testRowLock(func(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
		return ctx, nil, expected
	}))
	_, err := interceptor.DoAppend(context.Background(), newLockTestCASCommit(t, testVChannel), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		t.Fatal("rejected message reached the downstream append")
		return nil, nil
	})
	require.ErrorIs(t, err, expected)
	require.True(t, interceptor.vchannelLocker.TryLock(testVChannel))
	interceptor.vchannelLocker.Unlock(testVChannel)
}

// Replicated messages are serialized by the replicate interceptor, which also
// deduplicates them; they must reach it before any partial-update admission.
func TestRowLockSkipsReplicatedMessages(t *testing.T) {
	interceptor := newRowLockTestInterceptor(testRowLock(func(context.Context, message.MutableMessage) (context.Context, func(), error) {
		t.Fatal("replicated message took a row lock")
		return nil, nil, nil
	}))
	sourceID := walimplstest.NewTestMessageID(1)
	msg := newLockTestCASCommit(t, testVChannel).WithReplicateHeader(&message.ReplicateHeader{
		ClusterID:              "source",
		MessageID:              sourceID,
		LastConfirmedMessageID: sourceID,
		VChannel:               testVChannel,
	})
	appended := false
	_, err := interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		appended = true
		return nil, nil
	})
	require.NoError(t, err)
	require.True(t, appended)
}

// The partial-update builder runs after the lock builder and installs the hook
// on the shared build parameter, so the hook must be read at append time.
func TestRowLockHookIsReadAtAppendTime(t *testing.T) {
	param := &interceptors.InterceptorBuildParam{ChannelInfo: types.PChannelInfo{Name: testPChannelName}, TxnManager: new(txn.TxnManager)}
	interceptor := NewInterceptorBuilder().Build(param).(*lockAppendInterceptor)
	called := false
	param.RowLock = testRowLock(func(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
		called = true
		return ctx, func() {}, nil
	})
	_, err := interceptor.DoAppend(context.Background(), newLockTestInsertMessage(testVChannel), func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, nil
	})
	require.NoError(t, err)
	require.True(t, called)
}

// Structural exclusive messages keep the vchannel write lock, independent of
// partial-update CAS.
func TestManualFlushKeepsStructuralExclusiveLock(t *testing.T) {
	interceptor := newTestInterceptor()
	msg := message.NewManualFlushMessageBuilderV2().
		WithVChannel(testVChannel).
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
		WithBody(&message.ManualFlushMessageBody{}).
		MustBuildMutable()
	guard := interceptor.acquireLockGuard(msg)
	defer guard()
	require.False(t, interceptor.vchannelLocker.TryRLock(testVChannel))
}

func TestTopologyChangeAbortAfterAppend(t *testing.T) {
	resource.InitForTest(t)
	for _, tc := range []struct {
		name          string
		force, ignore bool
		err           error
		abort         bool
	}{
		{name: "promote", force: true, abort: true},
		{name: "ordinary topology change", abort: true},
		{name: "ignored promote", force: true, ignore: true},
		{name: "deduplicated", force: true, err: status.NewIgnoreOperation("already appended")},
		{name: "canceled writer", force: true, err: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := txn.NewTxnManager(types.PChannelInfo{Name: testPChannelName}, nil)
			defer manager.FailTxnAtVChannel("")
			begin := message.NewBeginTxnMessageBuilderV2().WithVChannel(testVChannel).
				WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
				MustBuildMutable().WithTimeTick(1).
				WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}).
				WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", MessageID: walimplstest.NewTestMessageID(1), LastConfirmedMessageID: walimplstest.NewTestMessageID(0)})
			session, err := manager.BeginNewTxn(context.Background(), message.MustAsMutableBeginTxnMessageV2(begin))
			require.NoError(t, err)
			interceptor := newTestInterceptor()
			interceptor.txnManager = manager
			msg := message.NewAlterReplicateConfigMessageBuilderV2().WithAllVChannel().
				WithHeader(&message.AlterReplicateConfigMessageHeader{ForcePromote: tc.force, Ignore: tc.ignore}).
				WithBody(&message.AlterReplicateConfigMessageBody{}).MustBuildMutable()
			_, err = interceptor.DoAppend(context.Background(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
				require.Equal(t, message.TxnStateInFlight, session.State(), "abort must follow durable append")
				return walimplstest.NewTestMessageID(2), tc.err
			})
			require.ErrorIs(t, err, tc.err)
			if tc.abort {
				require.Equal(t, message.TxnStateRollbacked, session.State())
			} else {
				require.Equal(t, message.TxnStateInFlight, session.State())
			}
			require.True(t, interceptor.glock.TryLock(), "all outcomes must release the WAL lock")
			interceptor.glock.Unlock()
		})
	}
}
