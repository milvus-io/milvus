package lock

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
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
		mocker := mockey.Mock((*txn.TxnManager).FailTxnAtVChannel).Return().Build()
		defer mocker.UnPatch()

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

		guard := interceptor.acquireLockGuard(context.Background(), msg)
		assert.False(t, interceptor.glock.TryRLock(), "pchannel-level message should hold the global write lock")
		guard()
		assert.True(t, interceptor.glock.TryRLock())
		interceptor.glock.RUnlock()
	})

	// Test: Exclusive message with pchannel name as vchannel should acquire global write lock (existing behavior).
	t.Run("ExclusiveWithPChannelName", func(t *testing.T) {
		mocker := mockey.Mock((*txn.TxnManager).FailTxnAtVChannel).Return().Build()
		defer mocker.UnPatch()

		interceptor := newTestInterceptor()
		msg := message.NewManualFlushMessageBuilderV2().
			WithVChannel(testPChannelName).
			WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
			WithBody(&message.ManualFlushMessageBody{}).
			MustBuildMutable()

		guard := interceptor.acquireLockGuard(context.Background(), msg)
		assert.False(t, interceptor.glock.TryRLock(), "glock should be write-locked for exclusive message with pchannel name")
		guard()
		assert.True(t, interceptor.glock.TryRLock())
		interceptor.glock.RUnlock()
	})

	// Test: Exclusive message on regular vchannel should acquire the global
	// read lock before the per-vchannel write lock.
	t.Run("ExclusiveOnRegularVChannel", func(t *testing.T) {
		mocker := mockey.Mock((*txn.TxnManager).FailTxnAtVChannel).Return().Build()
		defer mocker.UnPatch()

		interceptor := newTestInterceptor()
		msg := message.NewManualFlushMessageBuilderV2().
			WithVChannel(testVChannel).
			WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
			WithBody(&message.ManualFlushMessageBody{}).
			MustBuildMutable()

		guard := interceptor.acquireLockGuard(context.Background(), msg)
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
		mocker := mockey.Mock((*txn.TxnManager).FailTxnAtVChannel).Return().Build()
		defer mocker.UnPatch()

		interceptor := newTestInterceptor()
		controlChannel := funcutil.GetControlChannel(testPChannelName)
		msg := message.NewCreateCollectionMessageBuilderV1().
			WithVChannel(controlChannel).
			WithHeader(&messagespb.CreateCollectionMessageHeader{CollectionId: 1}).
			WithBody(&msgpb.CreateCollectionRequest{}).
			MustBuildMutable()

		guard := interceptor.acquireLockGuard(context.Background(), msg)
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
		failCalls := 0
		mocker := mockey.Mock((*txn.TxnManager).FailTxnAtVChannel).To(
			func(*txn.TxnManager, string) {
				failCalls++
			},
		).Build()
		defer mocker.UnPatch()

		interceptor := newTestInterceptor()
		msg := message.NewCommitTxnMessageBuilderV2().
			WithVChannel(testVChannel).
			WithHeader(&message.CommitTxnMessageHeader{}).
			WithBody(&message.CommitTxnMessageBody{}).
			MustBuildMutable()
		assert.NoError(t, message.MarkPartialUpdateCASCommit(msg))

		guard := interceptor.acquireLockGuard(context.Background(), msg)
		assert.False(t, interceptor.glock.TryLock())
		assert.True(t, interceptor.glock.TryRLock())
		interceptor.glock.RUnlock()
		assert.False(t, interceptor.vchannelLocker.TryLock(testVChannel))
		assert.True(t, interceptor.vchannelLocker.TryLock("other-vchannel"))
		interceptor.vchannelLocker.Unlock("other-vchannel")
		guard()
		assert.Equal(t, 0, failCalls)
	})

	// Test: Non-exclusive message on regular vchannel should acquire read locks on both glock and vchannel.
	t.Run("NonExclusiveOnRegularVChannel", func(t *testing.T) {
		mocker := mockey.Mock((*txn.TxnManager).FailTxnAtVChannel).Return().Build()
		defer mocker.UnPatch()

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

		guard := interceptor.acquireLockGuard(context.Background(), msg)
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

func TestPartialUpdateCASCommitWaitsForSameVChannelWriter(t *testing.T) {
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
		close(releaseWriter)
		require.NoError(t, <-writerDone)
		t.Fatal("CAS commit entered while a same-vchannel writer still held the read lock")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseWriter)
	require.NoError(t, <-writerDone)
	select {
	case <-commitEntered:
	case <-time.After(time.Second):
		t.Fatal("CAS commit did not resume after the writer released the lock")
	}
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
