package lock

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
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

	// Exclusive VChannel messages hold the global read lock and the local write lock.
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
		// A global writer must wait for this exclusive append.
		if interceptor.glock.TryLock() {
			interceptor.glock.Unlock()
			t.Error("global write lock acquired during exclusive VChannel append")
		}
		// Other VChannels may still acquire the global read lock.
		assert.True(t, interceptor.glock.TryRLock(), "glock should not be write-locked for exclusive message on regular vchannel")
		interceptor.glock.RUnlock()
		// Per-vchannel write lock should be held.
		assert.False(t, interceptor.vchannelLocker.TryLock(testVChannel), "vchannel lock should be held")
		if interceptor.vchannelLocker.TryRLock(testVChannel) {
			interceptor.vchannelLocker.RUnlock(testVChannel)
			t.Error("same-VChannel DML must be excluded")
		}
		// Other vchannels should not be blocked.
		assert.True(t, interceptor.vchannelLocker.TryLock("other-vchannel"), "other vchannels should not be blocked")
		interceptor.vchannelLocker.Unlock("other-vchannel")
		guard()
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

// The callback checks the locks synchronously, both in downstream append and in
// transaction cleanup. Cleanup must stay inside both locks even on append failure.
func TestExclusiveVChannelAppendCleanup(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "failure", err: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			interceptor := newTestInterceptor()
			checkLocks := func() {
				if interceptor.glock.TryLock() {
					interceptor.glock.Unlock()
					t.Error("global writer entered before exclusive append cleanup completed")
				}
				if interceptor.vchannelLocker.TryRLock(testVChannel) {
					interceptor.vchannelLocker.RUnlock(testVChannel)
					t.Error("same-VChannel DML entered before cleanup completed")
				}
			}
			appended, cleaned := false, false
			mocker := mockey.Mock((*txn.TxnManager).FailTxnAtVChannel).To(func(manager *txn.TxnManager, vchannel string) {
				assert.Same(t, interceptor.txnManager, manager)
				assert.Equal(t, testVChannel, vchannel)
				assert.True(t, appended)
				checkLocks()
				cleaned = true
			}).Build()
			defer mocker.UnPatch()

			_, err := interceptor.DoAppend(context.Background(), newExclusiveMessage(testVChannel), func(context.Context, message.MutableMessage) (message.MessageID, error) {
				checkLocks()
				appended = true
				return nil, tc.err
			})
			assert.ErrorIs(t, err, tc.err)
			assert.True(t, cleaned)
			require.True(t, interceptor.glock.TryLock(), "global lock must be released after cleanup")
			interceptor.glock.Unlock()
			require.True(t, interceptor.vchannelLocker.TryRLock(testVChannel))
			interceptor.vchannelLocker.RUnlock(testVChannel)
		})
	}
}

func newExclusiveMessage(vchannel string) message.MutableMessage {
	return message.NewManualFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
		WithBody(&message.ManualFlushMessageBody{}).
		MustBuildMutable()
}

func TestGlobalExclusiveBlocksVChannelAppend(t *testing.T) {
	interceptor := newTestInterceptor()
	cleanup := mockey.Mock((*txn.TxnManager).FailTxnAtVChannel).Return().Build()
	defer cleanup.UnPatch()

	guard := interceptor.acquireLockGuard(context.Background(), newExclusiveMessage(testPChannelName))
	// Signal entry into lock acquisition while retaining the real lock guard.
	// Patch this interceptor method, not sync.RWMutex used by unrelated code.
	attempted := make(chan struct{})
	var originalAcquire func(*lockAppendInterceptor, context.Context, message.MutableMessage) func()
	acquire := mockey.Mock((*lockAppendInterceptor).acquireLockGuard).
		To(func(r *lockAppendInterceptor, ctx context.Context, msg message.MutableMessage) func() {
			close(attempted)
			return originalAcquire(r, ctx, msg)
		}).Origin(&originalAcquire).Build()
	defer acquire.UnPatch()

	var release sync.Once
	defer release.Do(guard)
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := interceptor.DoAppend(context.Background(), newExclusiveMessage(testVChannel), func(context.Context, message.MutableMessage) (message.MessageID, error) {
			close(entered)
			return nil, nil
		})
		done <- err
	}()
	defer func() {
		release.Do(guard)
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("exclusive append did not resume after global unlock")
		}
	}()

	select {
	case <-attempted:
	case <-entered:
		t.Fatal("exclusive append bypassed the global write lock")
	case <-time.After(5 * time.Second):
		t.Fatal("exclusive append did not start lock acquisition")
	}
	// Global-before-local lock order: waiting for glock must not hold the
	// VChannel write lock. The bounded wait follows the observed lock-guard entry.
	require.True(t, interceptor.vchannelLocker.TryLock(testVChannel))
	interceptor.vchannelLocker.Unlock(testVChannel)
	select {
	case <-entered:
		t.Fatal("exclusive append entered downstream while global writer held the lock")
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(guard)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("exclusive append did not enter downstream after global unlock")
	}
}

func TestExclusiveVChannelAllowsOtherVChannel(t *testing.T) {
	interceptor := newTestInterceptor()
	cleanup := mockey.Mock((*txn.TxnManager).FailTxnAtVChannel).Return().Build()
	defer cleanup.UnPatch()

	guard := interceptor.acquireLockGuard(context.Background(), newExclusiveMessage(testVChannel))
	var release sync.Once
	defer release.Do(guard)
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Both an exclusive append and DML on another VChannel may proceed.
		_, err := interceptor.DoAppend(context.Background(), newExclusiveMessage("other-vchannel"), func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return nil, nil
		})
		assert.NoError(t, err)
		dml := message.NewInsertMessageBuilderV1().
			WithVChannel("other-vchannel").
			WithHeader(&messagespb.InsertMessageHeader{CollectionId: 2}).
			WithBody(&msgpb.InsertRequest{}).
			MustBuildMutable()
		_, err = interceptor.DoAppend(context.Background(), dml, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return nil, nil
		})
		assert.NoError(t, err)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("exclusive append blocked another VChannel")
		release.Do(guard)
		<-done
	}
}
