package lock

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

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

	// Test: Exclusive message on regular vchannel should acquire per-vchannel lock only (not global).
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
		// glock should NOT be write-locked.
		assert.True(t, interceptor.glock.TryRLock(), "glock should not be write-locked for exclusive message on regular vchannel")
		interceptor.glock.RUnlock()
		// Per-vchannel write lock should be held.
		assert.False(t, interceptor.vchannelLocker.TryLock(testVChannel), "vchannel lock should be held")
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
