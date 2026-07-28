package lock

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

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
