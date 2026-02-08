package lock

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
)

// TestAcquireLockGuard_PChannelLevelMessage tests that pchannel-level messages
// acquire the glock (global write lock) even when their VChannel is a control channel name
// that doesn't match the pchannel name directly.
func TestAcquireLockGuard_PChannelLevelMessage(t *testing.T) {
	mock := mockey.Mock((*txn.TxnManager).FailTxnAtVChannel).Return().Build()
	defer mock.UnPatch()

	interceptor := &lockAppendInterceptor{
		channel:        types.PChannelInfo{Name: "pchannel1"},
		glock:          sync.RWMutex{},
		vchannelLocker: lock.NewKeyLock[string](),
		txnManager:     &txn.TxnManager{},
	}

	// Create a FlushAll message that is pchannel-level.
	// Its VChannel will be a control channel name like "pchannel1_cchannel",
	// which is different from the pchannel name "pchannel1".
	msg := message.NewFlushAllMessageBuilderV2().
		WithHeader(&message.FlushAllMessageHeader{}).
		WithBody(&message.FlushAllMessageBody{}).
		WithVChannel("pchannel1_cchannel"). // control channel on this pchannel
		MustBuildMutable()

	// Manually set the pchannel-level property to simulate what
	// WithClusterLevelBroadcast does after SplitIntoMutableMessage.
	msg.Properties().(message.Properties).Set("_pcl", "")

	// This message's VChannel is "pchannel1_cchannel" which is != "" and != "pchannel1".
	// Without the IsPChannelLevel() fix, it would acquire per-vchannel lock instead of glock.
	// With the fix, it should acquire glock.
	guard := interceptor.acquireLockGuard(context.Background(), msg)

	// Verify glock is held (write lock) by trying to RLock it - should block.
	glockHeld := make(chan bool, 1)
	go func() {
		done := make(chan struct{})
		go func() {
			interceptor.glock.RLock()
			close(done)
			interceptor.glock.RUnlock()
		}()
		// Give the RLock goroutine time to attempt the lock.
		time.Sleep(10 * time.Millisecond)
		select {
		case <-done:
			glockHeld <- false // glock was not write-locked
		default:
			glockHeld <- true // glock is write-locked (RLock blocked)
		}
	}()

	assert.True(t, <-glockHeld, "glock should be write-locked for pchannel-level message")
	guard()
}

// TestAcquireLockGuard_RegularExclusiveMessage tests that regular exclusive messages
// with a specific VChannel acquire per-vchannel lock, not glock.
func TestAcquireLockGuard_RegularExclusiveMessage(t *testing.T) {
	mock := mockey.Mock((*txn.TxnManager).FailTxnAtVChannel).Return().Build()
	defer mock.UnPatch()

	interceptor := &lockAppendInterceptor{
		channel:        types.PChannelInfo{Name: "pchannel1"},
		glock:          sync.RWMutex{},
		vchannelLocker: lock.NewKeyLock[string](),
		txnManager:     &txn.TxnManager{},
	}

	msg := message.NewFlushAllMessageBuilderV2().
		WithHeader(&message.FlushAllMessageHeader{}).
		WithBody(&message.FlushAllMessageBody{}).
		WithVChannel("pchannel1_vchannel1").
		MustBuildMutable()

	// This is a regular exclusive message (not pchannel-level).
	// It should acquire per-vchannel lock, not glock.
	assert.False(t, msg.IsPChannelLevel())

	guard := interceptor.acquireLockGuard(context.Background(), msg)

	// glock should not be write-locked (should be able to RLock it).
	interceptor.glock.RLock()
	assert.False(t, msg.IsPChannelLevel())
	interceptor.glock.RUnlock()

	guard()
}
