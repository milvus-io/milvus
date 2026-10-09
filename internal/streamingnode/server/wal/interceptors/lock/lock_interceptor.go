package lock

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
)

type lockAppendInterceptor struct {
	channel        types.PChannelInfo
	glock          sync.RWMutex // glock is a wal level lock, it will acquire a highest level lock for wal.
	vchannelLocker *lock.KeyLock[string]
	txnManager     *txn.TxnManager
}

func (r *lockAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (message.MessageID, error) {
	g := r.acquireLockGuard(ctx, msg)
	defer g()

	return append(ctx, msg)
}

// acquireLockGuard acquires the lock for the vchannel and return a function as a guard.
func (r *lockAppendInterceptor) acquireLockGuard(_ context.Context, msg message.MutableMessage) func() {
	vchannel := msg.VChannel()
	if msg.MessageType().IsExclusiveRequired() {
		if vchannel == "" || vchannel == r.channel.Name || msg.IsPChannelLevel() {
			r.glock.Lock()
			return func() {
				// fail all transactions at all vchannels.
				r.txnManager.FailTxnAtVChannel("")
				r.glock.Unlock()
			}
		} else if !funcutil.IsControlChannel(vchannel) {
			r.glock.RLock()
			r.vchannelLocker.Lock(vchannel)
			return func() {
				// For exclusive messages, we need to fail all transactions at the vchannel.
				// Otherwise, the transaction message may cross the exclusive message.
				// e.g. an exclusive message like `ManualFlush` happens, it will flush all the growing segment.
				// But the transaction insert message that use those segments may not be committed,
				// if we allow it to be committed, a insert message can be seen after the manual flush message, lead to the wrong wal message order.
				// So we need to fail all transactions at the vchannel, it will be retried at client side with new txn.
				//
				// the append operation of exclusive message should be low rate, so it's acceptable to fail all transactions at the vchannel.
				r.txnManager.FailTxnAtVChannel(vchannel)
				r.vchannelLocker.Unlock(vchannel)
				r.glock.RUnlock()
			}
		}
		// Collection-scoped DDL is exclusive on its data VChannels. Its
		// control-channel copy uses the shared path below so DDL with
		// non-conflicting resource keys can append concurrently. Conflicting
		// DDL is serialized by the broadcaster resource-key lock. PChannel-level
		// messages already acquire the global write lock above.
	}
	if msg.MessageType() == message.MessageTypeCommitTxn &&
		message.HasPartialUpdateCAS(msg) && msg.ReplicateHeader() == nil {
		r.glock.RLock()
		r.vchannelLocker.Lock(vchannel)
		return func() {
			r.vchannelLocker.Unlock(vchannel)
			r.glock.RUnlock()
		}
	}
	r.glock.RLock()
	r.vchannelLocker.RLock(vchannel)
	return func() {
		r.vchannelLocker.RUnlock(vchannel)
		r.glock.RUnlock()
	}
}

// Close the interceptor release all the resources.
func (r *lockAppendInterceptor) Close() {}
