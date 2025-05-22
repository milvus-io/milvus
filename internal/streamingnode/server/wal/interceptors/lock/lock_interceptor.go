package lock

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
)

type lockAppendInterceptor struct {
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
	// Acquire the write lock for the vchannel.
	vchannel := msg.VChannel()
	if msg.MessageType().IsExclusiveRequired() {
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
		}
	}
	r.vchannelLocker.RLock(vchannel)
	return func() {
		r.vchannelLocker.RUnlock(vchannel)
	}
}

// Close the interceptor release all the resources.
func (r *lockAppendInterceptor) Close() {}
