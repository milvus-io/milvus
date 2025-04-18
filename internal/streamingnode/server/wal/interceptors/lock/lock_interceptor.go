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
	// Lock the vchannel to prevent other appenders from appending to the same vchannel.
	g := r.acqiureLockGuard(ctx, msg)
	defer g()
	return append(ctx, msg)
}

func (r *lockAppendInterceptor) acqiureLockGuard(ctx context.Context, msg message.MutableMessage) func() {
	// Acquire the write lock for the vchannel.
	vchannel := msg.VChannel()
	if msg.MessageType().IsExclusiveRequired() {
		r.vchannelLocker.Lock(vchannel)
		return func() {
			// For exclusive messages, we need to fail all transactions at the vchannel.
			// Otherwise, the transaction message may cross the exclusive message.
			// e.g. an exclusive message like `ManualFlush` happens, it will flush all the growing segment.
			// But the transaction message that use those segments may not be commited,
			// if we allow it to be commited, a insert message can be seen after the manual flush message, lead to the wrong semantic.
			// So we need to fail all transactions at the vchannel, it will be retried at client side with new txn.
			// TODO: we may make a better design for this in the future to avoid the retry at client side.
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
