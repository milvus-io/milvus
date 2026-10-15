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
	// Builders run in chain order; a later builder can install the row-lock hook.
	buildParam     *interceptors.InterceptorBuildParam
	replicateOrder *replicateAppendOrder
}

func (r *lockAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (msgID message.MessageID, err error) {
	finish, err := r.replicateOrder.acquire(ctx, msg)
	if err != nil {
		return nil, err
	}
	g := r.acquireLockGuard(ctx, msg)
	defer g()
	defer func() {
		if err == nil && msg.MessageType() == message.MessageTypeAlterReplicateConfig {
			header := message.MustAsMutableAlterReplicateConfigMessageV2(msg).Header()
			if header.ForcePromote && !header.Ignore {
				r.replicateOrder.forcePromoted()
			}
		}
		if finish != nil {
			finish(err)
		}
	}()

	// Row locks are taken under the shard lifecycle lock and before TimeTick
	// allocates the write timestamp, so a later lock holder always receives a
	// later timestamp. They are released after the downstream append returns,
	// which is after transaction completion, acknowledgement and MVCC update.
	// Replicated messages are already serialized by the replicate interceptor
	// and must reach it before any partial-update admission check.
	if rowLock := r.rowLock(); rowLock != nil && msg.ReplicateHeader() == nil {
		var release func()
		var err error
		if ctx, release, err = rowLock.Acquire(ctx, msg); err != nil {
			return nil, err
		}
		defer release()
	}
	return append(ctx, msg)
}

func (r *lockAppendInterceptor) rowLock() interceptors.MessageRowLock {
	if r.buildParam == nil {
		return nil
	}
	return r.buildParam.RowLock
}

// acquireLockGuard acquires the lock for the vchannel and return a function as a guard.
func (r *lockAppendInterceptor) acquireLockGuard(_ context.Context, msg message.MutableMessage) func() {
	vchannel := msg.VChannel()
	if msg.MessageType().IsExclusiveRequired() {
		if vchannel == "" || vchannel == r.channel.Name || msg.IsPChannelLevel() {
			r.glock.Lock()
			return func() {
				// Fail local transactions at all VChannels.
				r.txnManager.FailTxnAtVChannel("")
				r.glock.Unlock()
			}
		} else if !funcutil.IsControlChannel(vchannel) {
			r.glock.RLock()
			r.vchannelLocker.Lock(vchannel)
			return func() {
				// Exclusive messages fail locally originated transactions at the VChannel.
				// Local flush admission has already drained replicated transactions.
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
	// DML, including CAS commits, keeps the shard lifecycle stable with a
	// shared lock. Row-level exclusion is taken by PK in DoAppend.
	r.glock.RLock()
	r.vchannelLocker.RLock(vchannel)
	return func() {
		r.vchannelLocker.RUnlock(vchannel)
		r.glock.RUnlock()
	}
}

// Close the interceptor release all the resources.
func (r *lockAppendInterceptor) Close() {
	r.replicateOrder.close()
}

func (r *lockAppendInterceptor) GracefulClose() {
	r.replicateOrder.close()
}
