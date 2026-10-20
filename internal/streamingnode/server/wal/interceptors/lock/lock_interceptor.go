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
	buildParam *interceptors.InterceptorBuildParam
}

func (r *lockAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (msgID message.MessageID, err error) {
	g := r.acquireLockGuard(ctx, msg)
	defer func() {
		// A rejected or deduplicated operation is not a durable abort boundary.
		abort := err == nil
		if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
			abort = abort && !message.MustAsMutableAlterReplicateConfigMessageV2(msg).Header().Ignore
		}
		g(abort)
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
func (r *lockAppendInterceptor) acquireLockGuard(_ context.Context, msg message.MutableMessage) func(bool) {
	vchannel := msg.VChannel()
	if msg.MessageType().IsExclusiveRequired() {
		if vchannel == "" || vchannel == r.channel.Name || msg.IsPChannelLevel() {
			r.glock.Lock()
			return func(abort bool) {
				if abort {
					r.txnManager.FailTxnAtVChannel("")
				}
				r.glock.Unlock()
			}
		} else if !funcutil.IsControlChannel(vchannel) {
			r.glock.RLock()
			r.vchannelLocker.Lock(vchannel)
			return func(abort bool) {
				// A transaction cannot commit across a durable exclusive operation.
				if abort {
					r.txnManager.FailTxnAtVChannel(vchannel)
				}
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
	return func(_ bool) {
		r.vchannelLocker.RUnlock(vchannel)
		r.glock.RUnlock()
	}
}

// Close the interceptor release all the resources.
func (r *lockAppendInterceptor) Close() {}
