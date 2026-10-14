package partialupdate

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	streamingtimetick "github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type observedCommitGuard func(context.Context, message.MutableMessage) (context.Context, func(), error)

func (g observedCommitGuard) Acquire(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
	return g(ctx, msg)
}

// Err is first checked by rowLocks.acquire, after Acquire has captured the
// transaction's write set. This observes real lock admission without sleeps.
type commitGuardContext struct {
	context.Context
	once    sync.Once
	entered func()
}

func (ctx *commitGuardContext) Err() error {
	ctx.once.Do(ctx.entered)
	return ctx.Context.Err()
}

func commitAckManager(t *testing.T, env *partialUpdateChainTestEnv) *ack.AckManager {
	t.Helper()
	operator := resource.Resource().TimeTickInspector().MustGetOperator(env.buildParam.ChannelInfo)
	return operator.(interface{ AckManager() *ack.AckManager }).AckManager()
}

func confirmedCommitTimestamp(t *testing.T, manager *ack.AckManager) uint64 {
	t.Helper()
	details, err := manager.SyncAndGetAcknowledged(context.Background())
	require.NoError(t, err)
	var confirmed uint64
	for _, detail := range details {
		if detail.BeginTimestamp > confirmed {
			confirmed = detail.BeginTimestamp
		}
	}
	return confirmed
}

func receiveCommitValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not reach its expected synchronization point")
		var zero T
		return zero
	}
}

func TestPartialUpdateQueuedCommitsDoNotHoldConfirmedTimeTick(t *testing.T) {
	for _, depth := range []int{1, 8, 32} {
		t.Run(fmt.Sprintf("N%d", depth), func(t *testing.T) {
			env := newPartialUpdateChainTestEnv(t)
			require.False(t, exerciseCommitWatermarkQueue(t, env, depth),
				"queued provisional timestamps blocked confirmation of a disjoint ordinary write")
		})
	}
}

func exerciseCommitWatermarkQueue(
	t *testing.T,
	env *partialUpdateChainTestEnv,
	depth int,
) bool {
	t.Helper()
	readTS := env.allocateReadTS(t)
	first := env.prepareCASTxn(t, readTS, 10)
	waiters := make([]message.MutableMessage, depth)
	for i := range waiters {
		waiters[i] = newChainTestCASCommit(t, env.prepareCASTxn(t, readTS, 10))
	}
	atWAL := make(chan uint64, 1)
	releaseWAL, releasePK := make(chan struct{}), make(chan struct{})
	acknowledged, waiting := make(chan struct{}), make(chan uint64, depth)
	var walOnce, pkOnce sync.Once
	defer walOnce.Do(func() { close(releaseWAL) })
	defer pkOnce.Do(func() { close(releasePK) })
	env.buildParam.RowLock = observedCommitGuard(func(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
		if msg.MessageType() != message.MessageTypeCommitTxn {
			return env.partial.Acquire(ctx, msg)
		}
		if msg.TxnContext().TxnID != first.TxnID {
			ctx = &commitGuardContext{Context: ctx, entered: func() { waiting <- msg.TimeTick() }}
			return env.partial.Acquire(ctx, msg)
		}
		guardCtx, release, err := env.partial.Acquire(ctx, msg)
		if err != nil {
			return guardCtx, release, err
		}
		return guardCtx, func() {
			// TimeTick releases the guard after final acknowledgement and MVCC
			// publication. Hold the real PK lock here to isolate queue obstruction
			// from a legitimately unacknowledged WAL operation.
			close(acknowledged)
			<-releasePK
			release()
		}, err
	})
	firstDone := make(chan error, 1)
	go func() {
		_, err := env.append(newChainTestCASCommit(t, first), func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
			atWAL <- msg.TimeTick()
			<-releaseWAL
			return env.nextMessageID(), nil
		})
		firstDone <- err
	}()
	finalTS := receiveCommitValue(t, atWAL)
	done := make(chan error, depth)
	for _, commit := range waiters {
		go func(commit message.MutableMessage) {
			_, err := env.append(commit, nil)
			done <- err
		}(commit)
	}
	for range waiters {
		require.Greater(t, receiveCommitValue(t, waiting), finalTS)
	}
	manager := commitAckManager(t, env)
	// The active WAL append must still hold back the watermark.
	require.Less(t, confirmedCommitTimestamp(t, manager), finalTS)
	walOnce.Do(func() { close(releaseWAL) })
	receiveCommitValue(t, acknowledged)
	var ordinaryTS uint64
	_, err := env.append(newGuardTestDelete(20), func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
		ordinaryTS = msg.TimeTick()
		return env.nextMessageID(), nil
	})
	require.NoError(t, err)
	obstructed := confirmedCommitTimestamp(t, manager) < ordinaryTS
	pkOnce.Do(func() { close(releasePK) })
	require.NoError(t, receiveCommitValue(t, firstDone))
	for range waiters {
		// All waiting attempts read before the first commit, so rejection is
		// expected; they do not each perform another WAL append.
		err := receiveCommitValue(t, done)
		require.True(t, status.AsStreamingError(err).IsPartialUpdateRetryableCAS() || status.AsStreamingError(err).IsTxnExpired())
	}
	// Resolve completed sessions through the normal TimeTick cleanup path and
	// confirm that completed attempts no longer occupy the transaction registry.
	cleanupTS := confirmedCommitTimestamp(t, manager)
	_, err = env.append(streamingtimetick.NewTimeTickMsg(cleanupTS, env.nextMessageID(), 0, true), nil)
	require.NoError(t, err)
	for _, commit := range append(waiters, newChainTestCASCommit(t, first)) {
		_, err := env.txnManager.GetSessionOfTxn(commit.TxnContext().TxnID)
		require.True(t, status.AsStreamingError(err).IsTxnExpired())
	}
	requireTestRowLocksAvailable(t, env.partial.state.rowLocks, "v1", 10, 20)
	return obstructed
}

func TestPartialUpdateCommitExpiresDuringRealGuardWait(t *testing.T) {
	for _, cleanupFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("cleanup_before_snapshot_%t", cleanupFirst), func(t *testing.T) {
			env := newPartialUpdateChainTestEnv(t)
			cas := env.prepareCASTxn(t, env.allocateReadTS(t), 10)
			session, err := env.txnManager.GetSessionOfTxn(cas.TxnID)
			require.NoError(t, err)
			guards := env.partial.state.rowLocks
			key := pkLockKey{vchannel: "v1", kind: primarykey.KindInt64, int64Value: 10}
			guards.pks.Lock(key)
			unlock := func() { guards.pks.Unlock(key) }
			var once sync.Once
			defer once.Do(unlock)
			waiting := make(chan uint64, 1)
			resumeSnapshot := make(chan struct{})
			var snapshotOnce sync.Once
			defer snapshotOnce.Do(func() { close(resumeSnapshot) })
			env.buildParam.RowLock = observedCommitGuard(func(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
				if msg.MessageType() != message.MessageTypeCommitTxn {
					return env.partial.Acquire(ctx, msg)
				}
				if cleanupFirst {
					waiting <- msg.TimeTick()
					<-resumeSnapshot
					return env.partial.Acquire(ctx, msg)
				}
				ctx = &commitGuardContext{Context: ctx, entered: func() { waiting <- msg.TimeTick() }}
				return env.partial.Acquire(ctx, msg)
			})
			done, wal := make(chan error, 1), make(chan struct{}, 1)
			go func() {
				_, err := env.append(newChainTestCASCommit(t, cas), func(context.Context, message.MutableMessage) (message.MessageID, error) {
					wal <- struct{}{}
					return env.nextMessageID(), nil
				})
				done <- err
			}()
			preparedTS := receiveCommitValue(t, waiting)
			expiredTS := tsoutil.AddPhysicalDurationOnTs(preparedTS, cas.Keepalive+time.Second)
			// Advance final timestamp deterministically, without sleeping for the
			// transaction lease. Cleanup itself follows the real TimeTick path.
			var nextTS atomic.Uint64
			nextTS.Store(expiredTS)
			patch := mockey.Mock(mockey.GetMethod(resource.Resource().TSOAllocator(), "Allocate")).To(func(context.Context) (uint64, error) {
				return nextTS.Add(1), nil
			}).Build()
			defer patch.UnPatch()
			confirmedTS := confirmedCommitTimestamp(t, commitAckManager(t, env))
			require.GreaterOrEqual(t, confirmedTS, expiredTS, "waiting commit held back its own expiry watermark")
			tick := streamingtimetick.NewTimeTickMsg(confirmedTS, env.nextMessageID(), 0, true)
			_, err = env.append(tick, nil)
			require.NoError(t, err)
			require.Nil(t, env.partial.state.getTxn(cas.TxnID))
			_, err = env.txnManager.GetSessionOfTxn(cas.TxnID)
			require.True(t, status.AsStreamingError(err).IsTxnExpired(), "cleaned session lookup returned: %v", err)
			allocationsBeforeResume := patch.MockTimes()
			snapshotOnce.Do(func() { close(resumeSnapshot) })
			once.Do(unlock)
			err = receiveCommitValue(t, done)
			require.True(t, txn.IsCommitAdmissionRejected(err))
			if cleanupFirst {
				// Missing proof state is rejected by Acquire before a final
				// timestamp is allocated. The WAL must still remain untouched.
				require.True(t, status.AsStreamingError(err).IsPartialUpdateRetryableCAS(), "guarded commit returned: %v", err)
			} else {
				require.Greater(t, patch.MockTimes(), allocationsBeforeResume, "final timestamp allocation bypassed the expiry injection")
				require.True(t, status.AsStreamingError(err).IsTxnExpired(), "guarded commit returned: %v", err)
			}
			require.Equal(t, message.TxnStateRollbacked, session.State())
			require.Empty(t, wal)
			requireTestRowLocksAvailable(t, guards, "v1", 10)
		})
	}
}

var _ interceptors.MessageRowLock = observedCommitGuard(nil)
