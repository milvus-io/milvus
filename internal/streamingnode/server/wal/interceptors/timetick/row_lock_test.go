package timetick

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type testMessageRowLock func(context.Context, message.MutableMessage) (context.Context, func(), error)

func (g testMessageRowLock) Acquire(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
	return g(ctx, msg)
}

func TestTimeTickRowLockPrecedesFinalCommitTimestamp(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
	var provisionalTS, confirmedDuringGuard, finalTS uint64
	released := false
	type guardContextKey struct{}
	impl.buildParam.RowLock = testMessageRowLock(func(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
		provisionalTS = msg.TimeTick()
		require.NotZero(t, provisionalTS)
		require.Equal(t, message.TxnStateOnCommit, session.State())
		// Once the body is drained, waiting for PK locks must not prevent
		// confirmation of unrelated writes on the same PChannel.
		details, err := impl.operator.AckManager().SyncAndGetAcknowledged(ctx)
		require.NoError(t, err)
		provisionalAcknowledged := false
		for _, detail := range details {
			confirmedDuringGuard = max(confirmedDuringGuard, detail.BeginTimestamp)
			if detail.BeginTimestamp == provisionalTS {
				provisionalAcknowledged = true
				require.True(t, detail.IsSync)
				require.Nil(t, detail.Message)
			}
		}
		require.True(t, provisionalAcknowledged)
		require.Greater(t, confirmedDuringGuard, provisionalTS)
		return context.WithValue(ctx, guardContextKey{}, true), func() {
			released = true
			require.Equal(t, message.TxnStateCommitted, session.State())
			require.GreaterOrEqual(t, impl.operator.MVCCManager().GetMVCCOfVChannel("v1").Timetick, finalTS)
			requireCommitAckerSuccess(t, impl, session)
		}, nil
	})

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := impl.DoAppend(ctx, commit, func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		require.Equal(t, true, ctx.Value(guardContextKey{}))
		finalTS = msg.TimeTick()
		require.Greater(t, finalTS, confirmedDuringGuard)
		return walimplstest.NewTestMessageID(2), nil
	})
	require.NoError(t, err)
	require.True(t, released)
}

func TestTimeTickRowLockExpiryRejectsPreparedCommitAfterCleanup(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
	released := false
	impl.buildParam.RowLock = testMessageRowLock(func(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
		if msg.MessageType() != message.MessageTypeCommitTxn {
			return ctx, func() {}, nil
		}
		require.Equal(t, message.TxnStateOnCommit, session.State())
		expiredTS := tsoutil.AddPhysicalDurationOnTs(msg.TimeTick(), session.TxnContext().Keepalive+time.Second)
		// Simulate a lock wait long enough for TimeTick cleanup to expire the
		// session. Advance the allocator instead of sleeping for the lease.
		patch := mockey.Mock(mockey.GetMethod(resource.Resource().TSOAllocator(), "Allocate")).To(func(context.Context) (uint64, error) {
			expiredTS++
			return expiredTS, nil
		}).Build()
		t.Cleanup(func() { patch.UnPatch() })
		details, err := impl.operator.AckManager().SyncAndGetAcknowledged(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, details)
		confirmedTS := details[len(details)-1].BeginTimestamp
		require.GreaterOrEqual(t, confirmedTS, expiredTS)

		tick := NewTimeTickMsg(confirmedTS, walimplstest.NewTestMessageID(1), 0, true)
		_, err = impl.DoAppend(ctx, tick, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return walimplstest.NewTestMessageID(2), nil
		})
		require.NoError(t, err)
		_, err = impl.txnManager.GetSessionOfTxn(session.TxnContext().TxnID)
		require.True(t, status.AsStreamingError(err).IsTxnExpired())
		return ctx, func() { released = true }, nil
	})

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := impl.DoAppend(ctx, commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		t.Fatal("expired commit reached the WAL")
		return nil, nil
	})
	require.True(t, txn.IsCommitAdmissionRejected(err))
	require.True(t, status.AsStreamingError(err).IsTxnExpired())
	require.Equal(t, message.TxnStateRollbacked, session.State())
	require.True(t, released)
	requireCommitAckerError(t, impl, err)
}

func TestTimeTickRowLockFailureRejectsPreparedCommit(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
	expectedErr := errors.New("row lock admission failed")
	impl.buildParam.RowLock = testMessageRowLock(func(ctx context.Context, _ message.MutableMessage) (context.Context, func(), error) {
		require.Equal(t, message.TxnStateOnCommit, session.State())
		return ctx, nil, expectedErr
	})

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := impl.DoAppend(ctx, commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		t.Fatal("rejected commit reached the WAL")
		return nil, nil
	})
	require.ErrorIs(t, err, expectedErr)
	require.True(t, txn.IsCommitAdmissionRejected(err))
	require.Equal(t, message.TxnStateRollbacked, session.State())
	// The provisional timestamp is acknowledged without fabricating a WAL
	// message or transaction success.
	details, syncErr := impl.operator.AckManager().SyncAndGetAcknowledged(context.Background())
	require.NoError(t, syncErr)
	for _, detail := range details {
		if detail.Message != nil {
			require.NotEqual(t, message.MessageTypeCommitTxn, detail.Message.MessageType())
		}
	}
}
