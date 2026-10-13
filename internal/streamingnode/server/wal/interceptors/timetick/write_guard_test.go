package timetick

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type testMessageWriteGuard func(context.Context, message.MutableMessage) (context.Context, func(), error)

func (g testMessageWriteGuard) Acquire(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
	return g(ctx, msg)
}

func TestTimeTickWriteGuardPrecedesFinalCommitTimestamp(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
	var provisionalTS, finalTS uint64
	released := false
	type guardContextKey struct{}
	impl.buildParam.WriteGuard = testMessageWriteGuard(func(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
		provisionalTS = msg.TimeTick()
		require.NotZero(t, provisionalTS)
		require.Equal(t, message.TxnStateOnCommit, session.State())
		// Even a concurrent sync cannot publish past the outstanding provisional
		// acker while the commit waits for its PK locks.
		details, err := impl.operator.AckManager().SyncAndGetAcknowledged(ctx)
		require.NoError(t, err)
		for _, detail := range details {
			require.Less(t, detail.BeginTimestamp, provisionalTS)
		}
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
		require.Greater(t, finalTS, provisionalTS)
		return walimplstest.NewTestMessageID(2), nil
	})
	require.NoError(t, err)
	require.True(t, released)
}

func TestTimeTickWriteGuardFailureRejectsPreparedCommit(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
	expectedErr := errors.New("write guard admission failed")
	impl.buildParam.WriteGuard = testMessageWriteGuard(func(ctx context.Context, _ message.MutableMessage) (context.Context, func(), error) {
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
