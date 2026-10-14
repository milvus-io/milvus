package timetick

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
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
	for _, kind := range []string{"ordinary", "cas", "replicated"} {
		t.Run(kind, func(t *testing.T) {
			impl := newTestTimeTickAppendInterceptor(t)
			commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
			if kind == "cas" {
				require.NoError(t, message.MarkPartialUpdateCASCommit(commit))
			}
			if kind == "replicated" {
				commit = commit.WithReplicateHeader(&message.ReplicateHeader{
					ClusterID:              "source",
					MessageID:              walimplstest.NewTestMessageID(2),
					LastConfirmedMessageID: walimplstest.NewTestMessageID(0),
					TimeTick:               3,
					VChannel:               "v1",
				})
			}
			var provisionalTS, finalTS uint64
			released := false
			type guardContextKey struct{}
			impl.buildParam.WriteGuard = testMessageWriteGuard(func(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
				provisionalTS = msg.TimeTick()
				require.NotZero(t, provisionalTS)
				require.Equal(t, message.TxnStateOnCommit, session.State())
				// Preparation is finished: a sync may advance beyond its provisional
				// timestamp before the guard admits this commit.
				details, err := impl.operator.AckManager().SyncAndGetAcknowledged(ctx)
				require.NoError(t, err)
				require.NotEmpty(t, details)
				require.Greater(t, details[len(details)-1].BeginTimestamp, provisionalTS)
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
				// Once the final timestamp is allocated, visibility remains blocked
				// until WAL append and transaction completion finish.
				details, err := impl.operator.AckManager().SyncAndGetAcknowledged(ctx)
				require.NoError(t, err)
				for _, detail := range details {
					require.Less(t, detail.BeginTimestamp, finalTS)
				}
				return walimplstest.NewTestMessageID(2), nil
			})
			require.NoError(t, err)
			require.True(t, released)
		})
	}
}

func TestTimeTickCommitProtectsBodyDrainButNotGuardWait(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, session.AddNewMessage(ctx, 0))

	guardEntered := make(chan uint64, 1)
	releaseGuard := make(chan struct{})
	impl.buildParam.WriteGuard = testMessageWriteGuard(func(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
		guardEntered <- msg.TimeTick()
		select {
		case <-ctx.Done():
			return ctx, nil, ctx.Err()
		case <-releaseGuard:
			return ctx, func() {}, nil
		}
	})
	commitDone := make(chan error, 1)
	go func() {
		appendCtx := utility.WithExtraAppendResult(ctx, &utility.ExtraAppendResult{})
		_, err := impl.DoAppend(appendCtx, commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
			return walimplstest.NewTestMessageID(2), nil
		})
		commitDone <- err
		close(commitDone)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-commitDone:
		case <-time.After(time.Second):
		}
	})
	require.Eventually(t, func() bool { return session.State() == message.TxnStateOnCommit }, time.Second, time.Millisecond)

	marker, err := impl.operator.AckManager().Allocate(ctx)
	require.NoError(t, err)
	marker.Ack(ack.OptSync())
	details, err := impl.operator.AckManager().SyncAndGetAcknowledged(ctx)
	require.NoError(t, err)
	for _, detail := range details {
		require.Less(t, detail.BeginTimestamp, marker.Timestamp())
	}
	select {
	case <-guardEntered:
		t.Fatal("guard acquisition preceded transaction body drain")
	default:
	}

	session.AddNewMessageDoneAndKeepalive(marker.Timestamp())
	select {
	case <-guardEntered:
	case <-ctx.Done():
		t.Fatal("prepared commit did not reach its write guard")
	}
	details, err = impl.operator.AckManager().SyncAndGetAcknowledged(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, details)
	require.Greater(t, details[len(details)-1].BeginTimestamp, marker.Timestamp())
	close(releaseGuard)
	select {
	case err := <-commitDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("commit did not finish after guard admission")
	}
}

func TestTimeTickCancelledGuardWaitRejectsPreparedCommit(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	impl.buildParam.WriteGuard = testMessageWriteGuard(func(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
		details, err := impl.operator.AckManager().SyncAndGetAcknowledged(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, details)
		require.Greater(t, details[len(details)-1].BeginTimestamp, msg.TimeTick())
		cancel()
		return ctx, nil, ctx.Err()
	})
	appendCtx := utility.WithExtraAppendResult(ctx, &utility.ExtraAppendResult{})
	_, err := impl.DoAppend(appendCtx, commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		t.Fatal("cancelled commit reached the WAL")
		return nil, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, txn.IsCommitAdmissionRejected(err))
	require.Equal(t, message.TxnStateRollbacked, session.State())
	details, err := impl.operator.AckManager().SyncAndGetAcknowledged(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, details)
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
