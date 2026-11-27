package timetick

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestTimeTickCommitAppendSuccessCommits(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
	expectedMsgID := walimplstest.NewTestMessageID(2)

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	msgID, err := impl.DoAppend(ctx, commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return expectedMsgID, nil
	})

	require.NoError(t, err)
	require.True(t, expectedMsgID.EQ(msgID))
	require.Equal(t, message.TxnStateCommitted, session.State())
	requireCommitAckerSuccess(t, impl, session)
}

func TestTimeTickBodyCountMismatchNeverAppendsCommit(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
	message.MustAsMutableCommitTxnMessageV2(commit).OverwriteHeader(&message.CommitTxnMessageHeader{BodyCnt: 1})
	cleaned := false
	session.RegisterCleanup(func() { cleaned = true }, 0)
	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := impl.DoAppend(ctx, commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		t.Fatal("count mismatch must reject before WAL append")
		return nil, nil
	})
	require.True(t, status.AsStreamingError(err).IsTxnExpired())
	require.Equal(t, message.TxnStateRollbacked, session.State())
	require.True(t, cleaned)
	requireCommitAckerError(t, impl, err)
}

func TestTimeTickCommitAdmissionRejects(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
	expectedErr := errors.New("validation rejected")

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := impl.DoAppend(ctx, commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, txn.MarkCommitAdmissionRejected(expectedErr)
	})

	require.ErrorIs(t, err, expectedErr)
	require.True(t, txn.IsCommitAdmissionRejected(err))
	require.Equal(t, message.TxnStateRollbacked, session.State())
	requireCommitAckerError(t, impl, expectedErr)
}

func TestCommitCancellationKeepsReferencesForRecovery(t *testing.T) {
	for _, replicated := range []bool{false, true} {
		t.Run(map[bool]string{false: "local", true: "replicated"}[replicated], func(t *testing.T) {
			impl := newTestTimeTickAppendInterceptor(t)
			commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
			if replicated {
				commit = commit.WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", MessageID: walimplstest.NewTestMessageID(3), LastConfirmedMessageID: walimplstest.NewTestMessageID(0), TimeTick: 100, VChannel: "v1"})
			}
			cleaned := false
			session.RegisterCleanup(func() { cleaned = true }, 0)
			walCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := utility.WithExtraAppendResult(walCtx, &utility.ExtraAppendResult{})
			_, err := impl.DoAppend(ctx, commit, func(ctx context.Context, _ message.MutableMessage) (message.MessageID, error) {
				cancel() // The WAL lifecycle ends; this session is not reused for retry.
				return nil, ctx.Err()
			})
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, message.TxnStateOnCommit, session.State())
			require.False(t, cleaned)
			requireCommitAckerError(t, impl, err)
			impl.txnManager.FailTxnAtVChannel("")
			require.True(t, cleaned)
		})
	}
}

func requireCommitAckerSuccess(t *testing.T, impl *timeTickAppendInterceptor, session *txn.TxnSession) {
	t.Helper()
	details, err := impl.operator.AckManager().SyncAndGetAcknowledged(context.Background())
	require.NoError(t, err)
	for _, detail := range details {
		if detail.Message != nil && detail.Message.MessageType() == message.MessageTypeCommitTxn {
			require.NoError(t, detail.Err)
			require.Same(t, session, detail.TxnSession)
			return
		}
	}
	t.Fatal("commit acker was not completed with the committed transaction")
}

func requireCommitAckerError(t *testing.T, impl *timeTickAppendInterceptor, expectedErr error) {
	t.Helper()
	details, err := impl.operator.AckManager().SyncAndGetAcknowledged(context.Background())
	require.NoError(t, err)
	for _, detail := range details {
		if errors.Is(detail.Err, expectedErr) {
			return
		}
	}
	t.Fatal("commit acker did not retain the inner append error")
}

func newTestTimeTickAppendInterceptor(t *testing.T) *timeTickAppendInterceptor {
	t.Helper()
	paramtable.Init()
	resource.InitForTest(t)

	lastConfirmed := walimplstest.NewTestMessageID(0)
	lastTimeTick := NewTimeTickMsg(1, lastConfirmed, 0, true).IntoImmutableMessage(lastConfirmed)
	txnManager := txn.NewTxnManager(types.PChannelInfo{Name: "test"}, nil)
	<-txnManager.RecoverDone()

	param := &interceptors.InterceptorBuildParam{
		ChannelInfo:            types.PChannelInfo{Name: "test"},
		LastTimeTickMessage:    lastTimeTick,
		LastConfirmedMessageID: lastConfirmed,
		MVCCManager:            mvcc.NewMVCCManager(lastTimeTick.TimeTick()),
		TxnManager:             txnManager,
	}
	impl, ok := NewInterceptorBuilder().Build(param).(*timeTickAppendInterceptor)
	require.True(t, ok)
	t.Cleanup(impl.Close)
	return impl
}

func newCommitTxnMessageForExistingSession(
	t *testing.T,
	impl *timeTickAppendInterceptor,
	vchannel string,
) (message.MutableMessage, *txn.TxnSession) {
	t.Helper()

	var txnContext *message.TxnContext
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: time.Second.Milliseconds()}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable()
	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := impl.DoAppend(ctx, begin, func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
		txnContext = msg.TxnContext()
		return walimplstest.NewTestMessageID(1), nil
	})
	require.NoError(t, err)
	require.NotNil(t, txnContext)

	session, err := impl.txnManager.GetSessionOfTxn(txnContext.TxnID)
	require.NoError(t, err)

	return message.NewCommitTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(*txnContext), session
}
