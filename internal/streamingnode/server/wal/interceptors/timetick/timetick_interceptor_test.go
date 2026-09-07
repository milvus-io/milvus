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

func TestTimeTickCommitInnerErrorPreservesExistingStateTransition(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, session := newCommitTxnMessageForExistingSession(t, impl, "v1")
	expectedErr := errors.New("wal append failed")

	ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
	_, err := impl.DoAppend(ctx, commit, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, expectedErr
	})

	require.ErrorIs(t, err, expectedErr)
	require.False(t, txn.IsCommitAdmissionRejected(err))
	require.Equal(t, message.TxnStateCommitted, session.State())
	requireCommitAckerError(t, impl, expectedErr)
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
