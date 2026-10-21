package timetick

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestRecoveredTransactionPinsLaterAcknowledgements(t *testing.T) {
	resource.InitForTest(t)
	floor := walimplstest.NewTestMessageID(0)
	txnCtx := message.TxnContext{TxnID: 7, Keepalive: message.TxnKeepaliveInfinite}
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(txnCtx).WithTimeTick(1).WithLastConfirmed(floor).
		IntoImmutableMessage(walimplstest.NewTestMessageID(10))
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	body := message.CreateTestEmptyInsertMesage(1, nil).WithTxnContext(txnCtx).
		WithTimeTick(2).WithLastConfirmed(floor).IntoImmutableMessage(walimplstest.NewTestMessageID(11))
	builder.Add(body)
	metrics := metricsutil.NewScanMetrics(types.PChannelInfo{Name: t.Name()})
	defer metrics.Close()
	buffer := utility.NewTxnBuffer(mlog.With(), metrics.NewScannerMetrics())
	buffer.GetUncommittedMessageBuilder()[txnCtx.TxnID] = builder
	manager := txn.NewTxnManager(types.PChannelInfo{Name: t.Name()}, buffer.GetUncommittedMessageBuilder())
	barrier := NewTimeTickMsg(3, floor, 0, true).IntoImmutableMessage(walimplstest.NewTestMessageID(12))
	impl := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo: types.PChannelInfo{Name: t.Name()}, LastTimeTickMessage: barrier,
		LastConfirmedMessageID: floor, TxnManager: manager, MVCCManager: mvcc.NewMVCCManager(3),
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{TxnBuffer: buffer},
	}).(*timeTickAppendInterceptor)
	defer impl.Close()
	for n := int64(20); n < 24; n++ {
		ctx := utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
		var lcmid message.MessageID
		_, err := impl.DoAppend(ctx, message.CreateTestEmptyInsertMesage(1, nil),
			func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
				lcmid = msg.IntoImmutableMessage(walimplstest.NewTestMessageID(n)).LastConfirmedMessageID()
				return walimplstest.NewTestMessageID(n), nil
			})
		require.NoError(t, err)
		require.True(t, lcmid.LT(begin.MessageID()), "LCMID passed unresolved Begin")
		_, err = impl.operator.AckManager().SyncAndGetAcknowledged(context.Background())
		require.NoError(t, err)
	}
	manager.FailTxnAtVChannel("")
}

func TestRepeatedCommitReturnsOriginalResult(t *testing.T) {
	for _, cleanup := range []bool{false, true} {
		name := "before_cleanup"
		if cleanup {
			name = "after_cleanup"
		}
		t.Run(name, func(t *testing.T) {
			impl := newTestTimeTickAppendInterceptor(t)
			commit, _ := newCommitTxnMessageForExistingSession(t, impl, "v1")
			calls := 0
			appendFn := func(context.Context, message.MutableMessage) (message.MessageID, error) {
				calls++
				return walimplstest.NewTestMessageID(2), nil
			}
			original := &utility.ExtraAppendResult{}
			id, err := impl.DoAppend(utility.WithExtraAppendResult(context.Background(), original), commit, appendFn)
			require.NoError(t, err)
			// Drop the successful response; the producer resends exactly this Commit.
			if cleanup {
				impl.txnManager.CleanupTxnUntil(original.TimeTick + 1)
			}
			retried := &utility.ExtraAppendResult{}
			retryID, err := impl.DoAppend(utility.WithExtraAppendResult(context.Background(), retried), commit, appendFn)
			require.NoError(t, err)
			require.True(t, id.EQ(retryID))
			require.Equal(t, original, retried)
			require.Equal(t, 1, calls)
		})
	}
}

func TestConcurrentRepeatedCommitUsesOneResult(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, _ := newCommitTxnMessageForExistingSession(t, impl, "v1")
	txnCtx := *commit.TxnContext()
	started, finish := make(chan struct{}), make(chan struct{})
	results := make(chan *utility.ExtraAppendResult, 8)
	errs := make(chan error, 8)
	var calls atomic.Int32
	for n := 0; n < 8; n++ {
		go func() {
			msg := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").
				WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
				MustBuildMutable().WithTxnContext(txnCtx)
			extra := &utility.ExtraAppendResult{}
			_, err := impl.DoAppend(utility.WithExtraAppendResult(context.Background(), extra), msg,
				func(context.Context, message.MutableMessage) (message.MessageID, error) {
					if calls.Add(1) == 1 {
						close(started)
					}
					<-finish
					return walimplstest.NewTestMessageID(2), nil
				})
			results <- extra
			errs <- err
		}()
	}
	<-started
	close(finish)
	var original *utility.ExtraAppendResult
	for n := 0; n < 8; n++ {
		require.NoError(t, <-errs)
		result := <-results
		if original == nil {
			original = result
		}
		require.Equal(t, original, result)
	}
	require.EqualValues(t, 1, calls.Load())
	impl.txnManager.CleanupTxnUntil(original.TimeTick + 1)
	require.NoError(t, impl.txnManager.GracefulClose(context.Background()), "tombstones must not block closing")
	details, err := impl.operator.AckManager().SyncAndGetAcknowledged(context.Background())
	require.NoError(t, err)
	commits := 0
	for _, detail := range details {
		if detail.Message != nil && detail.Message.MessageType() == message.MessageTypeCommitTxn {
			commits++
		}
	}
	require.Equal(t, 1, commits, "duplicate responses must not produce ACKs")
}

func TestFailedCommitDoesNotCreateSuccessTombstone(t *testing.T) {
	impl := newTestTimeTickAppendInterceptor(t)
	commit, _ := newCommitTxnMessageForExistingSession(t, impl, "v1")
	_, err := impl.DoAppend(utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{}), commit,
		func(context.Context, message.MutableMessage) (message.MessageID, error) { return nil, context.Canceled })
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, impl.tombstones.Get(commit.TxnContext().TxnID, "v1"))
	impl.txnManager.FailTxnAtVChannel("")
}
