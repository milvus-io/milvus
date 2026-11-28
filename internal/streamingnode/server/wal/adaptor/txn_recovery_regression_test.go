package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/wab"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// Uses actual durable test-WAL reads and the ordinary scanner. No reader shares
// the old TxnBuffer, which would hide a bad checkpoint that skips Begin.
func TestRecoveredTxnSurvivesFreshScannerAndSecondRecovery(t *testing.T) {
	resource.InitForTest(t)
	ctx := context.Background()
	l := newRecoveryTestWAL(t)
	historical := appendRecoveryTestMessage(t, l, message.MessageTypeRecoveryBarrier, 1)
	begin := appendRecoveryTestMessage(t, l, message.MessageTypeBeginTxn, 2)
	body := appendRecoveryTestMessage(t, l, message.MessageTypeInsert, 3)
	appendRecoveryTestMessage(t, l, message.MessageTypeTimeTick, 4)
	barrier := appendRecoveryTestMessage(t, l, message.MessageTypeRecoveryBarrier, 5)
	ro := adaptImplsToROWAL(l, func() {})
	defer ro.Close()

	openStream := func(start message.MessageID, end message.ImmutableMessage) (recovery.RecoveryStream, *wab.WriteAheadBuffer) {
		buffer := wab.NewWriteAheadBuffer(l.Channel().Name, mlog.With(), 1024*1024, time.Hour, end)
		stream := newRecoveryStreamBuilder(ro, buffer, make(chan struct{})).Build(recovery.BuildRecoveryStreamParam{
			StartCheckpoint: start, RecoveryBarrier: end,
		})
		return stream, buffer
	}
	drain := func(stream recovery.RecoveryStream, end message.ImmutableMessage) []message.ImmutableTxnMessage {
		var transactions []message.ImmutableTxnMessage
		for {
			msg := receiveRecoveryTestMessage(t, stream)
			if transaction, ok := msg.(message.ImmutableTxnMessage); ok {
				transactions = append(transactions, transaction)
			}
			if msg.MessageType() == message.MessageTypeRecoveryBarrier && msg.MessageID().EQ(end.MessageID()) {
				return transactions
			}
		}
	}
	stream, buffer := openStream(historical.MessageID(), barrier)
	require.Empty(t, drain(stream, barrier))
	snapshot := &recovery.RecoverySnapshot{TxnBuffer: stream.TxnBuffer()}
	require.Len(t, snapshot.TxnBuffer.GetUncommittedMessageBuilder(), 1)
	_ = stream.Close()
	buffer.Close()
	manager := txn.NewTxnManager(l.Channel(), snapshot.TxnBuffer.GetUncommittedMessageBuilder())
	impl := timetick.NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo: l.Channel(), LastTimeTickMessage: barrier,
		LastConfirmedMessageID: determineLastConfirmedMessageID(barrier.MessageID(), snapshot.TxnBuffer),
		TxnManager:             manager, MVCCManager: mvcc.NewMVCCManager(barrier.TimeTick()), InitialRecoverSnapshot: snapshot,
	})
	defer func() {
		if impl != nil {
			impl.Close()
		}
	}()
	operator := resource.Resource().TimeTickInspector().MustGetOperator(l.Channel()).(interface{ AckManager() *ack.AckManager })
	persist := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		for {
			id, err := l.Append(ctx, msg)
			if err == nil {
				return id, nil
			}
			if err.Error() != "random error" {
				return nil, err
			}
		}
	}
	appendMsg := func(msg message.MutableMessage) message.ImmutableMessage {
		extra := &utility.ExtraAppendResult{}
		id, err := impl.DoAppend(utility.WithExtraAppendResult(ctx, extra), msg, persist)
		require.NoError(t, err)
		return msg.WithTimeTick(extra.TimeTick).WithLastConfirmed(extra.LastConfirmedMessageID).IntoImmutableMessage(id)
	}
	sync := func() message.ImmutableMessage {
		details, err := operator.AckManager().SyncAndGetAcknowledged(ctx)
		require.NoError(t, err)
		batch := ack.NewAckDetails()
		batch.AddDetails(details)
		msg := timetick.NewTimeTickMsg(batch.LastAllAcknowledgedTimestamp(), batch.EarliestLastConfirmedMessageID(), 0, true)
		id, err := impl.DoAppend(utility.WithExtraAppendResult(ctx, &utility.ExtraAppendResult{}), msg, persist)
		require.NoError(t, err)
		return msg.IntoImmutableMessage(id)
	}
	// Several real ACKs and TT batches must not unpin the recovered Begin.
	for n := 0; n < 3; n++ {
		appendMsg(message.CreateTestEmptyInsertMesage(1, nil))
		sync()
	}
	checkpoint := sync()
	require.True(t, checkpoint.LastConfirmedMessageID().LT(begin.MessageID()))
	commitMsg := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(*begin.TxnContext())
	commit := appendMsg(commitMsg)
	require.Greater(t, commit.TimeTick(), checkpoint.TimeTick())
	confirmed := sync()
	end := appendRecoveryTestMessage(t, l, message.MessageTypeRecoveryBarrier, confirmed.TimeTick()+1)
	for _, name := range []string{"fresh scanner", "second crash from pre-commit checkpoint"} {
		t.Run(name, func(t *testing.T) {
			reader, buffer := openStream(checkpoint.LastConfirmedMessageID(), end)
			defer buffer.Close()
			defer reader.Close()
			transactions := drain(reader, end)
			require.Len(t, transactions, 1)
			transaction := transactions[0]
			require.Greater(t, transaction.TimeTick(), checkpoint.TimeTick())
			require.True(t, transaction.Begin().MessageID().EQ(begin.MessageID()))
			require.True(t, transaction.Commit().MessageID().EQ(commit.MessageID()))
			require.Equal(t, 1, transaction.Size())
			require.NoError(t, transaction.RangeOver(func(msg message.ImmutableMessage) error {
				require.True(t, msg.MessageID().EQ(body.MessageID()))
				return nil
			}))
		})
	}
	// A handoff reconstructs a result from that replay, even though its active
	// session is absent. It must not append another Commit or allocate a new TT.
	reader, buffer := openStream(checkpoint.LastConfirmedMessageID(), end)
	results := utility.NewTxnTombstones()
	results.Advance(end.TimeTick())
	for _, transaction := range drain(reader, end) {
		results.Observe(transaction)
	}
	_ = reader.Close()
	buffer.Close()
	impl.Close()
	impl = nil
	reopened := timetick.NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo: l.Channel(), LastTimeTickMessage: end, LastConfirmedMessageID: end.MessageID(),
		TxnManager: txn.NewTxnManager(l.Channel(), nil), MVCCManager: mvcc.NewMVCCManager(end.TimeTick()),
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{TxnTombstones: results},
	})
	defer reopened.Close()
	extra := &utility.ExtraAppendResult{}
	id, err := reopened.DoAppend(utility.WithExtraAppendResult(ctx, extra), commitMsg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		t.Error("recovered tombstone must not append")
		return nil, context.Canceled
	})
	require.NoError(t, err)
	require.True(t, id.EQ(commit.MessageID()))
	require.Equal(t, commit.TimeTick(), extra.TimeTick)
	require.True(t, commit.LastConfirmedMessageID().EQ(extra.LastConfirmedMessageID))
}
