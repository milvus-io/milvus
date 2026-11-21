package adaptor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/ack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// Uses actual durable test-WAL reads and the ordinary scanner. No reader shares
// the old TxnBuffer, which would hide a bad checkpoint that skips Begin.
func TestRecoveredTxnSurvivesFreshScannerAndSecondRecovery(t *testing.T) {
	resource.InitForTest(t)
	ctx := context.Background()
	l := newRecoveryTestWAL(t)
	historical := appendRecoveryTestMessage(t, l, message.MessageTypeTimeTick, 1)
	begin := appendRecoveryTestMessage(t, l, message.MessageTypeBeginTxn, 2)
	body := appendRecoveryTestMessage(t, l, message.MessageTypeInsert, 3)
	appendRecoveryTestMessage(t, l, message.MessageTypeTimeTick, 4)
	barrier := appendRecoveryTestMessage(t, l, message.MessageTypeTimeTick, 5)
	ro := adaptImplsToROWAL(l, func() {})
	defer ro.Close()

	openStream := func(start message.MessageID, end message.ImmutableMessage) recovery.RecoveryStream {
		return newRecoveryStreamBuilder(ro).Build(recovery.BuildRecoveryStreamParam{
			StartCheckpoint: start, EndTimeTick: end.TimeTick(),
		})
	}
	drain := func(stream recovery.RecoveryStream, end message.ImmutableMessage) []message.ImmutableTxnMessage {
		var transactions []message.ImmutableTxnMessage
		for {
			msg := receiveRecoveryTestMessage(t, stream)
			if transaction, ok := msg.(message.ImmutableTxnMessage); ok {
				transactions = append(transactions, transaction)
			}
			if msg.MessageType() == message.MessageTypeTimeTick && msg.MessageID().EQ(end.MessageID()) {
				return transactions
			}
		}
	}
	stream := openStream(historical.MessageID(), barrier)
	require.Empty(t, drain(stream, barrier))
	snapshot := &recovery.RecoverySnapshot{TxnBuffer: stream.TxnBuffer()}
	require.Len(t, snapshot.TxnBuffer.GetUncommittedMessageBuilder(), 1)
	_ = stream.Close()
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
	end := appendRecoveryTestMessage(t, l, message.MessageTypeTimeTick, confirmed.TimeTick()+1)
	for _, name := range []string{"fresh scanner", "second crash from pre-commit checkpoint"} {
		t.Run(name, func(t *testing.T) {
			reader := openStream(checkpoint.LastConfirmedMessageID(), end)
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
	reader := openStream(checkpoint.LastConfirmedMessageID(), end)
	results := utility.NewTxnTombstones()
	results.Advance(end.TimeTick())
	for _, transaction := range drain(reader, end) {
		results.Observe(transaction)
	}
	_ = reader.Close()
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

func recoveryTestMessage(t *testing.T, kind message.MessageType, tt uint64) message.MutableMessage {
	t.Helper()
	var msg message.MutableMessage
	var err error
	switch kind {
	case message.MessageTypeTimeTick:
		msg, err = message.NewTimeTickMessageBuilderV1().WithVChannel("").WithHeader(&message.TimeTickMessageHeader{}).WithBody(&msgpb.TimeTickMsg{}).BuildMutable()
	case message.MessageTypeBeginTxn:
		msg, err = message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).BuildMutable()
	case message.MessageTypeInsert:
		msg, err = message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{}).WithBody(&msgpb.InsertRequest{}).BuildMutable()
	case message.MessageTypeCommitTxn:
		msg, err = message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).BuildMutable()
	case message.MessageTypeRollbackTxn:
		msg, err = message.NewRollbackTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.RollbackTxnMessageHeader{}).WithBody(&message.RollbackTxnMessageBody{}).BuildMutable()
	default:
		t.Fatalf("unsupported message type: %s", kind)
	}
	require.NoError(t, err)
	if kind != message.MessageTypeTimeTick {
		msg = msg.WithTxnContext(message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite})
	}
	return msg.WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(0))
}

func newRecoveryTestWAL(t *testing.T) walimpls.WALImpls {
	t.Helper()
	opener, err := registry.MustGetBuilder(message.WALNameTest).Build()
	require.NoError(t, err)
	t.Cleanup(opener.Close)
	l, err := opener.Open(context.Background(), &walimpls.OpenOption{Channel: types.PChannelInfo{
		Name: "recovery-" + funcutil.GenRandomStr(), Term: 1, AccessMode: types.AccessModeRW,
	}})
	require.NoError(t, err)
	t.Cleanup(l.Close)
	return l
}

func appendRecoveryTestMessage(t *testing.T, l walimpls.WALImpls, kind message.MessageType, tt uint64) message.ImmutableMessage {
	t.Helper()
	msg := recoveryTestMessage(t, kind, tt)
	// The test WAL occasionally rejects an append before writing it. Retry only
	// that injected failure; every successful append still has one physical record.
	var id message.MessageID
	var err error
	for attempt := 0; attempt < 100; attempt++ {
		id, err = l.Append(context.Background(), msg)
		if err == nil {
			break
		}
		require.EqualError(t, err, "random error")
	}
	require.NoError(t, err)
	return msg.IntoImmutableMessage(id)
}

func receiveRecoveryTestMessage(t *testing.T, stream recovery.RecoveryStream) message.ImmutableMessage {
	t.Helper()
	select {
	case msg, ok := <-stream.Chan():
		require.True(t, ok, "recovery stream closed")
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for recovery stream")
		return nil
	}
}
