package lock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func orderTxnMessage(kind message.MessageType, id message.TxnID, tt uint64) message.MutableMessage {
	var msg message.MutableMessage
	switch kind {
	case message.MessageTypeBeginTxn:
		msg = message.NewBeginTxnMessageBuilderV2().WithVChannel(testVChannel).
			WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable()
	case message.MessageTypeCommitTxn:
		msg = message.NewCommitTxnMessageBuilderV2().WithVChannel(testVChannel).
			WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable()
	default:
		msg = message.NewDeleteMessageBuilderV1().WithVChannel(testVChannel).
			WithHeader(&message.DeleteMessageHeader{}).WithBody(&message.DeleteRequest{}).MustBuildMutable()
	}
	return msg.WithTxnContext(message.TxnContext{TxnID: id, Keepalive: message.TxnKeepaliveInfinite}).
		WithReplicateHeader(&message.ReplicateHeader{
			ClusterID: "source", TimeTick: tt,
			MessageID: walimplstest.NewTestMessageID(int64(tt)), LastConfirmedMessageID: walimplstest.NewTestMessageID(1),
		})
}

func orderFlushMessage(vchannel string) message.MutableMessage {
	return message.NewManualFlushMessageBuilderV2().WithUnreplicable().WithVChannel(vchannel).
		WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable()
}

type orderAdmission struct {
	finish func(error)
	err    error
}

func orderAcquireAsync(ctx context.Context, order *replicateAppendOrder, msg message.MutableMessage) <-chan orderAdmission {
	result := make(chan orderAdmission, 1)
	go func() {
		finish, err := order.acquire(ctx, msg)
		result <- orderAdmission{finish, err}
	}()
	return result
}

func orderReceive(t *testing.T, ch <-chan orderAdmission) func(error) {
	t.Helper()
	select {
	case result := <-ch:
		require.NoError(t, result.err)
		return result.finish
	case <-time.After(5 * time.Second):
		t.Fatal("append admission did not complete")
		return nil
	}
}

func orderWaitFlushes(t *testing.T, order *replicateAppendOrder, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		order.mu.Lock()
		defer order.mu.Unlock()
		return len(order.flushes) == count
	}, 5*time.Second, time.Millisecond)
}

func TestLocalFlushBetweenReplicatedTransactions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	order := newReplicateAppendOrder(nil)
	begin, err := order.acquire(ctx, orderTxnMessage(message.MessageTypeBeginTxn, 1, 10))
	require.NoError(t, err)
	flush := orderAcquireAsync(ctx, order, orderFlushMessage(testVChannel))
	orderWaitFlushes(t, order, 1)
	next := orderAcquireAsync(ctx, order, orderTxnMessage(message.MessageTypeBeginTxn, 2, 20))
	// The Begin append was already admitted when Flush arrived.
	begin(nil)

	// Reconnect can replay old messages before reaching the current txn.
	duplicate, err := order.acquire(ctx, orderTxnMessage(message.MessageTypeBeginTxn, 99, 5))
	require.NoError(t, err)
	duplicate(status.NewIgnoreOperation("old transaction"))
	duplicate, err = order.acquire(ctx, orderTxnMessage(message.MessageTypeBeginTxn, 1, 10))
	require.NoError(t, err)
	duplicate(status.NewIgnoreOperation("current transaction"))
	body, err := order.acquire(ctx, orderTxnMessage(message.MessageTypeDelete, 1, 10))
	require.NoError(t, err)
	body(nil)
	commit, err := order.acquire(ctx, orderTxnMessage(message.MessageTypeCommitTxn, 1, 10))
	require.NoError(t, err)
	commit(context.DeadlineExceeded)
	require.NotNil(t, order.current, "failed Commit must not release Flush")
	select {
	case <-flush:
		t.Fatal("Flush overtook failed Commit")
	default:
	}
	commit, err = order.acquire(ctx, orderTxnMessage(message.MessageTypeCommitTxn, 1, 10))
	require.NoError(t, err)
	commit(nil)
	finishFlush := orderReceive(t, flush)
	select {
	case <-next:
		t.Fatal("new transaction overtook Flush append")
	default:
	}
	finishFlush(nil)
	orderReceive(t, next)(nil)
}

func TestLocalFlushCancellationAndFIFO(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	order := newReplicateAppendOrder(nil)
	begin, err := order.acquire(ctx, orderTxnMessage(message.MessageTypeBeginTxn, 1, 10))
	require.NoError(t, err)
	cancelCtx, cancelFirst := context.WithCancel(ctx)
	first := orderAcquireAsync(cancelCtx, order, orderFlushMessage(testVChannel))
	orderWaitFlushes(t, order, 1)
	second := orderAcquireAsync(ctx, order, orderFlushMessage(testVChannel))
	orderWaitFlushes(t, order, 2)
	third := orderAcquireAsync(ctx, order, orderFlushMessage(testVChannel))
	orderWaitFlushes(t, order, 3)
	cancelFirst()
	require.ErrorIs(t, (<-first).err, context.Canceled)
	orderWaitFlushes(t, order, 2)
	// Failed Begin has no admitted transaction left to drain.
	begin(context.Canceled)
	finishSecond := orderReceive(t, second)
	select {
	case <-third:
		t.Fatal("later Flush overtook earlier Flush append")
	default:
	}
	finishSecond(nil)
	orderReceive(t, third)(nil)
}

func TestLocalFlushPromotionAndShutdown(t *testing.T) {
	for _, promote := range []bool{false, true} {
		t.Run(map[bool]string{false: "shutdown", true: "promotion"}[promote], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			order := newReplicateAppendOrder(nil)
			begin, err := order.acquire(ctx, orderTxnMessage(message.MessageTypeBeginTxn, 1, 10))
			require.NoError(t, err)
			begin(nil)
			flush := orderAcquireAsync(ctx, order, orderFlushMessage(testVChannel))
			orderWaitFlushes(t, order, 1)
			if promote {
				order.forcePromoted()
				orderReceive(t, flush)(nil)
			} else {
				order.close()
				result := <-flush
				require.True(t, status.AsStreamingError(result.err).IsOnShutdown())
				require.Nil(t, result.finish)
			}
		})
	}
}

func TestLocalFlushClassification(t *testing.T) {
	require.True(t, isLocalFlush(orderFlushMessage(testVChannel)))
	require.False(t, isLocalFlush(orderFlushMessage(funcutil.GetControlChannel(testPChannelName))))
	legacy := message.NewManualFlushMessageBuilderV2().WithVChannel(testVChannel).
		WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable()
	require.False(t, isLocalFlush(legacy))
	all := message.NewFlushAllMessageBuilderV2().WithUnreplicable().WithVChannel(testPChannelName).
		WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{}).MustBuildMutable()
	require.True(t, isLocalFlush(all))
}

func TestLocalFlushRestoresReplicatedTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	buffer := utility.NewTxnBuffer(nil, nil)
	begin := orderTxnMessage(message.MessageTypeBeginTxn, 1, 10).WithTimeTick(100).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(2))
	buffer.GetUncommittedMessageBuilder()[1] = message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
	order := newReplicateAppendOrder(&recovery.RecoverySnapshot{
		TxnBuffer: buffer,
		PChannelControl: &streamingpb.PChannelRecoveryControlMeta{
			ReplicateCheckpoint: &commonpb.ReplicateCheckpoint{ClusterId: "source"},
		},
	})
	flush := orderAcquireAsync(ctx, order, orderFlushMessage(testVChannel))
	orderWaitFlushes(t, order, 1)
	commit, err := order.acquire(ctx, orderTxnMessage(message.MessageTypeCommitTxn, 1, 10))
	require.NoError(t, err)
	commit(nil)
	orderReceive(t, flush)(nil)
}

func TestLocalFlushWaitsForSuccessfulForcePromoteAppend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	interceptor := newTestInterceptor()
	appendOK := func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return walimplstest.NewTestMessageID(1), nil
	}
	_, err := interceptor.DoAppend(ctx, orderTxnMessage(message.MessageTypeBeginTxn, 1, 10), appendOK)
	require.NoError(t, err)
	flush := orderAcquireAsync(ctx, interceptor.replicateOrder, orderFlushMessage(testVChannel))
	orderWaitFlushes(t, interceptor.replicateOrder, 1)
	promote := message.NewAlterReplicateConfigMessageBuilderV2().WithVChannel(testPChannelName).
		WithHeader(&message.AlterReplicateConfigMessageHeader{ForcePromote: true}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).MustBuildMutable()
	_, err = interceptor.DoAppend(ctx, promote, func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return nil, context.DeadlineExceeded
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-flush:
		t.Fatal("failed ForcePromote must not release Flush")
	default:
	}
	_, err = interceptor.DoAppend(ctx, promote, appendOK)
	require.NoError(t, err)
	orderReceive(t, flush)(nil)
}
