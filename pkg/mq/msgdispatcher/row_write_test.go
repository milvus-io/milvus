package msgdispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mq/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newRowWriteReplayMessage(t *testing.T, channel string, tick uint64) message.ImmutableMessage {
	t.Helper()
	rows := &messagespb.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b"}}}}, RowIdsEncoding: &messagespb.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{1, 2}}}, PartitionIndexes: []uint32{0, 1}}
	mutable := message.NewUpsertMessageBuilderV2().WithVChannel(channel).WithHeader(&messagespb.UpsertMessageHeader{CollectionId: 1, DeleteRows: 2, Partitions: []*messagespb.PartitionSegmentAssignment{{PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100}}, {PartitionId: 20, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 200}}}}).WithBody(&messagespb.UpsertMessageBody{Rows: rows, DeleteEffects: []*messagespb.UpsertDeleteEffect{{Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}, KeySource: &messagespb.UpsertDeleteEffect_AllInsertedKeys{AllInsertedKeys: &messagespb.UpsertAllInsertedKeys{}}}}}).MustBuildMutable()
	return mutable.WithTimeTick(tick).WithLastConfirmed(rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(int64(tick)))
}

func TestRowWriteDispatcherExpandsSelectedChannelAtomically(t *testing.T) {
	d := &Dispatcher{targets: typeutil.NewConcurrentMap[string, *target]()}
	d.AddTarget(newTarget(&StreamConfig{VChannel: "pchannel_0_1v0"}, false))
	msg := newRowWriteReplayMessage(t, "pchannel_0_1v0", 100)
	pack := consumeReplayMessage(t, msg)
	grouped, err := d.groupAndParseMsgs(pack, adaptor.UnmashalerDispatcher)
	require.NoError(t, err)
	require.Len(t, grouped["pchannel_0_1v0"].Msgs, 3)
	deletion := grouped["pchannel_0_1v0"].Msgs[2].(*msgstream.DeleteMsg)
	require.Equal(t, []string{"a", "b"}, deletion.PrimaryKeys.GetStrId().Data)
	wire := msg.IntoImmutableMessageProto()
	wire.Payload = []byte{0xff}
	broken := message.NewImmutableMesasge(rmq.NewRmqID(3), wire.Payload, wire.Properties)
	// A malformed second body cannot let a valid first body escape its transaction.
	txn := newReplayTransaction(t, 101, rmq.NewRmqID(1), msg, broken)
	grouped, err = d.groupAndParseMsgs(consumeReplayMessage(t, txn), adaptor.UnmashalerDispatcher)
	require.Error(t, err)
	require.Nil(t, grouped)
	d.RemoveTarget("pchannel_0_1v0")
	grouped, err = d.groupAndParseMsgs(consumeReplayMessage(t, broken), adaptor.UnmashalerDispatcher)
	require.NoError(t, err)
	require.Empty(t, grouped, "unsubscribed payload must not be decoded")
}

// observedCompoundMessage signals an actual decoder result so lifecycle signals
// cannot race ahead of the failure being exercised.
type observedCompoundMessage struct {
	msgstream.ConsumeMsg
	attempts chan error
}

func (m *observedCompoundMessage) TryUnmarshalMessages() ([]msgstream.TsMsg, error) {
	messages, err := m.ConsumeMsg.(interface {
		TryUnmarshalMessages() ([]msgstream.TsMsg, error)
	}).TryUnmarshalMessages()
	m.attempts <- err
	return messages, err
}

func newFailedRowWritePack(t *testing.T) (*msgstream.ConsumeMsgPack, <-chan error) {
	t.Helper()
	msg := newRowWriteReplayMessage(t, "pchannel_0_1v0", 100)
	wire := msg.IntoImmutableMessageProto()
	pack := consumeReplayMessage(t, message.NewImmutableMesasge(msg.MessageID(), []byte{0xff}, wire.Properties))
	attempts := make(chan error, 1)
	pack.Msgs[0] = &observedCompoundMessage{ConsumeMsg: pack.Msgs[0], attempts: attempts}
	return pack, attempts
}

func newRowWriteDispatcher(t *testing.T, input chan *msgstream.ConsumeMsgPack) *Dispatcher {
	t.Helper()
	stream := msgstream.NewMockMsgStream(t)
	stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	stream.EXPECT().Chan().Return((<-chan *msgstream.ConsumeMsgPack)(input))
	stream.EXPECT().GetUnmarshalDispatcher().Return(adaptor.UnmashalerDispatcher)
	stream.EXPECT().Close().Return().Once()
	factory := &msgstream.MockMqFactory{NewMsgStreamFunc: func(context.Context) (msgstream.MsgStream, error) { return stream, nil }}
	d, err := NewDispatcher(context.Background(), factory, time.Now().UnixNano(), "pchannel_0", nil, common.SubscriptionPositionEarliest, 0, false)
	require.NoError(t, err)
	d.curTs.Store(10)
	return d
}

func awaitRowWriteDecodeFailure(t *testing.T, attempts <-chan error) {
	t.Helper()
	select {
	case err := <-attempts:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("compound decoder was not called")
	}
}

func awaitDispatcherOperation(t *testing.T, operation func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { operation(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatcher lifecycle operation blocked")
	}
}

func TestRowWriteDispatcherFailureCannotAdvanceToLaterPack(t *testing.T) {
	failed, attempts := newFailedRowWritePack(t)
	input := make(chan *msgstream.ConsumeMsgPack, 2)
	input <- failed
	input <- consumeReplayMessage(t, newRowWriteReplayMessage(t, "pchannel_0_1v1", 200))
	d := newRowWriteDispatcher(t, input)
	manager := NewDispatcherManager(d.pchannel, typeutil.QueryNodeRole, 1, newMockFactory(), false).(*dispatcherManager)
	manager.mainDispatcher = d
	for _, channel := range []string{"pchannel_0_1v0", "pchannel_0_1v1"} {
		_, err := manager.Add(context.Background(), NewStreamConfig(channel, nil, common.SubscriptionPositionEarliest))
		require.NoError(t, err)
		target, _ := manager.registeredTargets.Get(channel)
		d.AddTarget(target)
	}
	badTarget, _ := d.GetTarget("pchannel_0_1v0")
	remaining, _ := d.GetTarget("pchannel_0_1v1")
	d.Handle(start)
	for range 2 {
		awaitRowWriteDecodeFailure(t, attempts)
		awaitDispatcherOperation(t, func() { d.Handle(pause) })
		require.Empty(t, d.done, "pause must consume its stop signal")
		require.Equal(t, uint64(10), d.CurTs())
		require.Same(t, failed, d.pendingPack)
		require.Len(t, input, 1, "the successor must remain unread")
		require.Empty(t, badTarget.ch)
		require.Empty(t, remaining.ch)
		d.Handle(resume)
	}
	awaitRowWriteDecodeFailure(t, attempts)
	// Removing the failed channel pauses and resumes the existing worker.
	awaitDispatcherOperation(t, func() { manager.Remove(badTarget.vchannel) })
	select {
	case _, open := <-badTarget.ch:
		require.False(t, open)
	case <-time.After(5 * time.Second):
		t.Fatal("removed target was not closed")
	}
	// Empty progress for the removed channel's pack may precede the rows.
	for tick := uint64(100); tick <= 200; tick += 100 {
		select {
		case pack := <-remaining.ch:
			require.Equal(t, tick, pack.EndTs)
			if tick == 200 {
				require.Len(t, pack.Msgs, 3)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("remaining target stopped after resume")
		}
	}
	awaitDispatcherOperation(t, func() { d.Handle(pause) })
	require.Empty(t, d.done)
	require.Nil(t, d.pendingPack)
	require.Equal(t, uint64(200), d.CurTs())
	d.Handle(resume)
	input <- consumeReplayMessage(t, newRowWriteReplayMessage(t, remaining.vchannel, 300))
	select {
	case pack := <-remaining.ch:
		require.Equal(t, uint64(300), pack.EndTs)
		require.Len(t, pack.Msgs, 3)
	case <-time.After(5 * time.Second):
		t.Fatal("resumed dispatcher consumed a stale stop signal")
	}
	awaitDispatcherOperation(t, func() { manager.Remove(remaining.vchannel) })
	require.Zero(t, manager.NumConsumer())
}

func TestRowWriteDispatcherFailureAllowsManagerRemoval(t *testing.T) {
	for _, placement := range []string{"main", "deputy"} {
		t.Run(placement, func(t *testing.T) {
			failed, attempts := newFailedRowWritePack(t)
			input := make(chan *msgstream.ConsumeMsgPack, 2)
			input <- failed
			input <- consumeReplayMessage(t, newRowWriteReplayMessage(t, "pchannel_0_1v0", 200))
			d := newRowWriteDispatcher(t, input)
			manager := NewDispatcherManager(d.pchannel, typeutil.QueryNodeRole, 1, newMockFactory(), false).(*dispatcherManager)
			_, err := manager.Add(context.Background(), NewStreamConfig("pchannel_0_1v0", nil, common.SubscriptionPositionEarliest))
			require.NoError(t, err)
			target, _ := manager.registeredTargets.Get("pchannel_0_1v0")
			d.AddTarget(target)
			switch placement {
			case "main":
				manager.mainDispatcher = d
			case "deputy":
				manager.deputyDispatchers[d.ID()] = d
			}
			d.Handle(start)
			awaitRowWriteDecodeFailure(t, attempts)
			awaitDispatcherOperation(t, func() { manager.Remove(target.vchannel) })
			require.Zero(t, manager.NumConsumer(), "manager mutex must be released")
			require.Zero(t, manager.NumTarget())
			require.Len(t, input, 1)
			require.Equal(t, uint64(10), d.CurTs())
			require.Same(t, failed, d.pendingPack)
			_, open := <-target.ch
			require.False(t, open)
		})
	}
}
