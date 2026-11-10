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

func newRowWriteReplayMessage(t *testing.T) message.ImmutableMessage {
	t.Helper()
	rows := &messagespb.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b"}}}}, RowIdsEncoding: &messagespb.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{1, 2}}}, PartitionIndexes: []uint32{0, 1}}
	mutable := message.NewUpsertMessageBuilderV2().WithVChannel("pchannel_0_1v0").WithHeader(&messagespb.UpsertMessageHeader{CollectionId: 1, DeleteRows: 2, Partitions: []*messagespb.PartitionSegmentAssignment{{PartitionId: 10, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100}}, {PartitionId: 20, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 200}}}}).WithBody(&messagespb.UpsertMessageBody{Rows: rows, DeleteEffects: []*messagespb.UpsertDeleteEffect{{Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}, KeySource: &messagespb.UpsertDeleteEffect_AllInsertedKeys{AllInsertedKeys: &messagespb.UpsertAllInsertedKeys{}}}}}).MustBuildMutable()
	return mutable.WithTimeTick(100).WithLastConfirmed(rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(2))
}

func TestRowWriteDispatcherExpandsSelectedChannelAtomically(t *testing.T) {
	d := &Dispatcher{targets: typeutil.NewConcurrentMap[string, *target]()}
	d.AddTarget(newTarget(&StreamConfig{VChannel: "pchannel_0_1v0"}, false))
	msg := newRowWriteReplayMessage(t)
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

func TestRowWriteDispatcherFailureCannotAdvanceToLaterPack(t *testing.T) {
	msg := newRowWriteReplayMessage(t)
	wire := msg.IntoImmutableMessageProto()
	wire.Payload = []byte{0xff}
	broken := message.NewImmutableMesasge(rmq.NewRmqID(3), wire.Payload, wire.Properties)
	input := make(chan *msgstream.ConsumeMsgPack, 2)
	input <- consumeReplayMessage(t, broken)
	input <- consumeReplayMessage(t, msg)
	stream := msgstream.NewMockMsgStream(t)
	stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	stream.EXPECT().Chan().Return((<-chan *msgstream.ConsumeMsgPack)(input))
	stream.EXPECT().GetUnmarshalDispatcher().Return(adaptor.UnmashalerDispatcher)
	stream.EXPECT().Close().Return()
	factory := &msgstream.MockMqFactory{NewMsgStreamFunc: func(context.Context) (msgstream.MsgStream, error) { return stream, nil }}
	d, err := NewDispatcher(context.Background(), factory, time.Now().UnixNano(), "pchannel_0", nil, common.SubscriptionPositionEarliest, 0, false)
	require.NoError(t, err)
	target := newTarget(&StreamConfig{VChannel: "pchannel_0_1v0"}, false)
	d.AddTarget(target)
	d.curTs.Store(10)
	d.Handle(start)
	done := make(chan struct{})
	go func() { d.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatcher did not stop on corrupt compound message")
	}
	require.Equal(t, uint64(10), d.CurTs())
	require.Len(t, input, 1)
	require.NotNil(t, d.pendingPack, "pause/resume must retain the failed record")
	select {
	case <-target.ch:
		t.Fatal("failed record or successor was dispatched")
	default:
	}
	d.Handle(terminate)
}
