package utility

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func newReaderTransactionBody(t *testing.T, txn message.TxnContext, tick uint64, upsert bool) message.ImmutableMessage {
	t.Helper()
	rows := &messagespb.InsertRows{
		PrimaryKeys:      &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101, 102}}}},
		RowIdsEncoding:   &messagespb.InsertRows_DeltaRowIds{DeltaRowIds: &messagespb.DeltaEncodedRowIDs{Base: 1001, Deltas: []int64{1}}},
		PartitionIndexes: []uint32{0, 1},
		Columns:          []*schemapb.FieldData{{Type: schemapb.DataType_Int64, FieldId: 101, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{11, 12}}}}}}},
	}
	partitions := []*message.PartitionSegmentAssignment{
		{PartitionId: 10, Rows: 1, SegmentAssignment: &message.SegmentAssignment{SegmentId: 100}},
		{PartitionId: 20, Rows: 1, SegmentAssignment: &message.SegmentAssignment{SegmentId: 200}},
	}
	var msg message.MutableMessage
	if upsert {
		msg = message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithHeader(&message.UpsertMessageHeader{CollectionId: 1, Partitions: partitions, DeleteRows: 2}).WithBody(&message.UpsertMessageBody{
			Rows: rows, DeleteEffects: []*messagespb.UpsertDeleteEffect{{Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}, KeySource: &messagespb.UpsertDeleteEffect_AllInsertedKeys{AllInsertedKeys: &messagespb.UpsertAllInsertedKeys{}}}},
		}).MustBuildMutable()
	} else {
		msg = message.NewInsertMessageBuilderV2().WithVChannel("v1").WithHeader(&message.InsertMessageHeaderV2{CollectionId: 1, Partitions: partitions}).WithBody(&message.InsertMessageBody{Rows: rows}).MustBuildMutable()
	}
	return msg.WithTimeTick(tick).WithLastConfirmedUseMessageID().WithTxnContext(txn).IntoImmutableMessage(rmq.NewRmqID(idAllocator.Allocate()))
}

// Existing transaction fixtures use a test-only WAL identity. MsgPack conversion
// needs a production MQ identity; their same-as-message LastConfirmed property
// remains valid when wrapping the raw record in an RMQ identity.
func rowReaderMQIdentity(msg message.ImmutableMessage) message.ImmutableMessage {
	raw := msg.IntoImmutableMessageProto()
	return message.NewImmutableMesasge(rmq.NewRmqID(idAllocator.Allocate()), raw.GetPayload(), raw.GetProperties())
}

func TestTxnBufferRowWritesUseCommitVisibility(t *testing.T) {
	metrics := metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics()
	t.Cleanup(metrics.Close)
	buffer := NewTxnBuffer(mlog.With(), metrics)
	base := tsoutil.ComposeTSByTime(time.Now())
	txn := message.TxnContext{TxnID: 51, Keepalive: time.Second}
	begin := rowReaderMQIdentity(newBeginMessage(t, &txn, base))
	insert := newReaderTransactionBody(t, txn, tsoutil.AddPhysicalDurationOnTs(base, time.Millisecond), false)
	upsert := newReaderTransactionBody(t, txn, tsoutil.AddPhysicalDurationOnTs(base, 2*time.Millisecond), true)
	legacy := message.CreateTestInsertMessage(t, 300, 1, tsoutil.AddPhysicalDurationOnTs(base, 3*time.Millisecond), rmq.NewRmqID(3)).WithTxnContext(txn).IntoImmutableMessage(rmq.NewRmqID(3))
	commitTick := tsoutil.AddPhysicalDurationOnTs(base, 4*time.Millisecond)
	commit := rowReaderMQIdentity(newCommitMessage(t, &txn, commitTick))
	require.Empty(t, buffer.HandleImmutableMessages([]message.ImmutableMessage{begin, insert, upsert, legacy}, tsoutil.AddPhysicalDurationOnTs(base, 3*time.Millisecond)))
	require.Len(t, buffer.GetUncommittedMessageBuilder(), 1)
	committed := buffer.HandleImmutableMessages([]message.ImmutableMessage{commit}, commitTick)
	require.Len(t, committed, 1)
	require.Empty(t, buffer.GetUncommittedMessageBuilder())
	require.Zero(t, buffer.Bytes())
	transaction := message.AsImmutableTxnMessage(committed[0])
	require.Equal(t, 3, transaction.Size())
	require.Equal(t, commitTick, transaction.TimeTick())
	require.NoError(t, transaction.RangeOver(func(body message.ImmutableMessage) error {
		require.Equal(t, commitTick, body.TimeTick())
		require.True(t, body.LastConfirmedMessageID().EQ(commit.LastConfirmedMessageID()))
		return nil
	}))
	pack, err := adaptor.NewMsgPackFromMessage(committed[0])
	require.NoError(t, err)
	require.Equal(t, commitTick, pack.BeginTs)
	require.Equal(t, commitTick, pack.EndTs)
	require.Len(t, pack.Msgs, 6) // two partitions per V2 body, one delete, one legacy Insert
	inserts, deletes := 0, 0
	for _, effect := range pack.Msgs {
		require.Equal(t, commitTick, effect.BeginTs())
		require.Equal(t, commitTick, effect.EndTs())
		require.Equal(t, commitTick, effect.Position().GetTimestamp())
		switch effect.Type() {
		case commonpb.MsgType_Insert:
			inserts++
			row := effect.(*msgstream.InsertMsg)
			for _, tick := range row.Timestamps {
				require.Equal(t, commitTick, tick)
			}
			if row.GetSegmentID() != 300 {
				require.Equal(t, uint64(1), row.GetNumRows())
				switch row.GetPartitionID() {
				case 10:
					require.Equal(t, []int64{1001}, row.RowIDs)
					require.Equal(t, int64(100), row.GetSegmentID())
				case 20:
					require.Equal(t, []int64{1002}, row.RowIDs)
					require.Equal(t, int64(200), row.GetSegmentID())
				default:
					t.Fatalf("unexpected partition %d", row.GetPartitionID())
				}
			}
		case commonpb.MsgType_Delete:
			deletes++
			deletion := effect.(*msgstream.DeleteMsg)
			require.Equal(t, int64(-1), deletion.GetPartitionID())
			require.Equal(t, []int64{101, 102}, deletion.PrimaryKeys.GetIntId().GetData())
			require.Equal(t, []uint64{commitTick, commitTick}, deletion.Timestamps)
		default:
			t.Fatalf("unexpected effect %v", effect.Type())
		}
	}
	require.Equal(t, 5, inserts)
	require.Equal(t, 1, deletes)
}

func TestTxnBufferRowWritesRollbackAndExpiry(t *testing.T) {
	for _, terminal := range []string{"rollback", "expiry"} {
		t.Run(terminal, func(t *testing.T) {
			metrics := metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics()
			t.Cleanup(metrics.Close)
			buffer := NewTxnBuffer(mlog.With(), metrics)
			base := tsoutil.ComposeTSByTime(time.Now())
			txn := message.TxnContext{TxnID: 52, Keepalive: time.Second}
			bodyTick := tsoutil.AddPhysicalDurationOnTs(base, time.Millisecond)
			require.Empty(t, buffer.HandleImmutableMessages([]message.ImmutableMessage{
				newBeginMessage(t, &txn, base), newReaderTransactionBody(t, txn, bodyTick, false), newReaderTransactionBody(t, txn, bodyTick+1, true),
			}, bodyTick+1))
			require.Len(t, buffer.GetUncommittedMessageBuilder(), 1)
			if terminal == "rollback" {
				require.Empty(t, buffer.HandleImmutableMessages([]message.ImmutableMessage{newRollbackMessage(t, &txn, bodyTick+2)}, bodyTick+2))
			} else {
				require.Empty(t, buffer.HandleImmutableMessages(nil, tsoutil.AddPhysicalDurationOnTs(base, 2*time.Second)))
			}
			require.Empty(t, buffer.GetUncommittedMessageBuilder())
			require.Zero(t, buffer.Bytes())
			require.Empty(t, buffer.HandleImmutableMessages([]message.ImmutableMessage{newCommitMessage(t, &txn, tsoutil.AddPhysicalDurationOnTs(base, 3*time.Second))}, tsoutil.AddPhysicalDurationOnTs(base, 3*time.Second)))
		})
	}
}
