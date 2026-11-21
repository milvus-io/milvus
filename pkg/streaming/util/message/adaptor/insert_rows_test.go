package adaptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func rowWriteFixture(t *testing.T, upsert bool) message.ImmutableMessage {
	t.Helper()
	rows := &messagespb.InsertRows{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{101, 102, 103}}}}, RowIdsEncoding: &messagespb.InsertRows_DeltaRowIds{DeltaRowIds: &messagespb.DeltaEncodedRowIDs{Base: 1000, Deltas: []int64{2, -1}}}, PartitionIndexes: []uint32{0, 1, 0}, Columns: []*schemapb.FieldData{
		{FieldId: 101, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{4, 5, 6}}}}}},
		{FieldId: 102, Type: schemapb.DataType_SparseFloatVector, ValidData: []bool{true, false, true}, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 8, Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 8, Contents: [][]byte{typeutil.CreateSparseFloatRow([]uint32{7}, []float32{1}), typeutil.CreateSparseFloatRow([]uint32{2}, []float32{2})}}}}}},
	}}
	assignments := []*messagespb.PartitionSegmentAssignment{{PartitionId: 10, Rows: 2, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 100}}, {PartitionId: 20, Rows: 1, SegmentAssignment: &messagespb.SegmentAssignment{SegmentId: 200}}}
	var mutable message.MutableMessage
	var err error
	if upsert {
		mutable, err = message.NewUpsertMessageBuilderV2().WithHeader(&messagespb.UpsertMessageHeader{CollectionId: 1, Partitions: assignments, DeleteRows: 3}).WithBody(&messagespb.UpsertMessageBody{Rows: rows, DeleteEffects: []*messagespb.UpsertDeleteEffect{{Scope: &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}, KeySource: &messagespb.UpsertDeleteEffect_AllInsertedKeys{AllInsertedKeys: &messagespb.UpsertAllInsertedKeys{}}}}}).WithVChannel("p_1v0").BuildMutable()
	} else {
		mutable, err = message.NewInsertMessageBuilderV2().WithHeader(&messagespb.InsertMessageHeaderV2{CollectionId: 1, Partitions: assignments}).WithBody(&messagespb.InsertMessageBody{Rows: rows}).WithVChannel("p_1v0").BuildMutable()
	}
	require.NoError(t, err)
	return mutable.WithTimeTick(20).WithLastConfirmed(rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(2))
}

func TestRowWriteConsumerSelections(t *testing.T) {
	for _, upsert := range []bool{false, true} {
		t.Run(map[bool]string{false: "InsertV2", true: "Upsert"}[upsert], func(t *testing.T) {
			msg := rowWriteFixture(t, upsert)
			before := proto.Clone(msg.IntoImmutableMessageProto())
			pack, err := NewMsgPackFromMessage(msg)
			require.NoError(t, err)
			expected := 2
			if upsert {
				expected++
			}
			require.Len(t, pack.Msgs, expected)
			first := pack.Msgs[0].(*msgstream.InsertMsg)
			second := pack.Msgs[1].(*msgstream.InsertMsg)
			require.Equal(t, []int64{1000, 1001}, first.RowIDs)
			require.Equal(t, []int64{1002}, second.RowIDs)
			require.Equal(t, []int64{101, 103}, first.StandalonePrimaryKeys.GetIntId().Data)
			require.Equal(t, []int64{4, 6}, first.FieldsData[0].GetScalars().GetLongData().Data)
			for index, insert := range []*msgstream.InsertMsg{first, second} {
				sparse := insert.FieldsData[1]
				require.Equal(t, int64(8), sparse.GetVectors().GetDim())
				require.Equal(t, int64(8), sparse.GetVectors().GetSparseFloatVector().GetDim())
				if index == 0 {
					require.Equal(t, []bool{true, true}, sparse.ValidData)
					require.Len(t, sparse.GetVectors().GetSparseFloatVector().GetContents(), 2)
				} else {
					require.Equal(t, []bool{false}, sparse.ValidData)
					require.Empty(t, sparse.GetVectors().GetSparseFloatVector().GetContents())
				}
				require.NoError(t, insert.MaterializePrimaryKey(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}}))
				require.Equal(t, int64(100), insert.FieldsData[2].GetFieldId())
				require.NoError(t, insert.CheckAligned())
				for _, tick := range insert.Timestamps {
					require.Equal(t, uint64(20), tick)
				}
			}
			if upsert {
				deletion := pack.Msgs[2].(*msgstream.DeleteMsg)
				require.Equal(t, int64(-1), deletion.PartitionID)
				require.Equal(t, []int64{101, 102, 103}, deletion.PrimaryKeys.GetIntId().Data)
				require.Equal(t, []uint64{20, 20, 20}, deletion.Timestamps)
			}
			require.True(t, proto.Equal(before, msg.IntoImmutableMessageProto()))
		})
	}
}

func TestRowWriteLazyExpansionAndConversionFailure(t *testing.T) {
	msg := rowWriteFixture(t, true)
	pack, err := newConsumeMsgPackFromMessage(msg)
	require.NoError(t, err)
	require.Len(t, pack.Msgs, 1)
	compound := pack.Msgs[0].(interface {
		TryUnmarshalMessages() ([]msgstream.TsMsg, error)
	})
	expanded, err := compound.TryUnmarshalMessages()
	require.NoError(t, err)
	require.Len(t, expanded, 3)
	again, err := compound.TryUnmarshalMessages()
	require.NoError(t, err)
	require.Same(t, expanded[0], again[0])
	wire := msg.IntoImmutableMessageProto()
	wire.Payload = []byte{0xff}
	broken := message.NewImmutableMesasge(rmq.NewRmqID(3), wire.Payload, wire.Properties)
	converted, err := NewMsgPackFromMessage(broken)
	require.Error(t, err)
	require.Nil(t, converted)
	handler := NewBaseMsgPackAdaptorHandler()
	require.Error(t, handler.GenerateMsgPack(broken))
	require.Zero(t, handler.PendingMsgPack.Len())
	lazy := NewMsgPackAdaptorHandler()
	handled := lazy.Handle(message.HandleParam{Ctx: context.Background(), Message: nil})
	require.NoError(t, handled.Error)
}

func TestRowWriteConfirmationBarrierAdaptsToTimeTick(t *testing.T) {
	for _, mutable := range []message.MutableMessage{
		message.NewMVCCBarrierMessageBuilderV2().WithHeader(&message.MVCCBarrierMessageHeader{}).WithBody(&message.MVCCBarrierMessageBody{}).WithVChannel("").MustBuildMutable(),
		message.NewRecoveryBarrierMessageBuilderV2().WithHeader(&message.RecoveryBarrierMessageHeader{}).WithBody(&message.RecoveryBarrierMessageBody{}).WithVChannel("").MustBuildMutable(),
	} {
		msg := mutable.WithTimeTick(30).WithLastConfirmed(rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(3))
		pack, err := NewMsgPackFromMessage(msg)
		require.NoError(t, err)
		require.Empty(t, pack.Msgs)
		require.Equal(t, uint64(30), pack.BeginTs)
		require.Equal(t, uint64(30), pack.EndTs)
	}
}

func TestRowWriteIndependentDeleteKeysAndPartitionScope(t *testing.T) {
	original := rowWriteFixture(t, true)
	wire := original.IntoImmutableMessageProto()
	body := &messagespb.UpsertMessageBody{}
	require.NoError(t, proto.Unmarshal(wire.Payload, body))
	body.Rows.PrimaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b", "c"}}}}
	body.DeleteEffects = []*messagespb.UpsertDeleteEffect{{Scope: &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: 30}, KeySource: &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"old", "old", "别的"}}}}}}}
	payload, err := proto.Marshal(body)
	require.NoError(t, err)
	pack, err := NewMsgPackFromMessage(message.NewImmutableMesasge(rmq.NewRmqID(4), payload, wire.Properties))
	require.NoError(t, err)
	deletion := pack.Msgs[2].(*msgstream.DeleteMsg)
	require.Equal(t, int64(30), deletion.PartitionID)
	require.Equal(t, []string{"old", "old", "别的"}, deletion.PrimaryKeys.GetStrId().Data)
	require.Equal(t, int64(3), deletion.NumRows)
}

func TestRowWriteNestedAndNullableDenseSelections(t *testing.T) {
	original := rowWriteFixture(t, false)
	wire := original.IntoImmutableMessageProto()
	body := &messagespb.InsertMessageBody{}
	require.NoError(t, proto.Unmarshal(wire.Payload, body))
	arrayValue := func(value int32) *schemapb.ScalarField {
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{value, value + 1}}}}
	}
	vectorValue := func(value float32) *schemapb.VectorField {
		return &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{value, value + 1}}}}
	}
	body.Rows.Columns = []*schemapb.FieldData{
		{FieldId: 101, Type: schemapb.DataType_FloatVector, ValidData: []bool{true, false, true}, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 5, 6}}}}}},
		{FieldId: 102, Type: schemapb.DataType_ArrayOfVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{Dim: 2, ElementType: schemapb.DataType_FloatVector, Data: []*schemapb.VectorField{vectorValue(10), vectorValue(20), vectorValue(30)}}}}}},
		{FieldId: 103, Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{{FieldId: 104, Type: schemapb.DataType_Array, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int32, Data: []*schemapb.ScalarField{arrayValue(10), arrayValue(20), arrayValue(30)}}}}}}}}}},
	}
	before := proto.Clone(body)
	payload, err := proto.Marshal(body)
	require.NoError(t, err)
	pack, err := NewMsgPackFromMessage(message.NewImmutableMesasge(rmq.NewRmqID(4), payload, wire.Properties))
	require.NoError(t, err)
	first := pack.Msgs[0].(*msgstream.InsertMsg)
	second := pack.Msgs[1].(*msgstream.InsertMsg)
	require.Equal(t, []float32{1, 2, 5, 6}, first.FieldsData[0].GetVectors().GetFloatVector().Data)
	require.Equal(t, []bool{true, true}, first.FieldsData[0].ValidData)
	require.Empty(t, second.FieldsData[0].GetVectors().GetFloatVector().Data)
	require.Equal(t, []bool{false}, second.FieldsData[0].ValidData)
	require.Equal(t, []*schemapb.VectorField{vectorValue(10), vectorValue(30)}, first.FieldsData[1].GetVectors().GetVectorArray().Data)
	require.Equal(t, []*schemapb.ScalarField{arrayValue(10), arrayValue(30)}, first.FieldsData[2].GetStructArrays().Fields[0].GetScalars().GetArrayData().Data)
	require.True(t, proto.Equal(before, body))
}
