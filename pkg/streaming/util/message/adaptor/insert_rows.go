package adaptor

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// readRowWrite expands one logical WAL record at the 3.0 consumer boundary.
// Every conversion must finish before any effect or progress is published.
func readRowWrite(msg message.ImmutableMessage) ([]msgstream.TsMsg, error) {
	var rows *messagespb.InsertRows
	var partitions []*messagespb.PartitionSegmentAssignment
	var collection int64
	var effects []*messagespb.UpsertDeleteEffect
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		typed, err := message.AsImmutableInsertMessageV2(msg)
		if err != nil {
			return nil, err
		}
		body, err := typed.Body()
		if err != nil {
			return nil, err
		}
		rows = body.GetRows()
		partitions = typed.Header().GetPartitions()
		collection = typed.Header().GetCollectionId()
	case message.MessageTypeUpsert:
		typed, err := message.AsImmutableUpsertMessageV2(msg)
		if err != nil {
			return nil, err
		}
		body, err := typed.Body()
		if err != nil {
			return nil, err
		}
		rows = body.GetRows()
		effects = body.GetDeleteEffects()
		partitions = typed.Header().GetPartitions()
		collection = typed.Header().GetCollectionId()
	default:
		panic("unexpected row-write message")
	}
	count := len(rows.GetRowIds().GetData())
	selections := make([][]int64, len(partitions))
	if len(partitions) == 1 {
		selections[0] = make([]int64, count)
		for i := range selections[0] {
			selections[0][i] = int64(i)
		}
	} else {
		if len(rows.GetPartitionIndexes()) != count {
			return nil, merr.WrapErrServiceInternalMsg("WAL partition index count differs from rows")
		}
		for row, partition := range rows.GetPartitionIndexes() {
			if int(partition) >= len(partitions) {
				return nil, merr.WrapErrServiceInternalMsg("WAL partition index is out of range")
			}
			selections[partition] = append(selections[partition], int64(row))
		}
	}
	// Nullable vectors use compact physical storage; all columns share the logical selection.
	mappings := make([][]int64, len(rows.GetColumns()))
	for index, column := range rows.GetColumns() {
		if column.GetVectors() == nil || column.GetVectors().GetVectorArray() != nil || len(column.GetValidData()) == 0 {
			continue
		}
		if len(column.GetValidData()) != count {
			return nil, merr.WrapErrServiceInternalMsg("WAL vector validity count differs from rows")
		}
		mapping := make([]int64, count)
		physical := int64(0)
		for row, valid := range column.GetValidData() {
			mapping[row] = -1
			if valid {
				mapping[row] = physical
				physical++
			}
		}
		mappings[index] = mapping
	}
	result := make([]msgstream.TsMsg, 0, len(partitions)+len(effects))
	for index, partition := range partitions {
		selection := selections[index]
		if uint64(len(selection)) != partition.GetRows() {
			return nil, merr.WrapErrServiceInternalMsg("WAL partition row count differs from assignment")
		}
		request := &msgpb.InsertRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, Timestamp: msg.TimeTick()}, CollectionID: collection, PartitionID: partition.GetPartitionId(), SegmentID: partition.GetSegmentAssignment().GetSegmentId(), ShardName: msg.VChannel(), NumRows: uint64(len(selection)), Version: msgpb.InsertDataVersion_ColumnBased, RowIDs: make([]int64, len(selection)), Timestamps: make([]uint64, len(selection))}
		for target, row := range selection {
			request.RowIDs[target] = rows.GetRowIds().GetData()[row]
			request.Timestamps[target] = msg.TimeTick()
		}
		request.FieldsData = typeutil.PrepareResultFieldData(rows.GetColumns(), int64(len(selection)))
		for columnIndex, column := range rows.GetColumns() {
			physical := selection
			if mapping := mappings[columnIndex]; mapping != nil {
				physical = make([]int64, 0, len(selection))
				for _, row := range selection {
					if mapping[row] >= 0 {
						physical = append(physical, mapping[row])
					}
				}
			}
			typeutil.AppendFieldDataByColumn(request.FieldsData[columnIndex], column, physical, selection)
			preserveSparseDimensions(request.FieldsData[columnIndex], column)
		}
		keys, err := selectPrimaryKeys(rows.GetPrimaryKeys(), selection)
		if err != nil {
			return nil, err
		}
		insert := &msgstream.InsertMsg{InsertRequest: request, StandalonePrimaryKeys: keys}
		insert.SetTs(msg.TimeTick())
		insert.SetPosition(rowWritePosition(msg))
		result = append(result, insert)
	}
	for _, effect := range effects {
		partition := int64(-1)
		switch effect.GetScope().(type) {
		case *messagespb.UpsertDeleteEffect_AllPartitions:
		case *messagespb.UpsertDeleteEffect_PartitionId:
			partition = effect.GetPartitionId()
		default:
			return nil, merr.WrapErrServiceInternalMsg("WAL delete scope is missing")
		}
		keys := effect.GetPrimaryKeys()
		count := typeutil.GetSizeOfIDs(keys)
		request := &msgpb.DeleteRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete, Timestamp: msg.TimeTick()}, CollectionID: collection, PartitionID: partition, ShardName: msg.VChannel(), PrimaryKeys: keys, NumRows: int64(count), Timestamps: make([]uint64, count)}
		for row := range request.Timestamps {
			request.Timestamps[row] = msg.TimeTick()
		}
		deletion := &msgstream.DeleteMsg{DeleteRequest: request}
		deletion.SetTs(msg.TimeTick())
		deletion.SetPosition(rowWritePosition(msg))
		result = append(result, deletion)
	}
	return result, nil
}

func rowWritePosition(msg message.ImmutableMessage) *msgpb.MsgPosition {
	return &msgpb.MsgPosition{ChannelName: msg.VChannel(), MsgID: MustGetMQWrapperIDFromMessage(msg.LastConfirmedMessageID()).Serialize(), Timestamp: msg.TimeTick(), WALName: commonpb.WALName(msg.WALName())}
}

func selectPrimaryKeys(keys *schemapb.IDs, selection []int64) (*schemapb.IDs, error) {
	switch values := keys.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		result := make([]int64, len(selection))
		for target, row := range selection {
			if row >= int64(len(values.IntId.GetData())) {
				return nil, merr.WrapErrServiceInternalMsg("WAL primary key count differs from rows")
			}
			result[target] = values.IntId.Data[row]
		}
		return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: result}}}, nil
	case *schemapb.IDs_StrId:
		result := make([]string, len(selection))
		for target, row := range selection {
			if row >= int64(len(values.StrId.GetData())) {
				return nil, merr.WrapErrServiceInternalMsg("WAL primary key count differs from rows")
			}
			result[target] = values.StrId.Data[row]
		}
		return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: result}}}, nil
	default:
		return nil, merr.WrapErrServiceInternalMsg("WAL primary key encoding is missing")
	}
}

// Row selection preserves the source dimension bounds, including all-null selections.
func preserveSparseDimensions(dst, src *schemapb.FieldData) {
	if src.GetType() == schemapb.DataType_SparseFloatVector {
		dst.GetVectors().Dim = src.GetVectors().GetDim()
		if dst.GetVectors().GetSparseFloatVector() == nil {
			dst.GetVectors().Data = &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{}}
		}
		dst.GetVectors().GetSparseFloatVector().Dim = src.GetVectors().GetSparseFloatVector().GetDim()
	}
	for index, field := range src.GetStructArrays().GetFields() {
		preserveSparseDimensions(dst.GetStructArrays().GetFields()[index], field)
	}
}
