// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package messageutil

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// InsertRowsReader reads selected logical rows at the storage execution boundary.
// Source columns remain immutable. All columns use the same row selection.
type InsertRowsReader interface {
	ReadRows(context.Context, *schemapb.CollectionSchema, []int64) (*msgpb.InsertRequest, error)
}
type insertRowsReader struct{ msg message.ImmutableMessage }

func (r *insertRowsReader) ReadRows(ctx context.Context, schema *schemapb.CollectionSchema, rows []int64) (*msgpb.InsertRequest, error) {
	var source *msgpb.InsertRequest
	switch r.msg.MessageType() {
	case message.MessageTypeInsert:
		body, err := message.MustAsImmutableInsertMessageV1(r.msg).Body(ctx)
		if err != nil {
			return nil, err
		}
		if rows == nil {
			// Historical Insert messages already have one receiving partition.
			// Own the execution metadata and borrow immutable column payloads.
			result := &msgpb.InsertRequest{ShardName: r.msg.VChannel(), DbName: body.GetDbName(), CollectionName: body.GetCollectionName(), PartitionName: body.GetPartitionName(), DbID: body.GetDbID(), CollectionID: body.GetCollectionID(), PartitionID: body.GetPartitionID(), SegmentID: body.GetSegmentID(), RowIDs: body.GetRowIDs(), RowData: body.GetRowData(), NumRows: body.GetNumRows(), Version: body.GetVersion(), FieldsData: typeutil.CopyFieldDataMetadata(body.GetFieldsData()), Namespace: body.Namespace}
			if body.Base != nil {
				result.Base = proto.Clone(body.Base).(*commonpb.MsgBase)
			}
			result.Timestamps = make([]uint64, len(body.GetTimestamps()))
			for i := range result.Timestamps {
				result.Timestamps[i] = r.msg.TimeTick()
			}
			return result, nil
		}
		source = body
	case message.MessageTypeUpsert:
		upsert := message.MustAsImmutableUpsertMessageV2(r.msg)
		body, err := upsert.Body(ctx)
		if err != nil {
			return nil, err
		}
		pk, err := typeutil.GetPrimaryFieldSchema(schema)
		if err != nil {
			return nil, err
		}
		fields := append([]*schemapb.FieldData(nil), body.GetColumns()...)
		field := message.UpsertPrimaryKeyColumn(pk, body.GetPrimaryKeys())
		fields = append(fields, field)
		source = &msgpb.InsertRequest{CollectionID: upsert.Header().GetCollectionId(), NumRows: uint64(len(body.GetRowIds())), RowIDs: body.GetRowIds(), FieldsData: fields, Version: msgpb.InsertDataVersion_ColumnBased}
	default:
		panic("unexpected insert rows message type")
	}
	if rows == nil {
		rows = make([]int64, source.GetNumRows())
		for i := range rows {
			rows[i] = int64(i)
		}
	}
	result := &msgpb.InsertRequest{CollectionID: source.GetCollectionID(), NumRows: uint64(len(rows)), Version: msgpb.InsertDataVersion_ColumnBased, RowIDs: make([]int64, len(rows)), Timestamps: make([]uint64, len(rows))}
	for i, row := range rows {
		if row < 0 || uint64(row) >= source.GetNumRows() || row >= int64(len(source.GetRowIDs())) {
			return nil, merr.WrapErrServiceInternalMsg("insert selection is out of range")
		}
		result.RowIDs[i] = source.RowIDs[row]
		result.Timestamps[i] = r.msg.TimeTick()
	}
	result.FieldsData = typeutil.PrepareResultFieldData(source.GetFieldsData(), int64(len(rows)))
	for index, field := range source.GetFieldsData() {
		physical := rows
		valid := typeutil.GetFieldDataValidData(field)
		if field.GetVectors() != nil && field.GetVectors().GetVectorArray() == nil && len(valid) > 0 {
			var mapping []int64
			switch r.msg.MessageType() {
			case message.MessageTypeUpsert:
				var err error
				mapping, err = message.ReadUpsertCompactRows(ctx, r.msg, field.GetFieldId(), valid)
				if err != nil {
					return nil, err
				}
			case message.MessageTypeInsert:
				mapping = make([]int64, len(valid))
				cursor := int64(0)
				for logical, exists := range valid {
					mapping[logical] = -1
					if exists {
						mapping[logical] = cursor
						cursor++
					}
				}
			}
			physical = make([]int64, 0, len(rows))
			for _, row := range rows {
				if row >= int64(len(mapping)) {
					return nil, merr.WrapErrServiceInternalMsg("nullable vector validity count differs from logical rows")
				}
				if mapping[row] >= 0 {
					physical = append(physical, mapping[row])
				}
			}
		}
		typeutil.AppendFieldDataByColumn(result.FieldsData[index], field, physical, rows)
	}
	return result, nil
}

// AsInsertRowsReader returns rows and receiving-cluster segment assignments.
func AsInsertRowsReader(msg message.ImmutableMessage) (InsertRowsReader, []*messagespb.PartitionSegmentAssignment, bool) {
	if msg == nil {
		return nil, nil, false
	}
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		return &insertRowsReader{msg: msg}, message.MustAsImmutableInsertMessageV1(msg).Header().GetPartitions(), true
	case message.MessageTypeUpsert:
		return &insertRowsReader{msg: msg}, message.MustAsImmutableUpsertMessageV2(msg).Header().GetPartitions(), true
	}
	return nil, nil, false
}

// InsertSelections computes one logical selection for every partition.
func InsertSelections(ctx context.Context, msg message.ImmutableMessage) ([][]int64, error) {
	if msg.MessageType() != message.MessageTypeUpsert {
		return nil, nil
	}
	return message.ReadUpsertRowSelections(ctx, msg)
}

// DeleteRequests exposes all explicit effects with the final visibility timestamp.
func DeleteRequests(ctx context.Context, msg message.ImmutableMessage) ([]*msgpb.DeleteRequest, error) {
	if msg == nil {
		return nil, nil
	}
	if msg.MessageType() == message.MessageTypeDelete {
		typed, err := message.AsImmutableDeleteMessageV1(msg)
		if err != nil {
			return nil, err
		}
		body, err := typed.Body(ctx)
		if err != nil {
			return nil, err
		}
		copy := proto.Clone(body).(*msgpb.DeleteRequest)
		if copy.GetNumRows() < 0 {
			return nil, merr.WrapErrServiceInternalMsg("WAL delete row count is negative")
		}
		count := PrimaryKeyCount(copy.PrimaryKeys)
		if copy.PrimaryKeys == nil {
			count = len(copy.Int64PrimaryKeys)
		}
		if len(copy.SerializedExprPlan) > 0 {
			count = 1
		}
		if copy.Base == nil {
			copy.Base = &commonpb.MsgBase{}
		}
		copy.Base.Timestamp = msg.TimeTick()
		copy.ShardName = msg.VChannel()
		copy.Timestamps = make([]uint64, count)
		for i := range copy.Timestamps {
			copy.Timestamps[i] = msg.TimeTick()
		}
		return []*msgpb.DeleteRequest{copy}, nil
	}
	if msg.MessageType() != message.MessageTypeUpsert {
		return nil, nil
	}
	body, err := message.ReadUpsertProjection(ctx, msg)
	if err != nil {
		return nil, err
	}
	result := make([]*msgpb.DeleteRequest, 0, len(body.GetDeleteEffects()))
	for _, effect := range body.GetDeleteEffects() {
		partition := int64(-1)
		switch scope := effect.GetScope().(type) {
		case *messagespb.UpsertDeleteEffect_AllPartitions:
			if scope.AllPartitions == nil {
				return nil, merr.WrapErrServiceInternalMsg("missing Upsert delete scope")
			}
		case *messagespb.UpsertDeleteEffect_PartitionId:
			partition = scope.PartitionId
			if partition <= 0 {
				return nil, merr.WrapErrServiceInternalMsg("invalid Upsert delete partition")
			}
		default:
			return nil, merr.WrapErrServiceInternalMsg("missing Upsert delete scope")
		}
		keys := effect.GetPrimaryKeys()
		count := PrimaryKeyCount(keys)
		if count == 0 {
			return nil, merr.WrapErrServiceInternalMsg("empty Upsert delete keys")
		}
		times := make([]uint64, count)
		for i := range times {
			times[i] = msg.TimeTick()
		}
		result = append(result, &msgpb.DeleteRequest{CollectionID: message.MustAsImmutableUpsertMessageV2(msg).Header().GetCollectionId(), PartitionID: partition, PrimaryKeys: keys, NumRows: int64(count), Timestamps: times, ShardName: msg.VChannel()})
	}
	var deleted uint64
	for _, request := range result {
		deleted += uint64(request.GetNumRows())
	}
	if deleted != message.MustAsImmutableUpsertMessageV2(msg).Header().GetDeleteRows() {
		return nil, merr.WrapErrServiceInternalMsg("Upsert delete row count differs from explicit effects")
	}
	return result, nil
}

// AsDeleteRequest preserves the historical independent Delete projection.
func AsDeleteRequest(msg message.ImmutableMessage) (*msgpb.DeleteRequest, bool) {
	if msg == nil || msg.MessageType() != message.MessageTypeDelete {
		return nil, false
	}
	requests, err := DeleteRequests(context.Background(), msg)
	if err != nil {
		panic(err)
	}
	return requests[0], true
}
