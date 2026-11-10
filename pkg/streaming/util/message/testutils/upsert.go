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

// Package testutils builds Upsert WAL fixtures from existing test row data.
// It is not used by production producers or consumers.
package testutils

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func Body(insert *msgpb.InsertRequest, deletion *msgpb.DeleteRequest) *messagespb.UpsertMessageBody {
	body := &messagespb.UpsertMessageBody{Rows: &messagespb.InsertRows{}}
	if insert != nil {
		body.Rows.RowIdsEncoding = &messagespb.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: insert.RowIDs}}
		for _, column := range insert.GetFieldsData() {
			if column.GetFieldId() == 100 {
				switch data := column.GetScalars().GetData().(type) {
				case *schemapb.ScalarField_LongData:
					body.Rows.PrimaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: data.LongData}}
				case *schemapb.ScalarField_StringData:
					body.Rows.PrimaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: data.StringData}}
				}
			} else if column.GetFieldId() > 1 {
				body.Rows.Columns = append(body.Rows.Columns, column)
			}
		}
	}
	if deletion != nil {
		effect := &messagespb.UpsertDeleteEffect{KeySource: &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: deletion.PrimaryKeys}}
		if deletion.PartitionID == -1 {
			effect.Scope = &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}
		} else {
			effect.Scope = &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: deletion.PartitionID}
		}
		body.DeleteEffects = []*messagespb.UpsertDeleteEffect{effect}
	}
	return body
}

// Insert exposes mutable fixture fields for assertions ported from row requests.
func Insert(body *messagespb.UpsertMessageBody) *msgpb.InsertRequest {
	if body == nil {
		return nil
	}
	fields := append([]*schemapb.FieldData(nil), body.Rows.Columns...)
	field := &schemapb.FieldData{FieldId: 100}
	if body.GetRows().GetPrimaryKeys().GetIntId() != nil {
		field.Type = schemapb.DataType_Int64
		field.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: body.Rows.PrimaryKeys.GetIntId()}}}
	} else if body.GetRows().GetPrimaryKeys().GetStrId() != nil {
		field.Type = schemapb.DataType_VarChar
		field.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: body.Rows.PrimaryKeys.GetStrId()}}}
	}
	if field.Field != nil {
		fields = append(fields, field)
	}
	return &msgpb.InsertRequest{NumRows: uint64(len(body.Rows.GetRowIds().Data)), RowIDs: body.Rows.GetRowIds().Data, FieldsData: fields, Version: msgpb.InsertDataVersion_ColumnBased}
}

func Delete(body *messagespb.UpsertMessageBody) *msgpb.DeleteRequest {
	if len(body.GetDeleteEffects()) == 0 {
		return nil
	}
	effect := body.DeleteEffects[0]
	partition := int64(-1)
	if _, ok := effect.Scope.(*messagespb.UpsertDeleteEffect_PartitionId); ok {
		partition = effect.GetPartitionId()
	}
	count := len(effect.GetPrimaryKeys().GetIntId().GetData()) + len(effect.GetPrimaryKeys().GetStrId().GetData())
	return &msgpb.DeleteRequest{PrimaryKeys: effect.GetPrimaryKeys(), NumRows: int64(count), PartitionID: partition}
}
