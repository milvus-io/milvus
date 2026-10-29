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

package storage

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TODO: fill it
// info for each blob
type BlobInfo struct {
	Length int
}

// InsertData example row_schema: {float_field, int_field, float_vector_field, string_field}
// Data {<0, row_id>, <1, timestamp>, <100, float_field>, <101, int_field>, <102, float_vector_field>, <103, string_field>}
//
// system filed id:
// 0: unique row id
// 1: timestamp
// 100: first user field id
// 101: second user field id
// 102: ...
type InsertData struct {
	// TODO, data should be zero copy by passing data directly to event reader or change Data to map[FieldID]FieldDataArray
	Data  map[FieldID]FieldData // field id to field data
	Infos []BlobInfo
}

func NewInsertData(schema *schemapb.CollectionSchema, trackMemory ...bool) (*InsertData, error) {
	return NewInsertDataWithCap(schema, 0, false, trackMemory...)
}

func NewInsertDataWithFunctionOutputField(schema *schemapb.CollectionSchema, trackMemory ...bool) (*InsertData, error) {
	return NewInsertDataWithCap(schema, 0, true, trackMemory...)
}

func NewInsertDataWithCap(schema *schemapb.CollectionSchema, cap int, withFunctionOutput bool, trackMemory ...bool) (*InsertData, error) {
	if schema == nil {
		// A nil schema here is an internal invariant violation (callers within
		// Milvus always pass a schema), not a client-supplied missing parameter.
		// ErrParameterMissing defaults to InputError, so mark this one site as a
		// system error to keep it out of the client-fault (cause="user") bucket.
		return nil, merr.WrapErrAsSysError(merr.WrapErrParameterMissing("collection schema"))
	}

	idata := &InsertData{
		Data: make(map[FieldID]FieldData),
	}

	appendField := func(field *schemapb.FieldSchema) error {
		if field.IsPrimaryKey && field.GetNullable() {
			return merr.WrapErrParameterInvalidMsg("primary key field should not be nullable (field: %s)", field.Name)
		}
		if field.IsPartitionKey && field.GetNullable() {
			return merr.WrapErrParameterInvalidMsg("partition key field should not be nullable (field: %s)", field.Name)
		}
		if field.IsFunctionOutput {
			if field.IsPrimaryKey || field.IsPartitionKey {
				return merr.WrapErrParameterInvalidMsg("function output field should not be primary key or partition key (field: %s)", field.Name)
			}
			if field.GetNullable() {
				return merr.WrapErrParameterInvalidMsg("function output field should not be nullable (field: %s)", field.Name)
			}
			if !withFunctionOutput {
				return nil
			}
		}
		fieldData, err := NewFieldData(field.DataType, field, cap, trackMemory...)
		if err != nil {
			return err
		}
		idata.Data[field.FieldID] = fieldData
		return nil
	}

	for _, field := range schema.Fields {
		if err := appendField(field); err != nil {
			return nil, err
		}
	}
	for _, structField := range schema.StructArrayFields {
		for _, field := range structField.GetFields() {
			if err := appendField(field); err != nil {
				return nil, err
			}
		}
	}

	return idata, nil
}

func (iData *InsertData) IsEmpty() bool {
	if iData == nil {
		return true
	}

	timeFieldData, ok := iData.Data[common.TimeStampField]
	return (!ok) || (timeFieldData.RowNum() <= 0)
}

func (i *InsertData) GetRowNum() int {
	if i == nil || i.Data == nil || len(i.Data) == 0 {
		return 0
	}
	var rowNum int
	for _, data := range i.Data {
		rowNum = data.RowNum()
		if rowNum > 0 {
			break
		}
	}
	return rowNum
}

func (i *InsertData) GetMemorySize() int {
	var size int
	if i.Data == nil || len(i.Data) == 0 {
		return size
	}

	for _, data := range i.Data {
		size += data.GetMemorySize()
	}

	return size
}

func (i *InsertData) Append(row map[FieldID]interface{}) error {
	for fID, v := range row {
		field, ok := i.Data[fID]
		if !ok {
			return merr.WrapErrServiceInternalMsg("missing field when appending row, got %d", fID)
		}

		if err := field.AppendRow(v); err != nil {
			return merr.WrapErrParameterInvalidMsg("append data for field %d failed, err=%s", fID, err.Error())
		}
	}

	return nil
}

func (i *InsertData) GetRow(idx int) map[FieldID]interface{} {
	res := make(map[FieldID]interface{})
	for field, data := range i.Data {
		res[field] = data.GetRow(idx)
	}
	return res
}

func (i *InsertData) GetRowSize(idx int) int {
	size := 0
	for _, data := range i.Data {
		size += data.GetRowSize(idx)
	}
	return size
}
