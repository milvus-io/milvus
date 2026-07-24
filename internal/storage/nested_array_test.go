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
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func nestedArrayIntData(values ...int32) *schemapb.ScalarField {
	return &schemapb.ScalarField{
		Data: &schemapb.ScalarField_IntData{
			IntData: &schemapb.IntArray{Data: values},
		},
	}
}

func nestedArrayLongData(values ...int64) *schemapb.ScalarField {
	return &schemapb.ScalarField{
		Data: &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{Data: values},
		},
	}
}

func nestedArrayData(elementType schemapb.DataType, elements ...*schemapb.ScalarField) *schemapb.ScalarField {
	return &schemapb.ScalarField{
		Data: &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{
				Data:        elements,
				ElementType: elementType,
			},
		},
	}
}

func TestStorageV2V3NestedArraySize(t *testing.T) {
	row0 := nestedArrayData(
		schemapb.DataType_Int16,
		nestedArrayIntData(1, 2, 3),
		nestedArrayIntData(4),
	)
	row1 := nestedArrayData(
		schemapb.DataType_Int16,
		nestedArrayIntData(),
		nestedArrayIntData(5, 6),
	)

	data := &ArrayFieldData{
		// element_type is intentionally unset for a field described through
		// FieldSchema.element_schema.
		ElementType: schemapb.DataType_None,
		Data:        []*schemapb.ScalarField{row0, row1},
	}
	require.Equal(t, 8, data.GetRowSize(0))
	require.Equal(t, 4, data.GetRowSize(1))
	require.Equal(t, 13, data.GetMemorySize()) // 8 + 4 payload bytes + Nullable flag.

	deepRow := nestedArrayData(
		schemapb.DataType_Array,
		nestedArrayData(
			schemapb.DataType_Int64,
			nestedArrayLongData(1, 2),
			nestedArrayLongData(3),
		),
	)
	deepData := &ArrayFieldData{
		ElementType: schemapb.DataType_None,
		Data:        []*schemapb.ScalarField{deepRow},
	}
	require.Equal(t, 24, deepData.GetRowSize(0))
	require.Equal(t, 25, deepData.GetMemorySize())

	quadrupleRow := nestedArrayData(
		schemapb.DataType_Array,
		nestedArrayData(
			schemapb.DataType_Array,
			nestedArrayData(
				schemapb.DataType_Int32,
				nestedArrayIntData(7, 8),
				nestedArrayIntData(9),
			),
		),
	)
	quadrupleData := &ArrayFieldData{
		ElementType: schemapb.DataType_None,
		Data:        []*schemapb.ScalarField{quadrupleRow},
	}
	require.Equal(t, 12, quadrupleData.GetRowSize(0))
	require.Equal(t, 13, quadrupleData.GetMemorySize())
}

func TestStorageV2V3NestedArraySerdeRoundTrip(t *testing.T) {
	original := nestedArrayData(
		schemapb.DataType_Array,
		nestedArrayData(
			schemapb.DataType_Int32,
			nestedArrayIntData(1, 2),
			nestedArrayIntData(3, 4, 5),
		),
	)

	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "nested",
		DataType: schemapb.DataType_Array,
		ElementSchema: &schemapb.TypeSchema{
			DataType: schemapb.DataType_Array,
			ElementSchema: &schemapb.TypeSchema{
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int32,
			},
		},
	}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	arrowSchema, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()

	require.NoError(t, BuildRecord(builder, &InsertData{Data: map[FieldID]FieldData{
		field.GetFieldID(): &ArrayFieldData{Data: []*schemapb.ScalarField{original}},
	}}, schema))
	record := NewSimpleArrowRecord(builder.NewRecord(), map[FieldID]int{field.GetFieldID(): 0})
	defer record.Release()

	got, err := RecordToInsertData(record, schema, nil)
	require.NoError(t, err)
	require.True(t, proto.Equal(original, got.Data[field.GetFieldID()].(*ArrayFieldData).Data[0]))
}

func TestStorageV2V3QuadrupleNestedArraySerdeRoundTrip(t *testing.T) {
	original := nestedArrayData(
		schemapb.DataType_Array,
		nestedArrayData(
			schemapb.DataType_Array,
			nestedArrayData(
				schemapb.DataType_Int32,
				nestedArrayIntData(1, 2),
				nestedArrayIntData(),
				nestedArrayIntData(3),
			),
		),
	)

	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "nested_4d",
		DataType: schemapb.DataType_Array,
		ElementSchema: &schemapb.TypeSchema{
			DataType: schemapb.DataType_Array,
			ElementSchema: &schemapb.TypeSchema{
				DataType: schemapb.DataType_Array,
				ElementSchema: &schemapb.TypeSchema{
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
				},
			},
		},
	}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	arrowSchema, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()

	require.NoError(t, BuildRecord(builder, &InsertData{Data: map[FieldID]FieldData{
		field.GetFieldID(): &ArrayFieldData{Data: []*schemapb.ScalarField{original}},
	}}, schema))
	record := NewSimpleArrowRecord(builder.NewRecord(), map[FieldID]int{field.GetFieldID(): 0})
	defer record.Release()

	got, err := RecordToInsertData(record, schema, nil)
	require.NoError(t, err)
	roundTripped := got.Data[field.GetFieldID()].(*ArrayFieldData).Data[0]
	require.True(t, proto.Equal(original, roundTripped))
	require.Equal(t, schemapb.DataType_Array, roundTripped.GetArrayData().GetElementType())
	require.Equal(t, schemapb.DataType_Array, roundTripped.GetArrayData().GetData()[0].GetArrayData().GetElementType())
	require.Equal(t, schemapb.DataType_Int32, roundTripped.GetArrayData().GetData()[0].GetArrayData().GetData()[0].GetArrayData().GetElementType())
}
