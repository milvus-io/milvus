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

	"github.com/stretchr/testify/require"

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
		ElementType: schemapb.DataType_Array,
		Data:        []*schemapb.ScalarField{row0, row1},
	}
	require.Equal(t, 8, data.GetRowSize(0))
	require.Equal(t, 4, data.GetRowSize(1))
	require.Equal(t, 14, data.GetMemorySize()) // Payload plus Nullable and ElementNullable flags.

	deepRow := nestedArrayData(
		schemapb.DataType_Array,
		nestedArrayData(
			schemapb.DataType_Int64,
			nestedArrayLongData(1, 2),
			nestedArrayLongData(3),
		),
	)
	deepData := &ArrayFieldData{
		ElementType: schemapb.DataType_Array,
		Data:        []*schemapb.ScalarField{deepRow},
	}
	require.Equal(t, 24, deepData.GetRowSize(0))
	require.Equal(t, 26, deepData.GetMemorySize())

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
		ElementType: schemapb.DataType_Array,
		Data:        []*schemapb.ScalarField{quadrupleRow},
	}
	require.Equal(t, 12, quadrupleData.GetRowSize(0))
	require.Equal(t, 14, quadrupleData.GetMemorySize())
}

func TestStorageRejectsNestedArrayBeyondTwoLevels(t *testing.T) {
	for _, depth := range []int{3, 4} {
		node := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int32}}
		for range depth {
			node = &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: node}}
		}
		field := &schemapb.FieldSchema{FieldID: 100, Name: "nested", DataType: schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array, TypeSchema: node}
		_, err := ArrowTypeForField(field)
		require.ErrorContains(t, err, "exactly two list levels")
	}
}
