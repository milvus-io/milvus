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
	require.Equal(t, 8+2*nestedArrayElementOverhead, data.GetRowSize(0))
	require.Equal(t, 4+2*nestedArrayElementOverhead, data.GetRowSize(1))
	require.Equal(t, 14+4*nestedArrayElementOverhead, data.GetMemorySize()) // Payload, inner arrays, Nullable and ElementNullable flags.

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
	require.Equal(t, 24+3*nestedArrayElementOverhead, deepData.GetRowSize(0))
	require.Equal(t, 26+3*nestedArrayElementOverhead, deepData.GetMemorySize())

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
	require.Equal(t, 12+4*nestedArrayElementOverhead, quadrupleData.GetRowSize(0))
	require.Equal(t, 14+4*nestedArrayElementOverhead, quadrupleData.GetMemorySize())
}

// TestNestedArrayNullInnerArraySize pins the cost of null and empty inner
// arrays: they have no leaf payload but still occupy heap, and the inner
// arrays' leaf validity counts like the row's element validity.
func TestNestedArrayNullInnerArraySize(t *testing.T) {
	nullInner := nestedArrayData(schemapb.DataType_Int64, nestedArrayLongData(), nestedArrayLongData(), nestedArrayLongData())
	nullInner.ValidData = []bool{false, false, false}
	data := &ArrayFieldData{ElementType: schemapb.DataType_Array, ElementNullable: true, Data: []*schemapb.ScalarField{nullInner}}
	require.Equal(t, 3*nestedArrayElementOverhead, data.GetRowSize(0))
	require.Equal(t, 3*nestedArrayElementOverhead+3+1+1, data.GetMemorySize()) // + row validity and the two flags

	leaves := nestedArrayLongData(1, 2)
	leaves.ValidData = []bool{true, false}
	withLeafValidity := nestedArrayData(schemapb.DataType_Int64, leaves)
	data = &ArrayFieldData{ElementType: schemapb.DataType_Array, Data: []*schemapb.ScalarField{withLeafValidity}}
	require.Equal(t, nestedArrayElementOverhead+16+2, data.GetRowSize(0))

	// A single-level Array row has no inner arrays and no overhead.
	flat := &ArrayFieldData{ElementType: schemapb.DataType_Int64, Data: []*schemapb.ScalarField{nestedArrayLongData(1, 2)}}
	require.Equal(t, 16, flat.GetRowSize(0))
}

func TestStorageRejectsNestedArrayBeyondTwoLevels(t *testing.T) {
	for _, depth := range []int{3, 4} {
		node := &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int32}}
		for range depth {
			node = &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: node}}
		}
		field := &schemapb.FieldSchema{
			FieldID: 100, Name: "nested", DataType: schemapb.DataType_Array,
			ElementType: schemapb.DataType_Array, TypeSchema: node,
		}
		_, err := ArrowTypeForField(field)
		require.ErrorContains(t, err, "exactly two list levels")
	}
}
