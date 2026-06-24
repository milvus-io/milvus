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

package common

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	pkgcommon "github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func vectorArrayField(elementType schemapb.DataType, dim int) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:     100,
		Name:        "items[vec]",
		DataType:    schemapb.DataType_ArrayOfVector,
		ElementType: elementType,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: pkgcommon.DimKey, Value: strconv.Itoa(dim)},
			{Key: pkgcommon.MaxCapacityKey, Value: "8"},
		},
	}
}

func numbers(values ...string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, json.Number(value))
	}
	return out
}

func TestArrayOfVectorToFieldData_FixedDimElementTypes(t *testing.T) {
	float16Expected := append(typeutil.Float32ToFloat16Bytes(0.25), typeutil.Float32ToFloat16Bytes(0.5)...)
	bfloat16Expected := append(typeutil.Float32ToBFloat16Bytes(0.25), typeutil.Float32ToBFloat16Bytes(0.5)...)

	tests := []struct {
		name        string
		elementType schemapb.DataType
		dim         int
		vectors     []any
		verify      func(*testing.T, *schemapb.VectorField)
	}{
		{
			name:        "float",
			elementType: schemapb.DataType_FloatVector,
			dim:         2,
			vectors: []any{
				numbers("0.25", "0.5"),
				numbers("0.75", "1.0"),
			},
			verify: func(t *testing.T, field *schemapb.VectorField) {
				require.Equal(t, []float32{0.25, 0.5, 0.75, 1.0}, field.GetFloatVector().GetData())
			},
		},
		{
			name:        "binary",
			elementType: schemapb.DataType_BinaryVector,
			dim:         16,
			vectors: []any{
				numbers("3", "11"),
				numbers("5", "7"),
			},
			verify: func(t *testing.T, field *schemapb.VectorField) {
				require.Equal(t, []byte{3, 11, 5, 7}, field.GetBinaryVector())
			},
		},
		{
			name:        "float16",
			elementType: schemapb.DataType_Float16Vector,
			dim:         2,
			vectors: []any{
				numbers("0.25", "0.5"),
			},
			verify: func(t *testing.T, field *schemapb.VectorField) {
				require.Equal(t, float16Expected, field.GetFloat16Vector())
			},
		},
		{
			name:        "bfloat16",
			elementType: schemapb.DataType_BFloat16Vector,
			dim:         2,
			vectors: []any{
				numbers("0.25", "0.5"),
			},
			verify: func(t *testing.T, field *schemapb.VectorField) {
				require.Equal(t, bfloat16Expected, field.GetBfloat16Vector())
			},
		},
		{
			name:        "int8",
			elementType: schemapb.DataType_Int8Vector,
			dim:         2,
			vectors: []any{
				numbers("1", "-2"),
				numbers("3", "-4"),
			},
			verify: func(t *testing.T, field *schemapb.VectorField) {
				require.Equal(t, typeutil.Int8ArrayToBytes([]int8{1, -2, 3, -4}), field.GetInt8Vector())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field, err := ArrayOfVectorToFieldData(tt.vectors, vectorArrayField(tt.elementType, tt.dim), tt.dim)
			require.NoError(t, err)
			require.Equal(t, int64(tt.dim), field.GetDim())
			tt.verify(t, field)
		})
	}
}

func TestArrayOfVectorToFieldData_RejectsUnsupportedOrInvalidInput(t *testing.T) {
	_, err := ArrayOfVectorToFieldData(
		[]any{numbers("1", "2")},
		vectorArrayField(schemapb.DataType_SparseFloatVector, 2),
		2,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "SparseFloatVector")

	_, err = ArrayOfVectorToFieldData(
		[]any{numbers("1", "2", "3")},
		vectorArrayField(schemapb.DataType_Int8Vector, 2),
		2,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dimension")

	_, err = ArrayOfVectorToFieldData(
		[]any{numbers("1", "2", "3", "4")},
		vectorArrayField(schemapb.DataType_Float16Vector, 2),
		2,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dimension")

	_, err = ArrayOfVectorToFieldData(
		[]any{[]any{float64(1), float64(2)}},
		vectorArrayField(schemapb.DataType_FloatVector, 2),
		2,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected numeric vector element")

	_, err = ArrayOfVectorToFieldData(
		[]any{numbers("1", "256")},
		vectorArrayField(schemapb.DataType_BinaryVector, 16),
		16,
	)
	require.Error(t, err)
}
