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

package typeutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestAppendFieldDataAfterLegacyAllNullVector(t *testing.T) {
	for _, tc := range []struct {
		dataType schemapb.DataType
		vector   *schemapb.VectorField
	}{
		{schemapb.DataType_FloatVector, &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{7, 8}}}}},
		{schemapb.DataType_BinaryVector, &schemapb.VectorField{Dim: 16, Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{7, 8}}}},
		{schemapb.DataType_Float16Vector, &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{0, 7, 0, 8}}}},
		{schemapb.DataType_BFloat16Vector, &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{7, 0, 8, 0}}}},
		{schemapb.DataType_Int8Vector, &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{7, 8}}}},
	} {
		t.Run(tc.dataType.String(), func(t *testing.T) {
			for _, legacy := range []struct {
				name               string
				missingVectorField bool
			}{
				{name: "empty vectors wrapper"},
				{name: "omitted 3.0 vectors wrapper", missingVectorField: true},
			} {
				t.Run(legacy.name, func(t *testing.T) {
					missingVectorField := legacy.missingVectorField
					null := []*schemapb.FieldData{{
						FieldId: 100, Type: tc.dataType, ValidData: []bool{false},
						Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{}},
					}}
					if missingVectorField {
						// 3.0 omits the entire wrapper; older master emitted an empty one.
						null[0].Field = nil
					}
					src := []*schemapb.FieldData{{
						FieldId: 100, Type: tc.dataType,
						Field: &schemapb.FieldData_Vectors{Vectors: tc.vector},
					}}
					src[0].ValidData = []bool{true}
					for _, dst := range [][]*schemapb.FieldData{PrepareResultFieldData(null, 3), make([]*schemapb.FieldData, 1)} {
						AppendFieldData(dst, null, 0, -1)
						AppendFieldData(dst, src, 0, 0)
						AppendFieldData(dst, null, 0, -1)
						require.Equal(t, tc.vector.GetDim(), dst[0].GetVectors().GetDim())
						require.Equal(t, []bool{false, true, false}, dst[0].GetValidData())

						// A second row copy, as in hybrid assembly, must retain the
						// payload as well as the validity of the selected vector.
						assembled := make([]*schemapb.FieldData, 1)
						idxs := NewFieldDataIdxComputer(dst).Compute(1)
						AppendFieldData(assembled, dst, 1, idxs...)
						require.True(t, proto.Equal(src[0], assembled[0]), "expected %v, got %v", src[0], assembled[0])
					}
				})
			}
		})
	}
}
