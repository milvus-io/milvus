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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func scalarInt64(fieldID int64, name string, data []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_Int64, FieldId: fieldID, FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
		}},
	}
}

func floatVec(fieldID int64, name string, dim int64, data []float32) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector, FieldId: fieldID, FieldName: name,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: data}},
		}},
	}
}

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
			null := []*schemapb.FieldData{{
				FieldId: 100, Type: tc.dataType,
				Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{ValidData: []bool{false}}},
			}}
			src := []*schemapb.FieldData{{
				FieldId: 100, Type: tc.dataType,
				Field: &schemapb.FieldData_Vectors{Vectors: tc.vector},
			}}
			SetFieldDataValidData(src[0], []bool{true})
			for _, dst := range [][]*schemapb.FieldData{PrepareResultFieldData(null, 3), make([]*schemapb.FieldData, 1)} {
				AppendFieldData(dst, null, 0, -1)
				AppendFieldData(dst, src, 0, 0)
				AppendFieldData(dst, null, 0, -1)
				require.Equal(t, tc.vector.GetDim(), dst[0].GetVectors().GetDim())
				require.Equal(t, []bool{false, true, false}, GetFieldDataValidData(dst[0]))

				// A second row copy, as in hybrid assembly, must retain the
				// payload as well as the validity of the selected vector.
				assembled := make([]*schemapb.FieldData, 1)
				idxs := NewFieldDataIdxComputer(dst).Compute(1)
				AppendFieldData(assembled, dst, 1, idxs...)
				require.True(t, proto.Equal(src[0], assembled[0]), "expected %v, got %v", src[0], assembled[0])
			}
		})
	}
}

// A dst shorter than src violates the caller contract. The pre-existing code
// wrote dst[i] unguarded and panicked; keep it a hard failure so a broken
// caller cannot silently drop a column.
func TestAppendFieldDataPanicsOnShortDst(t *testing.T) {
	src := []*schemapb.FieldData{
		scalarInt64(100, "pk", []int64{10}),
		scalarInt64(101, "age", []int64{20}),
	}
	dst := make([]*schemapb.FieldData, 1) // one column short

	assert.PanicsWithValue(t,
		"AppendFieldData: dst has 1 columns, src has 2; callers must size dst to at least len(src)",
		func() { AppendFieldData(dst, src, 0) })
}

// AppendFieldData now resolves the destination column by index and only falls
// back to a FieldId map when dst and src are not parallel. This checks the two
// paths agree, including the cases that force the fallback.
func TestAppendFieldDataResolvesSameColumnAsFieldIdLookup(t *testing.T) {
	const rows = 4
	newSrc := func() []*schemapb.FieldData {
		return []*schemapb.FieldData{
			scalarInt64(100, "pk", []int64{10, 11, 12, 13}),
			scalarInt64(101, "age", []int64{20, 21, 22, 23}),
			floatVec(102, "vec", 2, []float32{0, 1, 2, 3, 4, 5, 6, 7}),
		}
	}

	t.Run("parallel dst (fast path)", func(t *testing.T) {
		src := newSrc()
		dst := PrepareResultFieldData(src, rows)
		for r := int64(0); r < rows; r++ {
			AppendFieldData(dst, src, r)
		}
		assert.Equal(t, []int64{10, 11, 12, 13}, dst[0].GetScalars().GetLongData().GetData())
		assert.Equal(t, []int64{20, 21, 22, 23}, dst[1].GetScalars().GetLongData().GetData())
		assert.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7}, dst[2].GetVectors().GetFloatVector().GetData())
	})

	t.Run("nil dst entries (lazy creation, as insert repack used to do)", func(t *testing.T) {
		src := newSrc()
		dst := make([]*schemapb.FieldData, len(src))
		for r := int64(0); r < rows; r++ {
			AppendFieldData(dst, src, r)
		}
		for i := range dst {
			assert.NotNil(t, dst[i])
			assert.Equal(t, src[i].FieldId, dst[i].FieldId)
		}
		assert.Equal(t, []int64{10, 11, 12, 13}, dst[0].GetScalars().GetLongData().GetData())
		assert.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7}, dst[2].GetVectors().GetFloatVector().GetData())
	})

	t.Run("dst permuted relative to src (forces the map fallback)", func(t *testing.T) {
		src := newSrc()
		prepared := PrepareResultFieldData(src, rows)
		// Reverse dst so index i no longer corresponds to src[i].
		dst := []*schemapb.FieldData{prepared[2], prepared[1], prepared[0]}
		for r := int64(0); r < rows; r++ {
			AppendFieldData(dst, src, r)
		}
		// Data must still land in the column with the matching FieldId.
		byID := map[int64]*schemapb.FieldData{}
		for _, fd := range dst {
			byID[fd.FieldId] = fd
		}
		assert.Equal(t, []int64{10, 11, 12, 13}, byID[100].GetScalars().GetLongData().GetData())
		assert.Equal(t, []int64{20, 21, 22, 23}, byID[101].GetScalars().GetLongData().GetData())
		assert.Equal(t, []float32{0, 1, 2, 3, 4, 5, 6, 7}, byID[102].GetVectors().GetFloatVector().GetData())
	})
}

func benchAppend(b *testing.B, src []*schemapb.FieldData, rows int64) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst := PrepareResultFieldData(src, rows)
		for r := int64(0); r < rows; r++ {
			AppendFieldData(dst, src, r)
		}
	}
}

func BenchmarkAppendFieldDataPerRow(b *testing.B) {
	const rows = 10000
	ids := make([]int64, rows)

	// Scalar-only output: the per-row field lookup dominates.
	b.Run("4_scalars", func(b *testing.B) {
		benchAppend(b, []*schemapb.FieldData{
			scalarInt64(100, "pk", ids), scalarInt64(101, "a", ids),
			scalarInt64(102, "b", ids), scalarInt64(103, "c", ids),
		}, rows)
	})

	// Vector output: data movement dominates, so the lookup is a smaller share.
	b.Run("3_scalars_1_vector_dim768", func(b *testing.B) {
		const dim = 768
		benchAppend(b, []*schemapb.FieldData{
			scalarInt64(100, "pk", ids), scalarInt64(101, "a", ids),
			scalarInt64(102, "b", ids),
			floatVec(103, "vec", dim, make([]float32, rows*dim)),
		}, rows)
	})
}
