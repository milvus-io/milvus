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

package column

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

type VectorArraySuite struct {
	suite.Suite
}

func (s *VectorArraySuite) TestFloatVectorArrayBasic() {
	dim := 4
	rows := [][][]float32{
		{{0.1, 0.2, 0.3, 0.4}, {0.5, 0.6, 0.7, 0.8}},
		{{1.1, 1.2, 1.3, 1.4}},
	}
	col := NewColumnFloatVectorArray("emb", dim, rows)

	s.Equal("emb", col.Name())
	s.Equal(entity.FieldTypeArray, col.Type())
	s.Equal(entity.FieldTypeFloatVector, col.ElementType())
	s.Equal(dim, col.Dim())
	s.Equal(2, col.Len())
	s.Equal(2, col.ValidCount())
	s.False(col.Nullable())

	v, err := col.Get(0)
	s.NoError(err)
	row0, ok := v.([]entity.FloatVector)
	s.Require().True(ok)
	s.Equal(2, len(row0))

	// Out-of-range Get.
	_, err = col.Get(-1)
	s.Error(err)
	_, err = col.Get(99)
	s.Error(err)

	// Scalar conversions are unsupported on vector array columns.
	_, err = col.GetAsInt64(0)
	s.Error(err)
	_, err = col.GetAsString(0)
	s.Error(err)
	_, err = col.GetAsDouble(0)
	s.Error(err)
	_, err = col.GetAsBool(0)
	s.Error(err)

	// Nullable helpers are no-ops / zero-valued for vector arrays.
	isNull, err := col.IsNull(0)
	s.NoError(err)
	s.False(isNull)
	s.Error(col.AppendNull())
	col.SetNullable(true)
	s.False(col.Nullable())
	s.NoError(col.ValidateNullable())
	col.CompactNullableValues()

	// AppendValue via both canonical shapes.
	s.NoError(col.AppendValue([]entity.FloatVector{entity.FloatVector([]float32{2.1, 2.2, 2.3, 2.4})}))
	s.NoError(col.AppendValue([][]float32{{3.1, 3.2, 3.3, 3.4}}))
	s.Equal(4, col.Len())

	// AppendValue rejects bad shapes.
	s.Error(col.AppendValue([]int{1, 2, 3}))

	// FieldData round-trip.
	fd := col.FieldData()
	s.Equal(schemapb.DataType_ArrayOfVector, fd.GetType())
	s.Equal("emb", fd.GetFieldName())
	va := fd.GetVectors().GetVectorArray()
	s.Require().NotNil(va)
	s.EqualValues(dim, va.GetDim())
	s.Equal(schemapb.DataType_FloatVector, va.GetElementType())
	s.Equal(4, len(va.GetData()))
}

func (s *VectorArraySuite) TestFloat16VectorArrayBasic() {
	dim := 4
	byteRow := make([]byte, dim*2)
	rows := [][][]byte{{byteRow, byteRow}, {byteRow}}
	col := NewColumnFloat16VectorArray("emb", dim, rows)

	s.Equal(entity.FieldTypeFloat16Vector, col.ElementType())
	s.Equal(2, col.Len())

	// AppendValue variants.
	s.NoError(col.AppendValue([]entity.Float16Vector{entity.Float16Vector(byteRow)}))
	s.NoError(col.AppendValue([][]byte{byteRow}))
	s.Error(col.AppendValue(123))
	s.Equal(4, col.Len())

	fd := col.FieldData()
	s.Equal(schemapb.DataType_ArrayOfVector, fd.GetType())
	s.Equal(schemapb.DataType_Float16Vector, fd.GetVectors().GetVectorArray().GetElementType())
}

func (s *VectorArraySuite) TestBFloat16VectorArrayBasic() {
	dim := 4
	byteRow := make([]byte, dim*2)
	rows := [][][]byte{{byteRow}}
	col := NewColumnBFloat16VectorArray("emb", dim, rows)

	s.Equal(entity.FieldTypeBFloat16Vector, col.ElementType())
	s.Equal(1, col.Len())

	s.NoError(col.AppendValue([]entity.BFloat16Vector{entity.BFloat16Vector(byteRow)}))
	s.NoError(col.AppendValue([][]byte{byteRow}))
	s.Error(col.AppendValue("bad"))
	s.Equal(3, col.Len())

	fd := col.FieldData()
	s.Equal(schemapb.DataType_BFloat16Vector, fd.GetVectors().GetVectorArray().GetElementType())
}

func (s *VectorArraySuite) TestBinaryVectorArrayBasic() {
	dim := 8 // binary dim is bits; 1 byte per vector.
	byteRow := make([]byte, dim/8)
	rows := [][][]byte{{byteRow, byteRow}}
	col := NewColumnBinaryVectorArray("emb", dim, rows)

	s.Equal(entity.FieldTypeBinaryVector, col.ElementType())
	s.Equal(1, col.Len())

	s.NoError(col.AppendValue([]entity.BinaryVector{entity.BinaryVector(byteRow)}))
	s.NoError(col.AppendValue([][]byte{byteRow}))
	s.Error(col.AppendValue(42))
	s.Equal(3, col.Len())

	fd := col.FieldData()
	s.Equal(schemapb.DataType_BinaryVector, fd.GetVectors().GetVectorArray().GetElementType())
}

func (s *VectorArraySuite) TestInt8VectorArrayBasic() {
	dim := 4
	rows := [][][]int8{{{1, 2, 3, 4}, {5, 6, 7, 8}}, {{9, 10, 11, 12}}}
	col := NewColumnInt8VectorArray("emb", dim, rows)

	s.Equal(entity.FieldTypeInt8Vector, col.ElementType())
	s.Equal(2, col.Len())

	s.NoError(col.AppendValue([]entity.Int8Vector{entity.Int8Vector([]int8{1, 2, 3, 4})}))
	s.NoError(col.AppendValue([][]int8{{5, 6, 7, 8}}))
	s.Error(col.AppendValue("bad"))
	s.Equal(4, col.Len())

	fd := col.FieldData()
	s.Equal(schemapb.DataType_Int8Vector, fd.GetVectors().GetVectorArray().GetElementType())
}

func (s *VectorArraySuite) TestBaseAppendValueRejectsWrongType() {
	dim := 4
	col := NewColumnFloat16VectorArray("emb", dim, nil)
	// The columnVectorArrayBase AppendValue path (via embedded struct) rejects mismatched types.
	s.Error(col.columnVectorArrayBase.AppendValue([]int{1, 2}))
	// The positive path is exercised in TestFloat16VectorArrayBasic.
}

func (s *VectorArraySuite) TestParseVectorArrayDataFloatSuccess() {
	dim := 4
	row := &schemapb.VectorField{
		Dim: int64(dim),
		Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5, 6, 7, 8}},
		},
	}
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfVector,
		FieldName: "emb",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_VectorArray{
					VectorArray: &schemapb.VectorArray{
						Dim:         int64(dim),
						ElementType: schemapb.DataType_FloatVector,
						Data:        []*schemapb.VectorField{row, row},
					},
				},
			},
		},
	}
	col, err := FieldDataColumn(fd, 0, -1)
	s.NoError(err)
	s.Equal(2, col.Len())
	fv, ok := col.(*ColumnFloatVectorArray)
	s.Require().True(ok)
	s.Equal(dim, fv.Dim())
}

func (s *VectorArraySuite) TestParseVectorArrayDataByteTypes() {
	dim := 4
	// Build a single payload holding 2 inner vectors of `dim*2` bytes each.
	fp16Row := &schemapb.VectorField{
		Dim:  int64(dim),
		Data: &schemapb.VectorField_Float16Vector{Float16Vector: make([]byte, dim*2*2)},
	}
	bf16Row := &schemapb.VectorField{
		Dim:  int64(dim),
		Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: make([]byte, dim*2*2)},
	}
	int8Row := &schemapb.VectorField{
		Dim:  int64(dim),
		Data: &schemapb.VectorField_Int8Vector{Int8Vector: make([]byte, dim*2)},
	}
	// Binary: dim=16 bits -> 2 bytes per inner vector.
	binRow := &schemapb.VectorField{
		Dim:  16,
		Data: &schemapb.VectorField_BinaryVector{BinaryVector: make([]byte, 2*2)},
	}

	cases := []struct {
		name    string
		elem    schemapb.DataType
		innerFD *schemapb.VectorField
		arrDim  int64
		wantCol Column
	}{
		{"float16", schemapb.DataType_Float16Vector, fp16Row, int64(dim), (*ColumnFloat16VectorArray)(nil)},
		{"bfloat16", schemapb.DataType_BFloat16Vector, bf16Row, int64(dim), (*ColumnBFloat16VectorArray)(nil)},
		{"int8", schemapb.DataType_Int8Vector, int8Row, int64(dim), (*ColumnInt8VectorArray)(nil)},
		{"binary", schemapb.DataType_BinaryVector, binRow, 16, (*ColumnBinaryVectorArray)(nil)},
	}

	for _, c := range cases {
		s.Run(c.name, func() {
			fd := &schemapb.FieldData{
				Type:      schemapb.DataType_ArrayOfVector,
				FieldName: "emb",
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: c.arrDim,
						Data: &schemapb.VectorField_VectorArray{
							VectorArray: &schemapb.VectorArray{
								Dim:         c.arrDim,
								ElementType: c.elem,
								Data:        []*schemapb.VectorField{c.innerFD},
							},
						},
					},
				},
			}
			col, err := FieldDataColumn(fd, 0, -1)
			s.NoError(err)
			s.Equal(1, col.Len())
			s.IsType(c.wantCol, col)
		})
	}
}

func (s *VectorArraySuite) TestParseVectorArrayDataUnsupportedElement() {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfVector,
		FieldName: "emb",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 4,
				Data: &schemapb.VectorField_VectorArray{
					VectorArray: &schemapb.VectorArray{
						Dim:         4,
						ElementType: schemapb.DataType_SparseFloatVector, // not supported inside ArrayOfVector
						Data:        nil,
					},
				},
			},
		},
	}
	_, err := FieldDataColumn(fd, 0, -1)
	s.Error(err)
}

func (s *VectorArraySuite) TestParseVectorArrayDataBeginEndClamping() {
	dim := 2
	row := &schemapb.VectorField{
		Dim: int64(dim),
		Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}},
		},
	}
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfVector,
		FieldName: "emb",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_VectorArray{
					VectorArray: &schemapb.VectorArray{
						Dim:         int64(dim),
						ElementType: schemapb.DataType_FloatVector,
						Data:        []*schemapb.VectorField{row, row, row},
					},
				},
			},
		},
	}
	// negative begin gets clamped to 0; end > len clamped to len.
	col, err := FieldDataColumn(fd, -5, 10)
	s.NoError(err)
	s.Equal(3, col.Len())

	// begin > end collapses to empty.
	col2, err := FieldDataColumn(fd, 10, 1)
	s.NoError(err)
	s.Equal(0, col2.Len())
}

func (s *VectorArraySuite) TestParseVectorArrayDataFallbackDim() {
	// Outer Dim=0 should fall back to the first non-zero inner VectorField.Dim.
	row := &schemapb.VectorField{
		Dim: 4,
		Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}},
		},
	}
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfVector,
		FieldName: "emb",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 0,
				Data: &schemapb.VectorField_VectorArray{
					VectorArray: &schemapb.VectorArray{
						Dim:         0,
						ElementType: schemapb.DataType_FloatVector,
						Data:        []*schemapb.VectorField{row},
					},
				},
			},
		},
	}
	col, err := FieldDataColumn(fd, 0, -1)
	s.NoError(err)
	fv, ok := col.(*ColumnFloatVectorArray)
	s.Require().True(ok)
	s.Equal(4, fv.Dim())
}

func (s *VectorArraySuite) TestSlice() {
	dim := 2
	rows := [][][]float32{
		{{1, 2}},
		{{3, 4}, {5, 6}},
		{{7, 8}},
		{{9, 10}, {11, 12}},
	}
	col := NewColumnFloatVectorArray("emb", dim, rows)

	sliced := col.Slice(1, 3)
	s.Equal(2, sliced.Len())

	// end clamped to len.
	sliced2 := col.Slice(2, 99)
	s.Equal(2, sliced2.Len())

	// end == -1 means to the end.
	sliced3 := col.Slice(1, -1)
	s.Equal(3, sliced3.Len())

	// start > end is clamped to end.
	sliced4 := col.Slice(3, 1)
	s.Equal(0, sliced4.Len())
}

func TestVectorArray(t *testing.T) {
	suite.Run(t, new(VectorArraySuite))
}
