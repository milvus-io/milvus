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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

type StructArraySuite struct {
	suite.Suite
}

func (s *StructArraySuite) TestBasic() {
	name := fmt.Sprintf("struct_array_%d", rand.Intn(100))

	// Each row holds a variable-length array of struct elements; sub-columns are *Array types.
	intRows := [][]int32{{1, 2}, {3}, {4, 5, 6}}
	floatRows := [][]float32{{1.1, 2.2}, {3.3}, {4.4, 5.5, 6.6}}
	strRows := [][]string{{"a", "b"}, {"c"}, {"d", "e", "f"}}

	int32Col := NewColumnInt32Array("int_field", intRows)
	floatCol := NewColumnFloatArray("float_field", floatRows)
	varcharCol := NewColumnVarCharArray("varchar_field", strRows)

	column := NewColumnStructArray(name, []Column{int32Col, floatCol, varcharCol})

	s.Equal(name, column.Name())
	s.Equal(entity.FieldTypeArray, column.Type())
	s.EqualValues(3, column.Len())

	fd := column.FieldData()
	s.Equal(schemapb.DataType_ArrayOfStruct, fd.GetType())
	s.Equal(name, fd.GetFieldName())

	structArrays := fd.GetStructArrays()
	s.NotNil(structArrays)
	s.Equal(3, len(structArrays.GetFields()))

	// Sub-fields must be Array (not flat scalars) to match server-side schema for struct sub-fields.
	for _, sub := range structArrays.GetFields() {
		s.Equal(schemapb.DataType_Array, sub.GetType())
	}

	val, err := column.Get(0)
	s.NoError(err)
	m, ok := val.(map[string]any)
	s.True(ok)
	s.Equal([]int32{1, 2}, m["int_field"])
	s.Equal([]float32{1.1, 2.2}, m["float_field"])
	s.Equal([]string{"a", "b"}, m["varchar_field"])

	val, err = column.Get(2)
	s.NoError(err)
	m = val.(map[string]any)
	s.Equal([]int32{4, 5, 6}, m["int_field"])
}

func (s *StructArraySuite) TestVectorSubField() {
	dim := 4
	rows := [][][]float32{
		{{0.1, 0.2, 0.3, 0.4}, {0.5, 0.6, 0.7, 0.8}},
		{{1.1, 1.2, 1.3, 1.4}},
	}
	idRows := [][]int64{{10, 20}, {30}}

	idCol := NewColumnInt64Array("id", idRows)
	embCol := NewColumnFloatVectorArray("emb", dim, rows)
	column := NewColumnStructArray("clips", []Column{idCol, embCol})

	fd := column.FieldData()
	s.Equal(schemapb.DataType_ArrayOfStruct, fd.GetType())
	s.Equal(2, len(fd.GetStructArrays().GetFields()))

	embFD := fd.GetStructArrays().GetFields()[1]
	s.Equal(schemapb.DataType_ArrayOfVector, embFD.GetType())
	va := embFD.GetVectors().GetVectorArray()
	s.NotNil(va)
	s.EqualValues(dim, va.GetDim())
	s.Equal(schemapb.DataType_FloatVector, va.GetElementType())
	s.Equal(2, len(va.GetData()))
	s.EqualValues(2*dim, len(va.GetData()[0].GetFloatVector().GetData()))
	s.EqualValues(1*dim, len(va.GetData()[1].GetFloatVector().GetData()))
}

func (s *StructArraySuite) TestSlice() {
	intRows := [][]int64{{10}, {20, 21}, {30, 31, 32}, {40}, {50, 51}}
	boolRows := [][]bool{{true}, {false, true}, {true, false, true}, {false}, {true, false}}

	int64Col := NewColumnInt64Array("id", intRows)
	boolCol := NewColumnBoolArray("flag", boolRows)
	column := NewColumnStructArray("struct_array_slice", []Column{int64Col, boolCol})

	sliced := column.Slice(1, 4)
	s.NotNil(sliced)
	s.EqualValues(3, sliced.Len())

	val, err := sliced.Get(0)
	s.NoError(err)
	m := val.(map[string]any)
	s.Equal([]int64{20, 21}, m["id"])
	s.Equal([]bool{false, true}, m["flag"])
}

func (s *StructArraySuite) TestAppendValue() {
	intCol := NewColumnInt32Array("a", nil)
	strCol := NewColumnVarCharArray("b", nil)
	column := NewColumnStructArray("rows", []Column{intCol, strCol})

	s.NoError(column.AppendValue(map[string]any{"a": []int32{1, 2}, "b": []string{"x", "y"}}))
	s.NoError(column.AppendValue(map[string]any{"a": []int32{3}, "b": []string{"z"}}))
	s.EqualValues(2, column.Len())

	// missing sub-field
	err := column.AppendValue(map[string]any{"a": []int32{4}})
	s.Error(err)

	// wrong shape (scalar instead of array)
	err = column.AppendValue(map[string]any{"a": int32(1), "b": []string{"q"}})
	s.Error(err)
}

func (s *StructArraySuite) TestParseStructArrayData() {
	int32FieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "age",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						ElementType: schemapb.DataType_Int32,
						Data: []*schemapb.ScalarField{
							{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{10, 11}}}},
							{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{20}}}},
							{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{30, 31, 32}}}},
						},
					},
				},
			},
		},
	}

	varcharFieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "name",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						ElementType: schemapb.DataType_VarChar,
						Data: []*schemapb.ScalarField{
							{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"alice", "ann"}}}},
							{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"bob"}}}},
							{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"c1", "c2", "c3"}}}},
						},
					},
				},
			},
		},
	}

	structArrayField := &schemapb.StructArrayField{
		Fields: []*schemapb.FieldData{int32FieldData, varcharFieldData},
	}

	col, err := parseStructArrayData("person", structArrayField, 0, -1)
	s.NoError(err)
	s.NotNil(col)
	s.Equal("person", col.Name())
	s.Equal(entity.FieldTypeArray, col.Type())

	val, err := col.Get(0)
	s.NoError(err)
	m := val.(map[string]any)
	s.Equal([]int32{10, 11}, m["age"])
	s.Equal([]string{"alice", "ann"}, m["name"])

	val, err = col.Get(1)
	s.NoError(err)
	m = val.(map[string]any)
	s.Equal([]int32{20}, m["age"])
	s.Equal([]string{"bob"}, m["name"])
}

func (s *StructArraySuite) TestParseTopLevelArrayOfStruct() {
	// Verify FieldDataColumn dispatches DataType_ArrayOfStruct to parseStructArrayData.
	int32Sub := &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: "x",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						ElementType: schemapb.DataType_Int32,
						Data: []*schemapb.ScalarField{
							{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2}}}},
						},
					},
				},
			},
		},
	}
	top := &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfStruct,
		FieldName: "wrap",
		Field: &schemapb.FieldData_StructArrays{
			StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{int32Sub}},
		},
	}
	col, err := FieldDataColumn(top, 0, -1)
	s.NoError(err)
	s.Equal("wrap", col.Name())
	val, err := col.Get(0)
	s.NoError(err)
	s.Equal([]int32{1, 2}, val.(map[string]any)["x"])
}

func (s *StructArraySuite) TestParseVectorArrayDataErrors() {
	mkFD := func(elemType schemapb.DataType, dim int64, rows []*schemapb.VectorField) *schemapb.FieldData {
		return &schemapb.FieldData{
			Type:      schemapb.DataType_ArrayOfVector,
			FieldName: "emb",
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: dim,
					Data: &schemapb.VectorField_VectorArray{
						VectorArray: &schemapb.VectorArray{
							Dim: dim, ElementType: elemType, Data: rows,
						},
					},
				},
			},
		}
	}

	s.Run("unknown dim rejected", func() {
		fd := mkFD(schemapb.DataType_FloatVector, 0, nil)
		_, err := FieldDataColumn(fd, 0, -1)
		s.Error(err)
	})

	s.Run("payload not a multiple of dim", func() {
		// dim=4 but row has 5 floats -> must error, not silently truncate.
		row := &schemapb.VectorField{
			Dim:  4,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5}}},
		}
		fd := mkFD(schemapb.DataType_FloatVector, 4, []*schemapb.VectorField{row})
		_, err := FieldDataColumn(fd, 0, -1)
		s.Error(err)
	})

	s.Run("binary dim not multiple of 8", func() {
		row := &schemapb.VectorField{
			Dim:  4,
			Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0}},
		}
		fd := mkFD(schemapb.DataType_BinaryVector, 4, []*schemapb.VectorField{row})
		_, err := FieldDataColumn(fd, 0, -1)
		s.Error(err)
	})

	s.Run("nil row rejected", func() {
		fd := mkFD(schemapb.DataType_FloatVector, 4, []*schemapb.VectorField{nil})
		_, err := FieldDataColumn(fd, 0, -1)
		s.Error(err)
	})
}

func (s *StructArraySuite) TestAppendValueRollback() {
	intCol := NewColumnInt32Array("a", nil)
	strCol := NewColumnVarCharArray("b", nil)
	col := NewColumnStructArray("rows", []Column{intCol, strCol}).(*columnStructArray)

	// Seed with one good row so both sub-columns are at length 1.
	s.NoError(col.AppendValue(map[string]any{"a": []int32{1}, "b": []string{"x"}}))
	s.EqualValues(1, col.Len())

	// Second row: sub-field "a" accepts the []int32, but "b" gets wrong type —
	// rollback must restore sub-column "a" to length 1 so the struct stays in lock-step.
	err := col.AppendValue(map[string]any{"a": []int32{2}, "b": 42})
	s.Error(err)
	s.EqualValues(1, col.fields[0].Len(), "sub-field 'a' must be rolled back")
	s.EqualValues(1, col.fields[1].Len(), "sub-field 'b' must not have been appended")
	s.EqualValues(1, col.Len(), "struct array length stays consistent after rollback")
}

func (s *StructArraySuite) TestLenMismatchPanics() {
	// Manually drift sub-column lengths to simulate a prior corruption and verify Len reports it.
	intCol := NewColumnInt32Array("a", [][]int32{{1}, {2}})
	strCol := NewColumnVarCharArray("b", [][]string{{"x"}})
	col := NewColumnStructArray("rows", []Column{intCol, strCol})

	s.Panics(func() { _ = col.Len() })
}

func TestStructArray(t *testing.T) {
	suite.Run(t, new(StructArraySuite))
}
