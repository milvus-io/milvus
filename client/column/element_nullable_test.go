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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
)

type ElementNullableSuite struct {
	suite.Suite
}

func ptr[T any](v T) *T {
	return &v
}

func longRow(values []int64, valid []bool) *schemapb.ScalarField {
	return &schemapb.ScalarField{
		Data:      &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
		ValidData: valid,
	}
}

func stringRow(values []string, valid []bool) *schemapb.ScalarField {
	return &schemapb.ScalarField{
		Data:      &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}},
		ValidData: valid,
	}
}

func arrayFieldData(name string, elementType schemapb.DataType, rows []*schemapb.ScalarField, rowValid []bool) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{Data: rows, ElementType: elementType},
				},
				ValidData: rowValid,
			},
		},
		ValidData: rowValid,
	}
}

func (s *ElementNullableSuite) TestScalarArrayCompact() {
	col, err := NewElementNullableColumnInt64Array("score",
		[][]int64{{1, 3}, {}, {7}},
		[][]bool{{true, false, true}, {false}, {true}})
	s.Require().NoError(err)
	s.True(col.ElementNullable())
	s.Equal(3, col.Len())

	v, err := col.Get(0)
	s.Require().NoError(err)
	s.Equal([]*int64{ptr[int64](1), nil, ptr[int64](3)}, v)
	n, err := col.ElementLen(1)
	s.NoError(err)
	s.Equal(1, n)
	isNull, err := col.ElementIsNull(0, 1)
	s.NoError(err)
	s.True(isNull)
	_, err = col.ElementIsNull(0, 3)
	s.Error(err)

	fd := col.FieldData()
	rows := fd.GetScalars().GetArrayData().GetData()
	s.Require().Len(rows, 3)
	s.Equal([]int64{1, 3}, rows[0].GetLongData().GetData())
	s.Equal([]bool{true, false, true}, rows[0].GetValidData())
	s.Empty(rows[1].GetLongData().GetData())
	s.Equal([]bool{false}, rows[1].GetValidData())

	// payload length must match the valid count in compact mode
	_, err = NewElementNullableColumnInt64Array("score", [][]int64{{1, 2}}, [][]bool{{true, false}})
	s.Error(err)
}

func (s *ElementNullableSuite) TestScalarArrayAppend() {
	col := NewColumnVarCharArray("lang", nil)
	s.Error(col.AppendValue([]*string{ptr("en")}), "not element nullable")
	col.SetElementNullable(true)
	col.SetNullable(true)

	s.NoError(col.AppendValue([]*string{ptr("en"), nil}))
	s.NoError(col.AppendValue([]string{"fr"}))
	s.NoError(col.AppendNull())
	s.NoError(col.appendNullElements(2))
	s.Equal(4, col.Len())

	v, err := col.Get(2)
	s.NoError(err)
	s.Nil(v)
	v, err = col.Get(3)
	s.NoError(err)
	s.Equal([]*string{nil, nil}, v)

	sliced := col.Slice(1, 4).(*ColumnVarCharArray)
	s.Equal(3, sliced.Len())
	v, err = sliced.Get(0)
	s.NoError(err)
	s.Equal([]*string{ptr("fr")}, v)
	valid, err := sliced.ElementValidData(2)
	s.NoError(err)
	s.Equal([]bool{false, false}, valid)

	fd := col.FieldData()
	s.Equal([]bool{true, true, false, true}, fd.GetValidData())
	s.Len(fd.GetScalars().GetArrayData().GetData(), 3)
}

func (s *ElementNullableSuite) TestScalarArraySparseCompaction() {
	col, err := NewElementNullableColumnInt64Array("score",
		[][]int64{{1, 0}, nil, {0}},
		[][]bool{{true, false}, nil, {false}},
		WithRowValidData([]bool{true, false, true}), WithElementSparseMode(true))
	s.Require().NoError(err)
	v, err := col.Get(0)
	s.NoError(err)
	s.Equal([]*int64{ptr[int64](1), nil}, v)

	col.CompactNullableValues()
	s.NoError(col.ValidateNullable())
	s.Equal([][]int64{{1}, {}}, col.Data())
	v, err = col.Get(2)
	s.NoError(err)
	s.Equal([]*int64{nil}, v)
}

func (s *ElementNullableSuite) TestParseScalarArray() {
	s.Run("dense", func() {
		fd := arrayFieldData("score", schemapb.DataType_Int64, []*schemapb.ScalarField{
			longRow([]int64{1, 0, 3}, []bool{true, false, true}),
			longRow(nil, nil),
		}, nil)
		col, err := FieldDataColumn(fd, 0, -1)
		s.Require().NoError(err)
		elementCol := col.(ElementNullableColumn)
		s.True(elementCol.ElementNullable())
		v, err := col.Get(0)
		s.NoError(err)
		s.Equal([]*int64{ptr[int64](1), nil, ptr[int64](3)}, v)
		n, err := elementCol.ElementLen(1)
		s.NoError(err)
		s.Zero(n)
	})

	s.Run("compact", func() {
		fd := arrayFieldData("score", schemapb.DataType_Int64, []*schemapb.ScalarField{
			longRow([]int64{1, 3}, []bool{true, false, true}),
		}, nil)
		col, err := FieldDataColumn(fd, 0, -1)
		s.Require().NoError(err)
		v, err := col.Get(0)
		s.NoError(err)
		s.Equal([]*int64{ptr[int64](1), nil, ptr[int64](3)}, v)
	})

	s.Run("null rows and range", func() {
		fd := arrayFieldData("score", schemapb.DataType_Int64, []*schemapb.ScalarField{
			longRow([]int64{1}, []bool{true}),
			longRow(nil, nil),
			longRow([]int64{0, 5}, []bool{false, true}),
		}, []bool{true, false, true})
		col, err := FieldDataColumn(fd, 1, 3)
		s.Require().NoError(err)
		s.Equal(2, col.Len())
		isNull, err := col.IsNull(0)
		s.NoError(err)
		s.True(isNull)
		v, err := col.Get(1)
		s.NoError(err)
		s.Equal([]*int64{nil, ptr[int64](5)}, v)
	})

	s.Run("no element validity", func() {
		fd := arrayFieldData("score", schemapb.DataType_Int64, []*schemapb.ScalarField{
			longRow([]int64{1, 2}, nil),
		}, nil)
		col, err := FieldDataColumn(fd, 0, -1)
		s.Require().NoError(err)
		s.False(col.(ElementNullableColumn).ElementNullable())
		v, err := col.Get(0)
		s.NoError(err)
		s.Equal([]int64{1, 2}, v)
	})

	s.Run("mismatch", func() {
		fd := arrayFieldData("score", schemapb.DataType_Int64, []*schemapb.ScalarField{
			longRow([]int64{1, 2}, []bool{true, false, false}),
		}, nil)
		_, err := FieldDataColumn(fd, 0, -1)
		s.Error(err)
	})
}

func (s *ElementNullableSuite) TestVectorArray() {
	v0 := []float32{1, 2}
	v2 := []float32{5, 6}
	col, err := NewElementNullableColumnFloatVectorArray("vec", 2, [][][]float32{{v0, v2}}, [][]bool{{true, false, true}})
	s.Require().NoError(err)
	got, err := col.Get(0)
	s.NoError(err)
	s.Equal([]entity.FloatVector{v0, nil, v2}, got)

	s.NoError(col.AppendValue([][]float32{nil, v0}))
	n, err := col.ElementLen(1)
	s.NoError(err)
	s.Equal(2, n)

	fd := col.FieldData()
	rows := fd.GetVectors().GetVectorArray().GetData()
	s.Require().Len(rows, 2)
	s.Equal([]float32{1, 2, 5, 6}, rows[0].GetFloatVector().GetData())
	s.Equal([]bool{true, false, true}, rows[0].GetValidData())
	s.Equal([]float32{1, 2}, rows[1].GetFloatVector().GetData())
	s.Equal([]bool{false, true}, rows[1].GetValidData())

	plain := NewColumnFloatVectorArray("vec", 2, nil)
	s.Error(plain.AppendValue([][]float32{nil}), "nil vector needs an element-nullable column")
}

func (s *ElementNullableSuite) TestParseVectorArray() {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_ArrayOfVector,
		FieldName: "vec",
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 2,
				Data: &schemapb.VectorField_VectorArray{
					VectorArray: &schemapb.VectorArray{
						Dim:         2,
						ElementType: schemapb.DataType_FloatVector,
						Data: []*schemapb.VectorField{
							{
								Dim:       2,
								Data:      &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}},
								ValidData: []bool{false, true},
							},
							{
								Dim:  2,
								Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}},
							},
						},
					},
				},
			},
		},
	}
	col, err := FieldDataColumn(fd, 0, -1)
	s.Require().NoError(err)
	elementCol := col.(ElementNullableColumn)
	s.True(elementCol.ElementNullable())
	got, err := col.Get(0)
	s.NoError(err)
	s.Equal([]entity.FloatVector{nil, {1, 2}}, got)

	// a compact payload that does not match the valid count is rejected
	fd.GetVectors().GetVectorArray().GetData()[0].ValidData = []bool{false, false, true}
	fd.GetVectors().GetVectorArray().GetData()[0].GetFloatVector().Data = []float32{1, 2, 3, 4}
	_, err = FieldDataColumn(fd, 0, -1)
	s.Error(err)

	// a dense payload, holding a vector for the null element too, is rejected
	fd.GetVectors().GetVectorArray().GetData()[0].ValidData = []bool{true, false}
	_, err = FieldDataColumn(fd, 0, -1)
	s.Error(err)
}

// storedInnerRow rebuilds one row through per-element storedInner lookups.
func storedInnerRow(c *columnNestedArrayBase[int64], row int) [][]int64 {
	n, _ := c.ElementLen(row)
	result := make([][]int64, n)
	for j := 0; j < n; j++ {
		q, valid, _ := c.storedInner(row, j)
		if valid {
			result[j] = c.values[row][q]
		}
	}
	return result
}

func (s *ElementNullableSuite) TestNestedArrayGetCompactRow() {
	valid := [][]bool{{false, true, true}, {true, false, true}, {true, true, false}, {true, true}}
	expected := []any{
		[][]int64{nil, {1}, {2, 3}},
		[][]int64{{4}, nil, {5}},
		[][]int64{{6}, {7, 8}, nil},
		[][]int64{{9}, {10, 11}},
	}

	compact, err := NewElementNullableColumnInt64ArrayArray("ints",
		[][][]int64{{{1}, {2, 3}}, {{4}, {5}}, {{6}, {7, 8}}, {{9}, {10, 11}}}, valid, nil)
	s.Require().NoError(err)
	sparse, err := NewElementNullableColumnInt64ArrayArray("ints",
		[][][]int64{{nil, {1}, {2, 3}}, {{4}, nil, {5}}, {{6}, {7, 8}, nil}, {{9}, {10, 11}}}, valid, nil,
		WithElementSparseMode(true))
	s.Require().NoError(err)

	for _, col := range []*ColumnInt64ArrayArray{compact, sparse} {
		for i, want := range expected {
			got, err := col.Get(i)
			s.Require().NoError(err)
			s.Equal(want, got)
			s.Equal(storedInnerRow(col.columnNestedArrayBase, i), got)
		}
	}

	leaf, err := NewElementNullableColumnInt64ArrayArray("ints",
		[][][]int64{{{1}}, {{2}, {3}}}, [][]bool{{false, true}, {true, false, true}},
		[][][]bool{{{true, false}}, {{true}, {true}}})
	s.Require().NoError(err)
	got, err := leaf.Get(0)
	s.Require().NoError(err)
	s.Equal([][]*int64{nil, {ptr[int64](1), nil}}, got)
	got, err = leaf.Get(1)
	s.Require().NoError(err)
	s.Equal([][]*int64{{ptr[int64](2)}, nil, {ptr[int64](3)}}, got)
}

func (s *ElementNullableSuite) TestNestedArray() {
	col, err := NewElementNullableColumnVarCharArrayArray("tags",
		[][][]string{{{"a"}, {}}},
		[][]bool{{true, false, true}},
		[][][]bool{{{true, false}, {}}})
	s.Require().NoError(err)
	s.Equal(entity.FieldTypeArray, col.ElementType())
	s.Equal(entity.FieldTypeVarChar, col.LeafType())

	got, err := col.Get(0)
	s.NoError(err)
	s.Equal([][]*string{{ptr("a"), nil}, nil, {}}, got)
	isNull, err := col.LeafIsNull(0, 0, 1)
	s.NoError(err)
	s.True(isNull)
	leafValid, err := col.LeafValidData(0, 1)
	s.NoError(err)
	s.Nil(leafValid)

	s.NoError(col.AppendValue([][]*string{nil, {ptr("b"), nil}}))
	s.NoError(col.AppendValue([][]string{{"c"}}))
	s.Equal(3, col.Len())

	fd := col.FieldData()
	arrayData := fd.GetScalars().GetArrayData()
	s.Equal(schemapb.DataType_Array, arrayData.GetElementType())
	rows := arrayData.GetData()
	s.Require().Len(rows, 3)
	s.Equal(schemapb.DataType_VarChar, rows[0].GetArrayData().GetElementType())
	s.Equal([]bool{true, false, true}, rows[0].GetValidData())
	inner := rows[0].GetArrayData().GetData()
	s.Require().Len(inner, 2, "null inner arrays are omitted")
	s.Equal([]string{"a"}, inner[0].GetStringData().GetData())
	s.Equal([]bool{true, false}, inner[0].GetValidData())
	s.Equal([]bool{false, true}, rows[1].GetValidData())
	s.Equal([]string{"b"}, rows[1].GetArrayData().GetData()[0].GetStringData().GetData())

	plain := NewColumnInt64ArrayArray("ints", nil)
	s.Error(plain.AppendValue([][]*int64{{nil}}), "nil leaf needs a leaf-nullable column")
	s.NoError(plain.AppendValue([][]int64{nil, {1}}))
	got, err = plain.Get(0)
	s.NoError(err)
	s.Equal([][]int64{{}, {1}}, got, "a nil inner array is empty when inner arrays are not nullable")
}

func (s *ElementNullableSuite) TestParseNestedArray() {
	fd := arrayFieldData("tags", schemapb.DataType_Array, []*schemapb.ScalarField{
		{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_VarChar,
				Data: []*schemapb.ScalarField{
					stringRow([]string{"a", ""}, []bool{true, false}),
					stringRow(nil, nil),
					stringRow(nil, nil),
				},
			}},
			ValidData: []bool{true, false, true},
		},
		{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_VarChar,
			}},
		},
	}, nil)
	col, err := FieldDataColumn(fd, 0, -1)
	s.Require().NoError(err)
	nested, ok := col.(*ColumnVarCharArrayArray)
	s.Require().True(ok)
	s.True(nested.ElementNullable())
	s.True(nested.LeafNullable())
	got, err := col.Get(0)
	s.NoError(err)
	s.Equal([][]*string{{ptr("a"), nil}, nil, {}}, got)
	n, err := nested.ElementLen(1)
	s.NoError(err)
	s.Zero(n)

	// writing a parsed column produces the compact input form
	nested.CompactNullableValues()
	rows := nested.FieldData().GetScalars().GetArrayData().GetData()
	s.Len(rows[0].GetArrayData().GetData(), 2)
	s.Equal([]string{"a"}, rows[0].GetArrayData().GetData()[0].GetStringData().GetData())
}

func (s *ElementNullableSuite) TestStructArrayOmittedSubField() {
	schema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("score").WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName("lang").WithDataType(entity.FieldTypeVarChar).WithNullable(true)).
		WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(2).WithNullable(true)).
		WithField(entity.NewField().WithName("tags").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeVarChar).
			WithNullable(true).WithElementNullable(true))
	col, err := NewColumnStructArrayFromSchema("clips", schema)
	s.Require().NoError(err)

	s.NoError(col.AppendValue(map[string]any{
		"score": []int64{1, 2},
		"lang":  []*string{ptr("en"), nil},
		"vec":   [][]float32{{1, 2}, nil},
		"tags":  [][]*string{{ptr("x"), nil}, nil},
	}))
	// lang, vec and tags omitted: two null elements each
	s.NoError(col.AppendValue(map[string]any{"score": []int64{3, 4}}))
	s.Equal(2, col.Len())

	v, err := col.Get(1)
	s.NoError(err)
	row := v.(map[string]any)
	s.Equal([]*string{nil, nil}, row["lang"])
	s.Equal([]entity.FloatVector{nil, nil}, row["vec"])
	s.Equal([][]*string{nil, nil}, row["tags"])

	// the omitted non-nullable sub-field is still required
	s.Error(col.AppendValue(map[string]any{"lang": []*string{nil}}))
	// sub-fields must agree on the element count
	s.Error(col.AppendValue(map[string]any{"score": []int64{1}, "lang": []string{"a", "b"}}))
	s.Equal(2, col.Len(), "failed appends are rolled back")
	s.NoError(col.ValidateNullable())

	fields := col.(StructArrayColumn).Fields()
	s.Len(fields, 4)
	lang := fields[1].(ElementNullableColumn)
	isNull, err := lang.ElementIsNull(0, 1)
	s.NoError(err)
	s.True(isNull)
}

func (s *ElementNullableSuite) TestParseStructArrayElementCountMismatch() {
	structArray := &schemapb.StructArrayField{Fields: []*schemapb.FieldData{
		arrayFieldData("score", schemapb.DataType_Int64, []*schemapb.ScalarField{longRow([]int64{1, 2}, nil)}, nil),
		arrayFieldData("lang", schemapb.DataType_VarChar, []*schemapb.ScalarField{stringRow([]string{"a"}, []bool{true})}, nil),
	}}
	_, err := parseStructArrayData("clips", structArray, 0, -1)
	s.Error(err)
}

func (s *ElementNullableSuite) TestApplyStructSchema() {
	schema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("score").WithDataType(entity.FieldTypeInt64).WithNullable(true)).
		WithField(entity.NewField().WithName("tags").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt32).
			WithNullable(true).WithElementNullable(true))
	// rows without any element validity, and a nested sub-field whose rows do not tell the leaf type
	structArray := &schemapb.StructArrayField{Fields: []*schemapb.FieldData{
		arrayFieldData("score", schemapb.DataType_Int64, []*schemapb.ScalarField{longRow(nil, nil)}, nil),
		arrayFieldData("tags", schemapb.DataType_Array, []*schemapb.ScalarField{{}}, nil),
	}}
	col, err := parseStructArrayData("clips", structArray, 0, -1)
	s.Require().NoError(err)
	s.Require().NoError(ApplyStructSchema(col, schema))

	fields := col.(StructArrayColumn).Fields()
	s.True(fields[0].(ElementNullableColumn).ElementNullable())
	tags, ok := fields[1].(*ColumnInt32ArrayArray)
	s.Require().True(ok)
	s.True(tags.ElementNullable())
	s.True(tags.LeafNullable())
	s.Equal(1, tags.Len())
}

func TestElementNullable(t *testing.T) {
	suite.Run(t, new(ElementNullableSuite))
}
