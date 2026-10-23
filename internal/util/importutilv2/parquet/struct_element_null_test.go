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

package parquet

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func enCapacity(capacity string) []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: capacity}}
}

// enScalarSubField builds a single-level Array struct sub-field.
func enScalarSubField(id int64, name string, elementType schemapb.DataType, elementNullable bool) *schemapb.FieldSchema {
	params := enCapacity("4")
	if typeutil.IsStringType(elementType) {
		params = append(params, &commonpb.KeyValuePair{Key: common.MaxLengthKey, Value: "8"})
	}
	return &schemapb.FieldSchema{
		FieldID:         id,
		Name:            name,
		DataType:        schemapb.DataType_Array,
		ElementType:     elementType,
		ElementNullable: elementNullable,
		TypeParams:      params,
	}
}

// enNestedSubField builds a nested Array<Array<leaf>> struct sub-field with
// outer capacity 4 and inner capacity 3.
func enNestedSubField(id int64, name string, leaf schemapb.DataType, elementNullable, leafNullable bool) *schemapb.FieldSchema {
	var leafParams []*commonpb.KeyValuePair
	if typeutil.IsStringType(leaf) {
		leafParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "4"}}
	}
	leafSchema := &schemapb.TypeSchema{
		Nullable:   leafNullable,
		Kind:       &schemapb.TypeSchema_LeafType{LeafType: leaf},
		TypeParams: leafParams,
	}
	inner := &schemapb.TypeSchema{
		Nullable:   elementNullable,
		Kind:       &schemapb.TypeSchema_ArrayElement{ArrayElement: leafSchema},
		TypeParams: enCapacity("3"),
	}
	root := &schemapb.TypeSchema{
		Kind:       &schemapb.TypeSchema_ArrayElement{ArrayElement: inner},
		TypeParams: enCapacity("4"),
	}
	return &schemapb.FieldSchema{
		FieldID:         id,
		Name:            name,
		DataType:        schemapb.DataType_Array,
		ElementType:     schemapb.DataType_Array,
		ElementNullable: elementNullable,
		TypeSchema:      root,
	}
}

// enVectorSubField builds an ArrayOfVector struct sub-field of dim 2.
func enVectorSubField(id int64, name string, elementType schemapb.DataType, elementNullable bool) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:         id,
		Name:            name,
		DataType:        schemapb.DataType_ArrayOfVector,
		ElementType:     elementType,
		ElementNullable: elementNullable,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "2"},
			{Key: common.MaxCapacityKey, Value: "4"},
		},
	}
}

func enChunk(t *testing.T, b array.Builder) *arrow.Chunked {
	t.Helper()
	arr := b.NewArray()
	b.Release()
	chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
	arr.Release()
	t.Cleanup(chunked.Release)
	return chunked
}

func TestConvertToArrowDataType_NestedArray(t *testing.T) {
	field := enNestedSubField(201, "s[tags]", schemapb.DataType_Int64, true, false)

	// Regression: the Array case used to call itself with identical arguments
	// when the element type is Array, recursing until the stack overflowed.
	whole, err := convertToArrowDataType(field, false)
	require.NoError(t, err)
	outer, ok := whole.(*arrow.ListType)
	require.True(t, ok)
	assert.True(t, outer.ElemField().Nullable)
	inner, ok := outer.Elem().(*arrow.ListType)
	require.True(t, ok)
	assert.Equal(t, arrow.INT64, inner.Elem().ID())
	assert.False(t, inner.ElemField().Nullable)

	element, err := convertToArrowDataType(field, true)
	require.NoError(t, err)
	assert.True(t, arrow.TypeEqual(inner, element))

	child, err := structChildArrowType(field)
	require.NoError(t, err)
	assert.True(t, arrow.TypeEqual(inner, child))

	varchar, err := convertToArrowDataType(enNestedSubField(202, "s[names]", schemapb.DataType_VarChar, false, true), true)
	require.NoError(t, err)
	assert.Equal(t, arrow.STRING, varchar.(*arrow.ListType).Elem().ID())
	assert.True(t, varchar.(*arrow.ListType).ElemField().Nullable)

	// An Array element type without a nested type_schema is a schema bug, not a recursion.
	noTypeSchema := &schemapb.FieldSchema{Name: "bad", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array}
	_, err = convertToArrowDataType(noTypeSchema, false)
	assert.ErrorIs(t, err, merr.ErrImportSysFailed)
	_, err = convertToArrowDataType(noTypeSchema, true)
	assert.ErrorIs(t, err, merr.ErrImportSysFailed)
}

func TestConvertToArrowSchemaForUT_StructChildNullability(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID:  200,
			Name:     "s",
			Nullable: true,
			Fields: []*schemapb.FieldSchema{
				enScalarSubField(201, "s[a]", schemapb.DataType_Int64, false),
				enScalarSubField(202, "s[b]", schemapb.DataType_VarChar, true),
				enNestedSubField(203, "s[tags]", schemapb.DataType_Int64, true, true),
				enVectorSubField(204, "s[emb]", schemapb.DataType_FloatVector, true),
			},
		}},
	}
	for _, sub := range schema.StructArrayFields[0].Fields {
		sub.Nullable = true
	}
	arrowSchema, err := ConvertToArrowSchemaForUT(schema, false)
	require.NoError(t, err)
	fields, ok := arrowSchema.FieldsByName("s")
	require.True(t, ok)
	structType := fields[0].Type.(*arrow.ListType).Elem().(*arrow.StructType)
	require.Equal(t, 4, structType.NumFields())

	// child nullability is the element-level one, not the row-level one
	assert.False(t, structType.Field(0).Nullable)
	assert.True(t, structType.Field(1).Nullable)
	assert.True(t, structType.Field(2).Nullable)
	assert.True(t, structType.Field(3).Nullable)

	tags := structType.Field(2).Type.(*arrow.ListType)
	assert.Equal(t, arrow.INT64, tags.Elem().ID())
	assert.True(t, tags.ElemField().Nullable)
}

func TestIsStructChildConvertible(t *testing.T) {
	legacyInt32 := enScalarSubField(201, "s[a]", schemapb.DataType_Int32, false)
	legacyInt32.Nullable = true
	nullableInt32 := enScalarSubField(202, "s[b]", schemapb.DataType_Int32, true)
	nestedLeafNullable := enNestedSubField(203, "s[c]", schemapb.DataType_Int64, false, true)
	nestedElementNullable := enNestedSubField(204, "s[d]", schemapb.DataType_Int64, true, false)
	legacyVector := enVectorSubField(205, "s[e]", schemapb.DataType_FloatVector, false)
	legacyVector.Nullable = true
	nullableVector := enVectorSubField(206, "s[f]", schemapb.DataType_FloatVector, true)

	tests := []struct {
		name  string
		src   arrow.DataType
		field *schemapb.FieldSchema
		want  bool
	}{
		{"legacy exact type", arrow.PrimitiveTypes.Int32, legacyInt32, true},
		{"legacy widening int8", arrow.PrimitiveTypes.Int8, legacyInt32, true},
		{"legacy narrowing int64", arrow.PrimitiveTypes.Int64, legacyInt32, false},
		// a row-nullable struct does not make a legacy child nullable
		{"legacy null type", arrow.Null, legacyInt32, false},
		{"element-nullable null type", arrow.Null, nullableInt32, true},
		{"element-nullable widening int16", arrow.PrimitiveTypes.Int16, nullableInt32, true},
		{"nested list", arrow.ListOf(arrow.PrimitiveTypes.Int64), nestedLeafNullable, true},
		{"nested list widening", arrow.ListOf(arrow.PrimitiveTypes.Int32), nestedLeafNullable, true},
		{"nested list of string", arrow.ListOf(arrow.BinaryTypes.String), nestedLeafNullable, false},
		{"nested scalar instead of list", arrow.PrimitiveTypes.Int64, nestedLeafNullable, false},
		{"nested list<null> nullable leaf", arrow.ListOf(arrow.Null), nestedLeafNullable, true},
		{"nested list<null> non-nullable leaf", arrow.ListOf(arrow.Null), nestedElementNullable, false},
		{"nested null type element-nullable", arrow.Null, nestedElementNullable, true},
		{"nested null type non-element-nullable", arrow.Null, nestedLeafNullable, false},
		{"vector list", arrow.ListOf(arrow.PrimitiveTypes.Float32), legacyVector, true},
		{"vector list<null>", arrow.ListOf(arrow.Null), legacyVector, false},
		{"vector null type legacy", arrow.Null, legacyVector, false},
		{"vector null type element-nullable", arrow.Null, nullableVector, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := isStructChildConvertible(tt.src, tt.field)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func enStructSchema(subFields ...*schemapb.FieldSchema) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 200,
			Name:    "s",
			Fields:  subFields,
		}},
	}
}

func enArrowStructSchema(children ...arrow.Field) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.ListOf(arrow.StructOf(children...))},
	}, nil)
}

func TestIsSchemaEqual_StructChildren(t *testing.T) {
	schema := enStructSchema(
		enScalarSubField(201, "s[a]", schemapb.DataType_Int64, false),
		enScalarSubField(202, "s[b]", schemapb.DataType_Int64, true),
	)
	a := arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64}
	b := arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	extra := arrow.Field{Name: "dropped", Type: arrow.BinaryTypes.String}

	t.Run("all children", func(t *testing.T) {
		assert.NoError(t, isSchemaEqual(schema, enArrowStructSchema(a, b)))
	})
	t.Run("missing element-nullable child", func(t *testing.T) {
		assert.NoError(t, isSchemaEqual(schema, enArrowStructSchema(a)))
	})
	t.Run("missing non-element-nullable child", func(t *testing.T) {
		err := isSchemaEqual(schema, enArrowStructSchema(b))
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "sub-field 'a' not found")
	})
	t.Run("extra child ignored", func(t *testing.T) {
		assert.NoError(t, isSchemaEqual(schema, enArrowStructSchema(a, b, extra)))
		assert.NoError(t, isSchemaEqual(schema, enArrowStructSchema(extra, a)))
	})
	t.Run("null-typed child", func(t *testing.T) {
		assert.NoError(t, isSchemaEqual(schema, enArrowStructSchema(a, arrow.Field{Name: "b", Type: arrow.Null, Nullable: true})))
		err := isSchemaEqual(schema, enArrowStructSchema(arrow.Field{Name: "a", Type: arrow.Null, Nullable: true}, b))
		assert.ErrorIs(t, err, merr.ErrImportFailed)
	})
	t.Run("no known child", func(t *testing.T) {
		// the element-nullable sub-fields are read from the outer list shape
		allNullable := enStructSchema(enScalarSubField(202, "s[b]", schemapb.DataType_Int64, true))
		assert.NoError(t, isSchemaEqual(allNullable, enArrowStructSchema(extra)))
		err := isSchemaEqual(schema, enArrowStructSchema(extra))
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "sub-field 'a' not found")
	})
}

func TestStructFieldReader_ElementNullableScalar(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int32, Nullable: true})
	lb := array.NewListBuilder(mem, structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	vb := sb.FieldBuilder(0).(*array.Int32Builder)
	appendElement := func(v *int32) {
		sb.Append(true)
		if v == nil {
			vb.AppendNull()
		} else {
			vb.Append(*v)
		}
	}
	one, three := int32(1), int32(3)
	lb.Append(true) // [1, null, 3]
	appendElement(&one)
	appendElement(nil)
	appendElement(&three)
	lb.AppendNull() // null row
	lb.Append(true) // []
	lb.Append(true) // [null]
	appendElement(nil)
	chunked := enChunk(t, lb)

	field := enScalarSubField(202, "s[b]", schemapb.DataType_Int32, true)
	field.Nullable = true
	reader := &StructFieldReader{structName: "s", field: field}
	data, validData, err := reader.readArrayField(chunked)
	require.NoError(t, err)
	rows := data.([]*schemapb.ScalarField)
	require.Len(t, rows, 4)
	assert.Equal(t, []bool{true, false, true, true}, validData.([]bool))

	assert.Equal(t, []int32{1, 0, 3}, rows[0].GetIntData().GetData())
	assert.Equal(t, []bool{true, false, true}, typeutil.GetArrayElementValidData(rows[0]))
	assert.Empty(t, rows[2].GetIntData().GetData())
	assert.Empty(t, typeutil.GetArrayElementValidData(rows[2]))
	assert.Equal(t, []int32{0}, rows[3].GetIntData().GetData())
	assert.Equal(t, []bool{false}, typeutil.GetArrayElementValidData(rows[3]))

	t.Run("legacy rejects null element", func(t *testing.T) {
		legacy := enScalarSubField(201, "s[a]", schemapb.DataType_Int32, false)
		legacy.Nullable = true
		reader := &StructFieldReader{structName: "s", field: legacy}
		_, _, err := reader.readArrayField(chunked)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "array element is not allowed to be null")
	})
	t.Run("capacity counts null elements", func(t *testing.T) {
		small := enScalarSubField(202, "s[b]", schemapb.DataType_Int32, true)
		small.Nullable = true
		small.TypeParams = enCapacity("2")
		reader := &StructFieldReader{structName: "s", field: small}
		_, _, err := reader.readArrayField(chunked)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}

func TestStructFieldReader_NullTypedScalarChild(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{Name: "b", Type: arrow.Null, Nullable: true})
	lb := array.NewListBuilder(mem, structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	nb := sb.FieldBuilder(0).(*array.NullBuilder)
	lb.Append(true)
	for i := 0; i < 2; i++ {
		sb.Append(true)
		nb.AppendNull()
	}
	chunked := enChunk(t, lb)

	field := enScalarSubField(202, "s[b]", schemapb.DataType_VarChar, true)
	reader := &StructFieldReader{structName: "s", field: field}
	data, validData, err := reader.readArrayField(chunked)
	require.NoError(t, err)
	assert.Nil(t, validData)
	rows := data.([]*schemapb.ScalarField)
	require.Len(t, rows, 1)
	assert.Equal(t, []string{"", ""}, rows[0].GetStringData().GetData())
	assert.Equal(t, []bool{false, false}, typeutil.GetArrayElementValidData(rows[0]))

	legacy := &StructFieldReader{structName: "s", field: enScalarSubField(201, "s[a]", schemapb.DataType_VarChar, false)}
	_, _, err = legacy.readArrayField(chunked)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
}

func TestStructFieldReader_NullStructElementRejected(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	lb := array.NewListBuilder(mem, structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	vb := sb.FieldBuilder(0).(*array.Int64Builder)
	lb.Append(true)
	sb.Append(true)
	vb.Append(1)
	sb.AppendNull()
	chunked := enChunk(t, lb)

	for _, field := range []*schemapb.FieldSchema{
		enScalarSubField(201, "s[a]", schemapb.DataType_Int64, false),
		enScalarSubField(202, "s[b]", schemapb.DataType_Int64, true),
	} {
		reader := &StructFieldReader{structName: "s", field: field}
		_, _, err := reader.readArrayField(chunked)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "element 1 of struct field 's' is null")
	}
}

func TestStructFieldReader_ScalarWideningAndRange(t *testing.T) {
	mem := memory.NewGoAllocator()
	build := func(values ...int32) *arrow.Chunked {
		structType := arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32})
		lb := array.NewListBuilder(mem, structType)
		sb := lb.ValueBuilder().(*array.StructBuilder)
		vb := sb.FieldBuilder(0).(*array.Int32Builder)
		lb.Append(true)
		for _, v := range values {
			sb.Append(true)
			vb.Append(v)
		}
		return enChunk(t, lb)
	}

	t.Run("int32 into Int64 and Double", func(t *testing.T) {
		chunked := build(1, -2)
		reader := &StructFieldReader{structName: "s", field: enScalarSubField(201, "s[a]", schemapb.DataType_Int64, false)}
		data, _, err := reader.readArrayField(chunked)
		require.NoError(t, err)
		assert.Equal(t, []int64{1, -2}, data.([]*schemapb.ScalarField)[0].GetLongData().GetData())
		assert.Empty(t, typeutil.GetArrayElementValidData(data.([]*schemapb.ScalarField)[0]))

		reader = &StructFieldReader{structName: "s", field: enScalarSubField(201, "s[a]", schemapb.DataType_Double, false)}
		data, _, err = reader.readArrayField(chunked)
		require.NoError(t, err)
		assert.Equal(t, []float64{1, -2}, data.([]*schemapb.ScalarField)[0].GetDoubleData().GetData())
	})
	t.Run("Int8 and Int16 range", func(t *testing.T) {
		reader := &StructFieldReader{structName: "s", field: enScalarSubField(201, "s[a]", schemapb.DataType_Int8, true)}
		data, _, err := reader.readArrayField(build(127, -128))
		require.NoError(t, err)
		assert.Equal(t, []int32{127, -128}, data.([]*schemapb.ScalarField)[0].GetIntData().GetData())
		_, _, err = reader.readArrayField(build(300))
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "out of range")

		reader = &StructFieldReader{structName: "s", field: enScalarSubField(201, "s[a]", schemapb.DataType_Int16, false)}
		_, _, err = reader.readArrayField(build(40000))
		assert.ErrorIs(t, err, merr.ErrImportFailed)
	})
}

// enNestedChunk builds list<struct<tags: list<int64>>> from rows of inner
// arrays; a nil row is a null struct row, a nil inner array a null element and
// a nil leaf a null value.
func enNestedChunk(t *testing.T, rows [][][]*int64, nullRow []bool) *arrow.Chunked {
	t.Helper()
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true})
	lb := array.NewListBuilder(mem, structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	innerBuilder := sb.FieldBuilder(0).(*array.ListBuilder)
	leafBuilder := innerBuilder.ValueBuilder().(*array.Int64Builder)
	for i, row := range rows {
		if nullRow[i] {
			lb.AppendNull()
			continue
		}
		lb.Append(true)
		for _, inner := range row {
			sb.Append(true)
			if inner == nil {
				innerBuilder.AppendNull()
				continue
			}
			innerBuilder.Append(true)
			for _, v := range inner {
				if v == nil {
					leafBuilder.AppendNull()
				} else {
					leafBuilder.Append(*v)
				}
			}
		}
	}
	return enChunk(t, lb)
}

func enInt64(v int64) *int64 { return &v }

func TestStructFieldReader_NestedArray(t *testing.T) {
	// row 0: [[1, null], null, []], row 1: null row, row 2: []
	chunked := enNestedChunk(t, [][][]*int64{
		{{enInt64(1), nil}, nil, {}},
		nil,
		{},
	}, []bool{false, true, false})

	field := enNestedSubField(203, "s[tags]", schemapb.DataType_Int64, true, true)
	field.Nullable = true
	reader := &StructFieldReader{structName: "s", field: field}
	data, validData, err := reader.readNestedArrayField(chunked)
	require.NoError(t, err)
	assert.Equal(t, []bool{true, false, true}, validData.([]bool))
	rows := data.([]*schemapb.ScalarField)
	require.Len(t, rows, 3)

	row := rows[0].GetArrayData()
	require.NotNil(t, row)
	assert.Equal(t, schemapb.DataType_Int64, row.GetElementType())
	assert.Equal(t, []bool{true, false, true}, typeutil.GetArrayElementValidData(rows[0]))
	require.Len(t, row.GetData(), 3)
	assert.Equal(t, []int64{1, 0}, row.GetData()[0].GetLongData().GetData())
	assert.Equal(t, []bool{true, false}, typeutil.GetArrayElementValidData(row.GetData()[0]))
	// a null inner array is a typed empty leaf without valid_data
	require.NotNil(t, row.GetData()[1].GetLongData())
	assert.Empty(t, row.GetData()[1].GetLongData().GetData())
	assert.Empty(t, typeutil.GetArrayElementValidData(row.GetData()[1]))
	require.NotNil(t, row.GetData()[2].GetLongData())
	assert.Empty(t, row.GetData()[2].GetLongData().GetData())

	assert.Equal(t, schemapb.DataType_Int64, rows[1].GetArrayData().GetElementType())
	assert.Empty(t, rows[2].GetArrayData().GetData())
	assert.Empty(t, typeutil.GetArrayElementValidData(rows[2]))

	t.Run("non-nullable leaf has no leaf valid_data", func(t *testing.T) {
		chunked := enNestedChunk(t, [][][]*int64{{{enInt64(4), enInt64(5)}, nil}}, []bool{false})
		reader := &StructFieldReader{structName: "s", field: enNestedSubField(203, "s[tags]", schemapb.DataType_Int64, true, false)}
		data, validData, err := reader.readNestedArrayField(chunked)
		require.NoError(t, err)
		assert.Nil(t, validData)
		row := data.([]*schemapb.ScalarField)[0]
		assert.Equal(t, []bool{true, false}, typeutil.GetArrayElementValidData(row))
		assert.Equal(t, []int64{4, 5}, row.GetArrayData().GetData()[0].GetLongData().GetData())
		assert.Empty(t, typeutil.GetArrayElementValidData(row.GetArrayData().GetData()[0]))
	})
	t.Run("non-element-nullable has no element valid_data", func(t *testing.T) {
		chunked := enNestedChunk(t, [][][]*int64{{{enInt64(4)}, {}}}, []bool{false})
		reader := &StructFieldReader{structName: "s", field: enNestedSubField(203, "s[tags]", schemapb.DataType_Int64, false, false)}
		data, _, err := reader.readNestedArrayField(chunked)
		require.NoError(t, err)
		row := data.([]*schemapb.ScalarField)[0]
		assert.Empty(t, typeutil.GetArrayElementValidData(row))
		assert.Len(t, row.GetArrayData().GetData(), 2)
	})
	t.Run("null inner array rejected when not element-nullable", func(t *testing.T) {
		reader := &StructFieldReader{structName: "s", field: enNestedSubField(203, "s[tags]", schemapb.DataType_Int64, false, true)}
		reader.field.Nullable = true
		_, _, err := reader.readNestedArrayField(chunked)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
	})
	t.Run("null leaf rejected when leaf not nullable", func(t *testing.T) {
		reader := &StructFieldReader{structName: "s", field: enNestedSubField(203, "s[tags]", schemapb.DataType_Int64, true, false)}
		reader.field.Nullable = true
		_, _, err := reader.readNestedArrayField(chunked)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "leaf is not allowed to be null")
	})
	t.Run("inner capacity", func(t *testing.T) {
		chunked := enNestedChunk(t, [][][]*int64{{{enInt64(1), enInt64(2), enInt64(3), enInt64(4)}}}, []bool{false})
		reader := &StructFieldReader{structName: "s", field: enNestedSubField(203, "s[tags]", schemapb.DataType_Int64, true, true)}
		_, _, err := reader.readNestedArrayField(chunked)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
	t.Run("outer capacity counts null inner arrays", func(t *testing.T) {
		chunked := enNestedChunk(t, [][][]*int64{{nil, nil, nil, nil, nil}}, []bool{false})
		reader := &StructFieldReader{structName: "s", field: enNestedSubField(203, "s[tags]", schemapb.DataType_Int64, true, true)}
		_, _, err := reader.readNestedArrayField(chunked)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}

func TestStructFieldReader_NestedArrayNullTypes(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("null-typed child", func(t *testing.T) {
		structType := arrow.StructOf(arrow.Field{Name: "tags", Type: arrow.Null, Nullable: true})
		lb := array.NewListBuilder(mem, structType)
		sb := lb.ValueBuilder().(*array.StructBuilder)
		nb := sb.FieldBuilder(0).(*array.NullBuilder)
		lb.Append(true)
		sb.Append(true)
		nb.AppendNull()
		chunked := enChunk(t, lb)

		reader := &StructFieldReader{structName: "s", field: enNestedSubField(203, "s[tags]", schemapb.DataType_VarChar, true, false)}
		data, _, err := reader.readNestedArrayField(chunked)
		require.NoError(t, err)
		row := data.([]*schemapb.ScalarField)[0]
		assert.Equal(t, []bool{false}, typeutil.GetArrayElementValidData(row))
		require.Len(t, row.GetArrayData().GetData(), 1)
		require.NotNil(t, row.GetArrayData().GetData()[0].GetStringData())

		reader = &StructFieldReader{structName: "s", field: enNestedSubField(203, "s[tags]", schemapb.DataType_VarChar, false, false)}
		_, _, err = reader.readNestedArrayField(chunked)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
	})
	t.Run("list<null> child", func(t *testing.T) {
		structType := arrow.StructOf(arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.Null), Nullable: true})
		lb := array.NewListBuilder(mem, structType)
		sb := lb.ValueBuilder().(*array.StructBuilder)
		ib := sb.FieldBuilder(0).(*array.ListBuilder)
		nb := ib.ValueBuilder().(*array.NullBuilder)
		lb.Append(true)
		sb.Append(true)
		ib.Append(true)
		nb.AppendNull()
		nb.AppendNull()
		chunked := enChunk(t, lb)

		reader := &StructFieldReader{structName: "s", field: enNestedSubField(203, "s[tags]", schemapb.DataType_Int32, false, true)}
		data, _, err := reader.readNestedArrayField(chunked)
		require.NoError(t, err)
		inner := data.([]*schemapb.ScalarField)[0].GetArrayData().GetData()[0]
		assert.Equal(t, []int32{0, 0}, inner.GetIntData().GetData())
		assert.Equal(t, []bool{false, false}, typeutil.GetArrayElementValidData(inner))
	})
	t.Run("VarChar leaf max_length from the leaf node", func(t *testing.T) {
		structType := arrow.StructOf(arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true})
		lb := array.NewListBuilder(mem, structType)
		sb := lb.ValueBuilder().(*array.StructBuilder)
		ib := sb.FieldBuilder(0).(*array.ListBuilder)
		vb := ib.ValueBuilder().(*array.StringBuilder)
		lb.Append(true)
		sb.Append(true)
		ib.Append(true)
		vb.Append("abcd")
		vb.Append("abcde")
		chunked := enChunk(t, lb)

		reader := &StructFieldReader{structName: "s", field: enNestedSubField(203, "s[tags]", schemapb.DataType_VarChar, false, false)}
		_, _, err := reader.readNestedArrayField(chunked)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}

// enVectorChunk builds list<struct<emb: list<float32>>>; a nil vector is a null element.
func enVectorChunk(t *testing.T, rows [][][]float32) *arrow.Chunked {
	t.Helper()
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{Name: "emb", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: true})
	lb := array.NewListBuilder(mem, structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	vecBuilder := sb.FieldBuilder(0).(*array.ListBuilder)
	floatBuilder := vecBuilder.ValueBuilder().(*array.Float32Builder)
	for _, row := range rows {
		lb.Append(true)
		for _, vec := range row {
			sb.Append(true)
			if vec == nil {
				vecBuilder.AppendNull()
				continue
			}
			vecBuilder.Append(true)
			floatBuilder.AppendValues(vec, nil)
		}
	}
	return enChunk(t, lb)
}

func TestStructFieldReader_ElementNullableVector(t *testing.T) {
	chunked := enVectorChunk(t, [][][]float32{
		{{1, 2}, nil, {3, 4}},
		{nil},
		{},
	})

	reader := &StructFieldReader{structName: "s", field: enVectorSubField(205, "s[emb]", schemapb.DataType_FloatVector, true), dim: 2}
	data, validData, err := reader.readArrayOfVectorField(chunked)
	require.NoError(t, err)
	assert.Nil(t, validData)
	rows := data.([]*schemapb.VectorField)
	require.Len(t, rows, 3)
	assert.Equal(t, int64(2), rows[0].GetDim())
	assert.Equal(t, []float32{1, 2, 3, 4}, rows[0].GetFloatVector().GetData())
	assert.Equal(t, []bool{true, false, true}, typeutil.GetVectorArrayElementValidData(rows[0]))
	require.NotNil(t, rows[1].GetFloatVector())
	assert.Empty(t, rows[1].GetFloatVector().GetData())
	assert.Equal(t, []bool{false}, typeutil.GetVectorArrayElementValidData(rows[1]))
	assert.Empty(t, typeutil.GetVectorArrayElementValidData(rows[2]))

	t.Run("legacy rejects null vector", func(t *testing.T) {
		reader := &StructFieldReader{structName: "s", field: enVectorSubField(205, "s[emb]", schemapb.DataType_FloatVector, false), dim: 2}
		_, _, err := reader.readArrayOfVectorField(chunked)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
	})
	t.Run("legacy has no element valid_data", func(t *testing.T) {
		chunked := enVectorChunk(t, [][][]float32{{{1, 2}}})
		reader := &StructFieldReader{structName: "s", field: enVectorSubField(205, "s[emb]", schemapb.DataType_FloatVector, false), dim: 2}
		data, _, err := reader.readArrayOfVectorField(chunked)
		require.NoError(t, err)
		assert.Empty(t, typeutil.GetVectorArrayElementValidData(data.([]*schemapb.VectorField)[0]))
	})
	t.Run("dim mismatch of a valid vector", func(t *testing.T) {
		chunked := enVectorChunk(t, [][][]float32{{nil, {1, 2, 3}}})
		_, _, err := reader.readArrayOfVectorField(chunked)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
	})
}

func TestStructFieldReader_ElementNullableByteVector(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{Name: "emb", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint8), Nullable: true})
	lb := array.NewListBuilder(mem, structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	vecBuilder := sb.FieldBuilder(0).(*array.ListBuilder)
	byteBuilder := vecBuilder.ValueBuilder().(*array.Uint8Builder)
	lb.Append(true)
	sb.Append(true)
	vecBuilder.AppendNull()
	sb.Append(true)
	vecBuilder.Append(true)
	byteBuilder.AppendValues([]uint8{1, 2, 3, 4}, nil)
	chunked := enChunk(t, lb)

	reader := &StructFieldReader{structName: "s", field: enVectorSubField(205, "s[emb]", schemapb.DataType_Float16Vector, true), dim: 2}
	data, _, err := reader.readArrayOfVectorField(chunked)
	require.NoError(t, err)
	row := data.([]*schemapb.VectorField)[0]
	assert.Equal(t, []byte{1, 2, 3, 4}, row.GetFloat16Vector())
	assert.Equal(t, []bool{false, true}, typeutil.GetVectorArrayElementValidData(row))
}

// enVectorPayloadChunk builds list<struct<emb: list<leaf>>> rows; a nil vector
// is a null vector.
func enVectorPayloadChunk(t *testing.T, leaf arrow.DataType, rows [][][]int) *arrow.Chunked {
	t.Helper()
	structType := arrow.StructOf(arrow.Field{Name: "emb", Type: arrow.ListOf(leaf), Nullable: true})
	lb := array.NewListBuilder(memory.NewGoAllocator(), structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	vecBuilder := sb.FieldBuilder(0).(*array.ListBuilder)
	for _, row := range rows {
		lb.Append(true)
		for _, vec := range row {
			sb.Append(true)
			if vec == nil {
				vecBuilder.AppendNull()
				continue
			}
			vecBuilder.Append(true)
			for _, v := range vec {
				switch b := vecBuilder.ValueBuilder().(type) {
				case *array.Float32Builder:
					b.Append(float32(v))
				case *array.Int8Builder:
					b.Append(int8(v))
				case *array.Uint8Builder:
					b.Append(uint8(v))
				default:
					require.FailNow(t, "unexpected leaf builder", "%T", b)
				}
			}
		}
	}
	return enChunk(t, lb)
}

// TestStructFieldReader_VectorPayloadCapacity checks that the payload of a row
// is allocated for its present vectors only: the null vectors of a typed list
// child must not leave capacity behind, which the batch memory accounting
// (it counts the payload length) would not see.
func TestStructFieldReader_VectorPayloadCapacity(t *testing.T) {
	const elements = 4
	allNull := make([][]int, elements)
	fewValid := make([][]int, elements)
	cases := []struct {
		name        string
		elementType schemapb.DataType
		leaf        arrow.DataType
		width       int // payload units per vector, dim 2
		payload     func(*schemapb.VectorField) int
	}{
		{
			"float", schemapb.DataType_FloatVector, arrow.PrimitiveTypes.Float32, 2,
			func(v *schemapb.VectorField) int { return cap(v.GetFloatVector().GetData()) },
		},
		{
			"int8", schemapb.DataType_Int8Vector, arrow.PrimitiveTypes.Int8, 2,
			func(v *schemapb.VectorField) int { return cap(v.GetInt8Vector()) },
		},
		{
			"float16", schemapb.DataType_Float16Vector, arrow.PrimitiveTypes.Uint8, 4,
			func(v *schemapb.VectorField) int { return cap(v.GetFloat16Vector()) },
		},
		{
			"bfloat16", schemapb.DataType_BFloat16Vector, arrow.PrimitiveTypes.Uint8, 4,
			func(v *schemapb.VectorField) int { return cap(v.GetBfloat16Vector()) },
		},
		{
			"binary", schemapb.DataType_BinaryVector, arrow.PrimitiveTypes.Uint8, 1,
			func(v *schemapb.VectorField) int { return cap(v.GetBinaryVector()) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vec := make([]int, tc.width)
			for i := range vec {
				vec[i] = i + 1
			}
			fewValid[1] = vec
			chunked := enVectorPayloadChunk(t, tc.leaf, [][][]int{allNull, fewValid})
			reader := &StructFieldReader{structName: "s", field: enVectorSubField(205, "s[emb]", tc.elementType, true), dim: 2}
			data, _, err := reader.readArrayOfVectorField(chunked)
			require.NoError(t, err)
			rows := data.([]*schemapb.VectorField)
			require.Len(t, rows, 2)

			assert.Equal(t, 0, tc.payload(rows[0]), "all-null row")
			assert.Equal(t, make([]bool, elements), typeutil.GetVectorArrayElementValidData(rows[0]))

			assert.Equal(t, tc.width, tc.payload(rows[1]), "row with one present vector")
			assert.Equal(t, []bool{false, true, false, false}, typeutil.GetVectorArrayElementValidData(rows[1]))
		})
	}
}

// enShapeChunk builds list<struct<dropped: int64>> rows of the given element
// counts; a negative count is a null row.
func enShapeChunk(t *testing.T, counts ...int) *arrow.Chunked {
	t.Helper()
	structType := arrow.StructOf(arrow.Field{Name: "dropped", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	lb := array.NewListBuilder(memory.NewGoAllocator(), structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	vb := sb.FieldBuilder(0).(*array.Int64Builder)
	for _, count := range counts {
		if count < 0 {
			lb.AppendNull()
			continue
		}
		lb.Append(true)
		for j := 0; j < count; j++ {
			sb.Append(true)
			vb.Append(int64(j))
		}
	}
	return enChunk(t, lb)
}

// TestStructFieldReader_NullElementsOnly covers the reader of a sub-field
// whose struct column carries none of the current sub-fields: the rows come
// from the outer list shape and equal what the DataNode fill builds.
func TestStructFieldReader_NullElementsOnly(t *testing.T) {
	subFields := []*schemapb.FieldSchema{
		enScalarSubField(202, "s[b]", schemapb.DataType_VarChar, true),
		enNestedSubField(204, "s[tags]", schemapb.DataType_Int64, true, true),
		enVectorSubField(205, "s[emb]", schemapb.DataType_FloatVector, true),
	}
	counts := []int{2, -1, 0, 1}

	for _, nullable := range []bool{true, false} {
		for _, base := range subFields {
			field := proto.Clone(base).(*schemapb.FieldSchema)
			field.Nullable = nullable
			name := field.GetName()
			if nullable {
				name += "/nullable"
			}
			t.Run(name, func(t *testing.T) {
				reader := &StructFieldReader{structName: "s", field: field, dim: 2, nullElementsOnly: true}
				if !nullable {
					_, _, err := reader.readNullElementsField(enShapeChunk(t, counts...))
					assert.ErrorIs(t, err, merr.ErrImportFailed)

					data, validData, err := reader.readNullElementsField(enShapeChunk(t, 2, 0))
					require.NoError(t, err)
					assert.Nil(t, validData)
					requireNullElementRows(t, field, data, []int{2, 0})
					return
				}
				data, validData, err := reader.readNullElementsField(enShapeChunk(t, counts...))
				require.NoError(t, err)
				assert.Equal(t, []bool{true, false, true, true}, validData)
				requireNullElementRows(t, field, data, counts)
			})
		}
	}

	t.Run("null struct slot rejected", func(t *testing.T) {
		structType := arrow.StructOf(arrow.Field{Name: "dropped", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
		lb := array.NewListBuilder(memory.NewGoAllocator(), structType)
		sb := lb.ValueBuilder().(*array.StructBuilder)
		lb.Append(true)
		sb.AppendNull()
		reader := &StructFieldReader{structName: "s", field: subFields[0], nullElementsOnly: true}
		_, _, err := reader.readNullElementsField(enChunk(t, lb))
		assert.ErrorIs(t, err, merr.ErrImportFailed)
		assert.Contains(t, err.Error(), "cannot be null")
	})
	t.Run("capacity checked", func(t *testing.T) {
		reader := &StructFieldReader{structName: "s", field: subFields[0], nullElementsOnly: true}
		_, _, err := reader.readNullElementsField(enShapeChunk(t, 5))
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}

// requireNullElementRows checks rows against the storage helpers the DataNode
// fill uses; a negative count is a null row.
func requireNullElementRows(t *testing.T, field *schemapb.FieldSchema, data any, counts []int) {
	t.Helper()
	for i, count := range counts {
		switch rows := data.(type) {
		case []*schemapb.ScalarField:
			require.Len(t, rows, len(counts))
			if count < 0 {
				assert.Nil(t, rows[i])
				continue
			}
			want, err := storage.NullElementsArrayRow(field, count)
			require.NoError(t, err)
			assert.True(t, proto.Equal(want, rows[i]), "row %d: want %v, got %v", i, want, rows[i])
		case []*schemapb.VectorField:
			require.Len(t, rows, len(counts))
			if count < 0 {
				assert.True(t, proto.Equal(emptyVectorArrayRow(2, field.GetElementType()), rows[i]))
				continue
			}
			want, err := storage.NullElementsVectorArrayRow(field, 2, count)
			require.NoError(t, err)
			assert.True(t, proto.Equal(want, rows[i]), "row %d: want %v, got %v", i, want, rows[i])
		default:
			require.FailNow(t, "unexpected rows", "%T", data)
		}
	}
}

func TestStructFieldReader_NullTypedVectorChild(t *testing.T) {
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{Name: "emb", Type: arrow.Null, Nullable: true})
	lb := array.NewListBuilder(mem, structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	nb := sb.FieldBuilder(0).(*array.NullBuilder)
	lb.Append(true)
	for i := 0; i < 3; i++ {
		sb.Append(true)
		nb.AppendNull()
	}
	chunked := enChunk(t, lb)

	reader := &StructFieldReader{structName: "s", field: enVectorSubField(205, "s[emb]", schemapb.DataType_FloatVector, true), dim: 2}
	data, _, err := reader.readArrayOfVectorField(chunked)
	require.NoError(t, err)
	row := data.([]*schemapb.VectorField)[0]
	require.NotNil(t, row.GetFloatVector())
	assert.Empty(t, row.GetFloatVector().GetData())
	assert.Equal(t, []bool{false, false, false}, typeutil.GetVectorArrayElementValidData(row))

	reader = &StructFieldReader{structName: "s", field: enVectorSubField(205, "s[emb]", schemapb.DataType_FloatVector, false), dim: 2}
	_, _, err = reader.readArrayOfVectorField(chunked)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
}

// TestCreateFieldReaders_StructSubFieldEvolution reads a parquet file whose
// struct column lacks an element-nullable sub-field and carries an unknown
// one, through the real column readers.
func TestCreateFieldReaders_StructSubFieldEvolution(t *testing.T) {
	ctx := context.Background()
	schema := enStructSchema(
		enScalarSubField(201, "s[a]", schemapb.DataType_Int64, false),
		enScalarSubField(202, "s[b]", schemapb.DataType_VarChar, true),
		enScalarSubField(203, "s[c]", schemapb.DataType_Int32, true),
		enNestedSubField(204, "s[tags]", schemapb.DataType_Int64, true, true),
		enVectorSubField(205, "s[emb]", schemapb.DataType_FloatVector, true),
	)

	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "dropped", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		arrow.Field{Name: "emb", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: true},
	)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.ListOf(structType)},
	}, nil)

	pkBuilder := array.NewInt64Builder(mem)
	pkBuilder.AppendValues([]int64{1, 2}, nil)
	pkArray := pkBuilder.NewArray()
	pkBuilder.Release()
	defer pkArray.Release()

	lb := array.NewListBuilder(mem, structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	aBuilder := sb.FieldBuilder(0).(*array.Int64Builder)
	droppedBuilder := sb.FieldBuilder(1).(*array.Int64Builder)
	bBuilder := sb.FieldBuilder(2).(*array.StringBuilder)
	tagsBuilder := sb.FieldBuilder(3).(*array.ListBuilder)
	tagBuilder := tagsBuilder.ValueBuilder().(*array.Int64Builder)
	embBuilder := sb.FieldBuilder(4).(*array.ListBuilder)
	floatBuilder := embBuilder.ValueBuilder().(*array.Float32Builder)

	// row 0: two elements; row 1: one element
	lb.Append(true)
	sb.Append(true)
	aBuilder.Append(10)
	droppedBuilder.Append(99)
	bBuilder.Append("x")
	tagsBuilder.Append(true)
	tagBuilder.Append(1)
	tagBuilder.AppendNull()
	embBuilder.Append(true)
	floatBuilder.AppendValues([]float32{0.1, 0.2}, nil)
	sb.Append(true)
	aBuilder.Append(11)
	droppedBuilder.AppendNull()
	bBuilder.AppendNull()
	tagsBuilder.AppendNull()
	embBuilder.AppendNull()
	lb.Append(true)
	sb.Append(true)
	aBuilder.Append(12)
	droppedBuilder.Append(98)
	bBuilder.Append("y")
	tagsBuilder.Append(true)
	embBuilder.Append(true)
	floatBuilder.AppendValues([]float32{0.3, 0.4}, nil)
	structArray := lb.NewArray()
	lb.Release()
	defer structArray.Release()

	record := array.NewRecord(arrowSchema, []arrow.Array{pkArray, structArray}, 2)
	defer record.Release()

	var buf bytes.Buffer
	fw, err := pqarrow.NewFileWriter(arrowSchema, &buf, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(2)), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, fw.Write(record))
	require.NoError(t, fw.Close())

	openFileReader := func() *pqarrow.FileReader {
		pqReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)
		t.Cleanup(func() { _ = pqReader.Close() })
		fileReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 64}, memory.DefaultAllocator)
		require.NoError(t, err)
		return fileReader
	}

	crs, err := CreateFieldReaders(ctx, openFileReader(), schema)
	require.NoError(t, err)
	assert.Len(t, crs, 5)
	assert.NotContains(t, crs, int64(203))

	read := func(fieldID int64) (any, any) {
		data, validData, err := crs[fieldID].Next(64)
		require.NoError(t, err)
		return data, validData
	}

	aRows, _ := read(201)
	assert.Equal(t, []int64{10, 11}, aRows.([]*schemapb.ScalarField)[0].GetLongData().GetData())
	assert.Equal(t, []int64{12}, aRows.([]*schemapb.ScalarField)[1].GetLongData().GetData())

	bRows, _ := read(202)
	assert.Equal(t, []string{"x", ""}, bRows.([]*schemapb.ScalarField)[0].GetStringData().GetData())
	assert.Equal(t, []bool{true, false}, typeutil.GetArrayElementValidData(bRows.([]*schemapb.ScalarField)[0]))

	tagRows, _ := read(204)
	tags := tagRows.([]*schemapb.ScalarField)[0]
	assert.Equal(t, []bool{true, false}, typeutil.GetArrayElementValidData(tags))
	assert.Equal(t, []int64{1, 0}, tags.GetArrayData().GetData()[0].GetLongData().GetData())
	assert.Equal(t, []bool{true, false}, typeutil.GetArrayElementValidData(tags.GetArrayData().GetData()[0]))
	assert.Equal(t, []bool{true}, typeutil.GetArrayElementValidData(tagRows.([]*schemapb.ScalarField)[1]))

	embRows, _ := read(205)
	emb := embRows.([]*schemapb.VectorField)[0]
	assert.Equal(t, []float32{0.1, 0.2}, emb.GetFloatVector().GetData())
	assert.Equal(t, []bool{true, false}, typeutil.GetVectorArrayElementValidData(emb))

	t.Run("missing non-element-nullable sub-field", func(t *testing.T) {
		strict := enStructSchema(
			enScalarSubField(201, "s[a]", schemapb.DataType_Int64, false),
			enScalarSubField(206, "s[z]", schemapb.DataType_Int64, false),
		)
		_, err := CreateFieldReaders(ctx, openFileReader(), strict)
		assert.ErrorIs(t, err, merr.ErrImportFailed)
	})
}
