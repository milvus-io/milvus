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
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestGenerateEmptyArray(t *testing.T) {
	type testCase struct {
		tag         string
		field       *schemapb.FieldSchema
		expectErr   bool
		expectNull  bool
		expectValue any
	}

	cases := []testCase{
		{
			tag: "no_default_value",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int8,
				Nullable: true,
			},
			expectErr:  false,
			expectNull: true,
		},
		{
			tag: "int8",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int8,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_IntData{
						IntData: 10,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: int8(10),
		},
		{
			tag: "int16",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int16,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_IntData{
						IntData: 16,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: int16(16),
		},
		{
			tag: "int32",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int32,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_IntData{
						IntData: 32,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: int32(32),
		},
		{
			tag: "int64",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int64,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_LongData{
						LongData: 64,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: int64(64),
		},
		{
			tag: "bool",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Bool,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_BoolData{
						BoolData: true,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: true,
		},
		{
			tag: "float",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Float,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_FloatData{
						FloatData: 0.1,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: float32(0.1),
		},
		{
			tag: "double",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Double,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_DoubleData{
						DoubleData: 1.2,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: float64(1.2),
		},
		{
			tag: "varchar",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_VarChar,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_StringData{
						StringData: "varchar",
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: "varchar",
		},
		{
			tag: "invalid_schema_datatype",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_FloatVector,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_IntData{
						IntData: 10,
					},
				},
			},
			expectErr: true,
		},
		{
			tag: "invalid_schema_nullable",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int8,
				Nullable: false,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_IntData{
						IntData: 10,
					},
				},
			},
			expectErr: true,
		},
		{
			tag: "internal_default_json",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_JSON,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_BytesData{
						BytesData: []byte(`{}`),
					},
				},
			},
			expectValue: []byte(`{}`),
		},
		{
			tag: "geometry",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Geometry,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_StringData{
						StringData: "POINT (1 2)",
					},
				},
			},
			expectValue: func() []byte {
				wkb, err := common.ConvertWKTToWKB("POINT (1 2)")
				require.NoError(t, err)
				return wkb
			}(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			rowNum := rand.Intn(100) + 1
			a, err := GenerateEmptyArrayFromSchema(tc.field, rowNum)
			switch {
			case tc.expectErr:
				assert.Error(t, err)
			case tc.expectNull:
				assert.NoError(t, err)
				assert.EqualValues(t, rowNum, a.Len())
				for i := range rowNum {
					assert.True(t, a.IsNull(i))
				}
			default:
				assert.NoError(t, err)
				assert.EqualValues(t, rowNum, a.Len())
				for i := range rowNum {
					value, deserErr := serdeMap[tc.field.DataType].deserialize(a, i, schemapb.DataType_None, 0, false, false)
					assert.True(t, a.IsValid(i))
					assert.NoError(t, deserErr)
					assert.Equal(t, tc.expectValue, value)
				}
			}
		})
	}
}

func TestRecordBuilderFillsNullableGeometryDefault(t *testing.T) {
	const defaultWKT = "POINT (1 2)"
	defaultWKB, err := common.ConvertWKTToWKB(defaultWKT)
	require.NoError(t, err)

	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "geom",
		DataType: schemapb.DataType_Geometry,
		Nullable: true,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{StringData: defaultWKT},
		},
	}

	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	builder.AppendNull()
	srcArr := builder.NewArray()
	defer srcArr.Release()

	src := NewSimpleArrowRecord(array.NewRecord(
		arrow.NewSchema([]arrow.Field{{Name: field.Name, Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil),
		[]arrow.Array{srcArr},
		int64(srcArr.Len()),
	), map[FieldID]int{field.FieldID: 0})
	defer src.Release()

	rb := NewRecordBuilder(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}})
	defer rb.Release()
	require.NoError(t, rb.Append(src, 0, 1))

	rebuilt := rb.Build()
	defer rebuilt.Release()
	out := rebuilt.Column(field.FieldID).(*array.Binary)
	require.True(t, out.IsValid(0))
	require.Equal(t, defaultWKB, out.Value(0))
	require.NotEmpty(t, out.Value(0))
	_, err = common.ConvertWKBToWKT(out.Value(0))
	require.NoError(t, err)
}

func TestRecordBuilderCachesNullableGeometryDefaultWKB(t *testing.T) {
	const defaultWKT = "POINT (1 2)"
	defaultWKB, err := common.ConvertWKTToWKB(defaultWKT)
	require.NoError(t, err)

	convertCalls := 0
	patch := mockey.Mock(common.ConvertWKTToWKB).To(func(wkt string) ([]byte, error) {
		convertCalls++
		require.Equal(t, defaultWKT, wkt)
		return defaultWKB, nil
	}).Build()
	defer patch.UnPatch()

	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "geom",
		DataType: schemapb.DataType_Geometry,
		Nullable: true,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{StringData: defaultWKT},
		},
	}

	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	builder.AppendNulls(5)
	srcArr := builder.NewArray()
	defer srcArr.Release()

	src := NewSimpleArrowRecord(array.NewRecord(
		arrow.NewSchema([]arrow.Field{{Name: field.Name, Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil),
		[]arrow.Array{srcArr},
		int64(srcArr.Len()),
	), map[FieldID]int{field.FieldID: 0})
	defer src.Release()

	rb := NewRecordBuilder(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}})
	defer rb.Release()
	require.NoError(t, rb.Append(src, 0, 2))
	require.NoError(t, rb.Append(src, 2, 5))
	require.Equal(t, 1, convertCalls)

	rebuilt := rb.Build()
	defer rebuilt.Release()
	out := rebuilt.Column(field.FieldID).(*array.Binary)
	for i := 0; i < out.Len(); i++ {
		require.True(t, out.IsValid(i))
		require.Equal(t, defaultWKB, out.Value(i))
	}
}

func TestGenerateEmptyArrayFromSchemaNullableDenseVectorUsesBinaryArrow(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "nullable_float_vector",
		DataType: schemapb.DataType_FloatVector,
		Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "4"},
		},
	}

	arr, err := GenerateEmptyArrayFromSchema(field, 3)
	defer arr.Release()

	assert.NoError(t, err)
	assert.Equal(t, arrow.BinaryTypes.Binary, arr.DataType())
	assert.Equal(t, 3, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		assert.True(t, arr.IsNull(i))
	}
}

func TestGenerateEmptyArrayFromSchemaNullableTextUsesBinaryArrow(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "added_text",
		DataType: schemapb.DataType_Text,
		Nullable: true,
	}

	arr, err := GenerateEmptyArrayFromSchema(field, 3)
	defer arr.Release()

	assert.NoError(t, err)
	assert.Equal(t, arrow.BinaryTypes.Binary, arr.DataType())
	assert.Equal(t, 3, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		assert.True(t, arr.IsNull(i))
	}
}

func TestGenerateEmptyArrayFromSchemaNullableVectorAppendsToRecordBuilder(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "embeddings_new",
		DataType: schemapb.DataType_FloatVector,
		Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "4"},
		},
	}
	arr, err := GenerateEmptyArrayFromSchema(field, 3)
	assert.NoError(t, err)
	defer arr.Release()
	assert.IsType(t, &array.Binary{}, arr)

	rec := NewSimpleArrowRecord(array.NewRecord(
		arrow.NewSchema([]arrow.Field{{Name: field.Name, Type: arr.DataType(), Nullable: true}}, nil),
		[]arrow.Array{arr},
		int64(arr.Len()),
	), map[FieldID]int{field.FieldID: 0})
	defer rec.Release()

	rb := NewRecordBuilder(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}})
	defer rb.Release()
	assert.NoError(t, rb.Append(rec, 0, arr.Len()))
	rebuilt := rb.Build()
	defer rebuilt.Release()
	assert.Equal(t, arr.Len(), rebuilt.Column(field.FieldID).Len())
	for i := 0; i < arr.Len(); i++ {
		assert.True(t, rebuilt.Column(field.FieldID).IsNull(i))
	}
}

func TestAppendValueAtRejectsNullFixedSizeBinaryChild(t *testing.T) {
	valueType := &arrow.FixedSizeBinaryType{ByteWidth: 8}
	sourceBuilder := array.NewListBuilder(memory.DefaultAllocator, valueType)
	defer sourceBuilder.Release()
	sourceBuilder.Append(true)
	sourceValues := sourceBuilder.ValueBuilder().(*array.FixedSizeBinaryBuilder)
	sourceValues.Append(make([]byte, valueType.ByteWidth))
	sourceValues.AppendNull()
	sourceValues.Append(make([]byte, valueType.ByteWidth))
	source := sourceBuilder.NewArray()
	defer source.Release()

	targetBuilder := array.NewListBuilder(memory.DefaultAllocator, valueType)
	defer targetBuilder.Release()
	size, err := appendValueAt(
		targetBuilder,
		source,
		0,
		&schemapb.FieldSchema{DataType: schemapb.DataType_ArrayOfVector},
		appendValueDefault{},
	)
	require.ErrorContains(t, err, "contains null child")
	require.Zero(t, size)
}

func TestAppendValueAtElementNullableArrayOfVectorUsesCompactBinaryPayload(t *testing.T) {
	const dim = 2
	sourceBuilder := array.NewListBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer sourceBuilder.Release()
	sourceBuilder.Append(true)
	sourceValues := sourceBuilder.ValueBuilder().(*array.BinaryBuilder)
	sourceValues.Append(make([]byte, dim*4))
	sourceValues.AppendNull()
	sourceValues.Append(make([]byte, dim*4))
	source := sourceBuilder.NewArray()
	defer source.Release()

	targetBuilder := array.NewListBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer targetBuilder.Release()
	size, err := appendValueAt(
		targetBuilder,
		source,
		0,
		&schemapb.FieldSchema{
			DataType:        schemapb.DataType_ArrayOfVector,
			ElementType:     schemapb.DataType_FloatVector,
			ElementNullable: true,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "2"},
			},
		},
		appendValueDefault{},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2*dim*4), size)

	result := targetBuilder.NewArray().(*array.List)
	defer result.Release()
	child := result.ListValues().(*array.Binary)
	require.Equal(t, 3, child.Len())
	require.True(t, child.IsNull(1))
}

func TestRecordBuilderNullableDenseVectorPreservesDimMetadata(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  100,
				Name:     "nullable_float_vector",
				DataType: schemapb.DataType_FloatVector,
				Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
		},
	}

	rb := NewRecordBuilder(schema)
	rec := rb.Build()
	defer rec.Release()

	field := rec.(*simpleArrowRecord).ArrowSchema().Field(0)
	assert.Equal(t, arrow.BinaryTypes.Binary, field.Type)
	dim, ok := field.Metadata.GetValue("dim")
	assert.True(t, ok)
	assert.Equal(t, "4", dim)
}

func TestRecordBuilderBuildReleasesCreatorRefs(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	original := memory.DefaultAllocator
	memory.DefaultAllocator = alloc
	defer func() { memory.DefaultAllocator = original }()

	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
	}}

	func() {
		ints := array.NewInt64Builder(alloc)
		defer ints.Release()
		ints.AppendValues([]int64{1, 2, 3}, nil)
		intArr := ints.NewInt64Array()
		defer intArr.Release()
		strs := array.NewStringBuilder(alloc)
		defer strs.Release()
		strs.AppendValues([]string{"a", "b", "c"}, nil)
		strArr := strs.NewStringArray()
		defer strArr.Release()

		src := NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema([]arrow.Field{
			{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
			{Name: "text", Type: arrow.BinaryTypes.String},
		}, nil), []arrow.Array{intArr, strArr}, 3), map[FieldID]int{100: 0, 101: 1})
		defer src.Release()

		rb := NewRecordBuilder(schema)
		defer rb.Release()
		require.NoError(t, rb.Append(src, 0, 3))

		// Build hands back the sole owner of its columns: a single Release
		// must return every column buffer to the allocator.
		rec := rb.Build()
		require.Equal(t, 3, rec.Column(100).Len())
		require.Equal(t, 3, rec.Column(101).Len())
		rec.Release()
	}()
	alloc.AssertSize(t, 0)
}
