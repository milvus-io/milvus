// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func runtimeEnumSchema(dt schemapb.DataType) *schemapb.FieldSchema {
	schema := &schemapb.FieldSchema{Name: "field", FieldID: 100, DataType: dt, Nullable: true}
	switch dt {
	case schemapb.DataType_Array:
		schema.ElementType, schema.ElementNullable = schemapb.DataType_Int16, true
	case schemapb.DataType_ArrayOfVector:
		schema.ElementType, schema.ElementNullable = schemapb.DataType_Float16Vector, true
		fallthrough
	case schemapb.DataType_FloatVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector:
		schema.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}
	case schemapb.DataType_BinaryVector:
		schema.TypeParams = []*commonpb.KeyValuePair{{Key: "dim", Value: "16"}}
	case schemapb.DataType_Decimal:
		schema.TypeParams = []*commonpb.KeyValuePair{{Key: "precision", Value: "18"}, {Key: "scale", Value: "2"}}
	}
	return schema
}

func TestRuntimeSchemaCoversEveryProtocolMilvusType(t *testing.T) {
	for enum, name := range schemapb.DataType_name {
		t.Run(name, func(t *testing.T) {
			dt := schemapb.DataType(enum)
			if dt == schemapb.DataType_None {
				_, err := ArrowFieldFromMilvusSchema(runtimeEnumSchema(dt))
				require.Error(t, err)
				return
			}
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			logical := runtimeEnumSchema(dt)
			children := []arrow.Field{}
			if dt == schemapb.DataType_Struct || dt == schemapb.DataType_ArrayOfStruct {
				child, err := ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "child", DataType: schemapb.DataType_Int64})
				require.NoError(t, err)
				children = append(children, child)
			}
			field, err := ArrowFieldFromMilvusSchema(logical, children...)
			require.NoError(t, err)
			require.NoError(t, ValidateRuntimeArrowSchema(arrow.NewSchema([]arrow.Field{field}, nil)))
			restored, err := MilvusSchemaFromArrowField(field)
			require.NoError(t, err)
			require.True(t, proto.Equal(logical, restored), "logical metadata did not round-trip")
			for _, count := range []int{0, 2} {
				values := array.MakeArrayOfNull(pool, field.Type, count)
				require.NoError(t, ValidateArrowArrayAgainstField(values, field))
				if count > 0 {
					nonnullable := field
					nonnullable.Nullable = false
					require.Error(t, ValidateArrowArrayAgainstField(values, nonnullable))
				}
				values.Release()
			}
		})
	}
}

func TestRuntimeSchemaRejectsInvalidLogicalAnnotationsEvenForEmptyArrays(t *testing.T) {
	base, err := ArrowFieldFromMilvusSchema(runtimeEnumSchema(schemapb.DataType_Float16Vector))
	require.NoError(t, err)
	for _, test := range []struct {
		name string
		edit func(*arrow.Field)
	}{
		{"conflicting_tag", func(f *arrow.Field) {
			m := f.Metadata.ToMap()
			m[MetadataKeyMilvusDataType] = strconv.Itoa(int(schemapb.DataType_BFloat16Vector))
			f.Metadata = arrow.MetadataFrom(m)
		}},
		{"conflicting_dimension", func(f *arrow.Field) {
			m := f.Metadata.ToMap()
			m["dim"] = "3"
			f.Metadata = arrow.MetadataFrom(m)
		}},
		{"incorrect_physical_width", func(f *arrow.Field) { f.Type = &arrow.FixedSizeBinaryType{ByteWidth: 6} }},
		{"unknown_tag", func(f *arrow.Field) {
			f.Metadata = arrow.MetadataFrom(map[string]string{MetadataKeyMilvusDataType: "99999"})
		}},
		{"none_tag", func(f *arrow.Field) {
			f.Metadata = arrow.MetadataFrom(map[string]string{MetadataKeyMilvusDataType: "0"})
		}},
		{"missing_dimension", func(f *arrow.Field) {
			f.Metadata = arrow.MetadataFrom(map[string]string{MetadataKeyMilvusDataType: "102"})
		}},
		{"malformed_full_schema", func(f *arrow.Field) {
			f.Metadata = arrow.MetadataFrom(map[string]string{MetadataKeyMilvusFieldSchema: "{"})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			field := base
			test.edit(&field)
			require.Error(t, ValidateRuntimeArrowSchema(arrow.NewSchema([]arrow.Field{field}, nil)))
			values := array.MakeArrayOfNull(memory.DefaultAllocator, field.Type, 0)
			defer values.Release()
			require.Error(t, ValidateArrowArrayAgainstField(values, field))
		})
	}
}

func TestRuntimeNestedNullabilityRespectsReachableParentValues(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	child := arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: false}
	structure := arrow.StructOf(child)
	b := array.NewStructBuilder(pool, structure)
	defer b.Release()
	b.AppendNull() // Null parent masks the padding null in its nonnullable child.
	b.Append(true)
	b.FieldBuilder(0).(*array.Int64Builder).AppendNull()
	values := b.NewArray()
	defer values.Release()
	field := arrow.Field{Name: "summary", Type: structure, Nullable: true}
	nullParent := array.NewSlice(values, 0, 1)
	defer nullParent.Release()
	require.NoError(t, ValidateArrowArrayAgainstField(nullParent, field))
	liveParent := array.NewSlice(values, 1, 2)
	defer liveParent.Release()
	require.Error(t, ValidateArrowArrayAgainstField(liveParent, field))
}

func TestRuntimeRecursiveTypeSchemaRoundTrip(t *testing.T) {
	logical := &schemapb.FieldSchema{
		Name: "nested", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Array,
		TypeSchema: &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
			Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
				Nullable: true, Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64},
			}},
		}}},
	}
	field, err := ArrowFieldFromMilvusSchema(logical)
	require.NoError(t, err)
	require.True(t, arrow.TypeEqual(arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true}), field.Type))
	restored, err := MilvusSchemaFromArrowField(field)
	require.NoError(t, err)
	require.True(t, proto.Equal(logical, restored))
	// Inference must retain every list layer and its leaf type as well.
	unannotated := field
	unannotated.Metadata = arrow.Metadata{}
	inferred, err := MilvusSchemaFromArrowField(unannotated)
	require.NoError(t, err)
	regenerated, err := ArrowFieldFromMilvusSchema(inferred)
	require.NoError(t, err)
	require.True(t, arrow.TypeEqual(field.Type, regenerated.Type))
}
