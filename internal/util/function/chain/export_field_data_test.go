/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Use slices with a nonzero offset and an empty chunk between populated chunks.
// This catches exporters that read backing buffers or stop at an empty chunk.
func addSlicedExportColumn(t *testing.T, b *DataFrameBuilder, values arrow.Array, empty bool) {
	t.Helper()
	var chunks []arrow.Array
	if empty {
		chunks = []arrow.Array{array.NewSlice(values, 0, 0)}
		b.SetChunkSizes([]int64{0})
	} else {
		require.Equal(t, 4, values.Len())
		chunks = []arrow.Array{array.NewSlice(values, 1, 2), array.NewSlice(values, 2, 2), array.NewSlice(values, 2, 4)}
		b.SetChunkSizes([]int64{1, 0, 2})
	}
	require.NoError(t, b.AddColumnFromChunks("value", chunks))
}

func scalarExportValues(field *schemapb.FieldData) any {
	s := field.GetScalars()
	switch field.Type {
	case schemapb.DataType_Bool:
		return s.GetBoolData().GetData()
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return s.GetIntData().GetData()
	case schemapb.DataType_Int64:
		return s.GetLongData().GetData()
	case schemapb.DataType_Timestamptz:
		return s.GetTimestamptzData().GetData()
	case schemapb.DataType_Float:
		return s.GetFloatData().GetData()
	case schemapb.DataType_Double:
		return s.GetDoubleData().GetData()
	default:
		return s.GetStringData().GetData()
	}
}

func TestExportFieldDataScalarMatrix(t *testing.T) {
	for _, tc := range []struct {
		logical     schemapb.DataType
		physical    arrow.DataType
		input, zero string
		want        any
	}{
		{schemapb.DataType_Bool, arrow.FixedWidthTypes.Boolean, `[false,true,%s,false]`, "false", []bool{true, false, false}},
		{schemapb.DataType_Int8, arrow.PrimitiveTypes.Int8, `[0,-128,%s,127]`, "0", []int32{-128, 0, 127}},
		{schemapb.DataType_Int16, arrow.PrimitiveTypes.Int16, `[0,-32768,%s,32767]`, "0", []int32{-32768, 0, 32767}},
		{schemapb.DataType_Int32, arrow.PrimitiveTypes.Int32, `[0,-2147483648,%s,2147483647]`, "0", []int32{-2147483648, 0, 2147483647}},
		{schemapb.DataType_Int64, arrow.PrimitiveTypes.Int64, `[0,-9007199254740993,%s,9007199254740993]`, "0", []int64{-9007199254740993, 0, 9007199254740993}},
		{schemapb.DataType_Float, arrow.PrimitiveTypes.Float32, `[0,-1.25,%s,2.5]`, "0", []float32{-1.25, 0, 2.5}},
		{schemapb.DataType_Double, arrow.PrimitiveTypes.Float64, `[0,-1.25,%s,2.5]`, "0", []float64{-1.25, 0, 2.5}},
		{schemapb.DataType_VarChar, arrow.BinaryTypes.String, `["skip","你好",%s,"last"]`, `""`, []string{"你好", "", "last"}},
		{schemapb.DataType_String, arrow.BinaryTypes.String, `["skip","你好",%s,"last"]`, `""`, []string{"你好", "", "last"}},
		{schemapb.DataType_Text, arrow.BinaryTypes.String, `["skip","你好",%s,"last"]`, `""`, []string{"你好", "", "last"}},
		{schemapb.DataType_Timestamptz, arrow.PrimitiveTypes.Int64, `[0,-1000,%s,3000]`, "0", []int64{-1000, 0, 3000}},
	} {
		for _, declared := range []bool{false, true} {
			for _, mode := range []string{"values", "nullable", "all_null", "empty"} {
				t.Run(fmt.Sprintf("%s/declared=%t/%s", tc.logical, declared, mode), func(t *testing.T) {
					pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
					defer pool.AssertSize(t, 0)
					b := NewDataFrameBuilder()
					defer b.Release()
					middle := tc.zero
					if mode == "nullable" {
						middle = "null"
					}
					input := fmt.Sprintf(tc.input, middle)
					if mode == "all_null" {
						input = `[null,null,null,null]`
					}
					values, _, err := array.FromJSON(pool, tc.physical, strings.NewReader(input), array.WithUseNumber())
					require.NoError(t, err)
					defer values.Release()
					addSlicedExportColumn(t, b, values, mode == "empty")
					nullable := mode == "nullable" || mode == "all_null"
					if declared {
						b.SetFieldSchema("value", &schemapb.FieldSchema{Name: "value", FieldID: 123, DataType: tc.logical, Nullable: nullable})
					} else {
						require.NoError(t, b.SetFieldType("value", tc.logical))
						b.SetFieldNullable("value", nullable).SetFieldID("value", 999)
					}
					df := b.Build()
					out, err := exportFieldData(df, "value")
					df.Release()
					require.NoError(t, err)
					require.Equal(t, "value", out.FieldName)
					require.Equal(t, tc.logical, out.Type)
					if declared {
						require.Equal(t, int64(123), out.FieldId)
					} else {
						require.Zero(t, out.FieldId)
					}
					require.NotNil(t, out.GetScalars())
					require.NotNil(t, out.GetScalars().GetData())
					switch mode {
					case "empty":
						require.Empty(t, scalarExportValues(out))
						require.Empty(t, typeutil.GetFieldDataValidData(out))
					case "all_null":
						require.Len(t, scalarExportValues(out), 3)
						require.Equal(t, []bool{false, false, false}, typeutil.GetFieldDataValidData(out))
					default:
						require.Equal(t, tc.want, scalarExportValues(out))
						if nullable {
							require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(out))
						} else {
							require.Empty(t, typeutil.GetFieldDataValidData(out))
						}
					}
				})
			}
		}
	}
}

func vectorExportBytes(v *schemapb.VectorField) []byte {
	switch v.Data.(type) {
	case *schemapb.VectorField_FloatVector:
		return arrow.Float32Traits.CastToBytes(v.GetFloatVector().GetData())
	case *schemapb.VectorField_BinaryVector:
		return v.GetBinaryVector()
	case *schemapb.VectorField_Float16Vector:
		return v.GetFloat16Vector()
	case *schemapb.VectorField_Bfloat16Vector:
		return v.GetBfloat16Vector()
	case *schemapb.VectorField_Int8Vector:
		return v.GetInt8Vector()
	default:
		return nil
	}
}

func TestExportFieldDataDenseVectorMatrix(t *testing.T) {
	for _, tc := range []struct {
		logical schemapb.DataType
		dim     int64
		row     []byte
	}{
		{schemapb.DataType_FloatVector, 2, arrow.Float32Traits.CastToBytes([]float32{1.25, -2.5})},
		{schemapb.DataType_BinaryVector, 16, []byte{0xAA, 0x55}},
		{schemapb.DataType_Float16Vector, 2, []byte{0, 0x3c, 0, 0xc0}},
		{schemapb.DataType_BFloat16Vector, 2, []byte{0x80, 0x3f, 0, 0xc0}},
		{schemapb.DataType_Int8Vector, 2, []byte{0x80, 0x7f}},
	} {
		for _, declared := range []bool{false, true} {
			for _, mode := range []string{"values", "nullable", "empty"} {
				t.Run(fmt.Sprintf("%s/declared=%t/%s", tc.logical, declared, mode), func(t *testing.T) {
					pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
					defer pool.AssertSize(t, 0)
					vb := array.NewFixedSizeBinaryBuilder(pool, &arrow.FixedSizeBinaryType{ByteWidth: len(tc.row)})
					vb.Append(make([]byte, len(tc.row)))
					vb.Append(tc.row)
					if mode == "nullable" {
						vb.AppendNull()
					} else {
						vb.Append(tc.row)
					}
					vb.Append(tc.row)
					values := vb.NewArray()
					vb.Release()
					defer values.Release()
					b := NewDataFrameBuilder()
					defer b.Release()
					addSlicedExportColumn(t, b, values, mode == "empty")
					if declared {
						b.SetFieldSchema("value", &schemapb.FieldSchema{Name: "value", FieldID: 123, DataType: tc.logical, Nullable: mode == "nullable", TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: fmt.Sprint(tc.dim)}}})
					} else {
						require.NoError(t, b.SetFieldType("value", tc.logical))
						b.SetFieldNullable("value", mode == "nullable").SetFieldID("value", 999)
					}
					df := b.Build()
					out, err := exportFieldData(df, "value")
					df.Release()
					require.NoError(t, err)
					require.Equal(t, tc.logical, out.Type)
					require.Equal(t, tc.dim, out.GetVectors().Dim)
					require.NotNil(t, out.GetVectors().GetData())
					if declared {
						require.Equal(t, int64(123), out.FieldId)
					} else {
						require.Zero(t, out.FieldId)
					}
					count := 3
					if mode == "nullable" {
						count = 2
						require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(out))
					}
					if mode == "empty" {
						count = 0
					}
					var want []byte
					for i := 0; i < count; i++ {
						want = append(want, tc.row...)
					}
					if count == 0 {
						require.Empty(t, vectorExportBytes(out.GetVectors()))
					} else {
						require.Equal(t, want, vectorExportBytes(out.GetVectors()))
					}
				})
			}
		}
	}
}

func TestExportFieldDataMetadataErrors(t *testing.T) {
	for _, failure := range []string{"missing_column", "missing_metadata", "invalid_origin", "missing_declaration", "unsupported_arrow_type", "nonnullable_null"} {
		t.Run(failure, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			b := NewDataFrameBuilder().SetChunkSizes([]int64{1})
			defer b.Release()
			typ := arrow.PrimitiveTypes.Int64
			input := `[1]`
			if failure == "unsupported_arrow_type" {
				typ = arrow.PrimitiveTypes.Uint64
			}
			if failure == "nonnullable_null" {
				input = `[null]`
			}
			values, _, err := array.FromJSON(pool, typ, strings.NewReader(input))
			require.NoError(t, err)
			require.NoError(t, b.AddColumnFromChunks("value", []arrow.Array{values}))
			df := b.Build()
			defer df.Release()
			name := "value"
			switch failure {
			case "missing_column":
				name = "absent"
			case "missing_metadata":
				delete(df.fieldMetadata, name)
			case "invalid_origin":
				df.fieldMetadata[name] = columnMetadata{origin: columnOrigin(255)}
			case "missing_declaration":
				df.fieldMetadata[name] = columnMetadata{origin: columnDeclared}
			}
			out, err := exportFieldData(df, name)
			require.Nil(t, out)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
}

func TestExportFieldDataBinaryLayouts(t *testing.T) {
	arrayRow := &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{-7, 42}}}}
	encoded, err := proto.Marshal(arrayRow)
	require.NoError(t, err)
	sparse0 := typeutil.CreateSparseFloatRow([]uint32{1}, []float32{2})
	sparse1 := typeutil.CreateSparseFloatRow([]uint32{99}, []float32{3})
	for _, tc := range []struct {
		logical     schemapb.DataType
		first, last []byte
	}{
		{schemapb.DataType_JSON, []byte(`{"a":1}`), []byte(`{"a":2}`)},
		{schemapb.DataType_Geometry, []byte{1, 2, 3}, []byte{4, 5, 6}},
		{schemapb.DataType_Array, encoded, encoded},
		{schemapb.DataType_SparseFloatVector, sparse0, sparse1},
	} {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/empty=%t", tc.logical, empty), func(t *testing.T) {
				pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer pool.AssertSize(t, 0)
				bb := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
				bb.Append(tc.first)
				bb.Append(tc.first)
				bb.AppendNull()
				bb.Append(tc.last)
				values := bb.NewArray()
				bb.Release()
				defer values.Release()
				b := NewDataFrameBuilder()
				defer b.Release()
				addSlicedExportColumn(t, b, values, empty)
				b.SetFieldSchema("value", &schemapb.FieldSchema{Name: "value", FieldID: 123, DataType: tc.logical, ElementType: schemapb.DataType_Int64, Nullable: true})
				df := b.Build()
				out, err := exportFieldData(df, "value")
				df.Release()
				require.NoError(t, err)
				require.Equal(t, int64(123), out.FieldId)
				require.Equal(t, tc.logical, out.Type)
				if !empty {
					require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(out))
				}
				switch tc.logical {
				case schemapb.DataType_JSON, schemapb.DataType_Geometry:
					rows := out.GetScalars().GetJsonData().GetData()
					if tc.logical == schemapb.DataType_Geometry {
						rows = out.GetScalars().GetGeometryData().GetData()
					}
					if empty {
						require.Empty(t, rows)
					} else {
						require.Len(t, rows, 3)
						require.Equal(t, tc.first, rows[0])
						require.Empty(t, rows[1])
						require.Equal(t, tc.last, rows[2])
					}
				case schemapb.DataType_Array:
					a := out.GetScalars().GetArrayData()
					require.Equal(t, schemapb.DataType_Int64, a.ElementType)
					if empty {
						require.Empty(t, a.Data)
					} else {
						require.Len(t, a.Data, 3)
						require.True(t, proto.Equal(arrayRow, a.Data[0]))
						require.True(t, proto.Equal(arrayRow, a.Data[2]))
					}
				case schemapb.DataType_SparseFloatVector:
					v := out.GetVectors().GetSparseFloatVector()
					if empty {
						require.Empty(t, v.Contents)
						require.Zero(t, v.Dim)
					} else {
						require.Equal(t, [][]byte{sparse0, sparse1}, v.Contents)
						require.Equal(t, int64(100), v.Dim)
						require.Equal(t, v.Dim, out.GetVectors().Dim)
					}
				}
			})
		}
	}
}

func TestExportFieldDataStructMatrix(t *testing.T) {
	typ := arrow.StructOf(arrow.Field{Name: "at", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}, arrow.Field{Name: "text", Type: arrow.ListOf(arrow.BinaryTypes.String)})
	for _, declared := range []bool{false, true} {
		for _, mode := range []string{"values", "empty", "null_document", "null_element", "null_child"} {
			t.Run(fmt.Sprintf("declared=%t/%s", declared, mode), func(t *testing.T) {
				pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer pool.AssertSize(t, 0)
				input := `[{"at":[],"text":[]},{"at":[7,9],"text":["你好","b"]},{"at":[],"text":[]},{"at":[11],"text":["last"]}]`
				switch mode {
				case "null_document":
					input = `[{"at":[],"text":[]},{"at":[7],"text":["first"]},null,{"at":[],"text":[]}]`
				case "null_element":
					input = `[{"at":[],"text":[]},{"at":null,"text":null},{"at":[],"text":[]},{"at":[],"text":[]}]`
				case "null_child":
					input = `[{"at":[],"text":[]},{"at":[null],"text":["first"]},{"at":[],"text":[]},{"at":[],"text":[]}]`
				}
				values, _, err := array.FromJSON(pool, typ, strings.NewReader(input))
				require.NoError(t, err)
				defer values.Release()
				b := NewDataFrameBuilder()
				defer b.Release()
				addSlicedExportColumn(t, b, values, mode == "empty")
				if declared {
					// Schema order differs from Arrow order, and IDs are deliberately non-positional.
					b.SetStructArraySchema("value", &schemapb.StructArrayFieldSchema{Name: "value", FieldID: 123, Nullable: true, Fields: []*schemapb.FieldSchema{
						{Name: "text", FieldID: 201, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Text, Nullable: true},
						{Name: "at", FieldID: 207, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Timestamptz, Nullable: true},
					}})
				} else {
					b.SetFieldID("value", 999)
				}
				df := b.Build()
				out, err := exportFieldData(df, "value")
				df.Release()
				if mode == "null_element" || mode == "null_child" || (mode == "null_document" && !declared) {
					require.Nil(t, out)
					require.ErrorIs(t, err, merr.ErrServiceInternal)
					require.Equal(t, merr.SystemError, merr.GetErrorType(err))
					return
				}
				require.NoError(t, err)
				require.Equal(t, schemapb.DataType_ArrayOfStruct, out.Type)
				fields := out.GetStructArrays().GetFields()
				require.Len(t, fields, 2)
				textIndex, atIndex := 1, 0
				if declared {
					textIndex, atIndex = 0, 1
					require.Equal(t, int64(123), out.FieldId)
					require.Equal(t, int64(201), fields[0].FieldId)
					require.Equal(t, int64(207), fields[1].FieldId)
				} else {
					require.Zero(t, out.FieldId)
					require.Equal(t, int64(1), fields[0].FieldId)
					require.Equal(t, int64(2), fields[1].FieldId)
				}
				texts := fields[textIndex].GetScalars().GetArrayData()
				ats := fields[atIndex].GetScalars().GetArrayData()
				require.Equal(t, "text", fields[textIndex].FieldName)
				require.Equal(t, "at", fields[atIndex].FieldName)
				if declared {
					require.Equal(t, schemapb.DataType_Text, texts.ElementType)
					require.Equal(t, schemapb.DataType_Timestamptz, ats.ElementType)
				} else {
					require.Equal(t, schemapb.DataType_VarChar, texts.ElementType)
					require.Equal(t, schemapb.DataType_Int64, ats.ElementType)
				}
				if mode == "empty" {
					require.Empty(t, texts.Data)
					require.Empty(t, ats.Data)
					return
				}
				require.Len(t, texts.Data, 3)
				require.Len(t, ats.Data, 3)
				if mode == "null_document" {
					for _, f := range fields {
						require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(f))
					}
					return
				}
				require.Equal(t, []string{"你好", "b"}, texts.Data[0].GetStringData().Data)
				require.Empty(t, texts.Data[1].GetStringData().Data)
				require.Equal(t, []string{"last"}, texts.Data[2].GetStringData().Data)
				for i, want := range [][]int64{{7, 9}, {}, {11}} {
					got := ats.Data[i].GetLongData().GetData()
					if declared {
						got = ats.Data[i].GetTimestamptzData().GetData()
					}
					if len(want) == 0 {
						require.Empty(t, got)
					} else {
						require.Equal(t, want, got)
					}
				}
			})
		}
	}
}

func TestExportFieldDataRejectsStandaloneComputedLists(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   arrow.DataType
		input string
	}{
		{"ints", arrow.ListOf(arrow.PrimitiveTypes.Int64), `[[],[-7,42],[],[9]]`},
		{"strings", arrow.ListOf(arrow.BinaryTypes.String), `[[],["你好","b"],[],["last"]]`},
		{"nested", arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64)), `[[],[[1,2],[]],[],[[9]]]`},
	} {
		for _, empty := range []bool{false, true} {
			for _, typed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/empty=%t/typed=%t", tc.name, empty, typed), func(t *testing.T) {
					pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
					defer pool.AssertSize(t, 0)
					values, _, err := array.FromJSON(pool, tc.typ, strings.NewReader(tc.input))
					require.NoError(t, err)
					defer values.Release()
					b := NewDataFrameBuilder()
					defer b.Release()
					addSlicedExportColumn(t, b, values, empty)
					if typed {
						require.NoError(t, b.SetFieldType("value", schemapb.DataType_Array))
					}
					df := b.Build()
					defer df.Release()
					out, err := exportFieldData(df, "value")
					require.Nil(t, out)
					require.ErrorIs(t, err, merr.ErrServiceInternal)
					require.Equal(t, merr.SystemError, merr.GetErrorType(err))
				})
			}
		}
	}
}

func TestExportFieldDataVectorArraysAndStructChildren(t *testing.T) {
	for _, tc := range []struct {
		logical schemapb.DataType
		dim     int64
		row     []byte
	}{
		{schemapb.DataType_FloatVector, 2, arrow.Float32Traits.CastToBytes([]float32{1.25, -2.5})},
		{schemapb.DataType_BinaryVector, 16, []byte{0xAA, 0x55}},
		{schemapb.DataType_Float16Vector, 2, []byte{0, 0x3c, 0, 0xc0}},
		{schemapb.DataType_BFloat16Vector, 2, []byte{0x80, 0x3f, 0, 0xc0}},
		{schemapb.DataType_Int8Vector, 2, []byte{0x80, 0x7f}},
	} {
		for _, structChild := range []bool{false, true} {
			for _, mode := range []string{"values", "empty", "wrong_dimension"} {
				t.Run(fmt.Sprintf("%s/struct=%t/%s", tc.logical, structChild, mode), func(t *testing.T) {
					pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
					defer pool.AssertSize(t, 0)
					var element arrow.DataType = &arrow.FixedSizeBinaryType{ByteWidth: len(tc.row)}
					lb := array.NewListBuilder(pool, element)
					appendVector := func() { lb.ValueBuilder().(*array.FixedSizeBinaryBuilder).Append(tc.row) }
					lb.Append(true)
					appendVector() // Discarded prefix.
					lb.Append(true)
					appendVector()
					appendVector()
					lb.AppendNull()
					lb.Append(true) // Empty array, distinct from a null document.
					values := lb.NewArray()
					lb.Release()
					if structChild {
						parent, err := array.NewStructArrayWithNulls([]arrow.Array{values}, []string{"vector"}, values.Data().Buffers()[0], values.NullN(), 0)
						require.NoError(t, err)
						values.Release()
						values = parent
					}
					defer values.Release()
					b := NewDataFrameBuilder()
					defer b.Release()
					addSlicedExportColumn(t, b, values, mode == "empty")
					dim := tc.dim
					if mode == "wrong_dimension" {
						dim++
					}
					field := &schemapb.FieldSchema{Name: "value", FieldID: 123, DataType: schemapb.DataType_ArrayOfVector, ElementType: tc.logical, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: fmt.Sprint(dim)}}}
					if structChild {
						field.Name = "vector"
						field.FieldID = 207
						b.SetStructArraySchema("value", &schemapb.StructArrayFieldSchema{Name: "value", FieldID: 123, Nullable: true, Fields: []*schemapb.FieldSchema{field}})
					} else {
						b.SetFieldSchema("value", field)
					}
					df := b.Build()
					out, err := exportFieldData(df, "value")
					df.Release()
					if mode == "wrong_dimension" {
						require.Nil(t, out)
						require.ErrorIs(t, err, merr.ErrServiceInternal)
						require.Equal(t, merr.SystemError, merr.GetErrorType(err))
						return
					}
					require.NoError(t, err)
					require.Equal(t, int64(123), out.FieldId)
					if structChild {
						require.Equal(t, schemapb.DataType_ArrayOfStruct, out.Type)
						require.Len(t, out.GetStructArrays().GetFields(), 1)
						out = out.GetStructArrays().Fields[0]
						require.Equal(t, int64(207), out.FieldId)
						require.Equal(t, "vector", out.FieldName)
					}
					require.Equal(t, schemapb.DataType_ArrayOfVector, out.Type)
					v := out.GetVectors().GetVectorArray()
					require.Equal(t, tc.dim, v.Dim)
					require.Equal(t, tc.logical, v.ElementType)
					if mode == "empty" {
						require.Empty(t, v.Data)
						return
					}
					require.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(out))
					require.Len(t, v.Data, 3)
					want := append(append([]byte{}, tc.row...), tc.row...)
					require.Equal(t, want, vectorExportBytes(v.Data[0]))
					require.Empty(t, vectorExportBytes(v.Data[1]))
					require.Empty(t, vectorExportBytes(v.Data[2]))
				})
			}
		}
	}
}

func TestExportFieldDataNestedComputedStructChild(t *testing.T) {
	typ := arrow.StructOf(arrow.Field{Name: "items", Type: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int64))})
	for _, tc := range []struct {
		name, input string
		invalid     bool
	}{
		{"values", `[{"items":[[1,2],[]]},{"items":[]},{"items":[[3]]}]`, false},
		{"empty", `[]`, false},
		{"null_document", `[null]`, true},
		{"null_inner_list", `[{"items":[null]}]`, true},
		{"null_scalar", `[{"items":[[null]]}]`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			values, _, err := array.FromJSON(pool, typ, strings.NewReader(tc.input))
			require.NoError(t, err)
			b := NewDataFrameBuilder().SetChunkSizes([]int64{int64(values.Len())})
			defer b.Release()
			require.NoError(t, b.AddColumnFromChunks("value", []arrow.Array{values}))
			df := b.Build()
			out, err := exportFieldData(df, "value")
			df.Release()
			if tc.invalid {
				require.Nil(t, out)
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.Equal(t, merr.SystemError, merr.GetErrorType(err))
				return
			}
			require.NoError(t, err)
			require.Zero(t, out.FieldId)
			require.Equal(t, schemapb.DataType_ArrayOfStruct, out.Type)
			require.Len(t, out.GetStructArrays().Fields, 1)
			outer := out.GetStructArrays().Fields[0].GetScalars().GetArrayData()
			require.Equal(t, schemapb.DataType_Array, outer.ElementType)
			if tc.name == "empty" {
				require.Empty(t, outer.Data)
				return
			}
			require.Len(t, outer.Data, 3)
			first := outer.Data[0].GetArrayData()
			require.Equal(t, schemapb.DataType_Int64, first.ElementType)
			require.Len(t, first.Data, 2)
			require.Equal(t, []int64{1, 2}, first.Data[0].GetLongData().Data)
			require.Empty(t, first.Data[1].GetLongData().Data)
			require.Empty(t, outer.Data[1].GetArrayData().Data)
			require.Equal(t, []int64{3}, outer.Data[2].GetArrayData().Data[0].GetLongData().Data)
		})
	}
}
