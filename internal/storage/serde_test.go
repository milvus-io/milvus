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
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/bitutil"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type MockRecordWriter struct {
	writefn func(Record) error
	closefn func() error
}

var _ RecordWriter = (*MockRecordWriter)(nil)

func (w *MockRecordWriter) Write(record Record) error {
	return w.writefn(record)
}

func (w *MockRecordWriter) Close() error {
	return w.closefn()
}

func (w *MockRecordWriter) GetWrittenUncompressed() uint64 {
	return 0
}

func TestSerDe(t *testing.T) {
	type args struct {
		dt schemapb.DataType
		v  any
	}
	tests := []struct {
		name  string
		args  args
		want  interface{}
		want1 bool
	}{
		{"test bool", args{dt: schemapb.DataType_Bool, v: true}, true, true},
		{"test bool null", args{dt: schemapb.DataType_Bool, v: nil}, nil, true},
		{"test bool negative", args{dt: schemapb.DataType_Bool, v: -1}, nil, false},
		{"test int8", args{dt: schemapb.DataType_Int8, v: int8(1)}, int8(1), true},
		{"test int8 null", args{dt: schemapb.DataType_Int8, v: nil}, nil, true},
		{"test int8 negative", args{dt: schemapb.DataType_Int8, v: true}, nil, false},
		{"test int16", args{dt: schemapb.DataType_Int16, v: int16(1)}, int16(1), true},
		{"test int16 null", args{dt: schemapb.DataType_Int16, v: nil}, nil, true},
		{"test int16 negative", args{dt: schemapb.DataType_Int16, v: true}, nil, false},
		{"test int32", args{dt: schemapb.DataType_Int32, v: int32(1)}, int32(1), true},
		{"test int32 null", args{dt: schemapb.DataType_Int32, v: nil}, nil, true},
		{"test int32 negative", args{dt: schemapb.DataType_Int32, v: true}, nil, false},
		{"test int64", args{dt: schemapb.DataType_Int64, v: int64(1)}, int64(1), true},
		{"test int64 null", args{dt: schemapb.DataType_Int64, v: nil}, nil, true},
		{"test int64 negative", args{dt: schemapb.DataType_Int64, v: true}, nil, false},
		{"test float32", args{dt: schemapb.DataType_Float, v: float32(1)}, float32(1), true},
		{"test float32 null", args{dt: schemapb.DataType_Float, v: nil}, nil, true},
		{"test float32 negative", args{dt: schemapb.DataType_Float, v: -1}, nil, false},
		{"test float64", args{dt: schemapb.DataType_Double, v: float64(1)}, float64(1), true},
		{"test float64 null", args{dt: schemapb.DataType_Double, v: nil}, nil, true},
		{"test float64 negative", args{dt: schemapb.DataType_Double, v: -1}, nil, false},
		{"test string", args{dt: schemapb.DataType_String, v: "test"}, "test", true},
		{"test string null", args{dt: schemapb.DataType_String, v: nil}, nil, true},
		{"test string negative", args{dt: schemapb.DataType_String, v: -1}, nil, false},
		{"test varchar", args{dt: schemapb.DataType_VarChar, v: "test"}, "test", true},
		{"test varchar null", args{dt: schemapb.DataType_VarChar, v: nil}, nil, true},
		{"test varchar negative", args{dt: schemapb.DataType_VarChar, v: -1}, nil, false},
		{"test array negative", args{dt: schemapb.DataType_Array, v: "{}"}, nil, false},
		{"test array null", args{dt: schemapb.DataType_Array, v: nil}, nil, true},
		{"test json", args{dt: schemapb.DataType_JSON, v: []byte("{}")}, []byte("{}"), true},
		{"test json null", args{dt: schemapb.DataType_JSON, v: nil}, nil, true},
		{"test json negative", args{dt: schemapb.DataType_JSON, v: -1}, nil, false},
		{"test float vector", args{dt: schemapb.DataType_FloatVector, v: []float32{1.0}}, []float32{1.0}, true},
		{"test float vector null", args{dt: schemapb.DataType_FloatVector, v: nil}, nil, true},
		{"test float vector negative", args{dt: schemapb.DataType_FloatVector, v: []int{1}}, nil, false},
		{"test bool vector", args{dt: schemapb.DataType_BinaryVector, v: []byte{0xff}}, []byte{0xff}, true},
		{"test float16 vector", args{dt: schemapb.DataType_Float16Vector, v: []byte{0xff, 0xff}}, []byte{0xff, 0xff}, true},
		{"test bfloat16 vector", args{dt: schemapb.DataType_BFloat16Vector, v: []byte{0xff, 0xff}}, []byte{0xff, 0xff}, true},
		{"test bfloat16 vector null", args{dt: schemapb.DataType_BFloat16Vector, v: nil}, nil, true},
		{"test bfloat16 vector negative", args{dt: schemapb.DataType_BFloat16Vector, v: -1}, nil, false},
		{"test int8 vector", args{dt: schemapb.DataType_Int8Vector, v: []int8{10}}, []int8{10}, true},
		{"test sparse float vector", args{dt: schemapb.DataType_SparseFloatVector, v: []byte{1, 2, 3, 4}}, []byte{1, 2, 3, 4}, true},
		{"test sparse float vector null", args{dt: schemapb.DataType_SparseFloatVector, v: nil}, nil, true},
		{"test sparse float vector negative", args{dt: schemapb.DataType_SparseFloatVector, v: -1}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := tt.args.dt
			v := tt.args.v
			builder := array.NewBuilder(memory.DefaultAllocator, serdeMap[dt].arrowType(1, schemapb.DataType_None, false))
			serdeMap[dt].serialize(builder, v, schemapb.DataType_None, 0, false)
			// assert.True(t, ok)
			a := builder.NewArray()
			got, err := serdeMap[dt].deserialize(a, 0, schemapb.DataType_None, 0, false, false)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("deserialize() got = %v, want %v", got, tt.want)
			}
			gotOk := err == nil
			if gotOk != tt.want1 {
				t.Errorf("deserialize() got error = %v, want success = %v", err, tt.want1)
			}
		})
	}
}

func TestSerDeCopy(t *testing.T) {
	tests := []struct {
		name string
		dt   schemapb.DataType
		v    any
	}{
		{"test string copy", schemapb.DataType_String, "test"},
		{"test string no copy", schemapb.DataType_String, "test"},
		{"test binary copy", schemapb.DataType_JSON, []byte{1, 2, 3}},
		{"test binary no copy", schemapb.DataType_JSON, []byte{1, 2, 3}},
		{"test bool copy", schemapb.DataType_Bool, true},
		{"test bool no copy", schemapb.DataType_Bool, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := tt.dt
			v := tt.v
			builder := array.NewBuilder(memory.DefaultAllocator, serdeMap[dt].arrowType(1, schemapb.DataType_None, false))
			defer builder.Release()
			serdeMap[dt].serialize(builder, v, schemapb.DataType_None, 0, false)
			a := builder.NewArray()

			// Test deserialize with shouldCopy parameter
			copy, err := serdeMap[dt].deserialize(a, 0, schemapb.DataType_None, 0, true, false)
			if err != nil {
				t.Errorf("deserialize() failed for %s: %v", tt.name, err)
			}
			if !reflect.DeepEqual(copy, tt.v) {
				t.Errorf("deserialize() got = %v, want %v", copy, tt.v)
			}
			ref, _ := serdeMap[dt].deserialize(a, 0, schemapb.DataType_None, 0, false, false)
			// check the unsafe pointers of copy and ref are different
			switch v := copy.(type) {
			case []byte:
				if unsafe.Pointer(&v[0]) == unsafe.Pointer(&ref.([]byte)[0]) {
					t.Errorf("deserialize() got same pointer for %v", tt.v)
				}
			case string:
				if unsafe.StringData(v) == unsafe.StringData(ref.(string)) {
					t.Errorf("deserialize() got same pointer for %v", tt.v)
				}
			}

			a.Release()
		})
	}
}

func BenchmarkDeserializeReader(b *testing.B) {
	len := 1000000
	blobs, err := generateTestData(len)
	assert.NoError(b, err)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs), false)
		assert.NoError(b, err)
		defer reader.Close()
		for i := 0; i < len; i++ {
			_, err = reader.NextValue()
			assert.NoError(b, err)
		}
		_, err = reader.NextValue()
		assert.Equal(b, io.EOF, err)
	}
}

func TestCalculateArraySize(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	tests := []struct {
		name         string
		arrayBuilder func() arrow.Array
		expectedSize uint64
	}{
		{
			name: "Empty array",
			arrayBuilder: func() arrow.Array {
				b := array.NewInt32Builder(mem)
				defer b.Release()
				return b.NewArray()
			},
			expectedSize: 0,
		},
		{
			name: "Fixed-length array",
			arrayBuilder: func() arrow.Array {
				b := array.NewInt32Builder(mem)
				defer b.Release()
				b.AppendValues([]int32{1, 2, 3, 4}, nil)
				return b.NewArray()
			},
			expectedSize: 20, // 4 elements * 4 bytes + bitmap(4bytes)
		},
		{
			name: "Variable-length string array",
			arrayBuilder: func() arrow.Array {
				b := array.NewStringBuilder(mem)
				defer b.Release()
				b.AppendValues([]string{"hello", "world"}, nil)
				return b.NewArray()
			},
			expectedSize: 23, // bytes: "hello" (5 bytes) + "world" (5 bytes)
			// offsets: 2+1 elements * 4 bytes
			// bitmap(1 byte)
		},
		{
			name: "Nested list array",
			arrayBuilder: func() arrow.Array {
				b := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int32)
				defer b.Release()
				valueBuilder := b.ValueBuilder().(*array.Int32Builder)

				b.Append(true)
				valueBuilder.AppendValues([]int32{1, 2, 3}, nil)

				b.Append(true)
				valueBuilder.AppendValues([]int32{4, 5}, nil)

				b.Append(true)
				valueBuilder.AppendValues([]int32{}, nil)

				return b.NewArray()
			},
			expectedSize: 44, // child buffer: 5 elements * 4 bytes, plus bitmap (4bytes)
			// offsets: 3+1 elements * 4 bytes
			// bitmap(4 bytes)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arr := tt.arrayBuilder()
			defer arr.Release()

			size := arr.Data().SizeInBytes()
			if size != tt.expectedSize {
				t.Errorf("Expected size %d, got %d", tt.expectedSize, size)
			}
		})
	}
}

func TestArrayOfVectorArrowType(t *testing.T) {
	dim := 128 // Test dimension
	tests := []struct {
		name          string
		elementType   schemapb.DataType
		dim           int
		expectedChild arrow.DataType
	}{
		{
			name:          "FloatVector",
			elementType:   schemapb.DataType_FloatVector,
			dim:           dim,
			expectedChild: &arrow.FixedSizeBinaryType{ByteWidth: dim * 4},
		},
		{
			name:          "BinaryVector",
			elementType:   schemapb.DataType_BinaryVector,
			dim:           dim,
			expectedChild: &arrow.FixedSizeBinaryType{ByteWidth: (dim + 7) / 8},
		},
		{
			name:          "Float16Vector",
			elementType:   schemapb.DataType_Float16Vector,
			dim:           dim,
			expectedChild: &arrow.FixedSizeBinaryType{ByteWidth: dim * 2},
		},
		{
			name:          "BFloat16Vector",
			elementType:   schemapb.DataType_BFloat16Vector,
			dim:           dim,
			expectedChild: &arrow.FixedSizeBinaryType{ByteWidth: dim * 2},
		},
		{
			name:          "Int8Vector",
			elementType:   schemapb.DataType_Int8Vector,
			dim:           dim,
			expectedChild: &arrow.FixedSizeBinaryType{ByteWidth: dim},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nonNullableArrowType := getArrayOfVectorArrowType(tt.elementType, tt.dim, false)
			assert.NotNil(t, nonNullableArrowType)

			listType, ok := nonNullableArrowType.(*arrow.ListType)
			assert.True(t, ok)
			assert.Equal(t, tt.expectedChild, listType.Elem())

			elementNullableArrowType := getArrayOfVectorArrowType(tt.elementType, tt.dim, true)
			listType, ok = elementNullableArrowType.(*arrow.ListType)
			assert.True(t, ok)
			assert.Equal(t, arrow.BinaryTypes.Binary, listType.Elem())
		})
	}

	for _, elementType := range []schemapb.DataType{
		schemapb.DataType_SparseFloatVector,
		schemapb.DataType_Float,
	} {
		for _, elementNullable := range []bool{false, true} {
			t.Run(fmt.Sprintf("reject_%s_element_nullable_%t", elementType, elementNullable), func(t *testing.T) {
				assert.Panics(t, func() {
					getArrayOfVectorArrowType(elementType, dim, elementNullable)
				})
			})
		}
	}
}

func TestArrayOfVectorSerialization(t *testing.T) {
	tests := []struct {
		name        string
		elementType schemapb.DataType
		dim         int
		vectors     []*schemapb.VectorField
	}{
		{
			name:        "FloatVector array",
			elementType: schemapb.DataType_FloatVector,
			dim:         4,
			vectors: []*schemapb.VectorField{
				{
					Dim: 4,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: []float32{1.0, 2.0, 3.0, 4.0},
						},
					},
				},
				{
					Dim: 4,
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: []float32{5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0},
						},
					},
				},
			},
		},
		{
			name:        "Float16Vector array",
			elementType: schemapb.DataType_Float16Vector,
			dim:         4,
			vectors: []*schemapb.VectorField{
				{
					Dim: 4,
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}, // 4 dims * 2 bytes
					},
				},
				{
					Dim: 4,
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: []byte{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}, // 8 dims * 2 bytes (2 vectors)
					},
				},
			},
		},
		{
			name:        "BFloat16Vector array",
			elementType: schemapb.DataType_BFloat16Vector,
			dim:         4,
			vectors: []*schemapb.VectorField{
				{
					Dim: 4,
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}, // 4 dims * 2 bytes
					},
				},
				{
					Dim: 4,
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: []byte{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}, // 8 dims * 2 bytes (2 vectors)
					},
				},
			},
		},
		{
			name:        "Int8Vector array",
			elementType: schemapb.DataType_Int8Vector,
			dim:         4,
			vectors: []*schemapb.VectorField{
				{
					Dim: 4,
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: []byte{1, 2, 3, 4}, // 4 dims * 1 byte
					},
				},
				{
					Dim: 4,
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: []byte{5, 6, 7, 8, 9, 10, 11, 12}, // 8 dims * 1 byte (2 vectors)
					},
				},
			},
		},
		{
			name:        "BinaryVector array",
			elementType: schemapb.DataType_BinaryVector,
			dim:         32, // Must be multiple of 8
			vectors: []*schemapb.VectorField{
				{
					Dim: 32,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: []byte{1, 2, 3, 4}, // 32 dims / 8 = 4 bytes per vector
					},
				},
				{
					Dim: 32,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: []byte{5, 6, 7, 8, 9, 10, 11, 12}, // 2 vectors * 4 bytes
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := serdeMap[schemapb.DataType_ArrayOfVector]

			arrowType := entry.arrowType(tt.dim, tt.elementType, false)
			assert.NotNil(t, arrowType)

			builder := array.NewBuilder(memory.DefaultAllocator, arrowType)
			defer builder.Release()

			for _, vector := range tt.vectors {
				err := entry.serialize(builder, vector, tt.elementType, tt.dim, false)
				assert.NoError(t, err)
			}

			arr := builder.NewArray()
			defer arr.Release()

			for i, expectedVector := range tt.vectors {
				result, err := entry.deserialize(arr, i, tt.elementType, tt.dim, false, false)
				assert.NoError(t, err)

				if expectedVector == nil {
					assert.Nil(t, result)
				} else {
					resultVector, ok := result.(*schemapb.VectorField)
					assert.True(t, ok)
					assert.NotNil(t, resultVector)

					assert.Equal(t, expectedVector.GetDim(), resultVector.GetDim())

					switch tt.elementType {
					case schemapb.DataType_FloatVector:
						expectedData := expectedVector.GetFloatVector().GetData()
						resultData := resultVector.GetFloatVector().GetData()
						assert.Equal(t, expectedData, resultData)
					case schemapb.DataType_Float16Vector:
						expectedData := expectedVector.GetFloat16Vector()
						resultData := resultVector.GetFloat16Vector()
						assert.Equal(t, expectedData, resultData)
					case schemapb.DataType_BFloat16Vector:
						expectedData := expectedVector.GetBfloat16Vector()
						resultData := resultVector.GetBfloat16Vector()
						assert.Equal(t, expectedData, resultData)
					case schemapb.DataType_Int8Vector:
						expectedData := expectedVector.GetInt8Vector()
						resultData := resultVector.GetInt8Vector()
						assert.Equal(t, expectedData, resultData)
					case schemapb.DataType_BinaryVector:
						expectedData := expectedVector.GetBinaryVector()
						resultData := resultVector.GetBinaryVector()
						assert.Equal(t, expectedData, resultData)
					}
				}
			}
		})
	}
}

func TestArrayOfVectorSerializationRejectsInvalidPayloadLength(t *testing.T) {
	entry := serdeMap[schemapb.DataType_ArrayOfVector]
	arrowType := entry.arrowType(4, schemapb.DataType_FloatVector, false)
	builder := array.NewBuilder(memory.DefaultAllocator, arrowType)
	defer builder.Release()

	err := entry.serialize(builder, &schemapb.VectorField{
		Dim: 4,
		Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4, 5}},
		},
	}, schemapb.DataType_FloatVector, 4, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not divisible")
}

func TestArrayOfVectorSerializationRejectsInvalidElementValidity(t *testing.T) {
	tests := []struct {
		name            string
		elementType     schemapb.DataType
		dim             int
		row             *schemapb.VectorField
		elementNullable bool
		errorText       string
	}{
		{
			name:            "valid bitmap has too many physical vectors",
			elementType:     schemapb.DataType_FloatVector,
			dim:             2,
			row:             makeFloatVec(2, 1, 2),
			elementNullable: true,
			errorText:       "2 valid vectors, but compact physical payload has 1 vectors",
		},
		{
			name:        "valid bitmap has too few physical vectors",
			elementType: schemapb.DataType_Int8Vector,
			dim:         2,
			row: &schemapb.VectorField{
				Dim:       2,
				Data:      &schemapb.VectorField_Int8Vector{Int8Vector: []byte{1, 2, 3, 4}},
				ValidData: []bool{true},
			},
			elementNullable: true,
			errorText:       "1 valid vectors, but compact physical payload has 2 vectors",
		},
		{
			name:        "non-nullable schema rejects child validity",
			elementType: schemapb.DataType_BinaryVector,
			dim:         16,
			row: &schemapb.VectorField{
				Dim:       16,
				Data:      &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2}},
				ValidData: []bool{true},
			},
			errorText: "non-element-nullable ArrayOfVector row cannot carry element valid_data",
		},
		{
			name:            "nullable schema requires child validity",
			elementType:     schemapb.DataType_FloatVector,
			dim:             2,
			row:             makeFloatVec(2, 1, 2),
			elementNullable: true,
			errorText:       "requires element valid_data",
		},
	}
	tests[0].row.ValidData = []bool{true, true}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := serdeMap[schemapb.DataType_ArrayOfVector]
			builder := array.NewBuilder(memory.DefaultAllocator,
				entry.arrowType(tt.dim, tt.elementType, tt.elementNullable))
			defer builder.Release()

			err := entry.serialize(builder, tt.row, tt.elementType, tt.dim, tt.elementNullable)
			require.Error(t, err)
			require.ErrorIs(t, err, merr.ErrStorage)
			require.ErrorContains(t, err, tt.errorText)
			require.Zero(t, builder.Len())
			require.Zero(t, builder.(*array.ListBuilder).ValueBuilder().Len())
		})
	}
}

func TestDeserializeArrayOfVectorRejectsUnexpectedNullChild(t *testing.T) {
	builder := array.NewListBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: 8})
	defer builder.Release()
	builder.Append(true)
	builder.ValueBuilder().(*array.FixedSizeBinaryBuilder).AppendNull()
	column := builder.NewArray()
	defer column.Release()

	_, err := deserializeArrayOfVector(column, 0, schemapb.DataType_FloatVector, 2, true, false)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrStorage)
	require.ErrorContains(t, err, "non-element-nullable ArrayOfVector contains null child")
}

func TestElementNullableArrayOfVectorSerializationUsesSchemaDim(t *testing.T) {
	const schemaDim = 2
	entry := serdeMap[schemapb.DataType_ArrayOfVector]
	builder := array.NewBuilder(
		memory.DefaultAllocator,
		entry.arrowType(schemaDim, schemapb.DataType_FloatVector, true),
	)
	defer builder.Release()

	err := entry.serialize(builder, &schemapb.VectorField{
		Dim: 1,
		Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}},
		},
		ValidData: []bool{true},
	}, schemapb.DataType_FloatVector, schemaDim, true)
	require.NoError(t, err)
	require.Equal(t, 1, builder.Len())
}

func TestElementNullableArrayOfVectorNullChildrenHaveNoVectorPayload(t *testing.T) {
	const dim = 1024
	entry := serdeMap[schemapb.DataType_ArrayOfVector]
	builder := array.NewBuilder(
		memory.DefaultAllocator,
		entry.arrowType(dim, schemapb.DataType_FloatVector, true),
	)
	defer builder.Release()

	err := entry.serialize(builder, &schemapb.VectorField{
		Dim:       dim,
		Data:      &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}},
		ValidData: []bool{false, false, false, false},
	}, schemapb.DataType_FloatVector, dim, true)
	require.NoError(t, err)

	column := builder.NewArray().(*array.List)
	defer column.Release()
	child := column.ListValues().(*array.Binary)
	require.Equal(t, 4, child.Len())
	require.Equal(t, 4, child.NullN())
	if values := child.Data().Buffers()[2]; values != nil {
		require.Zero(t, values.Len())
	}

	value, err := entry.deserialize(column, 0, schemapb.DataType_FloatVector, dim, true, true)
	require.NoError(t, err)
	row := value.(*schemapb.VectorField)
	require.Equal(t, []bool{false, false, false, false}, row.GetValidData())
	require.Empty(t, row.GetFloatVector().GetData())
}

func TestDeserializeElementNullableArrayOfVectorRejectsMismatchedChildWidth(t *testing.T) {
	builder := array.NewListBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer builder.Release()
	builder.Append(true)
	builder.ValueBuilder().(*array.BinaryBuilder).Append(make([]byte, 7))
	column := builder.NewArray()
	defer column.Release()

	_, err := deserializeArrayOfVector(column, 0, schemapb.DataType_FloatVector, 2, true, true)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrStorage)
	require.ErrorContains(t, err, "byte width 7, expected 8")
}

func TestArrayOfVectorEmptyArray(t *testing.T) {
	tests := []struct {
		name        string
		elementType schemapb.DataType
		dim         int
	}{
		{"FloatVector empty", schemapb.DataType_FloatVector, 4},
		{"Float16Vector empty", schemapb.DataType_Float16Vector, 4},
		{"BFloat16Vector empty", schemapb.DataType_BFloat16Vector, 4},
		{"Int8Vector empty", schemapb.DataType_Int8Vector, 4},
		{"BinaryVector empty", schemapb.DataType_BinaryVector, 32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := serdeMap[schemapb.DataType_ArrayOfVector]

			arrowType := entry.arrowType(tt.dim, tt.elementType, false)
			assert.NotNil(t, arrowType)

			// Create empty VectorField based on element type
			var emptyVector *schemapb.VectorField
			switch tt.elementType {
			case schemapb.DataType_FloatVector:
				emptyVector = &schemapb.VectorField{
					Dim:  int64(tt.dim),
					Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{}}},
				}
			case schemapb.DataType_Float16Vector:
				emptyVector = &schemapb.VectorField{
					Dim:  int64(tt.dim),
					Data: &schemapb.VectorField_Float16Vector{Float16Vector: []byte{}},
				}
			case schemapb.DataType_BFloat16Vector:
				emptyVector = &schemapb.VectorField{
					Dim:  int64(tt.dim),
					Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{}},
				}
			case schemapb.DataType_Int8Vector:
				emptyVector = &schemapb.VectorField{
					Dim:  int64(tt.dim),
					Data: &schemapb.VectorField_Int8Vector{Int8Vector: []byte{}},
				}
			case schemapb.DataType_BinaryVector:
				emptyVector = &schemapb.VectorField{
					Dim:  int64(tt.dim),
					Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{}},
				}
			}

			builder := array.NewBuilder(memory.DefaultAllocator, arrowType)
			defer builder.Release()

			// Serialize empty vector
			err := entry.serialize(builder, emptyVector, tt.elementType, tt.dim, false)
			assert.NoError(t, err)

			arr := builder.NewArray()
			defer arr.Release()

			// Deserialize and verify
			result, err := entry.deserialize(arr, 0, tt.elementType, tt.dim, false, false)
			assert.NoError(t, err)
			assert.NotNil(t, result)

			resultVector, ok := result.(*schemapb.VectorField)
			assert.True(t, ok)
			assert.Equal(t, int64(tt.dim), resultVector.GetDim())

			// Verify data is empty
			switch tt.elementType {
			case schemapb.DataType_FloatVector:
				assert.Empty(t, resultVector.GetFloatVector().GetData())
			case schemapb.DataType_Float16Vector:
				assert.Empty(t, resultVector.GetFloat16Vector())
			case schemapb.DataType_BFloat16Vector:
				assert.Empty(t, resultVector.GetBfloat16Vector())
			case schemapb.DataType_Int8Vector:
				assert.Empty(t, resultVector.GetInt8Vector())
			case schemapb.DataType_BinaryVector:
				assert.Empty(t, resultVector.GetBinaryVector())
			}
		})
	}
}

func TestArrayOfVectorIntegration(t *testing.T) {
	tests := []struct {
		name            string
		elementType     schemapb.DataType
		dim             int
		elementTypeCode string // Expected element type code in metadata
		createVectors   func(dim int) []*schemapb.VectorField
	}{
		{
			name:            "FloatVector",
			elementType:     schemapb.DataType_FloatVector,
			dim:             4,
			elementTypeCode: "101",
			createVectors: func(dim int) []*schemapb.VectorField {
				return []*schemapb.VectorField{
					{
						Dim: int64(dim),
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: []float32{1.0, 2.0, 3.0, 4.0},
							},
						},
					},
					{
						Dim: int64(dim),
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{
								Data: []float32{5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0},
							},
						},
					},
				}
			},
		},
		{
			name:            "Float16Vector",
			elementType:     schemapb.DataType_Float16Vector,
			dim:             4,
			elementTypeCode: "102",
			createVectors: func(dim int) []*schemapb.VectorField {
				return []*schemapb.VectorField{
					{
						Dim: int64(dim),
						Data: &schemapb.VectorField_Float16Vector{
							Float16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8},
						},
					},
					{
						Dim: int64(dim),
						Data: &schemapb.VectorField_Float16Vector{
							Float16Vector: []byte{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24},
						},
					},
				}
			},
		},
		{
			name:            "BFloat16Vector",
			elementType:     schemapb.DataType_BFloat16Vector,
			dim:             4,
			elementTypeCode: "103",
			createVectors: func(dim int) []*schemapb.VectorField {
				return []*schemapb.VectorField{
					{
						Dim: int64(dim),
						Data: &schemapb.VectorField_Bfloat16Vector{
							Bfloat16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8},
						},
					},
					{
						Dim: int64(dim),
						Data: &schemapb.VectorField_Bfloat16Vector{
							Bfloat16Vector: []byte{9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24},
						},
					},
				}
			},
		},
		{
			name:            "Int8Vector",
			elementType:     schemapb.DataType_Int8Vector,
			dim:             4,
			elementTypeCode: "105",
			createVectors: func(dim int) []*schemapb.VectorField {
				return []*schemapb.VectorField{
					{
						Dim: int64(dim),
						Data: &schemapb.VectorField_Int8Vector{
							Int8Vector: []byte{1, 2, 3, 4},
						},
					},
					{
						Dim: int64(dim),
						Data: &schemapb.VectorField_Int8Vector{
							Int8Vector: []byte{5, 6, 7, 8, 9, 10, 11, 12},
						},
					},
				}
			},
		},
		{
			name:            "BinaryVector",
			elementType:     schemapb.DataType_BinaryVector,
			dim:             32,
			elementTypeCode: "100",
			createVectors: func(dim int) []*schemapb.VectorField {
				return []*schemapb.VectorField{
					{
						Dim: int64(dim),
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: []byte{1, 2, 3, 4},
						},
					},
					{
						Dim: int64(dim),
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: []byte{5, 6, 7, 8, 9, 10, 11, 12},
						},
					},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     100,
						Name:        "vec_array",
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: tt.elementType,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: "dim", Value: fmt.Sprintf("%d", tt.dim)},
						},
					},
				},
			}

			insertData := &InsertData{
				Data: map[FieldID]FieldData{
					100: &VectorArrayFieldData{
						Data:        tt.createVectors(tt.dim),
						ElementType: tt.elementType,
						Dim:         int64(tt.dim),
					},
				},
			}

			arrowSchema, err := ConvertToArrowSchema(schema, false)
			assert.NoError(t, err)
			assert.NotNil(t, arrowSchema)

			recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
			defer recordBuilder.Release()

			err = BuildRecord(recordBuilder, insertData, schema)
			assert.NoError(t, err)

			record := recordBuilder.NewRecord()
			defer record.Release()

			assert.Equal(t, int64(2), record.NumRows())
			assert.Equal(t, int64(1), record.NumCols())

			field := arrowSchema.Field(0)
			assert.True(t, field.HasMetadata())

			elementTypeStr, ok := field.Metadata.GetValue("elementType")
			assert.True(t, ok)
			assert.Equal(t, tt.elementTypeCode, elementTypeStr)

			dimStr, ok := field.Metadata.GetValue("dim")
			assert.True(t, ok)
			assert.Equal(t, fmt.Sprintf("%d", tt.dim), dimStr)
		})
	}
}

func TestActualSizeInBytesSlicedFixedSizeBinary(t *testing.T) {
	dim := 128
	byteWidth := dim * 4
	totalRows := 1000

	builder := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: byteWidth})
	defer builder.Release()

	for i := 0; i < totalRows; i++ {
		vec := make([]byte, byteWidth)
		for j := range vec {
			vec[j] = byte((i + j) % 256)
		}
		builder.Append(vec)
	}

	arr := builder.NewArray().(*array.FixedSizeBinary)
	defer arr.Release()

	t.Run("Full array", func(t *testing.T) {
		actualSize := ActualSizeInBytes(arr.Data())
		expectedSize := uint64(bitutil.BytesForBits(int64(totalRows))) + uint64(totalRows*byteWidth)

		assert.Equal(t, expectedSize, actualSize)
		t.Logf("Full array - ActualSize: %d, Expected: %d", actualSize, expectedSize)
	})

	t.Run("Sliced array [100:200]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 100, 200).(*array.FixedSizeBinary)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())
		expectedSize := uint64(bitutil.BytesForBits(int64(slicedLen))) + uint64(slicedLen*byteWidth)

		assert.Equal(t, 100, slicedLen)
		assert.Equal(t, expectedSize, actualSize)
		assert.Less(t, actualSize, ActualSizeInBytes(arr.Data()))

		t.Logf("Sliced [100:200] - ActualSize: %d, Expected: %d (length: %d)", actualSize, expectedSize, slicedLen)
	})

	t.Run("Sliced array [0:10]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 0, 10).(*array.FixedSizeBinary)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())
		expectedSize := uint64(bitutil.BytesForBits(int64(slicedLen))) + uint64(slicedLen*byteWidth)

		assert.Equal(t, 10, slicedLen)
		assert.Equal(t, expectedSize, actualSize)

		t.Logf("Sliced [0:10] - ActualSize: %d, Expected: %d", actualSize, expectedSize)
	})

	t.Run("Sliced array [990:1000]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 990, 1000).(*array.FixedSizeBinary)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())
		expectedSize := uint64(bitutil.BytesForBits(int64(slicedLen))) + uint64(slicedLen*byteWidth)

		assert.Equal(t, 10, slicedLen)
		assert.Equal(t, expectedSize, actualSize)

		t.Logf("Sliced [990:1000] - ActualSize: %d, Expected: %d", actualSize, expectedSize)
	})
}

func TestActualSizeInBytesSlicedString(t *testing.T) {
	totalRows := 100
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < totalRows; i++ {
		builder.Append(string(make([]byte, i+10)))
	}

	arr := builder.NewArray().(*array.String)
	defer arr.Release()

	t.Run("Full array", func(t *testing.T) {
		actualSize := ActualSizeInBytes(arr.Data())
		expectedDataSize := (10 + 109) * 50
		expectedOffsetSize := (totalRows + 1) * 4
		expectedNullBitmapSize := bitutil.BytesForBits(int64(totalRows))
		expectedTotal := uint64(expectedNullBitmapSize + int64(expectedOffsetSize) + int64(expectedDataSize))

		assert.GreaterOrEqual(t, actualSize, expectedTotal)
		t.Logf("Full array - ActualSize: %d", actualSize)
	})

	t.Run("Sliced array [10:20]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 10, 20).(*array.String)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())

		assert.Equal(t, 10, slicedLen)
		assert.Less(t, actualSize, ActualSizeInBytes(arr.Data()))

		t.Logf("Sliced [10:20] - ActualSize: %d (length: %d)", actualSize, slicedLen)
	})

	t.Run("Sliced array [0:5]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 0, 5).(*array.String)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())

		assert.Equal(t, 5, slicedLen)
		assert.Less(t, actualSize, ActualSizeInBytes(arr.Data()))

		t.Logf("Sliced [0:5] - ActualSize: %d", actualSize)
	})
}

func TestActualSizeInBytesSlicedInt64(t *testing.T) {
	totalRows := 1000
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < totalRows; i++ {
		builder.Append(int64(i))
	}

	arr := builder.NewArray().(*array.Int64)
	defer arr.Release()

	t.Run("Full array", func(t *testing.T) {
		actualSize := ActualSizeInBytes(arr.Data())
		expectedSize := uint64(bitutil.BytesForBits(int64(totalRows))) + uint64(totalRows*8)

		assert.Equal(t, expectedSize, actualSize)
		t.Logf("Full array - ActualSize: %d, Expected: %d", actualSize, expectedSize)
	})

	t.Run("Sliced array [100:200]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 100, 200).(*array.Int64)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())
		expectedSize := uint64(bitutil.BytesForBits(int64(slicedLen))) + uint64(slicedLen*8)

		assert.Equal(t, 100, slicedLen)
		assert.Equal(t, expectedSize, actualSize)

		t.Logf("Sliced [100:200] - ActualSize: %d, Expected: %d", actualSize, expectedSize)
	})

	t.Run("Sliced array [500:501]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 500, 501).(*array.Int64)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())
		expectedSize := uint64(bitutil.BytesForBits(int64(slicedLen))) + uint64(slicedLen*8)

		assert.Equal(t, 1, slicedLen)
		assert.Equal(t, expectedSize, actualSize)

		t.Logf("Sliced [500:501] - ActualSize: %d, Expected: %d", actualSize, expectedSize)
	})
}

func TestActualSizeInBytesSlicedList(t *testing.T) {
	pool := memory.DefaultAllocator

	listBuilder := array.NewListBuilder(pool, arrow.PrimitiveTypes.Int32)
	defer listBuilder.Release()

	valueBuilder := listBuilder.ValueBuilder().(*array.Int32Builder)

	totalRows := 100
	for i := 0; i < totalRows; i++ {
		listBuilder.Append(true)
		numElements := i%10 + 1
		for j := 0; j < numElements; j++ {
			valueBuilder.Append(int32(i*10 + j))
		}
	}

	arr := listBuilder.NewArray().(*array.List)
	defer arr.Release()

	t.Run("Full array", func(t *testing.T) {
		actualSize := ActualSizeInBytes(arr.Data())

		nullBitmapSize := bitutil.BytesForBits(int64(totalRows))
		offsetSize := (totalRows + 1) * 4
		childSize := ActualSizeInBytes(arr.ListValues().Data())
		expectedSize := uint64(nullBitmapSize+int64(offsetSize)) + childSize

		assert.Equal(t, expectedSize, actualSize)
		t.Logf("Full array - ActualSize: %d, Expected: %d", actualSize, expectedSize)
	})

	t.Run("Sliced array [10:20]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 10, 20).(*array.List)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())

		assert.Equal(t, 10, slicedLen)
		assert.Less(t, actualSize, ActualSizeInBytes(arr.Data()))

		t.Logf("Sliced [10:20] - ActualSize: %d (length: %d)", actualSize, slicedLen)
	})

	t.Run("Sliced array [0:1]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 0, 1).(*array.List)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())

		assert.Equal(t, 1, slicedLen)
		assert.Less(t, actualSize, ActualSizeInBytes(arr.Data()))

		t.Logf("Sliced [0:1] - ActualSize: %d", actualSize)
	})
}

func TestActualSizeInBytesSlicedFloat32(t *testing.T) {
	totalRows := 500
	builder := array.NewFloat32Builder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < totalRows; i++ {
		builder.Append(float32(i) * 1.5)
	}

	arr := builder.NewArray().(*array.Float32)
	defer arr.Release()

	t.Run("Full array", func(t *testing.T) {
		actualSize := ActualSizeInBytes(arr.Data())
		expectedSize := uint64(bitutil.BytesForBits(int64(totalRows))) + uint64(totalRows*4)

		assert.Equal(t, expectedSize, actualSize)
		t.Logf("Full array - ActualSize: %d, Expected: %d", actualSize, expectedSize)
	})

	t.Run("Sliced array [200:300]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 200, 300).(*array.Float32)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())
		expectedSize := uint64(bitutil.BytesForBits(int64(slicedLen))) + uint64(slicedLen*4)

		assert.Equal(t, 100, slicedLen)
		assert.Equal(t, expectedSize, actualSize)

		t.Logf("Sliced [200:300] - ActualSize: %d, Expected: %d", actualSize, expectedSize)
	})
}

func TestActualSizeInBytesSlicedBool(t *testing.T) {
	totalRows := 1024
	builder := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer builder.Release()

	for i := 0; i < totalRows; i++ {
		builder.Append(i%2 == 0)
	}

	arr := builder.NewArray().(*array.Boolean)
	defer arr.Release()

	t.Run("Full array", func(t *testing.T) {
		actualSize := ActualSizeInBytes(arr.Data())
		expectedSize := uint64(bitutil.BytesForBits(int64(totalRows)) * 2)

		assert.Equal(t, expectedSize, actualSize)
		t.Logf("Full array - ActualSize: %d, Expected: %d", actualSize, expectedSize)
	})

	t.Run("Sliced array [512:768]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 512, 768).(*array.Boolean)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())
		expectedSize := uint64(bitutil.BytesForBits(int64(slicedLen)) * 2)

		assert.Equal(t, 256, slicedLen)
		assert.Equal(t, expectedSize, actualSize)

		t.Logf("Sliced [512:768] - ActualSize: %d, Expected: %d", actualSize, expectedSize)
	})
}

func TestActualSizeInBytesSlicedBinary(t *testing.T) {
	totalRows := 50
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer builder.Release()

	for i := 0; i < totalRows; i++ {
		data := make([]byte, i+5)
		for j := range data {
			data[j] = byte(i)
		}
		builder.Append(data)
	}

	arr := builder.NewArray().(*array.Binary)
	defer arr.Release()

	t.Run("Full array", func(t *testing.T) {
		actualSize := ActualSizeInBytes(arr.Data())

		t.Logf("Full array - ActualSize: %d", actualSize)
	})

	t.Run("Sliced array [10:30]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 10, 30).(*array.Binary)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())

		assert.Equal(t, 20, slicedLen)
		assert.Less(t, actualSize, ActualSizeInBytes(arr.Data()))

		t.Logf("Sliced [10:30] - ActualSize: %d (length: %d)", actualSize, slicedLen)
	})

	t.Run("Sliced array [0:10]", func(t *testing.T) {
		sliced := array.NewSlice(arr, 0, 10).(*array.Binary)
		defer sliced.Release()

		slicedLen := sliced.Len()
		actualSize := ActualSizeInBytes(sliced.Data())

		assert.Equal(t, 10, slicedLen)
		assert.Less(t, actualSize, ActualSizeInBytes(arr.Data()))

		t.Logf("Sliced [0:10] - ActualSize: %d", actualSize)
	})
}

func TestActualSizeInBytesCompareWithDataSizeInBytes(t *testing.T) {
	dim := 768
	byteWidth := dim * 4
	totalRows := 1000

	builder := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: byteWidth})
	defer builder.Release()

	for i := 0; i < totalRows; i++ {
		vec := make([]byte, byteWidth)
		for j := range vec {
			vec[j] = byte((i + j) % 256)
		}
		builder.Append(vec)
	}

	arr := builder.NewArray().(*array.FixedSizeBinary)
	defer arr.Release()

	t.Run("Full array comparison", func(t *testing.T) {
		actualSize := ActualSizeInBytes(arr.Data())
		arrowSize := arr.Data().SizeInBytes()

		t.Logf("Full array - ActualSizeInBytes: %d, Data().SizeInBytes(): %d", actualSize, arrowSize)
		t.Logf("Difference: %d bytes (%.2f%%)",
			int64(arrowSize)-int64(actualSize),
			float64(int64(arrowSize)-int64(actualSize))/float64(actualSize)*100)
	})

	t.Run("Sliced array [100:200] comparison", func(t *testing.T) {
		sliced := array.NewSlice(arr, 100, 200).(*array.FixedSizeBinary)
		defer sliced.Release()

		actualSize := ActualSizeInBytes(sliced.Data())
		arrowSize := sliced.Data().SizeInBytes()
		expectedSize := uint64(100 * byteWidth)

		t.Logf("Sliced [100:200] - ActualSizeInBytes: %d, Data().SizeInBytes(): %d", actualSize, arrowSize)
		t.Logf("Expected actual data: %d bytes", expectedSize)
		t.Logf("ActualSizeInBytes correctly accounts for slice: %v", actualSize < uint64(totalRows*byteWidth))

		assert.Less(t, actualSize, uint64(totalRows*byteWidth))
	})
}

func TestBuildRecord_NullableArrayOfVector(t *testing.T) {
	dim := 4
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:     100,
				Name:        "vec_array",
				DataType:    schemapb.DataType_ArrayOfVector,
				ElementType: schemapb.DataType_FloatVector,
				Nullable:    true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: fmt.Sprintf("%d", dim)},
				},
			},
		},
	}

	vec0 := makeFloatVec(dim, 1, 2, 3, 4)
	vec1 := makeFloatVec(dim)
	vec2 := makeFloatVec(dim, 5, 6, 7, 8)

	insertData := &InsertData{
		Data: map[FieldID]FieldData{
			100: &VectorArrayFieldData{
				Data:        []*schemapb.VectorField{vec0, vec1, vec2},
				ElementType: schemapb.DataType_FloatVector,
				Dim:         int64(dim),
				ValidData:   []bool{true, false, true},
				Nullable:    true,
			},
		},
	}

	arrowSchema, err := ConvertToArrowSchema(schema, false)
	assert.NoError(t, err)

	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer recordBuilder.Release()

	err = BuildRecord(recordBuilder, insertData, schema)
	assert.NoError(t, err)

	record := recordBuilder.NewRecord()
	defer record.Release()

	assert.Equal(t, int64(3), record.NumRows())

	// Verify metadata preserved (elementType + dim)
	field := arrowSchema.Field(0)
	assert.True(t, field.HasMetadata())

	elementTypeStr, ok := field.Metadata.GetValue("elementType")
	assert.True(t, ok)
	assert.Equal(t, fmt.Sprintf("%d", int32(schemapb.DataType_FloatVector)), elementTypeStr)

	dimStr, ok := field.Metadata.GetValue("dim")
	assert.True(t, ok)
	assert.Equal(t, fmt.Sprintf("%d", dim), dimStr)

	// Verify null bitmap
	col := record.Column(0)
	assert.True(t, col.IsValid(0))
	assert.True(t, col.IsNull(1))
	assert.True(t, col.IsValid(2))

	field2Col := map[FieldID]int{100: 0}
	simpleRecord := NewSimpleArrowRecord(record, field2Col)
	rb := NewRecordBuilder(schema)
	err = rb.Append(simpleRecord, 0, int(record.NumRows()))
	assert.NoError(t, err)

	rebuiltRecord := rb.Build()
	defer rebuiltRecord.Release()
	assert.True(t, rebuiltRecord.Column(100).IsValid(0))
	assert.True(t, rebuiltRecord.Column(100).IsNull(1))
	assert.True(t, rebuiltRecord.Column(100).IsValid(2))
}

func TestBuildRecord_ElementNullableArrayRoundTrip(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:         100,
				Name:            "arr",
				DataType:        schemapb.DataType_Array,
				ElementType:     schemapb.DataType_Int64,
				ElementNullable: true,
			},
		},
	}
	insertData := &InsertData{
		Data: map[FieldID]FieldData{
			100: &ArrayFieldData{
				ElementType:     schemapb.DataType_Int64,
				ElementNullable: true,
				Data: []*schemapb.ScalarField{
					{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{10, 0}},
						},
						ValidData: []bool{true, false},
					},
					{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{20}},
						},
						ValidData: []bool{true},
					},
				},
			},
		},
	}

	arrowSchema, err := ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer recordBuilder.Release()

	require.NoError(t, BuildRecord(recordBuilder, insertData, schema))
	record := recordBuilder.NewRecord()
	defer record.Release()

	entry := serdeMap[schemapb.DataType_Array]
	value, err := entry.deserialize(record.Column(0), 0, schemapb.DataType_Int64, 0, true, true)
	require.NoError(t, err)
	row := value.(*schemapb.ScalarField)
	assert.Equal(t, []bool{true, false}, row.GetValidData())
	assert.Equal(t, []int64{10, 0}, row.GetLongData().GetData())
}

func TestBuildRecord_ElementNullableArrayOfVectorRoundTrip(t *testing.T) {
	tests := []struct {
		name          string
		elementType   schemapb.DataType
		dim           int
		mixedRow      func() *schemapb.VectorField
		allNullRow    func() *schemapb.VectorField
		assertPayload func(*testing.T, *schemapb.VectorField)
		assertEmpty   func(*testing.T, *schemapb.VectorField)
	}{
		{
			name:        "float vector",
			elementType: schemapb.DataType_FloatVector,
			dim:         2,
			mixedRow: func() *schemapb.VectorField {
				row := makeFloatVec(2, 1, 2, 3, 4)
				row.ValidData = []bool{true, false, true}
				return row
			},
			allNullRow: func() *schemapb.VectorField {
				return &schemapb.VectorField{
					Dim:       2,
					Data:      &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}},
					ValidData: []bool{false, false},
				}
			},
			assertPayload: func(t *testing.T, row *schemapb.VectorField) {
				assert.Equal(t, []float32{1, 2, 3, 4}, row.GetFloatVector().GetData())
			},
			assertEmpty: func(t *testing.T, row *schemapb.VectorField) {
				require.NotNil(t, row.GetFloatVector())
				assert.Empty(t, row.GetFloatVector().GetData())
			},
		},
		{
			name:        "binary vector",
			elementType: schemapb.DataType_BinaryVector,
			dim:         16,
			mixedRow: func() *schemapb.VectorField {
				return &schemapb.VectorField{
					Dim:       16,
					Data:      &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2, 3, 4}},
					ValidData: []bool{true, false, true},
				}
			},
			allNullRow: func() *schemapb.VectorField {
				return &schemapb.VectorField{
					Dim:       16,
					Data:      &schemapb.VectorField_BinaryVector{BinaryVector: nil},
					ValidData: []bool{false, false},
				}
			},
			assertPayload: func(t *testing.T, row *schemapb.VectorField) {
				assert.Equal(t, []byte{1, 2, 3, 4}, row.GetBinaryVector())
			},
			assertEmpty: func(t *testing.T, row *schemapb.VectorField) {
				_, ok := row.GetData().(*schemapb.VectorField_BinaryVector)
				assert.True(t, ok)
				assert.Empty(t, row.GetBinaryVector())
			},
		},
		{
			name:        "float16 vector",
			elementType: schemapb.DataType_Float16Vector,
			dim:         2,
			mixedRow: func() *schemapb.VectorField {
				return &schemapb.VectorField{
					Dim:       2,
					Data:      &schemapb.VectorField_Float16Vector{Float16Vector: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
					ValidData: []bool{true, false, true},
				}
			},
			allNullRow: func() *schemapb.VectorField {
				return &schemapb.VectorField{
					Dim:       2,
					Data:      &schemapb.VectorField_Float16Vector{Float16Vector: nil},
					ValidData: []bool{false, false},
				}
			},
			assertPayload: func(t *testing.T, row *schemapb.VectorField) {
				assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, row.GetFloat16Vector())
			},
			assertEmpty: func(t *testing.T, row *schemapb.VectorField) {
				_, ok := row.GetData().(*schemapb.VectorField_Float16Vector)
				assert.True(t, ok)
				assert.Empty(t, row.GetFloat16Vector())
			},
		},
		{
			name:        "bfloat16 vector",
			elementType: schemapb.DataType_BFloat16Vector,
			dim:         2,
			mixedRow: func() *schemapb.VectorField {
				return &schemapb.VectorField{
					Dim:       2,
					Data:      &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{9, 10, 11, 12, 13, 14, 15, 16}},
					ValidData: []bool{true, false, true},
				}
			},
			allNullRow: func() *schemapb.VectorField {
				return &schemapb.VectorField{
					Dim:       2,
					Data:      &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: nil},
					ValidData: []bool{false, false},
				}
			},
			assertPayload: func(t *testing.T, row *schemapb.VectorField) {
				assert.Equal(t, []byte{9, 10, 11, 12, 13, 14, 15, 16}, row.GetBfloat16Vector())
			},
			assertEmpty: func(t *testing.T, row *schemapb.VectorField) {
				_, ok := row.GetData().(*schemapb.VectorField_Bfloat16Vector)
				assert.True(t, ok)
				assert.Empty(t, row.GetBfloat16Vector())
			},
		},
		{
			name:        "int8 vector",
			elementType: schemapb.DataType_Int8Vector,
			dim:         2,
			mixedRow: func() *schemapb.VectorField {
				return &schemapb.VectorField{
					Dim:       2,
					Data:      &schemapb.VectorField_Int8Vector{Int8Vector: []byte{17, 18, 19, 20}},
					ValidData: []bool{true, false, true},
				}
			},
			allNullRow: func() *schemapb.VectorField {
				return &schemapb.VectorField{
					Dim:       2,
					Data:      &schemapb.VectorField_Int8Vector{Int8Vector: nil},
					ValidData: []bool{false, false},
				}
			},
			assertPayload: func(t *testing.T, row *schemapb.VectorField) {
				assert.Equal(t, []byte{17, 18, 19, 20}, row.GetInt8Vector())
			},
			assertEmpty: func(t *testing.T, row *schemapb.VectorField) {
				_, ok := row.GetData().(*schemapb.VectorField_Int8Vector)
				assert.True(t, ok)
				assert.Empty(t, row.GetInt8Vector())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := &schemapb.FieldSchema{
				FieldID:         100,
				Name:            "vec_arr",
				DataType:        schemapb.DataType_ArrayOfVector,
				ElementType:     tt.elementType,
				Nullable:        true,
				ElementNullable: true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: fmt.Sprintf("%d", tt.dim)},
				},
			}
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
			insertData := &InsertData{Data: map[FieldID]FieldData{
				100: &VectorArrayFieldData{
					ElementType:     tt.elementType,
					ElementNullable: true,
					Nullable:        true,
					Dim:             int64(tt.dim),
					Data: []*schemapb.VectorField{
						tt.mixedRow(),
						tt.allNullRow(),
						{},
					},
					ValidData: []bool{true, true, false},
				},
			}}

			arrowSchema, err := ConvertToArrowSchema(schema, false)
			require.NoError(t, err)
			recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
			defer recordBuilder.Release()

			require.NoError(t, BuildRecord(recordBuilder, insertData, schema))
			record := recordBuilder.NewRecord()
			defer record.Release()

			assertRoundTrip := func(t *testing.T, column arrow.Array) {
				entry := serdeMap[schemapb.DataType_ArrayOfVector]
				value, err := entry.deserialize(column, 0, tt.elementType, tt.dim, true, true)
				require.NoError(t, err)
				mixedRow := value.(*schemapb.VectorField)
				assert.Equal(t, []bool{true, false, true}, mixedRow.GetValidData())
				tt.assertPayload(t, mixedRow)

				value, err = entry.deserialize(column, 1, tt.elementType, tt.dim, true, true)
				require.NoError(t, err)
				allNullRow := value.(*schemapb.VectorField)
				assert.Equal(t, []bool{false, false}, allNullRow.GetValidData())
				tt.assertEmpty(t, allNullRow)

				value, err = entry.deserialize(column, 2, tt.elementType, tt.dim, true, true)
				require.NoError(t, err)
				assert.Nil(t, value)
			}

			listArray := record.Column(0).(*array.List)
			assert.True(t, listArray.IsValid(0))
			assert.True(t, listArray.IsValid(1))
			assert.True(t, listArray.IsNull(2))
			child := listArray.ListValues().(*array.Binary)
			require.Equal(t, 5, child.Len())
			assert.True(t, child.IsValid(0))
			assert.True(t, child.IsNull(1))
			assert.True(t, child.IsValid(2))
			assert.True(t, child.IsNull(3))
			assert.True(t, child.IsNull(4))
			assert.Equal(t, len(child.Value(0))+len(child.Value(2)), child.Data().Buffers()[2].Len())
			assertRoundTrip(t, record.Column(0))

			var parquetBuffer bytes.Buffer
			writer, err := newSingleFieldRecordWriter(field, &parquetBuffer)
			require.NoError(t, err)
			require.NoError(t, writer.Write(NewSimpleArrowRecord(record, map[FieldID]int{100: 0})))
			require.NoError(t, writer.Close())

			parquetReader, err := file.NewParquetReader(bytes.NewReader(parquetBuffer.Bytes()))
			require.NoError(t, err)
			defer parquetReader.Close()
			arrowReader, err := pqarrow.NewFileReader(
				parquetReader,
				pqarrow.ArrowReadProperties{BatchSize: 1024},
				memory.DefaultAllocator,
			)
			require.NoError(t, err)
			recordReader, err := arrowReader.GetRecordReader(context.Background(), nil, nil)
			require.NoError(t, err)
			defer recordReader.Release()
			require.True(t, recordReader.Next())
			parquetRecord := recordReader.Record()
			assertRoundTrip(t, parquetRecord.Column(0))

			roundTripData, err := RecordToInsertData(
				NewSimpleArrowRecord(parquetRecord, map[FieldID]int{100: 0}),
				schema,
				typeutil.NewSet[int64](100),
			)
			require.NoError(t, err)
			fieldData := roundTripData.Data[100].(*VectorArrayFieldData)
			assert.Equal(t, []bool{true, true, false}, fieldData.ValidData)
			require.Len(t, fieldData.Data, 3)
			assert.Equal(t, []bool{true, false, true}, fieldData.Data[0].GetValidData())
			tt.assertPayload(t, fieldData.Data[0])
			assert.Equal(t, []bool{false, false}, fieldData.Data[1].GetValidData())
			tt.assertEmpty(t, fieldData.Data[1])
			assert.Nil(t, fieldData.GetRow(2))

			require.False(t, recordReader.Next())
			require.ErrorIs(t, recordReader.Err(), io.EOF)
		})
	}
}

func TestBuildRecordRejectsElementNullableMismatch(t *testing.T) {
	t.Run("array", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:         100,
					Name:            "arr",
					DataType:        schemapb.DataType_Array,
					ElementType:     schemapb.DataType_Int64,
					ElementNullable: true,
				},
			},
		}
		insertData := &InsertData{
			Data: map[FieldID]FieldData{
				100: &ArrayFieldData{
					ElementType:     schemapb.DataType_Int64,
					ElementNullable: false,
					Data: []*schemapb.ScalarField{
						{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{1}},
							},
						},
					},
				},
			},
		}

		arrowSchema, err := ConvertToArrowSchema(schema, false)
		require.NoError(t, err)
		recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer recordBuilder.Release()

		err = BuildRecord(recordBuilder, insertData, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "element_nullable mismatch")
	})

	t.Run("array of vector", func(t *testing.T) {
		dim := 4
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:         100,
					Name:            "vec_arr",
					DataType:        schemapb.DataType_ArrayOfVector,
					ElementType:     schemapb.DataType_FloatVector,
					ElementNullable: true,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: fmt.Sprintf("%d", dim)},
					},
				},
			},
		}
		insertData := &InsertData{
			Data: map[FieldID]FieldData{
				100: &VectorArrayFieldData{
					ElementType:     schemapb.DataType_FloatVector,
					ElementNullable: false,
					Dim:             int64(dim),
					Data: []*schemapb.VectorField{
						makeFloatVec(dim, 1, 2, 3, 4),
					},
				},
			},
		}

		arrowSchema, err := ConvertToArrowSchema(schema, false)
		require.NoError(t, err)
		recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer recordBuilder.Release()

		err = BuildRecord(recordBuilder, insertData, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "element_nullable mismatch")
	})

	t.Run("array reverse", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:     100,
					Name:        "arr",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int64,
				},
			},
		}
		insertData := &InsertData{
			Data: map[FieldID]FieldData{
				100: &ArrayFieldData{
					ElementType:     schemapb.DataType_Int64,
					ElementNullable: true,
					Data: []*schemapb.ScalarField{
						{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{Data: []int64{1}},
							},
							ValidData: []bool{true},
						},
					},
				},
			},
		}

		arrowSchema, err := ConvertToArrowSchema(schema, false)
		require.NoError(t, err)
		recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer recordBuilder.Release()

		err = BuildRecord(recordBuilder, insertData, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "element_nullable mismatch")
	})

	t.Run("array of vector reverse", func(t *testing.T) {
		dim := 4
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:     100,
					Name:        "vec_arr",
					DataType:    schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: fmt.Sprintf("%d", dim)},
					},
				},
			},
		}
		insertData := &InsertData{
			Data: map[FieldID]FieldData{
				100: &VectorArrayFieldData{
					ElementType:     schemapb.DataType_FloatVector,
					ElementNullable: true,
					Dim:             int64(dim),
					Data: []*schemapb.VectorField{
						makeFloatVec(dim, 1, 2, 3, 4),
					},
				},
			},
		}
		insertData.Data[100].(*VectorArrayFieldData).Data[0].ValidData = []bool{true}

		arrowSchema, err := ConvertToArrowSchema(schema, false)
		require.NoError(t, err)
		recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer recordBuilder.Release()

		err = BuildRecord(recordBuilder, insertData, schema)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "element_nullable mismatch")
	})

	t.Run("array of vector child validity conflicts with schema", func(t *testing.T) {
		dim := 4
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{
				FieldID:     100,
				Name:        "vec_arr",
				DataType:    schemapb.DataType_ArrayOfVector,
				ElementType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: fmt.Sprintf("%d", dim)},
				},
			},
		}}
		row := makeFloatVec(dim, 1, 2, 3, 4)
		row.ValidData = []bool{true}
		insertData := &InsertData{Data: map[FieldID]FieldData{
			100: &VectorArrayFieldData{
				ElementType: schemapb.DataType_FloatVector,
				Dim:         int64(dim),
				Data:        []*schemapb.VectorField{row},
			},
		}}

		arrowSchema, err := ConvertToArrowSchema(schema, false)
		require.NoError(t, err)
		recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer recordBuilder.Release()

		err = BuildRecord(recordBuilder, insertData, schema)
		require.Error(t, err)
		require.ErrorIs(t, err, merr.ErrStorage)
		require.ErrorContains(t, err, "non-element-nullable ArrayOfVector row cannot carry element valid_data")
	})
}

func TestBuildRecord_ElementNullableArrayOfVectorRejectsMissingTypedData(t *testing.T) {
	tests := []struct {
		name        string
		elementType schemapb.DataType
		dim         int
		errorText   string
	}{
		{"float vector", schemapb.DataType_FloatVector, 2, "FloatVector data is nil"},
		{"binary vector", schemapb.DataType_BinaryVector, 16, "BinaryVector data is nil"},
		{"float16 vector", schemapb.DataType_Float16Vector, 2, "Float16Vector data is nil"},
		{"bfloat16 vector", schemapb.DataType_BFloat16Vector, 2, "BFloat16Vector data is nil"},
		{"int8 vector", schemapb.DataType_Int8Vector, 2, "Int8Vector data is nil"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{
					FieldID:         100,
					Name:            "vec_arr",
					DataType:        schemapb.DataType_ArrayOfVector,
					ElementType:     tt.elementType,
					ElementNullable: true,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: fmt.Sprintf("%d", tt.dim)},
					},
				},
			}}
			insertData := &InsertData{Data: map[FieldID]FieldData{
				100: &VectorArrayFieldData{
					ElementType:     tt.elementType,
					ElementNullable: true,
					Dim:             int64(tt.dim),
					Data: []*schemapb.VectorField{
						{Dim: int64(tt.dim), ValidData: []bool{false}},
					},
				},
			}}

			arrowSchema, err := ConvertToArrowSchema(schema, false)
			require.NoError(t, err)
			recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
			defer recordBuilder.Release()

			err = BuildRecord(recordBuilder, insertData, schema)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorText)
		})
	}
}
