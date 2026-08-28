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

package segcore

import (
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func buildTestRecord(pool memory.Allocator, fields []arrow.Field, builders func([]array.Builder)) arrow.Record {
	schema := arrow.NewSchema(fields, nil)
	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()
	builders(bldr.Fields())
	return bldr.NewRecord()
}

func dimTypeParams(dim int) []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.Itoa(dim)}}
}

func TestArrowFieldsToProto_Int64(t *testing.T) {
	pool := memory.NewGoAllocator()
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "pk", Type: arrow.PrimitiveTypes.Int64}},
		func(bs []array.Builder) {
			b := bs[0].(*array.Int64Builder)
			b.AppendValues([]int64{10, 20, 30}, nil)
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "pk", result[0].GetFieldName())
	assert.Equal(t, int64(100), result[0].GetFieldId())
	assert.Equal(t, schemapb.DataType_Int64, result[0].GetType())
	assert.Equal(t, []int64{10, 20, 30}, result[0].GetScalars().GetLongData().GetData())
}

func TestArrowFieldsToProto_Float32(t *testing.T) {
	pool := memory.NewGoAllocator()
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "score", Type: arrow.PrimitiveTypes.Float32}},
		func(bs []array.Builder) {
			b := bs[0].(*array.Float32Builder)
			b.AppendValues([]float32{1.5, 2.5, 3.5}, nil)
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{FieldID: 101, Name: "score", DataType: schemapb.DataType_Float},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, []float32{1.5, 2.5, 3.5}, result[0].GetScalars().GetFloatData().GetData())
}

func TestArrowFieldsToProto_Double(t *testing.T) {
	pool := memory.NewGoAllocator()
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "score", Type: arrow.PrimitiveTypes.Float64}},
		func(bs []array.Builder) {
			b := bs[0].(*array.Float64Builder)
			b.AppendValues([]float64{1.1, 2.2, 3.3}, nil)
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{FieldID: 102, Name: "score", DataType: schemapb.DataType_Double},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, []float64{1.1, 2.2, 3.3}, result[0].GetScalars().GetDoubleData().GetData())
}

func TestArrowFieldsToProto_Bool(t *testing.T) {
	pool := memory.NewGoAllocator()
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "flag", Type: arrow.FixedWidthTypes.Boolean}},
		func(bs []array.Builder) {
			b := bs[0].(*array.BooleanBuilder)
			b.AppendValues([]bool{true, false, true}, nil)
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{FieldID: 103, Name: "flag", DataType: schemapb.DataType_Bool},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, []bool{true, false, true}, result[0].GetScalars().GetBoolData().GetData())
}

func TestArrowFieldsToProto_String(t *testing.T) {
	pool := memory.NewGoAllocator()
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "name", Type: arrow.BinaryTypes.String}},
		func(bs []array.Builder) {
			b := bs[0].(*array.StringBuilder)
			b.AppendValues([]string{"alice", "bob", "carol"}, nil)
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{FieldID: 104, Name: "name", DataType: schemapb.DataType_VarChar},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, []string{"alice", "bob", "carol"}, result[0].GetScalars().GetStringData().GetData())
}

func TestArrowFieldsToProto_FloatVector(t *testing.T) {
	pool := memory.NewGoAllocator()
	dim := 4
	bytesPerVec := dim * 4
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: bytesPerVec}}},
		func(bs []array.Builder) {
			b := bs[0].(*array.FixedSizeBinaryBuilder)
			// Write 2 vectors of dim=4
			for i := 0; i < 2; i++ {
				vec := make([]float32, dim)
				for j := range vec {
					vec[j] = float32(i*dim + j)
				}
				b.Append(arrow.Float32Traits.CastToBytes(vec))
			}
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{
			FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
			TypeParams: dimTypeParams(dim),
		},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, int64(dim), result[0].GetVectors().GetDim())
	floatVec := result[0].GetVectors().GetFloatVector().GetData()
	assert.Len(t, floatVec, 8) // 2 vectors * dim 4
	assert.InDelta(t, float32(0), floatVec[0], 1e-6)
	assert.InDelta(t, float32(4), floatVec[4], 1e-6)
}

func TestArrowFieldsToProto_BinaryVector(t *testing.T) {
	pool := memory.NewGoAllocator()
	dim := 16
	bytesPerVec := dim / 8
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: bytesPerVec}}},
		func(bs []array.Builder) {
			b := bs[0].(*array.FixedSizeBinaryBuilder)
			b.Append([]byte{0xFF, 0x00})
			b.Append([]byte{0x0F, 0xF0})
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{
			FieldID: 105, Name: "vec", DataType: schemapb.DataType_BinaryVector,
			TypeParams: dimTypeParams(dim),
		},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, int64(dim), result[0].GetVectors().GetDim())
	assert.Equal(t, []byte{0xFF, 0x00, 0x0F, 0xF0}, result[0].GetVectors().GetBinaryVector())
}

func TestArrowFieldsToProto_Float16Vector(t *testing.T) {
	pool := memory.NewGoAllocator()
	dim := 4
	bytesPerVec := dim * 2
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: bytesPerVec}}},
		func(bs []array.Builder) {
			b := bs[0].(*array.FixedSizeBinaryBuilder)
			b.Append(make([]byte, bytesPerVec))
			raw := make([]byte, bytesPerVec)
			for i := range raw {
				raw[i] = byte(i + 1)
			}
			b.Append(raw)
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{
			FieldID: 106, Name: "vec", DataType: schemapb.DataType_Float16Vector,
			TypeParams: dimTypeParams(dim),
		},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, int64(dim), result[0].GetVectors().GetDim())
	data := result[0].GetVectors().GetFloat16Vector()
	assert.Len(t, data, 2*bytesPerVec)
	assert.Equal(t, byte(1), data[bytesPerVec])
}

func TestArrowFieldsToProto_BFloat16Vector(t *testing.T) {
	pool := memory.NewGoAllocator()
	dim := 4
	bytesPerVec := dim * 2
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: bytesPerVec}}},
		func(bs []array.Builder) {
			b := bs[0].(*array.FixedSizeBinaryBuilder)
			b.Append(make([]byte, bytesPerVec))
			raw := make([]byte, bytesPerVec)
			for i := range raw {
				raw[i] = byte(i + 1)
			}
			b.Append(raw)
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{
			FieldID: 107, Name: "vec", DataType: schemapb.DataType_BFloat16Vector,
			TypeParams: dimTypeParams(dim),
		},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, int64(dim), result[0].GetVectors().GetDim())
	data := result[0].GetVectors().GetBfloat16Vector()
	assert.Len(t, data, 2*bytesPerVec)
	assert.Equal(t, byte(1), data[bytesPerVec])
}

func TestArrowFieldsToProto_MultiColumn(t *testing.T) {
	pool := memory.NewGoAllocator()
	dim := 768
	bytesPerVec := dim * 4
	rec := buildTestRecord(
		pool,
		[]arrow.Field{
			{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
			{Name: "score", Type: arrow.PrimitiveTypes.Float32},
			{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: bytesPerVec}},
		},
		func(bs []array.Builder) {
			pkB := bs[0].(*array.Int64Builder)
			pkB.AppendValues([]int64{1, 2}, nil)

			scoreB := bs[1].(*array.Float32Builder)
			scoreB.AppendValues([]float32{0.1, 0.2}, nil)

			vecB := bs[2].(*array.FixedSizeBinaryBuilder)
			for i := 0; i < 2; i++ {
				vec := make([]float32, dim)
				for j := range vec {
					vec[j] = float32(i*dim + j)
				}
				vecB.Append(arrow.Float32Traits.CastToBytes(vec))
			}
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "score", DataType: schemapb.DataType_Float},
		{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: dimTypeParams(dim)},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 3)
	assert.Equal(t, []int64{1, 2}, result[0].GetScalars().GetLongData().GetData())
	assert.Equal(t, []float32{0.1, 0.2}, result[1].GetScalars().GetFloatData().GetData())
	assert.Equal(t, int64(dim), result[2].GetVectors().GetDim())
	assert.Len(t, result[2].GetVectors().GetFloatVector().GetData(), 2*dim)
}

func TestArrowFieldsToProto_EmptyRecord(t *testing.T) {
	pool := memory.NewGoAllocator()
	rec := buildTestRecord(
		pool,
		[]arrow.Field{
			{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
		func(bs []array.Builder) {},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
		{FieldID: 104, Name: "name", DataType: schemapb.DataType_VarChar},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Empty(t, result[0].GetScalars().GetLongData().GetData())
	assert.Empty(t, result[1].GetScalars().GetStringData().GetData())
}

// TestArrowFieldsToProto_Int8_via_Int32 verifies that Int8 schema fields
// are correctly read from arrow.Int32 columns (C++ FieldDataToArrow stores
// Int8/Int16/Int32 as arrow::int32 because protobuf uses int_data for all).
func TestArrowFieldsToProto_Int8_via_Int32(t *testing.T) {
	pool := memory.NewGoAllocator()
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "age", Type: arrow.PrimitiveTypes.Int32}},
		func(bs []array.Builder) {
			b := bs[0].(*array.Int32Builder)
			b.AppendValues([]int32{-1, 0, 127}, nil)
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{FieldID: 109, Name: "age", DataType: schemapb.DataType_Int8},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, schemapb.DataType_Int8, result[0].GetType())
	assert.Equal(t, []int32{-1, 0, 127}, result[0].GetScalars().GetIntData().GetData())
}

// TestArrowFieldsToProto_Int16_via_Int32 verifies Int16 schema fields
// from arrow.Int32 columns.
func TestArrowFieldsToProto_Int16_via_Int32(t *testing.T) {
	pool := memory.NewGoAllocator()
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "val", Type: arrow.PrimitiveTypes.Int32}},
		func(bs []array.Builder) {
			b := bs[0].(*array.Int32Builder)
			b.AppendValues([]int32{-32768, 0, 32767}, nil)
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{FieldID: 110, Name: "val", DataType: schemapb.DataType_Int16},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, schemapb.DataType_Int16, result[0].GetType())
	assert.Equal(t, []int32{-32768, 0, 32767}, result[0].GetScalars().GetIntData().GetData())
}

// TestArrowFieldsToProto_JSON verifies JSON fields from arrow.Binary columns.
func TestArrowFieldsToProto_JSON(t *testing.T) {
	pool := memory.NewGoAllocator()
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "meta", Type: arrow.BinaryTypes.Binary}},
		func(bs []array.Builder) {
			b := bs[0].(*array.BinaryBuilder)
			b.Append([]byte(`{"a":1}`))
			b.Append([]byte(`{"b":2}`))
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{FieldID: 108, Name: "meta", DataType: schemapb.DataType_JSON},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "meta", result[0].GetFieldName())
	assert.Equal(t, schemapb.DataType_JSON, result[0].GetType())
	assert.NotNil(t, result[0].GetField())
	jsonData := result[0].GetScalars().GetJsonData().GetData()
	assert.Len(t, jsonData, 2)
	assert.Equal(t, []byte(`{"a":1}`), jsonData[0])
	assert.Equal(t, []byte(`{"b":2}`), jsonData[1])
}

// TestArrowFieldsToProto_UnmappedDataType covers the default branch of the
// type switch: a DataType this converter does not (yet) handle must return
// an error instead of silently producing an empty field.
func TestArrowFieldsToProto_UnmappedDataType(t *testing.T) {
	pool := memory.NewGoAllocator()
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "arr", Type: arrow.BinaryTypes.Binary}},
		func(bs []array.Builder) {
			b := bs[0].(*array.BinaryBuilder)
			b.Append([]byte{1, 2, 3})
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{FieldID: 111, Name: "arr", DataType: schemapb.DataType_None},
	}
	_, err := ArrowFieldsToProto(rec, schemas)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "arr")
}

// TestArrowFieldsToProto_FloatVector_DimFallback covers the case where the
// FieldSchema is missing a resolvable "dim" (typeutil.GetDim fails): the
// converter must fall back to the Arrow column's own byte width instead of
// silently producing a zero-length vector.
func TestArrowFieldsToProto_FloatVector_DimFallback(t *testing.T) {
	pool := memory.NewGoAllocator()
	dim := 4
	bytesPerVec := dim * 4
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: bytesPerVec}}},
		func(bs []array.Builder) {
			b := bs[0].(*array.FixedSizeBinaryBuilder)
			for i := 0; i < 2; i++ {
				vec := make([]float32, dim)
				for j := range vec {
					vec[j] = float32(i*dim + j)
				}
				b.Append(arrow.Float32Traits.CastToBytes(vec))
			}
		},
	)
	defer rec.Release()

	// No TypeParams at all, so typeutil.GetDim fails and the fallback path
	// (byteWidth / 4) must kick in.
	schemas := []*schemapb.FieldSchema{
		{FieldID: 109, Name: "vec", DataType: schemapb.DataType_FloatVector},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, int64(dim), result[0].GetVectors().GetDim())
	floatVec := result[0].GetVectors().GetFloatVector().GetData()
	assert.Len(t, floatVec, 2*dim)
	assert.InDelta(t, float32(0), floatVec[0], 1e-6)
	assert.InDelta(t, float32(4), floatVec[4], 1e-6)
}

// TestArrowFieldsToProto_BinaryVector_DimFallback mirrors the FloatVector
// fallback test for BinaryVector's byteWidth*8 fallback formula.
func TestArrowFieldsToProto_BinaryVector_DimFallback(t *testing.T) {
	pool := memory.NewGoAllocator()
	dim := 16
	bytesPerVec := dim / 8
	rec := buildTestRecord(
		pool,
		[]arrow.Field{{Name: "vec", Type: &arrow.FixedSizeBinaryType{ByteWidth: bytesPerVec}}},
		func(bs []array.Builder) {
			b := bs[0].(*array.FixedSizeBinaryBuilder)
			b.Append([]byte{0xFF, 0x00})
			b.Append([]byte{0x0F, 0xF0})
		},
	)
	defer rec.Release()

	schemas := []*schemapb.FieldSchema{
		{FieldID: 110, Name: "vec", DataType: schemapb.DataType_BinaryVector},
	}
	result, err := ArrowFieldsToProto(rec, schemas)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, int64(dim), result[0].GetVectors().GetDim())
	assert.Equal(t, []byte{0xFF, 0x00, 0x0F, 0xF0}, result[0].GetVectors().GetBinaryVector())
}
