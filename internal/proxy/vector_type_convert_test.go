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

package proxy

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestIsVectorTypeMatch(t *testing.T) {
	tests := []struct {
		name            string
		placeholderType commonpb.PlaceholderType
		fieldType       schemapb.DataType
		expected        bool
	}{
		{"fp32 to fp32", commonpb.PlaceholderType_FloatVector, schemapb.DataType_FloatVector, true},
		{"fp16 to fp16", commonpb.PlaceholderType_Float16Vector, schemapb.DataType_Float16Vector, true},
		{"bf16 to bf16", commonpb.PlaceholderType_BFloat16Vector, schemapb.DataType_BFloat16Vector, true},
		{"binary to binary", commonpb.PlaceholderType_BinaryVector, schemapb.DataType_BinaryVector, true},
		{"int8 to int8", commonpb.PlaceholderType_Int8Vector, schemapb.DataType_Int8Vector, true},
		{"sparse to sparse", commonpb.PlaceholderType_SparseFloatVector, schemapb.DataType_SparseFloatVector, true},
		{"fp32 to fp16", commonpb.PlaceholderType_FloatVector, schemapb.DataType_Float16Vector, false},
		{"fp32 to bf16", commonpb.PlaceholderType_FloatVector, schemapb.DataType_BFloat16Vector, false},
		{"fp16 to fp32", commonpb.PlaceholderType_Float16Vector, schemapb.DataType_FloatVector, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isVectorTypeMatch(tt.placeholderType, tt.fieldType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidatePlaceholderGroupDimensions(t *testing.T) {
	newField := func(dataType, elementType schemapb.DataType, dim int64) *schemapb.FieldSchema {
		return &schemapb.FieldSchema{
			Name:        "vector",
			DataType:    dataType,
			ElementType: elementType,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: fmt.Sprintf("%d", dim)},
			},
		}
	}

	tests := []struct {
		name            string
		field           *schemapb.FieldSchema
		placeholderType commonpb.PlaceholderType
		valueSizes      []int
		wantErr         bool
	}{
		{"float vector", newField(schemapb.DataType_FloatVector, schemapb.DataType_None, 4), commonpb.PlaceholderType_FloatVector, []int{16, 16}, false},
		{"float vector mismatch", newField(schemapb.DataType_FloatVector, schemapb.DataType_None, 4), commonpb.PlaceholderType_FloatVector, []int{16, 12}, true},
		{"float16 vector", newField(schemapb.DataType_Float16Vector, schemapb.DataType_None, 4), commonpb.PlaceholderType_Float16Vector, []int{8}, false},
		{"bfloat16 vector mismatch", newField(schemapb.DataType_BFloat16Vector, schemapb.DataType_None, 4), commonpb.PlaceholderType_BFloat16Vector, []int{6}, true},
		{"binary vector", newField(schemapb.DataType_BinaryVector, schemapb.DataType_None, 16), commonpb.PlaceholderType_BinaryVector, []int{2}, false},
		{"binary vector mismatch", newField(schemapb.DataType_BinaryVector, schemapb.DataType_None, 16), commonpb.PlaceholderType_BinaryVector, []int{1}, true},
		{"int8 vector", newField(schemapb.DataType_Int8Vector, schemapb.DataType_None, 4), commonpb.PlaceholderType_Int8Vector, []int{4}, false},
		{"int8 vector mismatch", newField(schemapb.DataType_Int8Vector, schemapb.DataType_None, 4), commonpb.PlaceholderType_Int8Vector, []int{5}, true},
		{"array element level", newField(schemapb.DataType_ArrayOfVector, schemapb.DataType_FloatVector, 4), commonpb.PlaceholderType_FloatVector, []int{16}, false},
		{"array element level rejects multiple vectors", newField(schemapb.DataType_ArrayOfVector, schemapb.DataType_FloatVector, 4), commonpb.PlaceholderType_FloatVector, []int{32}, true},
		{"float embedding list", newField(schemapb.DataType_ArrayOfVector, schemapb.DataType_FloatVector, 4), commonpb.PlaceholderType_EmbListFloatVector, []int{16, 48}, false},
		{"float embedding list mismatch", newField(schemapb.DataType_ArrayOfVector, schemapb.DataType_FloatVector, 4), commonpb.PlaceholderType_EmbListFloatVector, []int{20}, true},
		{"float16 embedding list", newField(schemapb.DataType_ArrayOfVector, schemapb.DataType_Float16Vector, 4), commonpb.PlaceholderType_EmbListFloat16Vector, []int{8, 16}, false},
		{"bfloat16 embedding list mismatch", newField(schemapb.DataType_ArrayOfVector, schemapb.DataType_BFloat16Vector, 4), commonpb.PlaceholderType_EmbListBFloat16Vector, []int{10}, true},
		{"binary embedding list", newField(schemapb.DataType_ArrayOfVector, schemapb.DataType_BinaryVector, 16), commonpb.PlaceholderType_EmbListBinaryVector, []int{2, 4}, false},
		{"binary embedding list mismatch", newField(schemapb.DataType_ArrayOfVector, schemapb.DataType_BinaryVector, 16), commonpb.PlaceholderType_EmbListBinaryVector, []int{3}, true},
		{"int8 embedding list", newField(schemapb.DataType_ArrayOfVector, schemapb.DataType_Int8Vector, 4), commonpb.PlaceholderType_EmbListInt8Vector, []int{4, 12}, false},
		{"int8 embedding list mismatch", newField(schemapb.DataType_ArrayOfVector, schemapb.DataType_Int8Vector, 4), commonpb.PlaceholderType_EmbListInt8Vector, []int{5}, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := make([][]byte, 0, len(test.valueSizes))
			for _, size := range test.valueSizes {
				values = append(values, make([]byte, size))
			}

			_, _, err := ConvertPlaceholderGroup(
				mustMarshalPlaceholderGroup(t, test.placeholderType, values),
				test.field,
			)
			if test.wantErr {
				assert.ErrorIs(t, err, merr.ErrParameterInvalid)
				assert.Equal(t, merr.InputError, merr.GetErrorType(err))
				assert.False(t, merr.IsRetryableErr(err))
				assert.Contains(t, err.Error(), "vector dimension mismatch")
				assert.Contains(t, err.Error(), "vector")
				return
			}
			assert.NoError(t, err)
		})
	}

	for _, test := range []struct {
		name            string
		field           *schemapb.FieldSchema
		placeholderType commonpb.PlaceholderType
		value           []byte
	}{
		{"sparse vector uses sparse parser", &schemapb.FieldSchema{Name: "sparse", DataType: schemapb.DataType_SparseFloatVector}, commonpb.PlaceholderType_SparseFloatVector, []byte{1, 2, 3}},
		{"varchar uses function executor", newField(schemapb.DataType_FloatVector, schemapb.DataType_None, 4), commonpb.PlaceholderType_VarChar, []byte("query")},
		{"incompatible vector type uses type validation", newField(schemapb.DataType_FloatVector, schemapb.DataType_None, 4), commonpb.PlaceholderType_BinaryVector, []byte{0}},
		{"incompatible embedding list uses type validation", newField(schemapb.DataType_ArrayOfVector, schemapb.DataType_FloatVector, 4), commonpb.PlaceholderType_EmbListBinaryVector, []byte{0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := ConvertPlaceholderGroup(
				mustMarshalPlaceholderGroup(t, test.placeholderType, [][]byte{test.value}),
				test.field,
			)
			assert.NoError(t, err)
		})
	}

	t.Run("malformed placeholder group", func(t *testing.T) {
		_, _, err := ConvertPlaceholderGroup(
			[]byte{0xff},
			newField(schemapb.DataType_FloatVector, schemapb.DataType_None, 4),
		)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("invalid collection schema is a system error", func(t *testing.T) {
		fieldSchema := &schemapb.FieldSchema{
			Name:     "vector",
			DataType: schemapb.DataType_FloatVector,
		}
		_, _, err := ConvertPlaceholderGroup(
			mustMarshalPlaceholderGroup(t, commonpb.PlaceholderType_FloatVector, [][]byte{make([]byte, 16)}),
			fieldSchema,
		)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
	})

	t.Run("invalid function output is a function error", func(t *testing.T) {
		err := validatePlaceholderGroupDimensions(
			mustMarshalPlaceholderGroup(t, commonpb.PlaceholderType_FloatVector, [][]byte{make([]byte, 12)}),
			newField(schemapb.DataType_FloatVector, schemapb.DataType_None, 4),
			merr.WrapErrFunctionFailedMsg,
		)
		assert.ErrorIs(t, err, merr.ErrFunctionFailed)
		assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
		assert.False(t, merr.IsRetryableErr(err))
	})
}

func TestConvertPlaceholderGroupValidatesDimensionsAfterConversion(t *testing.T) {
	for _, dataType := range []schemapb.DataType{
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
	} {
		t.Run(dataType.String(), func(t *testing.T) {
			fieldSchema := &schemapb.FieldSchema{
				Name:     "vector",
				DataType: dataType,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "4"},
				},
			}
			_, _, err := ConvertPlaceholderGroup(
				createFloat32PlaceholderGroup([][]float32{{0.1, 0.2, 0.3}}),
				fieldSchema,
			)
			assert.ErrorIs(t, err, merr.ErrParameterInvalid)
			assert.Contains(t, err.Error(), "expected dimension 4")
		})
	}
}

func mustMarshalPlaceholderGroup(t *testing.T, placeholderType commonpb.PlaceholderType, values [][]byte) []byte {
	t.Helper()
	bytes, err := proto.Marshal(&commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{
				Tag:    "$0",
				Type:   placeholderType,
				Values: values,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal placeholder group: %v", err)
	}
	return bytes
}

func TestConvertPlaceholderGroupAllowsFloat16Underflow(t *testing.T) {
	vectors := [][]float32{{0.0, 1e-9, -1e-9, 0.5}}
	phgBytes := createFloat32PlaceholderGroup(vectors)
	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_Float16Vector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "4"},
		},
	}

	convertedBytes, _, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
	assert.NoError(t, err)

	var resultPhg commonpb.PlaceholderGroup
	assert.NoError(t, proto.Unmarshal(convertedBytes, &resultPhg))
	assertPlaceholderVectorsMatchFloat32(t, commonpb.PlaceholderType_Float16Vector, schemapb.DataType_Float16Vector, resultPhg.Placeholders, vectors)
}

func TestConvertPlaceholderGroupRejectsInvalidFloatInput(t *testing.T) {
	for _, tt := range []struct {
		name      string
		dataType  schemapb.DataType
		vectors   [][]float32
		errSubstr string
	}{
		{
			name:      "float16 rejects NaN",
			dataType:  schemapb.DataType_Float16Vector,
			vectors:   [][]float32{{0.0, float32(math.NaN())}},
			errSubstr: "not a number or infinity",
		},
		{
			name:      "bfloat16 rejects infinity",
			dataType:  schemapb.DataType_BFloat16Vector,
			vectors:   [][]float32{{0.0, float32(math.Inf(1))}},
			errSubstr: "not a number or infinity",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			phgBytes := createFloat32PlaceholderGroup(tt.vectors)
			fieldSchema := &schemapb.FieldSchema{DataType: tt.dataType}

			_, _, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)

			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.errSubstr)
		})
	}
}

func assertFP16BF16BytesMatchFloat32(
	t *testing.T,
	dataType schemapb.DataType,
	floatData []float32,
	got []byte,
) {
	t.Helper()
	var want []byte
	var decoded []float32
	var tolerance float64
	switch dataType {
	case schemapb.DataType_Float16Vector:
		want = typeutil.Float32ArrayToFloat16Bytes(floatData)
		decoded = typeutil.Float16BytesToFloat32Vector(got)
		tolerance = 1e-3
	case schemapb.DataType_BFloat16Vector:
		want = typeutil.Float32ArrayToBFloat16Bytes(floatData)
		decoded = typeutil.BFloat16BytesToFloat32Vector(got)
		tolerance = 1e-2
	default:
		t.Fatalf("unsupported data type: %s", dataType.String())
	}
	assert.Equal(t, want, got)
	assert.InDeltaSlice(t, floatData, decoded, tolerance)
}

func assertPlaceholderVectorsMatchFloat32(
	t *testing.T,
	placeholderType commonpb.PlaceholderType,
	fieldType schemapb.DataType,
	placeholders []*commonpb.PlaceholderValue,
	inputs [][]float32,
) {
	t.Helper()
	assert.Len(t, placeholders, 1)
	placeholder := placeholders[0]
	assert.Equal(t, placeholderType, placeholder.GetType())
	assert.Len(t, placeholder.GetValues(), len(inputs))
	for i, input := range inputs {
		switch fieldType {
		case schemapb.DataType_Float16Vector:
			assertFP16BF16BytesMatchFloat32(t, fieldType, input, placeholder.GetValues()[i])
		case schemapb.DataType_BFloat16Vector:
			assertFP16BF16BytesMatchFloat32(t, fieldType, input, placeholder.GetValues()[i])
		default:
			assert.Equal(t, typeutil.Float32ArrayToBytes(input), placeholder.GetValues()[i])
		}
	}
}

func fp16BF16SchemaInfo(dataType schemapb.DataType, dim int64) *schemaInfo {
	return mustNewSchemaInfo(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  100,
				Name:     "vector",
				DataType: dataType,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: fmt.Sprintf("%d", dim)},
				},
			},
		},
	})
}

func createFloat32PlaceholderGroup(vectors [][]float32) []byte {
	values := make([][]byte, len(vectors))
	for i, vec := range vectors {
		values[i] = typeutil.Float32ArrayToBytes(vec)
	}

	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_FloatVector,
			Values: values,
		}},
	}

	bytes, _ := proto.Marshal(phg)
	return bytes
}

func TestConvertPlaceholderGroupToFloat16(t *testing.T) {
	vectors := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
		{0.5, 0.6, 0.7, 0.8},
	}
	phgBytes := createFloat32PlaceholderGroup(vectors)

	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_Float16Vector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "4"},
		},
	}

	convertedBytes, _, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
	assert.NoError(t, err)

	var resultPhg commonpb.PlaceholderGroup
	err = proto.Unmarshal(convertedBytes, &resultPhg)
	assert.NoError(t, err)

	assert.Equal(t, commonpb.PlaceholderType_Float16Vector, resultPhg.Placeholders[0].Type)
	assert.Equal(t, 4*2, len(resultPhg.Placeholders[0].Values[0])) // 4 dimensions * 2 bytes
}

func TestConvertPlaceholderGroupToBFloat16(t *testing.T) {
	vectors := [][]float32{
		{0.1, 0.2, 0.3, 0.4},
	}
	phgBytes := createFloat32PlaceholderGroup(vectors)

	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_BFloat16Vector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "4"},
		},
	}

	convertedBytes, _, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
	assert.NoError(t, err)

	var resultPhg commonpb.PlaceholderGroup
	err = proto.Unmarshal(convertedBytes, &resultPhg)
	assert.NoError(t, err)

	assert.Equal(t, commonpb.PlaceholderType_BFloat16Vector, resultPhg.Placeholders[0].Type)
	assert.Equal(t, 4*2, len(resultPhg.Placeholders[0].Values[0]))
}

func TestConvertPlaceholderGroupTypeMismatch(t *testing.T) {
	// Test: Float16Vector searching BFloat16Vector field -> should error
	// (Only fp32 -> fp16/bf16 conversion is supported)
	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_Float16Vector,
			Values: [][]byte{{0, 0, 0, 0}},
		}},
	}
	phgBytes, _ := proto.Marshal(phg)

	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_BFloat16Vector,
	}

	_, _, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "vector type must be the same")
}

func TestConvertPlaceholderGroupPassThrough(t *testing.T) {
	// Test: Float16Vector searching FloatVector field -> pass through
	// Let downstream handle the type mismatch
	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_Float16Vector,
			Values: [][]byte{{0, 0, 0, 0}},
		}},
	}
	phgBytes, _ := proto.Marshal(phg)

	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
	}

	result, _, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
	assert.NoError(t, err)
	assert.Equal(t, phgBytes, result)
}

func TestConvertPlaceholderGroupNoConversionNeeded(t *testing.T) {
	vectors := [][]float32{{0.1, 0.2, 0.3, 0.4}}
	phgBytes := createFloat32PlaceholderGroup(vectors)

	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "4"},
		},
	}

	convertedBytes, _, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
	assert.NoError(t, err)
	assert.Equal(t, phgBytes, convertedBytes)
}

func TestConvertPlaceholderGroupOverflowToFloat16Infinity(t *testing.T) {
	vectors := [][]float32{{0.1, 100000.0, 0.3, 0.4}}
	phgBytes := createFloat32PlaceholderGroup(vectors)

	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_Float16Vector,
	}

	_, _, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nan or infinity")
}

func TestNormalizeFp32ToFp16Bf16VectorFieldData(t *testing.T) {
	newTestSchema := func(dataType schemapb.DataType) *schemaInfo {
		return mustNewSchemaInfo(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "vector",
					DataType: dataType,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "2"},
					},
				},
			},
		})
	}

	for _, tt := range []struct {
		name      string
		dataType  schemapb.DataType
		fieldData *schemapb.FieldData
		wantBytes func([]float32) []byte
	}{
		{
			name:      "float16",
			dataType:  schemapb.DataType_Float16Vector,
			fieldData: testFloatVectorFieldData("vector", []float32{0.1, 0.2, 0.3, 0.4}, 2),
			wantBytes: typeutil.Float32ArrayToFloat16Bytes,
		},
		{
			name:      "bfloat16",
			dataType:  schemapb.DataType_BFloat16Vector,
			fieldData: testFloatVectorFieldData("vector", []float32{0.1, 0.2, 0.3, 0.4}, 2),
			wantBytes: typeutil.Float32ArrayToBFloat16Bytes,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			floatData := append([]float32(nil), tt.fieldData.GetVectors().GetFloatVector().GetData()...)
			schema := newTestSchema(tt.dataType)
			err := fillFieldPropertiesOnly([]*schemapb.FieldData{tt.fieldData}, schema)
			assert.NoError(t, err)

			err = normalizeFP32ToFP16BF16VectorFieldData([]*schemapb.FieldData{tt.fieldData}, schema)
			assert.NoError(t, err)
			assert.Equal(t, tt.dataType, tt.fieldData.GetType())
			assert.Equal(t, int64(2), tt.fieldData.GetVectors().GetDim())
			if tt.dataType == schemapb.DataType_Float16Vector {
				assert.Equal(t, tt.wantBytes(floatData), tt.fieldData.GetVectors().GetFloat16Vector())
			} else {
				assert.Equal(t, tt.wantBytes(floatData), tt.fieldData.GetVectors().GetBfloat16Vector())
			}
		})
	}

	t.Run("skip non fp16 bf16 schema field", func(t *testing.T) {
		fieldData := testFloatVectorFieldData("vector", []float32{0.1, 0.2}, 2)
		schema := newTestSchema(schemapb.DataType_FloatVector)
		err := fillFieldPropertiesOnly([]*schemapb.FieldData{fieldData}, schema)
		assert.NoError(t, err)
		err = normalizeFP32ToFP16BF16VectorFieldData([]*schemapb.FieldData{fieldData}, schema)
		assert.NoError(t, err)
		assert.NotNil(t, fieldData.GetVectors().GetFloatVector())
	})

	t.Run("skip non float vector payload", func(t *testing.T) {
		fieldData := &schemapb.FieldData{FieldName: "vector"}
		schema := newTestSchema(schemapb.DataType_Float16Vector)
		err := normalizeFP32ToFP16BF16VectorFieldData([]*schemapb.FieldData{fieldData}, schema)
		assert.NoError(t, err)
	})

	t.Run("unknown field", func(t *testing.T) {
		fieldData := testFloatVectorFieldData("missing", []float32{0.1, 0.2}, 2)
		err := normalizeFP32ToFP16BF16VectorFieldData([]*schemapb.FieldData{fieldData}, newTestSchema(schemapb.DataType_Float16Vector))
		assert.Error(t, err)
	})

	t.Run("invalid fp32 value", func(t *testing.T) {
		fieldData := testFloatVectorFieldData("vector", []float32{float32(math.Inf(1)), 0.2}, 2)
		schema := newTestSchema(schemapb.DataType_Float16Vector)
		err := fillFieldPropertiesOnly([]*schemapb.FieldData{fieldData}, schema)
		assert.NoError(t, err)
		err = normalizeFP32ToFP16BF16VectorFieldData([]*schemapb.FieldData{fieldData}, schema)
		assert.Error(t, err)
	})
}

func testFloatVectorFieldData(fieldName string, data []float32, dim int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldName: fieldName,
		Type:      schemapb.DataType_FloatVector,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{Data: data},
				},
			},
		},
	}
}

func TestConvertPlaceholderGroupIntegration(t *testing.T) {
	dim := int64(128)
	nq := 10

	// Create fp32 vectors (simulating typical embedding model output)
	vectors := make([][]float32, nq)
	for i := 0; i < nq; i++ {
		vectors[i] = make([]float32, dim)
		for j := int64(0); j < dim; j++ {
			vectors[i][j] = float32(i+1) * 0.01 * float32(j+1) / float32(dim)
		}
	}

	// Create placeholder group
	values := make([][]byte, nq)
	for i, vec := range vectors {
		values[i] = typeutil.Float32ArrayToBytes(vec)
	}

	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_FloatVector,
			Values: values,
		}},
	}
	phgBytes, err := proto.Marshal(phg)
	assert.NoError(t, err)

	t.Run("convert to float16", func(t *testing.T) {
		fieldSchema := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Float16Vector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			},
		}

		convertedBytes, _, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
		assert.NoError(t, err)

		var resultPhg commonpb.PlaceholderGroup
		err = proto.Unmarshal(convertedBytes, &resultPhg)
		assert.NoError(t, err)

		assert.Equal(t, commonpb.PlaceholderType_Float16Vector, resultPhg.Placeholders[0].Type)
		assert.Equal(t, nq, len(resultPhg.Placeholders[0].Values))
		assert.Equal(t, int(dim)*2, len(resultPhg.Placeholders[0].Values[0])) // 2 bytes per float16
	})

	t.Run("convert to bfloat16", func(t *testing.T) {
		fieldSchema := &schemapb.FieldSchema{
			DataType: schemapb.DataType_BFloat16Vector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			},
		}

		convertedBytes, _, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
		assert.NoError(t, err)

		var resultPhg commonpb.PlaceholderGroup
		err = proto.Unmarshal(convertedBytes, &resultPhg)
		assert.NoError(t, err)

		assert.Equal(t, commonpb.PlaceholderType_BFloat16Vector, resultPhg.Placeholders[0].Type)
		assert.Equal(t, nq, len(resultPhg.Placeholders[0].Values))
		assert.Equal(t, int(dim)*2, len(resultPhg.Placeholders[0].Values[0])) // 2 bytes per bfloat16
	})

	t.Run("no conversion for matching type", func(t *testing.T) {
		fieldSchema := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: "128"},
			},
		}

		convertedBytes, _, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
		assert.NoError(t, err)
		assert.Equal(t, phgBytes, convertedBytes) // Should be unchanged
	})
}
