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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

func TestValidateFloat32ForFloat16(t *testing.T) {
	tests := []struct {
		name      string
		values    []float32
		expectErr bool
		errMsg    string
	}{
		{
			name:      "normal values",
			values:    []float32{0.0, 1.0, -1.0, 0.5, -0.5},
			expectErr: false,
		},
		{
			name:      "max boundary",
			values:    []float32{65504.0, -65504.0},
			expectErr: false,
		},
		{
			name:      "overflow positive",
			values:    []float32{0.0, 65505.0},
			expectErr: true,
			errMsg:    "exceeds float16 range",
		},
		{
			name:      "overflow negative",
			values:    []float32{-65505.0, 0.0},
			expectErr: true,
			errMsg:    "exceeds float16 range",
		},
		{
			name:      "underflow",
			values:    []float32{0.0, 1e-9},
			expectErr: true,
			errMsg:    "underflows float16",
		},
		{
			name:      "zero is valid",
			values:    []float32{0.0, 0.0, 0.0},
			expectErr: false,
		},
		{
			name:      "min positive normal",
			values:    []float32{6.104e-5},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFloat32ForFloat16(tt.values)
			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateFloat32ForBFloat16(t *testing.T) {
	tests := []struct {
		name      string
		values    []float32
		expectErr bool
	}{
		{
			name:      "normal values",
			values:    []float32{0.0, 1.0, -1.0, 0.5, -0.5},
			expectErr: false,
		},
		{
			name:      "large values within float32 range",
			values:    []float32{1e38, -1e38},
			expectErr: false,
		},
		{
			name:      "very small values",
			values:    []float32{1e-38, -1e-38},
			expectErr: false,
		},
		{
			name:      "infinity",
			values:    []float32{float32(math.Inf(1))},
			expectErr: true,
		},
		{
			name:      "negative infinity",
			values:    []float32{float32(math.Inf(-1))},
			expectErr: true,
		},
		{
			name:      "NaN",
			values:    []float32{float32(math.NaN())},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFloat32ForBFloat16(tt.values)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
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
	}

	convertedBytes, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
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
	}

	convertedBytes, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
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

	_, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
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

	result, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
	assert.NoError(t, err)
	assert.Equal(t, phgBytes, result)
}

func TestConvertPlaceholderGroupNoConversionNeeded(t *testing.T) {
	vectors := [][]float32{{0.1, 0.2, 0.3, 0.4}}
	phgBytes := createFloat32PlaceholderGroup(vectors)

	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
	}

	convertedBytes, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
	assert.NoError(t, err)
	assert.Equal(t, phgBytes, convertedBytes)
}

func TestConvertPlaceholderGroupOutOfRange(t *testing.T) {
	vectors := [][]float32{{0.1, 100000.0, 0.3, 0.4}}
	phgBytes := createFloat32PlaceholderGroup(vectors)

	fieldSchema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_Float16Vector,
	}

	_, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds float16 range")
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
		}

		convertedBytes, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
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
		}

		convertedBytes, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
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
		}

		convertedBytes, err := ConvertPlaceholderGroup(phgBytes, fieldSchema)
		assert.NoError(t, err)
		assert.Equal(t, phgBytes, convertedBytes) // Should be unchanged
	})
}
