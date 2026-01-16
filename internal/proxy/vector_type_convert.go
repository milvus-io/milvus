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
	"encoding/binary"
	"fmt"
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Vector type compatibility matrix:
//
// Query Type      | Field Type       | Action
// ----------------|------------------|------------------
// FloatVector     | FloatVector      | pass through
// FloatVector     | Float16Vector    | convert fp32->fp16
// FloatVector     | BFloat16Vector   | convert fp32->bf16
// Float16Vector   | Float16Vector    | pass through
// BFloat16Vector  | BFloat16Vector   | pass through
// BinaryVector    | BinaryVector     | pass through
// Int8Vector      | Int8Vector       | pass through
// SparseFloat     | SparseFloat      | pass through
// *               | *                | error (incompatible)

// placeholderTypeToDataType maps PlaceholderType to corresponding DataType
var placeholderTypeToDataType = map[commonpb.PlaceholderType]schemapb.DataType{
	commonpb.PlaceholderType_FloatVector:       schemapb.DataType_FloatVector,
	commonpb.PlaceholderType_Float16Vector:     schemapb.DataType_Float16Vector,
	commonpb.PlaceholderType_BFloat16Vector:    schemapb.DataType_BFloat16Vector,
	commonpb.PlaceholderType_BinaryVector:      schemapb.DataType_BinaryVector,
	commonpb.PlaceholderType_Int8Vector:        schemapb.DataType_Int8Vector,
	commonpb.PlaceholderType_SparseFloatVector: schemapb.DataType_SparseFloatVector,
}

// isVectorTypeMatch checks if the placeholder type matches the field data type exactly.
func isVectorTypeMatch(placeholderType commonpb.PlaceholderType, fieldType schemapb.DataType) bool {
	expectedDataType, ok := placeholderTypeToDataType[placeholderType]
	if !ok {
		return false
	}
	return expectedDataType == fieldType
}

const (
	// float16MaxValue is the maximum representable value in float16
	float16MaxValue = 65504.0
	// float16MinPositive is the minimum positive normal value in float16
	float16MinPositive = 6.103515625e-5
)

// validateFloat32ForFloat16 checks if all float32 values can be safely converted to float16.
// Returns error if any value exceeds float16 range or underflows.
func validateFloat32ForFloat16(values []float32) error {
	for i, v := range values {
		absV := float64(v)
		if absV < 0 {
			absV = -absV
		}

		// Check overflow
		if absV > float16MaxValue {
			return fmt.Errorf("value at dimension %d (%v) exceeds float16 range [-65504, 65504]", i, v)
		}

		// Check underflow (non-zero values smaller than min positive)
		if v != 0 && absV < float16MinPositive {
			return fmt.Errorf("value at dimension %d (%v) underflows float16 precision (min abs value: %v)", i, v, float16MinPositive)
		}
	}
	return nil
}

// validateFloat32ForBFloat16 checks if all float32 values can be safely converted to bfloat16.
// BFloat16 has the same exponent range as float32, so only need to check for Inf/NaN.
func validateFloat32ForBFloat16(values []float32) error {
	for i, v := range values {
		if math.IsInf(float64(v), 0) {
			return fmt.Errorf("value at dimension %d is infinity, cannot convert to bfloat16", i)
		}
		if math.IsNaN(float64(v)) {
			return fmt.Errorf("value at dimension %d is NaN, cannot convert to bfloat16", i)
		}
	}
	return nil
}

// ConvertPlaceholderGroup checks and converts placeholder group vector types if needed.
// If the placeholder type matches the field type, returns the original bytes unchanged.
// If the placeholder is fp32 and field is fp16/bf16, converts the vectors.
// Otherwise returns an error for incompatible types.
func ConvertPlaceholderGroup(phgBytes []byte, fieldSchema *schemapb.FieldSchema) ([]byte, error) {
	var phg commonpb.PlaceholderGroup
	if err := proto.Unmarshal(phgBytes, &phg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal placeholder group: %w", err)
	}

	if len(phg.Placeholders) == 0 {
		return phgBytes, nil
	}

	placeholder := phg.Placeholders[0]
	fieldType := fieldSchema.GetDataType()

	// Check if types already match
	if isVectorTypeMatch(placeholder.Type, fieldType) {
		return phgBytes, nil
	}

	// Check if conversion is supported (fp32 -> fp16/bf16)
	if placeholder.Type != commonpb.PlaceholderType_FloatVector {
		return nil, fmt.Errorf("vector type mismatch: query type %s is not compatible with field type %s",
			placeholder.Type.String(), fieldType.String())
	}

	switch fieldType {
	case schemapb.DataType_Float16Vector:
		return convertPlaceholderToFloat16(&phg)
	case schemapb.DataType_BFloat16Vector:
		return convertPlaceholderToBFloat16(&phg)
	default:
		return nil, fmt.Errorf("vector type mismatch: query type %s is not compatible with field type %s",
			placeholder.Type.String(), fieldType.String())
	}
}

// convertPlaceholderToFloat16 converts fp32 vectors in placeholder to fp16.
func convertPlaceholderToFloat16(phg *commonpb.PlaceholderGroup) ([]byte, error) {
	placeholder := phg.Placeholders[0]
	convertedValues := make([][]byte, len(placeholder.Values))

	for i, valueBytes := range placeholder.Values {
		floats, err := bytesToFloat32Array(valueBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse float32 vector at index %d: %w", i, err)
		}

		if err := validateFloat32ForFloat16(floats); err != nil {
			return nil, err
		}

		convertedValues[i] = typeutil.Float32ArrayToFloat16Bytes(floats)
	}

	placeholder.Type = commonpb.PlaceholderType_Float16Vector
	placeholder.Values = convertedValues

	return proto.Marshal(phg)
}

// convertPlaceholderToBFloat16 converts fp32 vectors in placeholder to bf16.
func convertPlaceholderToBFloat16(phg *commonpb.PlaceholderGroup) ([]byte, error) {
	placeholder := phg.Placeholders[0]
	convertedValues := make([][]byte, len(placeholder.Values))

	for i, valueBytes := range placeholder.Values {
		floats, err := bytesToFloat32Array(valueBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse float32 vector at index %d: %w", i, err)
		}

		if err := validateFloat32ForBFloat16(floats); err != nil {
			return nil, err
		}

		convertedValues[i] = typeutil.Float32ArrayToBFloat16Bytes(floats)
	}

	placeholder.Type = commonpb.PlaceholderType_BFloat16Vector
	placeholder.Values = convertedValues

	return proto.Marshal(phg)
}

// bytesToFloat32Array converts byte slice to float32 array.
func bytesToFloat32Array(data []byte) ([]float32, error) {
	if len(data)%4 != 0 {
		return nil, fmt.Errorf("invalid float32 vector data length: %d", len(data))
	}

	dim := len(data) / 4
	result := make([]float32, dim)
	for i := 0; i < dim; i++ {
		bits := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		result[i] = math.Float32frombits(bits)
	}
	return result, nil
}
