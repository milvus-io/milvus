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
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

var embeddingListPlaceholderTypeToDataType = map[commonpb.PlaceholderType]schemapb.DataType{
	commonpb.PlaceholderType_EmbListFloatVector:    schemapb.DataType_FloatVector,
	commonpb.PlaceholderType_EmbListFloat16Vector:  schemapb.DataType_Float16Vector,
	commonpb.PlaceholderType_EmbListBFloat16Vector: schemapb.DataType_BFloat16Vector,
	commonpb.PlaceholderType_EmbListBinaryVector:   schemapb.DataType_BinaryVector,
	commonpb.PlaceholderType_EmbListInt8Vector:     schemapb.DataType_Int8Vector,
}

// isVectorTypeMatch checks if the placeholder type matches the field data type exactly.
func isVectorTypeMatch(placeholderType commonpb.PlaceholderType, fieldType schemapb.DataType) bool {
	expectedDataType, ok := placeholderTypeToDataType[placeholderType]
	if !ok {
		return false
	}
	return expectedDataType == fieldType
}

// ConvertPlaceholderGroup validates vector dimensions and converts placeholder group
// vector types if needed.
// If the placeholder type matches the field type, returns the original bytes unchanged.
// If the placeholder is fp32 and field is fp16/bf16, converts the vectors.
// Otherwise returns an error for incompatible types.
// Also returns the original placeholder type (before conversion) for callers that need it.
func ConvertPlaceholderGroup(phgBytes []byte, fieldSchema *schemapb.FieldSchema) ([]byte, commonpb.PlaceholderType, error) {
	var phg commonpb.PlaceholderGroup
	if err := proto.Unmarshal(phgBytes, &phg); err != nil {
		return nil, 0, merr.WrapErrParameterInvalidMsg("failed to unmarshal placeholder group: %v", err)
	}

	if len(phg.Placeholders) == 0 {
		return phgBytes, 0, nil
	}

	placeholder := phg.Placeholders[0]
	phType := placeholder.Type
	fieldType := fieldSchema.GetDataType()
	if err := validateParsedPlaceholderGroupDimensions(&phg, fieldSchema, merr.WrapErrParameterInvalidMsg); err != nil {
		return nil, phType, err
	}

	// Check if types already match
	if isVectorTypeMatch(placeholder.Type, fieldType) {
		return phgBytes, phType, nil
	}

	// If placeholder is not a vector type (e.g., VarChar for text embedding), pass through.
	// Let downstream logic handle non-vector placeholders.
	if _, isVectorType := placeholderTypeToDataType[placeholder.Type]; !isVectorType {
		return phgBytes, phType, nil
	}

	// Only handle fp32 -> fp16/bf16 conversion.
	// For other field types (e.g., SparseFloatVector, BinaryVector), pass through
	// and let downstream logic handle the type mismatch with appropriate error messages.
	if fieldType != schemapb.DataType_Float16Vector && fieldType != schemapb.DataType_BFloat16Vector {
		return phgBytes, phType, nil
	}

	// Check if conversion is supported (fp32 -> fp16/bf16)
	if placeholder.Type != commonpb.PlaceholderType_FloatVector {
		return nil, phType, merr.WrapErrParameterInvalidMsg("vector type must be the same: field type %s, search type %s",
			fieldType.String(), placeholder.Type.String())
	}

	switch fieldType {
	case schemapb.DataType_Float16Vector:
		converted, err := convertPlaceholder(&phg, fieldSchema, commonpb.PlaceholderType_Float16Vector)
		return converted, phType, err
	case schemapb.DataType_BFloat16Vector:
		converted, err := convertPlaceholder(&phg, fieldSchema, commonpb.PlaceholderType_BFloat16Vector)
		return converted, phType, err
	default:
		// This should never be reached due to the check above, but keep for safety
		return phgBytes, phType, nil
	}
}

// validatePlaceholderGroupDimensions verifies the final, encoded query vectors against
// the collection schema. It intentionally ignores sparse and non-vector placeholders,
// which have their own parsing or function-execution paths.
func validatePlaceholderGroupDimensions(
	phgBytes []byte,
	fieldSchema *schemapb.FieldSchema,
	newDimensionError func(string, ...any) error,
) error {
	if fieldSchema == nil ||
		(!typeutil.IsFixDimVectorType(fieldSchema.GetDataType()) && !typeutil.IsArrayOfVectorType(fieldSchema.GetDataType())) {
		return nil
	}

	var phg commonpb.PlaceholderGroup
	if err := proto.Unmarshal(phgBytes, &phg); err != nil {
		return newDimensionError("failed to unmarshal placeholder group: %v", err)
	}
	return validateParsedPlaceholderGroupDimensions(&phg, fieldSchema, newDimensionError)
}

func validateParsedPlaceholderGroupDimensions(
	phg *commonpb.PlaceholderGroup,
	fieldSchema *schemapb.FieldSchema,
	newDimensionError func(string, ...any) error,
) error {
	if fieldSchema == nil ||
		(!typeutil.IsFixDimVectorType(fieldSchema.GetDataType()) && !typeutil.IsArrayOfVectorType(fieldSchema.GetDataType())) {
		return nil
	}

	var dim int64
	dimLoaded := false
	for _, placeholder := range phg.GetPlaceholders() {
		vectorType, isEmbeddingList, matchesField := placeholderVectorType(placeholder.GetType(), fieldSchema)
		if !matchesField {
			continue
		}
		if !dimLoaded {
			var err error
			dim, err = typeutil.GetDim(fieldSchema)
			if err != nil {
				return merr.WrapErrServiceInternalErr(err, "failed to get dimension for field %q", fieldSchema.GetName())
			}
			dimLoaded = true
		}
		bytesPerVector, isFixedDimension := vectorBytesForDimension(vectorType, dim)
		if !isFixedDimension {
			continue
		}
		if bytesPerVector <= 0 {
			return merr.WrapErrServiceInternalMsg(
				"invalid vector dimension %d for field %q", dim, fieldSchema.GetName())
		}

		for valueIndex, value := range placeholder.GetValues() {
			actualBytes := int64(len(value))
			if isEmbeddingList {
				if actualBytes%bytesPerVector != 0 {
					return newDimensionError(
						"vector dimension mismatch for field %q at placeholder %q, value %d: expected each vector to have dimension %d (%d bytes), actual byte length %d is not a multiple of %d",
						fieldSchema.GetName(), placeholder.GetTag(), valueIndex, dim, bytesPerVector, actualBytes, bytesPerVector)
				}
				continue
			}

			if actualBytes != bytesPerVector {
				return newDimensionError(
					"vector dimension mismatch for field %q at placeholder %q, value %d: expected dimension %d (%d bytes), actual byte length %d",
					fieldSchema.GetName(), placeholder.GetTag(), valueIndex, dim, bytesPerVector, actualBytes)
			}
		}
	}

	return nil
}

func placeholderVectorType(placeholderType commonpb.PlaceholderType, fieldSchema *schemapb.FieldSchema) (schemapb.DataType, bool, bool) {
	fieldType := fieldSchema.GetDataType()
	if typeutil.IsArrayOfVectorType(fieldType) {
		fieldType = fieldSchema.GetElementType()
	}

	if vectorType, ok := placeholderTypeToDataType[placeholderType]; ok {
		return vectorType, false, vectorType == fieldType
	}
	if vectorType, ok := embeddingListPlaceholderTypeToDataType[placeholderType]; ok {
		return vectorType, true, typeutil.IsArrayOfVectorType(fieldSchema.GetDataType()) && vectorType == fieldType
	}
	return schemapb.DataType_None, false, false
}

func vectorBytesForDimension(vectorType schemapb.DataType, dim int64) (int64, bool) {
	switch vectorType {
	case schemapb.DataType_FloatVector:
		return dim * 4, true
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return dim * 2, true
	case schemapb.DataType_BinaryVector:
		if dim%8 != 0 {
			return 0, true
		}
		return dim / 8, true
	case schemapb.DataType_Int8Vector:
		return dim, true
	default:
		return 0, false
	}
}

// convertPlaceholder converts fp32 vectors in placeholder to the target type.
func convertPlaceholder(
	phg *commonpb.PlaceholderGroup,
	fieldSchema *schemapb.FieldSchema,
	targetType commonpb.PlaceholderType,
) ([]byte, error) {
	placeholder := phg.Placeholders[0]
	convertedValues := make([][]byte, len(placeholder.Values))

	for i, valueBytes := range placeholder.Values {
		floats, err := bytesToFloat32Array(valueBytes)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("failed to parse float32 vector at index %d: %v", i, err)
		}

		convertedValues[i], err = typeutil.ConvertFloat32ToFP16BF16Bytes(floats, fieldSchema.GetDataType())
		if err != nil {
			return nil, err
		}
	}

	placeholder.Type = targetType
	placeholder.Values = convertedValues
	if err := validateParsedPlaceholderGroupDimensions(phg, fieldSchema, merr.WrapErrParameterInvalidMsg); err != nil {
		return nil, err
	}

	return proto.Marshal(phg)
}

func normalizeFP32ToFP16BF16VectorFieldData(columns []*schemapb.FieldData, schema *schemaInfo) error {
	for _, fieldData := range columns {
		fieldSchema, err := schema.schemaHelper.GetFieldFromNameDefaultJSON(fieldData.GetFieldName())
		if err != nil {
			return err
		}
		if err := normalizeFP32ToFP16BF16VectorField(fieldData, fieldSchema); err != nil {
			return err
		}
	}
	return nil
}

func normalizeFP32ToFP16BF16VectorField(fieldData *schemapb.FieldData, fieldSchema *schemapb.FieldSchema) error {
	fieldType := fieldSchema.GetDataType()
	if fieldType != schemapb.DataType_Float16Vector && fieldType != schemapb.DataType_BFloat16Vector {
		return nil
	}
	vectors := fieldData.GetVectors()
	if vectors == nil || vectors.GetFloatVector() == nil {
		return nil
	}
	converted, err := typeutil.ConvertFloat32ToFP16BF16Bytes(vectors.GetFloatVector().GetData(), fieldType)
	if err != nil {
		return err
	}
	switch fieldType {
	case schemapb.DataType_Float16Vector:
		vectors.Data = &schemapb.VectorField_Float16Vector{Float16Vector: converted}
	case schemapb.DataType_BFloat16Vector:
		vectors.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: converted}
	}
	return nil
}

// bytesToFloat32Array converts byte slice to float32 array.
func bytesToFloat32Array(data []byte) ([]float32, error) {
	if len(data)%4 != 0 {
		return nil, merr.WrapErrParameterInvalidMsg("invalid float32 vector data length: %d", len(data))
	}

	dim := len(data) / 4
	result := make([]float32, dim)
	for i := 0; i < dim; i++ {
		bits := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		result[i] = math.Float32frombits(bits)
	}
	return result, nil
}
