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

package common

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ArrayOfVectorToFieldData converts a user-facing list-of-vectors value into
// the per-row VectorField representation used by VectorArrayFieldData.
//
// A nil entry is a null vector. It is accepted only when the field is
// element-nullable: the payload then holds the non-null vectors only, and
// valid_data has one entry per logical element. A field that is not
// element-nullable carries no valid_data.
func ArrayOfVectorToFieldData(vectors []any, field *schemapb.FieldSchema, dim int) (*schemapb.VectorField, error) {
	elementNullable := field.GetElementNullable()
	var validData []bool
	if elementNullable {
		validData = make([]bool, len(vectors))
	}
	present := make([]positionedVector, 0, len(vectors))
	for i, rawVector := range vectors {
		if rawVector == nil {
			if !elementNullable {
				return nil, merr.WrapErrImportFailedMsg(
					"field '%s' element %d is null, but the sub-field is not element-nullable", field.GetName(), i)
			}
			continue
		}
		vector, err := asVector(rawVector)
		if err != nil {
			return nil, err
		}
		present = append(present, positionedVector{position: i, values: vector})
		if elementNullable {
			validData[i] = true
		}
	}

	row, err := presentVectorsToFieldData(present, field, dim)
	if err != nil {
		return nil, err
	}
	if elementNullable {
		typeutil.SetVectorArrayElementValidData(row, validData)
	}
	return row, nil
}

// positionedVector is a non-null vector with its logical element position,
// which error messages report.
type positionedVector struct {
	position int
	values   []any
}

func presentVectorsToFieldData(vectors []positionedVector, field *schemapb.FieldSchema, dim int) (*schemapb.VectorField, error) {
	switch field.GetElementType() {
	case schemapb.DataType_FloatVector:
		values, err := parseFloatVectorArray(vectors, field, dim)
		if err != nil {
			return nil, err
		}
		if err := typeutil.VerifyFloats32(values); err != nil {
			return nil, err
		}
		return &schemapb.VectorField{
			Dim: int64(dim),
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: values},
			},
		}, nil
	case schemapb.DataType_BinaryVector:
		values, err := parseByteVectorArray(vectors, field, binaryBytesPerVector(dim))
		if err != nil {
			return nil, err
		}
		return &schemapb.VectorField{
			Dim:  int64(dim),
			Data: &schemapb.VectorField_BinaryVector{BinaryVector: values},
		}, nil
	case schemapb.DataType_Float16Vector:
		values, err := parseHalfVectorArray(vectors, field, dim, typeutil.Float32ToFloat16Bytes)
		if err != nil {
			return nil, err
		}
		if err := typeutil.VerifyFloats16(values); err != nil {
			return nil, err
		}
		return &schemapb.VectorField{
			Dim:  int64(dim),
			Data: &schemapb.VectorField_Float16Vector{Float16Vector: values},
		}, nil
	case schemapb.DataType_BFloat16Vector:
		values, err := parseHalfVectorArray(vectors, field, dim, typeutil.Float32ToBFloat16Bytes)
		if err != nil {
			return nil, err
		}
		if err := typeutil.VerifyBFloats16(values); err != nil {
			return nil, err
		}
		return &schemapb.VectorField{
			Dim:  int64(dim),
			Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: values},
		}, nil
	case schemapb.DataType_Int8Vector:
		values, err := parseInt8VectorArray(vectors, field, dim)
		if err != nil {
			return nil, err
		}
		return &schemapb.VectorField{
			Dim:  int64(dim),
			Data: &schemapb.VectorField_Int8Vector{Int8Vector: values},
		}, nil
	case schemapb.DataType_SparseFloatVector:
		return nil, merr.WrapErrImportFailed("ArrayOfVector with SparseFloatVector element type is not implemented yet")
	default:
		return nil, merr.WrapErrImportFailedMsg("unsupported ArrayOfVector element type: %s", field.GetElementType().String())
	}
}

func parseFloatVectorArray(vectors []positionedVector, field *schemapb.FieldSchema, dim int) ([]float32, error) {
	values := make([]float32, 0, len(vectors)*dim)
	for _, vector := range vectors {
		if len(vector.values) != dim {
			return nil, vectorDimErr(field, vector.position, len(vector.values), dim)
		}
		for _, rawValue := range vector.values {
			value, err := asFloat32(rawValue)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
	}
	return values, nil
}

func parseByteVectorArray(vectors []positionedVector, field *schemapb.FieldSchema, bytesPerVector int) ([]byte, error) {
	values := make([]byte, 0, len(vectors)*bytesPerVector)
	for _, vector := range vectors {
		if len(vector.values) != bytesPerVector {
			return nil, vectorByteSizeErr(field, vector.position, len(vector.values), bytesPerVector)
		}
		for _, rawValue := range vector.values {
			value, err := asUint8(rawValue)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
	}
	return values, nil
}

func parseHalfVectorArray(
	vectors []positionedVector,
	field *schemapb.FieldSchema,
	dim int,
	convert func(float32) []byte,
) ([]byte, error) {
	values := make([]byte, 0, len(vectors)*dim*2)
	for _, vector := range vectors {
		if len(vector.values) != dim {
			return nil, vectorDimErr(field, vector.position, len(vector.values), dim)
		}
		for _, rawValue := range vector.values {
			value, err := asFloat32(rawValue)
			if err != nil {
				return nil, err
			}
			values = append(values, convert(value)...)
		}
	}
	return values, nil
}

func parseInt8VectorArray(vectors []positionedVector, field *schemapb.FieldSchema, dim int) ([]byte, error) {
	values := make([]byte, 0, len(vectors)*dim)
	for _, vector := range vectors {
		if len(vector.values) != dim {
			return nil, vectorDimErr(field, vector.position, len(vector.values), dim)
		}
		for _, rawValue := range vector.values {
			value, err := asInt8(rawValue)
			if err != nil {
				return nil, err
			}
			values = append(values, byte(value))
		}
	}
	return values, nil
}

func binaryBytesPerVector(dim int) int {
	return (dim + 7) / 8
}

func asVector(value any) ([]any, error) {
	vector, ok := value.([]any)
	if !ok {
		return nil, merr.WrapErrImportFailedMsg("expected slice as vector, but got %T", value)
	}
	return vector, nil
}

func asFloat32(value any) (float32, error) {
	v, ok := value.(json.Number)
	if !ok {
		return 0, numericTypeErr(value)
	}
	num, err := strconv.ParseFloat(v.String(), 32)
	if err != nil {
		return 0, numericParseErr(v, err)
	}
	return float32(num), nil
}

func asUint8(value any) (byte, error) {
	v, ok := value.(json.Number)
	if !ok {
		return 0, numericTypeErr(value)
	}
	num, err := strconv.ParseUint(v.String(), 0, 8)
	if err != nil {
		return 0, numericParseErr(v, err)
	}
	return byte(num), nil
}

func asInt8(value any) (int8, error) {
	v, ok := value.(json.Number)
	if !ok {
		return 0, numericTypeErr(value)
	}
	num, err := strconv.ParseInt(v.String(), 10, 8)
	if err != nil {
		return 0, numericParseErr(v, err)
	}
	return int8(num), nil
}

func vectorDimErr(field *schemapb.FieldSchema, position int, actual int, expected int) error {
	return merr.WrapErrImportFailed(
		fmt.Sprintf("vector dimension mismatch for field '%s': position=%d, actual=%d, expected=%d",
			field.GetName(), position, actual, expected))
}

func vectorByteSizeErr(field *schemapb.FieldSchema, position int, actual int, expected int) error {
	return merr.WrapErrImportFailed(
		fmt.Sprintf("vector byte size mismatch for field '%s': position=%d, actual=%d, expected=%d",
			field.GetName(), position, actual, expected))
}

func numericTypeErr(value any) error {
	return merr.WrapErrImportFailedMsg("expected numeric vector element, got type %T with value %v", value, value)
}

func numericParseErr(value json.Number, err error) error {
	return merr.WrapErrImportFailedMsg("failed to parse vector element '%s': %v", value.String(), err)
}
