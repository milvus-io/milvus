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

package parquet

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// StructFieldReader reads a specific field from a list<struct> column
type StructFieldReader struct {
	columnReader *pqarrow.ColumnReader
	field        *schemapb.FieldSchema
	fieldIndex   int
	dim          int
}

// NewStructFieldReader creates a reader for extracting a field from nested struct
func NewStructFieldReader(ctx context.Context, fileReader *pqarrow.FileReader, columnIndex int,
	fieldIndex int, field *schemapb.FieldSchema,
) (*FieldReader, error) {
	columnReader, err := fileReader.GetColumn(ctx, columnIndex)
	if err != nil {
		return nil, err
	}

	dim := 0
	if typeutil.IsVectorType(field.GetDataType()) && !typeutil.IsSparseFloatVectorType(field.GetDataType()) {
		d, err := typeutil.GetDim(field)
		if err != nil {
			return nil, err
		}
		dim = int(d)
	} else if field.GetDataType() == schemapb.DataType_ArrayOfVector {
		// For ArrayOfVector, get the dimension from the element type
		d, err := typeutil.GetDim(field)
		if err != nil {
			return nil, err
		}
		dim = int(d)
	}

	sfr := &StructFieldReader{
		columnReader: columnReader,
		field:        field,
		fieldIndex:   fieldIndex,
		dim:          dim,
	}

	fr := &FieldReader{
		columnIndex:  columnIndex,
		columnReader: columnReader,
		field:        field,
		dim:          dim,
		structReader: sfr,
	}

	return fr, nil
}

// Next extracts the specific field from struct array
func (r *StructFieldReader) Next(count int64) (any, any, error) {
	chunked, err := r.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}

	// If no more data, return nil to signal EOF
	if chunked.Len() == 0 {
		return nil, nil, nil
	}

	switch r.field.GetDataType() {
	case schemapb.DataType_Array:
		return r.readArrayField(chunked)
	case schemapb.DataType_ArrayOfVector:
		return r.readArrayOfVectorField(chunked)
	default:
		return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type for struct field: %v", r.field.GetDataType()))
	}
}

func (r *StructFieldReader) toScalarField(data []interface{}) (*schemapb.ScalarField, error) {
	// struct list can be empty, len(data) can be zero, build an empty ScalarField if len(data) is zero
	switch r.field.GetElementType() {
	case schemapb.DataType_Bool:
		boolData := make([]bool, len(data))
		for i, v := range data {
			val, ok := v.(bool)
			if !ok {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("expected bool for field '%s', got %T at index %d", r.field.GetName(), v, i))
			}
			boolData[i] = val
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{
				BoolData: &schemapb.BoolArray{Data: boolData},
			},
		}, nil
	case schemapb.DataType_Int8:
		intData := make([]int32, len(data))
		for i, v := range data {
			val, ok := v.(int8)
			if !ok {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("expected int8 for field '%s', got %T at index %d", r.field.GetName(), v, i))
			}
			intData[i] = int32(val)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: intData},
			},
		}, nil
	case schemapb.DataType_Int16:
		intData := make([]int32, len(data))
		for i, v := range data {
			val, ok := v.(int16)
			if !ok {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("expected int16 for field '%s', got %T at index %d", r.field.GetName(), v, i))
			}
			intData[i] = int32(val)
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: intData},
			},
		}, nil
	case schemapb.DataType_Int32:
		intData := make([]int32, len(data))
		for i, v := range data {
			val, ok := v.(int32)
			if !ok {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("expected int32 for field '%s', got %T at index %d", r.field.GetName(), v, i))
			}
			intData[i] = val
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: intData},
			},
		}, nil
	case schemapb.DataType_Int64:
		intData := make([]int64, len(data))
		for i, v := range data {
			val, ok := v.(int64)
			if !ok {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("expected int64 for field '%s', got %T at index %d", r.field.GetName(), v, i))
			}
			intData[i] = val
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: intData},
			},
		}, nil
	case schemapb.DataType_Float:
		floatData := make([]float32, len(data))
		for i, v := range data {
			val, ok := v.(float32)
			if !ok {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("expected float32 for field '%s', got %T at index %d", r.field.GetName(), v, i))
			}
			floatData[i] = val
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{Data: floatData},
			},
		}, nil
	case schemapb.DataType_Double:
		floatData := make([]float64, len(data))
		for i, v := range data {
			val, ok := v.(float64)
			if !ok {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("expected float64 for field '%s', got %T at index %d", r.field.GetName(), v, i))
			}
			floatData[i] = val
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{
				DoubleData: &schemapb.DoubleArray{Data: floatData},
			},
		}, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		strData := make([]string, len(data))
		for i, v := range data {
			val, ok := v.(string)
			if !ok {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("expected string for field '%s', got %T at index %d", r.field.GetName(), v, i))
			}
			strData[i] = val
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{Data: strData},
			},
		}, nil
	default:
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported element type for struct field: %v", r.field.GetElementType()))
	}
}

func (r *StructFieldReader) readArrayField(chunked *arrow.Chunked) (any, any, error) {
	result := make([]*schemapb.ScalarField, 0)
	for _, chunk := range chunked.Chunks() {
		listArray, ok := chunk.(*array.List)
		if !ok {
			return nil, nil, merr.WrapErrImportFailed("expected list array for struct field")
		}

		structArray, ok := listArray.ListValues().(*array.Struct)
		if !ok {
			return nil, nil, merr.WrapErrImportFailed("expected struct in list")
		}

		fieldArray := structArray.Field(r.fieldIndex)
		offsets := listArray.Offsets()

		for i := 0; i < len(offsets)-1; i++ {
			startIdx := offsets[i]
			endIdx := offsets[i+1]

			var combinedData []interface{}
			for structIdx := startIdx; structIdx < endIdx; structIdx++ {
				switch field := fieldArray.(type) {
				case *array.Boolean:
					if !field.IsNull(int(structIdx)) {
						combinedData = append(combinedData, field.Value(int(structIdx)))
					}
				case *array.Int8:
					if !field.IsNull(int(structIdx)) {
						combinedData = append(combinedData, field.Value(int(structIdx)))
					}
				case *array.Int16:
					if !field.IsNull(int(structIdx)) {
						combinedData = append(combinedData, field.Value(int(structIdx)))
					}
				case *array.Int32:
					if !field.IsNull(int(structIdx)) {
						combinedData = append(combinedData, field.Value(int(structIdx)))
					}
				case *array.Int64:
					if !field.IsNull(int(structIdx)) {
						combinedData = append(combinedData, field.Value(int(structIdx)))
					}
				case *array.Float32:
					if !field.IsNull(int(structIdx)) {
						combinedData = append(combinedData, field.Value(int(structIdx)))
					}
				case *array.Float64:
					if !field.IsNull(int(structIdx)) {
						combinedData = append(combinedData, field.Value(int(structIdx)))
					}
				case *array.String:
					if !field.IsNull(int(structIdx)) {
						combinedData = append(combinedData, field.Value(int(structIdx)))
					}
				}
			}

			// Create a single ScalarField for this row
			scalarField, err := r.toScalarField(combinedData)
			if err != nil {
				return nil, nil, err
			}
			if scalarField != nil {
				result = append(result, scalarField)
			}
		}
	}

	return result, nil, nil
}

func (r *StructFieldReader) readArrayOfVectorField(chunked *arrow.Chunked) (any, any, error) {
	var result []*schemapb.VectorField

	for _, chunk := range chunked.Chunks() {
		listArray, ok := chunk.(*array.List)
		if !ok {
			return nil, nil, merr.WrapErrImportFailed("expected list array for struct field")
		}

		structArray, ok := listArray.ListValues().(*array.Struct)
		if !ok {
			return nil, nil, merr.WrapErrImportFailed("expected struct in list")
		}

		// Get the field array - it should be a list<primitives> (one vector per struct)
		fieldArray, ok := structArray.Field(r.fieldIndex).(*array.List)
		if !ok {
			return nil, nil, merr.WrapErrImportFailed("expected list array for vector field")
		}

		offsets := listArray.Offsets()

		// Process each row
		for i := 0; i < len(offsets)-1; i++ {
			startIdx := offsets[i]
			endIdx := offsets[i+1]

			// Extract vectors based on element type
			switch r.field.GetElementType() {
			case schemapb.DataType_FloatVector:
				floatArr, ok := fieldArray.ListValues().(*array.Float32)
				if !ok {
					return nil, nil, merr.WrapErrImportFailed(
						fmt.Sprintf("expected Float32 array for FloatVector field '%s', got %T", r.field.GetName(), fieldArray.ListValues()))
				}
				var allVectors []float32
				for structIdx := startIdx; structIdx < endIdx; structIdx++ {
					vecStart, vecEnd := fieldArray.ValueOffsets(int(structIdx))
					if int(vecEnd-vecStart) != r.dim {
						return nil, nil, merr.WrapErrImportFailed(
							fmt.Sprintf("vector dimension mismatch for field '%s': position=%d, actual=%d, expected=%d",
								r.field.GetName(), structIdx, vecEnd-vecStart, r.dim))
					}
					for j := vecStart; j < vecEnd; j++ {
						allVectors = append(allVectors, floatArr.Value(int(j)))
					}
				}
				result = append(result, &schemapb.VectorField{
					Dim: int64(r.dim),
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{Data: allVectors},
					},
				})

			case schemapb.DataType_Float16Vector:
				uint8Arr, ok := fieldArray.ListValues().(*array.Uint8)
				if !ok {
					return nil, nil, merr.WrapErrImportFailed(
						fmt.Sprintf("expected Uint8 array for Float16Vector field '%s', got %T", r.field.GetName(), fieldArray.ListValues()))
				}
				expectedBytes := int64(r.dim * 2)
				var allVectors []byte
				for structIdx := startIdx; structIdx < endIdx; structIdx++ {
					vecStart, vecEnd := fieldArray.ValueOffsets(int(structIdx))
					if vecEnd-vecStart != expectedBytes {
						return nil, nil, merr.WrapErrImportFailed(
							fmt.Sprintf("vector dimension mismatch for field '%s': position=%d, actual_bytes=%d, expected_bytes=%d",
								r.field.GetName(), structIdx, vecEnd-vecStart, expectedBytes))
					}
					allVectors = append(allVectors, uint8Arr.Uint8Values()[vecStart:vecEnd]...)
				}
				result = append(result, &schemapb.VectorField{
					Dim: int64(r.dim),
					Data: &schemapb.VectorField_Float16Vector{
						Float16Vector: allVectors,
					},
				})

			case schemapb.DataType_BFloat16Vector:
				uint8Arr, ok := fieldArray.ListValues().(*array.Uint8)
				if !ok {
					return nil, nil, merr.WrapErrImportFailed(
						fmt.Sprintf("expected Uint8 array for BFloat16Vector field '%s', got %T", r.field.GetName(), fieldArray.ListValues()))
				}
				expectedBytes := int64(r.dim * 2)
				var allVectors []byte
				for structIdx := startIdx; structIdx < endIdx; structIdx++ {
					vecStart, vecEnd := fieldArray.ValueOffsets(int(structIdx))
					if vecEnd-vecStart != expectedBytes {
						return nil, nil, merr.WrapErrImportFailed(
							fmt.Sprintf("vector dimension mismatch for field '%s': position=%d, actual_bytes=%d, expected_bytes=%d",
								r.field.GetName(), structIdx, vecEnd-vecStart, expectedBytes))
					}
					allVectors = append(allVectors, uint8Arr.Uint8Values()[vecStart:vecEnd]...)
				}
				result = append(result, &schemapb.VectorField{
					Dim: int64(r.dim),
					Data: &schemapb.VectorField_Bfloat16Vector{
						Bfloat16Vector: allVectors,
					},
				})

			case schemapb.DataType_Int8Vector:
				int8Arr, ok := fieldArray.ListValues().(*array.Int8)
				if !ok {
					return nil, nil, merr.WrapErrImportFailed(
						fmt.Sprintf("expected Int8 array for Int8Vector field '%s', got %T", r.field.GetName(), fieldArray.ListValues()))
				}
				var allVectors []byte
				for structIdx := startIdx; structIdx < endIdx; structIdx++ {
					vecStart, vecEnd := fieldArray.ValueOffsets(int(structIdx))
					if int(vecEnd-vecStart) != r.dim {
						return nil, nil, merr.WrapErrImportFailed(
							fmt.Sprintf("vector dimension mismatch for field '%s': position=%d, actual=%d, expected=%d",
								r.field.GetName(), structIdx, vecEnd-vecStart, r.dim))
					}
					for j := vecStart; j < vecEnd; j++ {
						allVectors = append(allVectors, byte(int8Arr.Value(int(j))))
					}
				}
				result = append(result, &schemapb.VectorField{
					Dim: int64(r.dim),
					Data: &schemapb.VectorField_Int8Vector{
						Int8Vector: allVectors,
					},
				})

			case schemapb.DataType_BinaryVector:
				uint8Arr, ok := fieldArray.ListValues().(*array.Uint8)
				if !ok {
					return nil, nil, merr.WrapErrImportFailed(
						fmt.Sprintf("expected Uint8 array for BinaryVector field '%s', got %T", r.field.GetName(), fieldArray.ListValues()))
				}
				expectedBytes := int64(r.dim / 8)
				var allVectors []byte
				for structIdx := startIdx; structIdx < endIdx; structIdx++ {
					vecStart, vecEnd := fieldArray.ValueOffsets(int(structIdx))
					if vecEnd-vecStart != expectedBytes {
						return nil, nil, merr.WrapErrImportFailed(
							fmt.Sprintf("vector dimension mismatch for field '%s': position=%d, actual_bytes=%d, expected_bytes=%d",
								r.field.GetName(), structIdx, vecEnd-vecStart, expectedBytes))
					}
					allVectors = append(allVectors, uint8Arr.Uint8Values()[vecStart:vecEnd]...)
				}
				result = append(result, &schemapb.VectorField{
					Dim: int64(r.dim),
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: allVectors,
					},
				})

			case schemapb.DataType_SparseFloatVector:
				return nil, nil, merr.WrapErrImportFailed("ArrayOfVector with SparseFloatVector element type is not implemented yet")

			default:
				return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported ArrayOfVector element type: %v", r.field.GetElementType()))
			}
		}
	}

	return result, nil, nil
}
