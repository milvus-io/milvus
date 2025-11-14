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
	if len(data) == 0 {
		return nil, nil
	}

	switch r.field.GetElementType() {
	case schemapb.DataType_Bool:
		boolData := make([]bool, len(data))
		for i, v := range data {
			if val, ok := v.(bool); ok {
				boolData[i] = val
			}
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_BoolData{
				BoolData: &schemapb.BoolArray{Data: boolData},
			},
		}, nil
	case schemapb.DataType_Int8:
		intData := make([]int32, len(data))
		for i, v := range data {
			if val, ok := v.(int8); ok {
				intData[i] = int32(val)
			}
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: intData},
			},
		}, nil
	case schemapb.DataType_Int16:
		intData := make([]int32, len(data))
		for i, v := range data {
			if val, ok := v.(int16); ok {
				intData[i] = int32(val)
			}
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: intData},
			},
		}, nil
	case schemapb.DataType_Int32:
		intData := make([]int32, len(data))
		for i, v := range data {
			if val, ok := v.(int32); ok {
				intData[i] = val
			}
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: intData},
			},
		}, nil
	case schemapb.DataType_Int64:
		intData := make([]int64, len(data))
		for i, v := range data {
			if val, ok := v.(int64); ok {
				intData[i] = val
			}
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: intData},
			},
		}, nil
	case schemapb.DataType_Float:
		floatData := make([]float32, len(data))
		for i, v := range data {
			if val, ok := v.(float32); ok {
				floatData[i] = val
			}
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_FloatData{
				FloatData: &schemapb.FloatArray{Data: floatData},
			},
		}, nil
	case schemapb.DataType_Double:
		floatData := make([]float64, len(data))
		for i, v := range data {
			if val, ok := v.(float64); ok {
				floatData[i] = val
			}
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_DoubleData{
				DoubleData: &schemapb.DoubleArray{Data: floatData},
			},
		}, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		strData := make([]string, len(data))
		for i, v := range data {
			if val, ok := v.(string); ok {
				strData[i] = val
			}
		}
		return &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{
				StringData: &schemapb.StringArray{Data: strData},
			},
		}, nil
	default:
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported element type for struct field: %v", r.field.GetDataType()))
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
				var allVectors []float32
				for structIdx := startIdx; structIdx < endIdx; structIdx++ {
					vecStart, vecEnd := fieldArray.ValueOffsets(int(structIdx))
					if floatArr, ok := fieldArray.ListValues().(*array.Float32); ok {
						for j := vecStart; j < vecEnd; j++ {
							allVectors = append(allVectors, floatArr.Value(int(j)))
						}
					}
				}
				if len(allVectors) > 0 {
					vectorField := &schemapb.VectorField{
						Dim: int64(r.dim),
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{Data: allVectors},
						},
					}
					result = append(result, vectorField)
				}

			case schemapb.DataType_BinaryVector:
				return nil, nil, merr.WrapErrImportFailed("ArrayOfVector with BinaryVector element type is not implemented yet")

			case schemapb.DataType_Float16Vector:
				return nil, nil, merr.WrapErrImportFailed("ArrayOfVector with Float16Vector element type is not implemented yet")

			case schemapb.DataType_BFloat16Vector:
				return nil, nil, merr.WrapErrImportFailed("ArrayOfVector with BFloat16Vector element type is not implemented yet")

			case schemapb.DataType_Int8Vector:
				return nil, nil, merr.WrapErrImportFailed("ArrayOfVector with Int8Vector element type is not implemented yet")

			case schemapb.DataType_SparseFloatVector:
				return nil, nil, merr.WrapErrImportFailed("ArrayOfVector with SparseFloatVector element type is not implemented yet")

			default:
				return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported ArrayOfVector element type: %v", r.field.GetElementType()))
			}
		}
	}

	return result, nil, nil
}
