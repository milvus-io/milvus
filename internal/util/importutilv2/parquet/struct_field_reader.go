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
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	var validData []bool
	if r.field.GetNullable() {
		validData = make([]bool, 0)
	}
	maxCapacity, err := parameterutil.GetMaxCapacity(r.field)
	if err != nil {
		return nil, nil, err
	}
	var maxLength int64
	if typeutil.IsStringType(r.field.GetElementType()) {
		maxLength, err = parameterutil.GetMaxLength(r.field)
		if err != nil {
			return nil, nil, err
		}
	}

	appendNullRow := func() error {
		scalarField, err := r.toScalarField(nil)
		if err != nil {
			return err
		}
		result = append(result, scalarField)
		validData = append(validData, false)
		return nil
	}

	for _, chunk := range chunked.Chunks() {
		switch listArray := chunk.(type) {
		case *array.Null:
			if !r.field.GetNullable() {
				return nil, nil, WrapNullRowErr(r.field)
			}
			for i := 0; i < listArray.Len(); i++ {
				if err := appendNullRow(); err != nil {
					return nil, nil, err
				}
			}
		case *array.List:
			if !r.field.GetNullable() && listArray.NullN() > 0 {
				return nil, nil, WrapNullRowErr(r.field)
			}

			structArray, ok := listArray.ListValues().(*array.Struct)
			if !ok {
				return nil, nil, merr.WrapErrImportFailed("expected struct in list")
			}

			fieldArray := structArray.Field(r.fieldIndex)

			for i := 0; i < listArray.Len(); i++ {
				if listArray.IsNull(i) {
					if err := appendNullRow(); err != nil {
						return nil, nil, err
					}
					continue
				}

				startIdx, endIdx := listArray.ValueOffsets(i)

				var combinedData []interface{}
				for structIdx := startIdx; structIdx < endIdx; structIdx++ {
					if structArray.IsNull(int(structIdx)) {
						return nil, nil, WrapNullElementErr(r.field)
					}
					switch field := fieldArray.(type) {
					case *array.Null:
						return nil, nil, WrapNullElementErr(r.field)
					case *array.Boolean:
						if field.IsNull(int(structIdx)) {
							return nil, nil, WrapNullElementErr(r.field)
						}
						combinedData = append(combinedData, field.Value(int(structIdx)))
					case *array.Int8:
						if field.IsNull(int(structIdx)) {
							return nil, nil, WrapNullElementErr(r.field)
						}
						combinedData = append(combinedData, field.Value(int(structIdx)))
					case *array.Int16:
						if field.IsNull(int(structIdx)) {
							return nil, nil, WrapNullElementErr(r.field)
						}
						combinedData = append(combinedData, field.Value(int(structIdx)))
					case *array.Int32:
						if field.IsNull(int(structIdx)) {
							return nil, nil, WrapNullElementErr(r.field)
						}
						combinedData = append(combinedData, field.Value(int(structIdx)))
					case *array.Int64:
						if field.IsNull(int(structIdx)) {
							return nil, nil, WrapNullElementErr(r.field)
						}
						combinedData = append(combinedData, field.Value(int(structIdx)))
					case *array.Float32:
						if field.IsNull(int(structIdx)) {
							return nil, nil, WrapNullElementErr(r.field)
						}
						value := field.Value(int(structIdx))
						if err := typeutil.VerifyFloat(float64(value)); err != nil {
							return nil, nil, fmt.Errorf("float32 verification failed: %w", err)
						}
						combinedData = append(combinedData, value)
					case *array.Float64:
						if field.IsNull(int(structIdx)) {
							return nil, nil, WrapNullElementErr(r.field)
						}
						value := field.Value(int(structIdx))
						if err := typeutil.VerifyFloat(value); err != nil {
							return nil, nil, fmt.Errorf("float64 verification failed: %w", err)
						}
						combinedData = append(combinedData, value)
					case *array.String:
						if field.IsNull(int(structIdx)) {
							return nil, nil, WrapNullElementErr(r.field)
						}
						value := strings.Clone(field.Value(int(structIdx)))
						if err := common.CheckValidString(value, maxLength, r.field); err != nil {
							return nil, nil, err
						}
						combinedData = append(combinedData, value)
					default:
						return nil, nil, WrapTypeErr(r.field, fieldArray.DataType().Name())
					}
				}

				if err := common.CheckArrayCapacity(len(combinedData), maxCapacity, r.field); err != nil {
					return nil, nil, err
				}
				// Create a single ScalarField for this row
				scalarField, err := r.toScalarField(combinedData)
				if err != nil {
					return nil, nil, err
				}
				if scalarField != nil {
					result = append(result, scalarField)
				}
				if r.field.GetNullable() {
					validData = append(validData, true)
				}
			}
		default:
			return nil, nil, merr.WrapErrImportFailed("expected list array for struct field")
		}
	}

	return result, validData, nil
}

func (r *StructFieldReader) readArrayOfVectorField(chunked *arrow.Chunked) (any, any, error) {
	result := make([]*schemapb.VectorField, 0)
	var validData []bool
	if r.field.GetNullable() {
		validData = make([]bool, 0)
	}
	if _, err := vectorArrayBytesPerVector(r.field.GetElementType(), int64(r.dim)); err != nil {
		return nil, nil, err
	}
	maxCapacity, err := parameterutil.GetMaxCapacity(r.field)
	if err != nil {
		return nil, nil, err
	}

	for _, chunk := range chunked.Chunks() {
		switch listArray := chunk.(type) {
		case *array.Null:
			if !r.field.GetNullable() {
				return nil, nil, WrapNullRowErr(r.field)
			}
			for i := 0; i < listArray.Len(); i++ {
				result = append(result, emptyVectorArrayRow(int64(r.dim), r.field.GetElementType()))
				validData = append(validData, false)
			}
		case *array.List:
			if !r.field.GetNullable() && listArray.NullN() > 0 {
				return nil, nil, WrapNullRowErr(r.field)
			}

			structArray, ok := listArray.ListValues().(*array.Struct)
			if !ok {
				return nil, nil, merr.WrapErrImportFailed("expected struct in list")
			}

			fieldArray, ok := structArray.Field(r.fieldIndex).(*array.List)
			if !ok {
				return nil, nil, merr.WrapErrImportFailed("expected list array for vector field")
			}

			for i := 0; i < listArray.Len(); i++ {
				if listArray.IsNull(i) {
					result = append(result, emptyVectorArrayRow(int64(r.dim), r.field.GetElementType()))
					validData = append(validData, false)
					continue
				}

				startIdx, endIdx := listArray.ValueOffsets(i)
				for structIdx := startIdx; structIdx < endIdx; structIdx++ {
					if structArray.IsNull(int(structIdx)) {
						return nil, nil, WrapNullElementErr(r.field)
					}
				}
				if err = common.CheckArrayCapacity(int(endIdx-startIdx), maxCapacity, r.field); err != nil {
					return nil, nil, err
				}
				rowData, err := buildVectorArrayFieldFromList(r.field, int64(r.dim), fieldArray, startIdx, endIdx)
				if err != nil {
					return nil, nil, err
				}
				result = append(result, rowData)
				if r.field.GetNullable() {
					validData = append(validData, true)
				}
			}
		default:
			return nil, nil, merr.WrapErrImportFailed("expected list array for struct field")
		}
	}

	return result, validData, nil
}
