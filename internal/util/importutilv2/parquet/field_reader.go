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
	"github.com/samber/lo"
	"golang.org/x/exp/constraints"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/internal/util/nullutil"
	pkgcommon "github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/v3/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type FieldReader struct {
	columnIndex  int
	columnReader *pqarrow.ColumnReader

	dim            int
	field          *schemapb.FieldSchema
	sparseIsString bool

	// timezone is the collection's default timezone
	timezone string

	// structReader is non-nil when Struct Array field exists
	structReader *StructFieldReader
}

func NewFieldReader(ctx context.Context, reader *pqarrow.FileReader, columnIndex int, field *schemapb.FieldSchema, timezone string) (*FieldReader, error) {
	columnReader, err := reader.GetColumn(ctx, columnIndex)
	if err != nil {
		return nil, err
	}

	var dim int64 = 1
	if typeutil.IsVectorType(field.GetDataType()) && !typeutil.IsSparseFloatVectorType(field.GetDataType()) {
		dim, err = typeutil.GetDim(field)
		if err != nil {
			return nil, err
		}
	}

	// set a flag here to know whether a sparse vector is stored as JSON-format string or parquet struct
	// because we don't intend to check it every time the Next() is called
	sparseIsString := true
	if field.GetDataType() == schemapb.DataType_SparseFloatVector {
		_, sparseIsString = IsValidSparseVectorSchema(columnReader.Field().Type)
	}

	cr := &FieldReader{
		columnIndex:    columnIndex,
		columnReader:   columnReader,
		dim:            int(dim),
		field:          field,
		sparseIsString: sparseIsString,
		timezone:       timezone,
	}
	return cr, nil
}

func (c *FieldReader) Next(count int64) (any, any, error) {
	// Check if this FieldReader wraps a StructFieldReader
	if c.structReader != nil {
		return c.structReader.Next(count)
	}

	switch c.field.GetDataType() {
	case schemapb.DataType_Bool:
		if c.field.GetNullable() || c.field.GetDefaultValue() != nil {
			return ReadNullableBoolData(c, count)
		}
		data, err := ReadBoolData(c, count)
		return data, nil, err
	case schemapb.DataType_Int8:
		if c.field.GetNullable() || c.field.GetDefaultValue() != nil {
			return ReadNullableIntegerOrFloatData[int8](c, count)
		}
		data, err := ReadIntegerOrFloatData[int8](c, count)
		return data, nil, err
	case schemapb.DataType_Int16:
		if c.field.GetNullable() || c.field.GetDefaultValue() != nil {
			return ReadNullableIntegerOrFloatData[int16](c, count)
		}
		data, err := ReadIntegerOrFloatData[int16](c, count)
		return data, nil, err
	case schemapb.DataType_Int32:
		if c.field.GetNullable() || c.field.GetDefaultValue() != nil {
			return ReadNullableIntegerOrFloatData[int32](c, count)
		}
		data, err := ReadIntegerOrFloatData[int32](c, count)
		return data, nil, err
	case schemapb.DataType_Int64:
		if c.field.GetNullable() || c.field.GetDefaultValue() != nil {
			return ReadNullableIntegerOrFloatData[int64](c, count)
		}
		data, err := ReadIntegerOrFloatData[int64](c, count)
		return data, nil, err
	case schemapb.DataType_Float:
		if c.field.GetNullable() || c.field.GetDefaultValue() != nil {
			data, validData, err := ReadNullableIntegerOrFloatData[float32](c, count)
			if err != nil {
				return nil, nil, err
			}
			if data == nil {
				return nil, nil, nil
			}
			return data, validData, typeutil.VerifyFloats32(data.([]float32))
		}
		data, err := ReadIntegerOrFloatData[float32](c, count)
		if err != nil {
			return nil, nil, err
		}
		if data == nil {
			return nil, nil, nil
		}
		return data, nil, typeutil.VerifyFloats32(data.([]float32))
	case schemapb.DataType_Double:
		if c.field.GetNullable() || c.field.GetDefaultValue() != nil {
			data, validData, err := ReadNullableIntegerOrFloatData[float64](c, count)
			if err != nil {
				return nil, nil, err
			}
			if data == nil {
				return nil, nil, nil
			}
			return data, validData, typeutil.VerifyFloats64(data.([]float64))
		}
		data, err := ReadIntegerOrFloatData[float64](c, count)
		if err != nil {
			return nil, nil, err
		}
		if data == nil {
			return nil, nil, nil
		}
		return data, nil, typeutil.VerifyFloats64(data.([]float64))
	case schemapb.DataType_VarChar, schemapb.DataType_String, schemapb.DataType_Text:
		if c.field.GetNullable() || c.field.GetDefaultValue() != nil {
			return ReadNullableStringData(c, count)
		}
		isVarcharField := c.field.GetDataType() != schemapb.DataType_Text
		data, err := ReadStringData(c, count, isVarcharField)
		return data, nil, err
	case schemapb.DataType_JSON:
		// json has not support default_value
		if c.field.GetNullable() {
			return ReadNullableJSONData(c, count)
		}
		data, err := ReadJSONData(c, count)
		return data, nil, err
	case schemapb.DataType_Geometry:
		if c.field.GetNullable() || c.field.GetDefaultValue() != nil {
			return ReadNullableGeometryData(c, count)
		}
		data, err := ReadGeometryData(c, count)
		return data, nil, err
	case schemapb.DataType_Timestamptz:
		if c.field.GetNullable() || c.field.GetDefaultValue() != nil {
			return ReadNullableTimestamptzData(c, count)
		}
		data, err := ReadTimestamptzData(c, count)
		return data, nil, err
	case schemapb.DataType_BinaryVector:
		// vector not support default_value
		if c.field.GetNullable() {
			return ReadNullableBinaryData(c, count)
		}
		data, err := ReadBinaryData(c, count)
		return data, nil, err
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		// vector not support default_value
		if c.field.GetNullable() {
			if isFP16BF16FloatArrowType(c.columnReader.Field().Type) {
				return ReadNullableFP16BF16FloatVectorData(c, count)
			}
			return ReadNullableBinaryData(c, count)
		}
		if isFP16BF16FloatArrowType(c.columnReader.Field().Type) {
			data, err := ReadFP16BF16FloatVectorData(c, count)
			return data, nil, err
		}
		data, err := ReadBinaryData(c, count)
		return data, nil, err
	case schemapb.DataType_FloatVector:
		if c.field.GetNullable() {
			return ReadNullableFloatVectorData(c, count)
		}
		arrayData, err := ReadIntegerOrFloatArrayData[float32](c, count)
		if err != nil {
			return nil, nil, err
		}
		if arrayData == nil {
			return nil, nil, nil
		}
		vectors := lo.Flatten(arrayData.([][]float32))
		return vectors, nil, nil
	case schemapb.DataType_SparseFloatVector:
		if c.field.GetNullable() {
			return ReadNullableSparseFloatVectorData(c, count)
		}
		data, err := ReadSparseFloatVectorData(c, count)
		return data, nil, err
	case schemapb.DataType_Int8Vector:
		if c.field.GetNullable() {
			return ReadNullableInt8VectorData(c, count)
		}
		arrayData, err := ReadIntegerOrFloatArrayData[int8](c, count)
		if err != nil {
			return nil, nil, err
		}
		if arrayData == nil {
			return nil, nil, nil
		}
		vectors := lo.Flatten(arrayData.([][]int8))
		return vectors, nil, nil
	case schemapb.DataType_Array:
		// array has not supported default_value
		if c.field.GetNullable() {
			return ReadNullableArrayData(c, count)
		}
		data, err := ReadArrayData(c, count)
		return data, nil, err
	case schemapb.DataType_ArrayOfVector:
		if c.field.GetNullable() {
			return ReadNullableVectorArrayData(c, count)
		}
		data, err := ReadVectorArrayData(c, count)
		return data, nil, err
	default:
		return nil, nil, merr.WrapErrImportFailedMsg("unsupported data type '%s' for field '%s'",
			c.field.GetDataType().String(), c.field.GetName())
	}
}

func ReadBoolData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		if chunk.NullN() > 0 {
			return nil, WrapNullRowErr(pcr.field)
		}
		boolReader, ok := chunk.(*array.Boolean)

		if !ok {
			return nil, WrapTypeErr(pcr.field, chunk.DataType().Name())
		}
		for i := 0; i < dataNums; i++ {
			data = append(data, boolReader.Value(i))
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func fillWithDefaultValueImpl[T any](array []T, value T, validData []bool, field *schemapb.FieldSchema) (any, []bool, error) {
	rowNum := len(validData)
	for i, v := range validData {
		if !v {
			array[i] = value
		}
	}
	if !typeutil.IsVectorType(field.GetDataType()) {
		if field.GetNullable() {
			for i := range validData {
				validData[i] = true
			}
		} else {
			validData = []bool{}
		}
	}

	err := nullutil.CheckValidData(validData, field, rowNum)
	if err != nil {
		return nil, nil, err
	}
	return array, validData, nil
}

func ReadNullableBoolData(pcr *FieldReader, count int64) (any, []bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([]bool, 0, count)
	validData := make([]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		boolReader, ok := chunk.(*array.Boolean)
		if !ok {
			// the chunk type may be *array.Null if the data in chunk is all null
			_, ok := chunk.(*array.Null)
			if !ok {
				return nil, nil, WrapTypeErr(pcr.field, chunk.DataType().Name())
			}
			validData = append(validData, make([]bool, dataNums)...)
			data = append(data, make([]bool, dataNums)...)
		} else {
			validData = append(validData, bytesToValidData(dataNums, boolReader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, boolReader.Value(i))
			}
		}
	}
	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrParameterInvalid(len(data), len(validData), "length of data is not equal to length of valid_data")
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	if pcr.field.GetDefaultValue() != nil {
		defaultValue := pcr.field.GetDefaultValue().GetBoolData()
		return fillWithDefaultValueImpl(data, defaultValue, validData, pcr.field)
	}
	return data, validData, nil
}

func ReadIntegerOrFloatData[T constraints.Integer | constraints.Float](pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]T, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		if chunk.NullN() > 0 {
			return nil, WrapNullRowErr(pcr.field)
		}
		switch chunk.DataType().ID() {
		case arrow.INT8:
			int8Reader := chunk.(*array.Int8)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int8Reader.Value(i)))
			}
		case arrow.INT16:
			int16Reader := chunk.(*array.Int16)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int16Reader.Value(i)))
			}
		case arrow.INT32:
			int32Reader := chunk.(*array.Int32)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int32Reader.Value(i)))
			}
		case arrow.INT64:
			int64Reader := chunk.(*array.Int64)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int64Reader.Value(i)))
			}
		case arrow.FLOAT32:
			float32Reader := chunk.(*array.Float32)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(float32Reader.Value(i)))
			}
		case arrow.FLOAT64:
			float64Reader := chunk.(*array.Float64)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(float64Reader.Value(i)))
			}
		default:
			return nil, WrapTypeErr(pcr.field, chunk.DataType().Name())
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadNullableIntegerOrFloatData[T constraints.Integer | constraints.Float](pcr *FieldReader, count int64) (any, []bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([]T, 0, count)
	validData := make([]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		switch chunk.DataType().ID() {
		case arrow.INT8:
			int8Reader := chunk.(*array.Int8)
			validData = append(validData, bytesToValidData(dataNums, int8Reader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int8Reader.Value(i)))
			}
		case arrow.INT16:
			int16Reader := chunk.(*array.Int16)
			validData = append(validData, bytesToValidData(dataNums, int16Reader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int16Reader.Value(i)))
			}
		case arrow.INT32:
			int32Reader := chunk.(*array.Int32)
			validData = append(validData, bytesToValidData(dataNums, int32Reader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int32Reader.Value(i)))
			}
		case arrow.INT64:
			int64Reader := chunk.(*array.Int64)
			validData = append(validData, bytesToValidData(dataNums, int64Reader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int64Reader.Value(i)))
			}
		case arrow.FLOAT32:
			float32Reader := chunk.(*array.Float32)
			validData = append(validData, bytesToValidData(dataNums, float32Reader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(float32Reader.Value(i)))
			}
		case arrow.FLOAT64:
			float64Reader := chunk.(*array.Float64)
			validData = append(validData, bytesToValidData(dataNums, float64Reader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(float64Reader.Value(i)))
			}
		case arrow.NULL:
			// the chunk type may be *array.Null if the data in chunk is all null
			validData = append(validData, make([]bool, dataNums)...)
			data = append(data, make([]T, dataNums)...)
		default:
			return nil, nil, WrapTypeErr(pcr.field, chunk.DataType().Name())
		}
	}
	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrParameterInvalid(len(data), len(validData), "length of data is not equal to length of valid_data")
	}
	if len(data) == 0 {
		return nil, nil, nil
	}

	if pcr.field.GetDefaultValue() != nil {
		defaultValue, err := nullutil.GetDefaultValue(pcr.field)
		if err != nil {
			// won't happen
			return nil, nil, err
		}
		return fillWithDefaultValueImpl(data, defaultValue.(T), validData, pcr.field)
	}
	return data, validData, nil
}

// This method returns a []map[string]arrow.Array
// map[string]arrow.Array represents a struct
// For example 1:
//
//	  struct {
//		 name string
//	     age  int
//	  }
//
// The ReadStructData() will return a list like:
//
//	  [
//		 {"name": ["a", "b", "c"], "age": [4, 5, 6]},
//	     {"name": ["e", "f"], "age": [7, 8]}
//	  ]
//
// Value type of "name" is array.String, value type of "age" is array.Int32
// The length of the list is equal to the length of chunked.Chunks()
//
// For sparse vector, the map[string]arrow.Array is like {"indices": array.List, "values": array.List}
// For example 2:
//
//	  struct {
//		 indices []uint32
//	     values  []float32
//	  }
//
// The ReadStructData() will return a list like:
//
//	  [
//		 {"indices": [[1, 2, 3], [4, 5], [6, 7]], "values": [[0.1, 0.2, 0.3], [0.4, 0.5], [0.6, 0.7]]},
//	     {"indices": [[8], [9, 10]], "values": [[0.8], [0.9, 1.0]]}
//	  ]
//
// Value type of "indices" is array.List, element type is array.Uint32
// Value type of "values" is array.List, element type is array.Float32
// The length of the list is equal to the length of chunked.Chunks()
func ReadStructData(pcr *FieldReader, count int64) ([]map[string]arrow.Array, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]map[string]arrow.Array, 0, count)
	for _, chunk := range chunked.Chunks() {
		structReader, ok := chunk.(*array.Struct)
		if structReader.NullN() > 0 {
			return nil, WrapNullRowErr(pcr.field)
		}
		if !ok {
			return nil, WrapTypeErr(pcr.field, chunk.DataType().Name())
		}

		structType := structReader.DataType().(*arrow.StructType)
		st := make(map[string]arrow.Array)
		for k, field := range structType.Fields() {
			st[field.Name] = structReader.Field(k)
		}
		data = append(data, st)
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadNullableStructData(pcr *FieldReader, count int64) ([]map[string]arrow.Array, []bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([]map[string]arrow.Array, 0, count)
	validData := make([]bool, 0, count)

	for _, chunk := range chunked.Chunks() {
		structReader, ok := chunk.(*array.Struct)
		if !ok {
			return nil, nil, WrapTypeErr(pcr.field, chunk.DataType().Name())
		}

		structType := structReader.DataType().(*arrow.StructType)
		rows := structReader.Len()
		// Sparse storage: only store valid rows' data
		for i := 0; i < rows; i++ {
			validData = append(validData, !structReader.IsNull(i))
			if !structReader.IsNull(i) {
				st := make(map[string]arrow.Array)
				for k, field := range structType.Fields() {
					st[field.Name] = structReader.Field(k)
				}
				data = append(data, st)
			}
		}
	}
	if len(data) == 0 && len(validData) == 0 {
		return nil, nil, nil
	}
	return data, validData, nil
}

func ReadStringData(pcr *FieldReader, count int64, isVarcharField bool) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]string, 0, count)
	var maxLength int64
	if isVarcharField {
		maxLength, err = parameterutil.GetMaxLength(pcr.field)
		if err != nil {
			return nil, err
		}
	}
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		if chunk.NullN() > 0 {
			return nil, WrapNullRowErr(pcr.field)
		}
		stringReader, ok := chunk.(*array.String)
		if !ok {
			return nil, WrapTypeErr(pcr.field, chunk.DataType().Name())
		}
		for i := 0; i < dataNums; i++ {
			value := stringReader.Value(i)
			if isVarcharField {
				if err = common.CheckValidString(value, maxLength, pcr.field); err != nil {
					return nil, err
				}
			}
			data = append(data, value)
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

// readRawStringDataFromParquet handles the low-level logic of reading string chunks
// from the Parquet column, extracting data, validity mask, and performing VARCHAR length checks.
// It returns the raw string data and the corresponding validity mask.
func readRawStringDataFromParquet(pcr *FieldReader, count int64) ([]string, []bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	dataType := pcr.field.GetDataType()
	data := make([]string, 0, count)
	validData := make([]bool, 0, count)
	var maxLength int64
	isVarcharField := typeutil.IsStringType(dataType) && !typeutil.IsTextType(dataType)
	if isVarcharField {
		maxLength, err = parameterutil.GetMaxLength(pcr.field)
		if err != nil {
			return nil, nil, err
		}
	}
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		stringReader, ok := chunk.(*array.String)
		if !ok {
			// the chunk type may be *array.Null if the data in chunk is all null
			_, ok := chunk.(*array.Null)
			if !ok {
				return nil, nil, WrapTypeErr(pcr.field, chunk.DataType().Name())
			}
			validData = append(validData, make([]bool, dataNums)...)
			data = append(data, make([]string, dataNums)...)
		} else {
			validData = append(validData, bytesToValidData(dataNums, stringReader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				if stringReader.IsNull(i) {
					data = append(data, "")
					continue
				}
				value := stringReader.Value(i)
				if isVarcharField {
					if err = common.CheckValidString(value, maxLength, pcr.field); err != nil {
						return nil, nil, err
					}
				}
				data = append(data, value)
			}
		}
	}
	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrParameterInvalid(len(data), len(validData), "length of data is not equal to length of valid_data")
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	return data, validData, nil
}

func ReadNullableStringData(pcr *FieldReader, count int64) (any, []bool, error) {
	// Delegate I/O, Arrow iteration, and VARCHAR validation to the helper function.
	data, validData, err := readRawStringDataFromParquet(pcr, count)
	if err != nil {
		return nil, nil, err
	}
	if data == nil {
		return nil, nil, nil
	}
	if pcr.field.GetDefaultValue() != nil {
		// Fill default values for standard string fields (VARCHAR, String, Geometry).
		defaultValue := pcr.field.GetDefaultValue().GetStringData()
		// Assuming fillWithDefaultValueImpl is available
		return fillWithDefaultValueImpl(data, defaultValue, validData, pcr.field)
	}

	// Return raw data for convertible types or non-defaulted fields.
	return data, validData, nil
}

func ReadJSONData(pcr *FieldReader, count int64) (any, error) {
	// JSON field read data from string array Parquet
	data, err := ReadStringData(pcr, count, false)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	byteArr := make([][]byte, 0)
	for _, str := range data.([]string) {
		var dummy interface{}
		err = json.Unmarshal([]byte(str), &dummy)
		if err != nil {
			return nil, err
		}
		if pcr.field.GetIsDynamic() {
			var dummy2 map[string]interface{}
			err = json.Unmarshal([]byte(str), &dummy2)
			if err != nil {
				return nil, err
			}
		}
		byteArr = append(byteArr, []byte(str))
	}
	return byteArr, nil
}

func ReadNullableJSONData(pcr *FieldReader, count int64) (any, []bool, error) {
	// JSON field read data from string array Parquet
	data, validData, err := readRawStringDataFromParquet(pcr, count)
	if err != nil {
		return nil, nil, err
	}
	if data == nil {
		return nil, nil, nil
	}
	byteArr := make([][]byte, 0)
	defaultValue := []byte(nil)
	for i, str := range data {
		if !validData[i] {
			byteArr = append(byteArr, defaultValue)
			continue
		}
		var dummy interface{}
		err = json.Unmarshal([]byte(str), &dummy)
		if err != nil {
			return nil, nil, err
		}
		if pcr.field.GetIsDynamic() {
			var dummy2 map[string]interface{}
			err = json.Unmarshal([]byte(str), &dummy2)
			if err != nil {
				return nil, nil, err
			}
		}
		byteArr = append(byteArr, []byte(str))
	}
	return byteArr, validData, nil
}

// ReadNullableTimestamptzData reads Timestamptz data from the Parquet column,
// handling nullability by parsing the time string and converting it to the internal int64 format.
func ReadNullableTimestamptzData(pcr *FieldReader, count int64) (any, []bool, error) {
	// 1. Read the raw data as strings from the underlying Parquet column.
	// This is because Timestamptz data is initially stored as strings in the insertion layer
	// or represented as strings in the Parquet file for ease of parsing/validation.
	data, validData, err := readRawStringDataFromParquet(pcr, count)
	if err != nil {
		return nil, nil, err
	}
	// If no data was read (e.g., end of file), return nil.
	if data == nil {
		return nil, nil, nil
	}

	// 2. Initialize the target array for internal int64 timestamps (UTC microseconds).
	int64Ts := make([]int64, 0, len(data))
	defaultValue := pcr.field.GetDefaultValue().GetTimestamptzData()

	// 3. Iterate over the string array and convert each timestamp.
	for i, strValue := range data {
		// Check the validity mask: If it's null, append the zero value (0) and continue.
		if !validData[i] {
			int64Ts = append(int64Ts, defaultValue)
			continue
		}

		// Convert the ISO 8601 string to int64 (UTC microseconds).
		// The pcr.timezone is used as the default timezone if the string (strValue)
		// does not contain an explicit UTC offset (e.g., "+08:00").
		tz, err := timestamptz.ValidateAndReturnUnixMicroTz(strValue, pcr.timezone)
		if err != nil {
			return nil, nil, err
		}
		int64Ts = append(int64Ts, tz)
	}
	return int64Ts, validData, nil
}

// ReadTimestamptzData reads non-nullable Timestamptz data from the Parquet column.
// It assumes all values are present (non-null) and converts them to the internal int64 format.
func ReadTimestamptzData(pcr *FieldReader, count int64) (any, error) {
	// Read the raw data as strings. Since this is a non-nullable field, we use ReadStringData.
	data, err := ReadStringData(pcr, count, false)
	if err != nil {
		return nil, err
	}
	// If no data was read (e.g., end of file), return nil.
	if data == nil {
		return nil, nil
	}

	int64Ts := make([]int64, 0, len(data.([]string)))
	for _, strValue := range data.([]string) {
		// Convert the ISO 8601 string to int64 (UTC microseconds).
		// The pcr.timezone is used as the default if the string lacks an explicit offset.
		tz, err := timestamptz.ValidateAndReturnUnixMicroTz(strValue, pcr.timezone)
		if err != nil {
			return nil, err
		}
		int64Ts = append(int64Ts, tz)
	}

	// Return the converted int64 array.
	return int64Ts, nil
}

func ReadNullableGeometryData(pcr *FieldReader, count int64) (any, []bool, error) {
	// Geometry field read data from string array Parquet
	data, validData, err := readRawStringDataFromParquet(pcr, count)
	if err != nil {
		return nil, nil, err
	}
	if data == nil {
		return nil, nil, nil
	}
	wkbValues := make([][]byte, 0)
	defaultValueStr := pcr.field.GetDefaultValue().GetStringData()
	defaultValue := []byte(nil)
	if defaultValueStr != "" {
		defaultValue, _ = pkgcommon.ConvertWKTToWKB(defaultValueStr)
	}
	for i, wktValue := range data {
		if !validData[i] {
			wkbValues = append(wkbValues, defaultValue)
			continue
		}
		wkbValue, err := pkgcommon.ConvertWKTToWKB(wktValue)
		if err != nil {
			return nil, nil, err
		}
		wkbValues = append(wkbValues, wkbValue)
	}
	return wkbValues, validData, nil
}

func ReadGeometryData(pcr *FieldReader, count int64) (any, error) {
	// Geometry field read data from string array Parquet
	data, err := ReadStringData(pcr, count, false)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	wkbValues := make([][]byte, 0)
	for _, wktValue := range data.([]string) {
		wkbValue, err := pkgcommon.ConvertWKTToWKB(wktValue)
		if err != nil {
			return nil, err
		}
		wkbValues = append(wkbValues, wkbValue)
	}
	return wkbValues, nil
}

func ReadBinaryData(pcr *FieldReader, count int64) (any, error) {
	dataType := pcr.field.GetDataType()
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0, count)
	for _, chunk := range chunked.Chunks() {
		rows := chunk.Data().Len()
		switch chunk.DataType().ID() {
		case arrow.BINARY:
			binaryReader := chunk.(*array.Binary)
			for i := 0; i < rows; i++ {
				data = append(data, binaryReader.Value(i)...)
			}
		case arrow.LIST, arrow.FIXED_SIZE_LIST:
			if chunk.NullN() > 0 {
				return nil, WrapNullRowErr(pcr.field)
			}
			listReader, err := newListLikeArray(chunk, pcr.field)
			if err != nil {
				return nil, err
			}
			if err = checkListLikeVectorAligned(listReader, pcr.dim, dataType); err != nil {
				return nil, merr.WrapErrImportFailedMsg("length of vector is not aligned: %s, data type: %s", err.Error(), dataType.String())
			}
			uint8Reader, ok := listReader.ListValues().(*array.Uint8)
			if !ok {
				return nil, WrapTypeErr(pcr.field, listReader.ListValues().DataType().Name())
			}
			if canBulkCopyUint8ListValues(listReader, uint8Reader) {
				data = append(data, uint8Reader.Uint8Values()...)
				continue
			}
			for i := 0; i < listReader.Len(); i++ {
				start, end := listReader.ValueOffsets(i)
				for j := start; j < end; j++ {
					if uint8Reader.IsNull(int(j)) {
						return nil, WrapNullElementErr(pcr.field)
					}
					data = append(data, uint8Reader.Value(int(j)))
				}
			}
		default:
			return nil, WrapTypeErr(pcr.field, chunk.DataType().Name())
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadNullableBinaryData(pcr *FieldReader, count int64) (any, []bool, error) {
	dataType := pcr.field.GetDataType()
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([]byte, 0, count)
	validData := make([]bool, 0, count)

	// Sparse storage: only store valid rows' data
	for _, chunk := range chunked.Chunks() {
		rows := chunk.Data().Len()
		switch chunk.DataType().ID() {
		case arrow.NULL:
			for i := 0; i < rows; i++ {
				validData = append(validData, false)
			}
		case arrow.BINARY:
			binaryReader := chunk.(*array.Binary)
			expectedRowWidth, err := expectedVectorListLength(pcr.dim, dataType)
			if err != nil {
				return nil, nil, err
			}
			for i := 0; i < rows; i++ {
				if binaryReader.IsNull(i) {
					validData = append(validData, false)
				} else {
					value := binaryReader.Value(i)
					if len(value) != int(expectedRowWidth) {
						return nil, nil, merr.WrapErrImportFailedMsg("vector row width mismatch: field %s, row %d, expected %d bytes but got %d bytes, data type: %s",
							pcr.field.GetName(), len(validData), expectedRowWidth, len(value), dataType.String())
					}
					data = append(data, value...)
					validData = append(validData, true)
				}
			}
		case arrow.LIST, arrow.FIXED_SIZE_LIST:
			listReader, err := newListLikeArray(chunk, pcr.field)
			if err != nil {
				return nil, nil, err
			}
			if err = checkNullableListLikeVectorAligned(listReader, pcr.dim, dataType); err != nil {
				return nil, nil, merr.WrapErrImportFailedMsg("length of vector is not aligned: %s, data type: %s", err.Error(), dataType.String())
			}
			uint8Reader, ok := listReader.ListValues().(*array.Uint8)
			if !ok {
				return nil, nil, WrapTypeErr(pcr.field, listReader.ListValues().DataType().Name())
			}
			if canBulkCopyUint8ListValues(listReader, uint8Reader) {
				values := uint8Reader.Uint8Values()
				for i := 0; i < rows; i++ {
					if listReader.IsNull(i) {
						validData = append(validData, false)
					} else {
						start, end := listReader.ValueOffsets(i)
						data = append(data, values[int(start):int(end)]...)
						validData = append(validData, true)
					}
				}
				continue
			}
			for i := 0; i < rows; i++ {
				if listReader.IsNull(i) {
					validData = append(validData, false)
				} else {
					start, end := listReader.ValueOffsets(i)
					for j := start; j < end; j++ {
						if uint8Reader.IsNull(int(j)) {
							return nil, nil, WrapNullElementErr(pcr.field)
						}
						data = append(data, uint8Reader.Value(int(j)))
					}
					validData = append(validData, true)
				}
			}
		default:
			return nil, nil, WrapTypeErr(pcr.field, chunk.DataType().Name())
		}
	}
	if len(data) == 0 && len(validData) == 0 {
		return nil, nil, nil
	}
	return data, validData, nil
}

func parseSparseFloatRowVector(str string) ([]byte, uint32, error) {
	rowVec, err := typeutil.CreateSparseFloatRowFromJSON([]byte(str))
	if err != nil {
		return nil, 0, merr.WrapErrImportFailedMsg("Invalid JSON string for SparseFloatVector: '%s', err = %v", str, err)
	}
	elemCount := len(rowVec) / 8
	maxIdx := uint32(0)

	if elemCount > 0 {
		maxIdx = typeutil.SparseFloatRowIndexAt(rowVec, elemCount-1) + 1
	}

	return rowVec, maxIdx, nil
}

// This method accepts input from ReadStructData()
// For sparse vector, the map[string]arrow.Array is like {"indices": array.List, "values": array.List}
// Although "indices" and "values" is two-dim list, the array.List provides ListValues() and ValueOffsets()
// to return one-dim list. We use the start/end position of ValueOffsets() to get the correct sparse vector
// from ListValues().
// Note that arrow.Uint32.Value(int i) accepts an int32 value, the max length of indices/values is max value of int32
func parseSparseFloatVectorStructRow(st map[string]arrow.Array, row int) ([]byte, uint32, error) {
	indices, ok1 := st[sparseVectorIndice]
	values, ok2 := st[sparseVectorValues]
	if !ok1 || !ok2 {
		return nil, 0, merr.WrapErrImportFailed("Invalid parquet struct for SparseFloatVector: 'indices' or 'values' missed")
	}

	indicesList, ok1 := indices.(*array.List)
	valuesList, ok2 := values.(*array.List)
	if !ok1 || !ok2 {
		return nil, 0, merr.WrapErrImportFailed("Invalid parquet struct for SparseFloatVector: 'indices' or 'values' is not list")
	}

	// Len() is the number of rows in this row group
	if indices.Len() != values.Len() {
		msg := fmt.Sprintf("Invalid parquet struct for SparseFloatVector: number of rows of 'indices' and 'values' mismatched, '%d' vs '%d'", indices.Len(), values.Len())
		return nil, 0, merr.WrapErrImportFailed(msg)
	}
	if row < 0 || row >= indicesList.Len() {
		msg := fmt.Sprintf("Invalid parquet struct for SparseFloatVector: row index %d out of range, rows=%d", row, indicesList.Len())
		return nil, 0, merr.WrapErrImportFailed(msg)
	}
	if indicesList.IsNull(row) || valuesList.IsNull(row) {
		return nil, 0, merr.WrapErrImportFailed("Invalid parquet struct for SparseFloatVector: 'indices' or 'values' is null for a valid sparse row")
	}

	// technically, DataType() of array.List must be arrow.ListType, but we still check here to ensure safety
	indicesListType, ok1 := indicesList.DataType().(*arrow.ListType)
	valuesListType, ok2 := valuesList.DataType().(*arrow.ListType)
	if !ok1 || !ok2 {
		return nil, 0, merr.WrapErrImportFailed("Invalid parquet struct for SparseFloatVector: incorrect arrow type of 'indices' or 'values'")
	}

	indexDataType := indicesListType.Elem().ID()
	valueDataType := valuesListType.Elem().ID()

	// The array.Uint32/array.Int64/array.Float32/array.Float64 are derived from arrow.Array
	// The ListValues() returns arrow.Array interface, but the arrow.Array doesn't have Value(int) interface
	// To call array.Uint32.Value(int), we need to explicitly cast the ListValues() to array.Uint32
	// So, we declare two methods here to avoid type casting in the "for" loop
	type GetIndex func(position int) uint32
	type GetValue func(position int) float32

	var getIndexFunc GetIndex
	switch indexDataType {
	case arrow.INT32:
		indicesList := indicesList.ListValues().(*array.Int32)
		getIndexFunc = func(position int) uint32 {
			return (uint32)(indicesList.Value(position))
		}
	case arrow.UINT32:
		indicesList := indicesList.ListValues().(*array.Uint32)
		getIndexFunc = func(position int) uint32 {
			return indicesList.Value(position)
		}
	case arrow.INT64:
		indicesList := indicesList.ListValues().(*array.Int64)
		getIndexFunc = func(position int) uint32 {
			return (uint32)(indicesList.Value(position))
		}
	case arrow.UINT64:
		indicesList := indicesList.ListValues().(*array.Uint64)
		getIndexFunc = func(position int) uint32 {
			return (uint32)(indicesList.Value(position))
		}
	default:
		msg := fmt.Sprintf("Invalid parquet struct for SparseFloatVector: index type must be uint32/int32/uint64/int64 but actual type is '%s'", indicesListType.Elem().Name())
		return nil, 0, merr.WrapErrImportFailed(msg)
	}

	var getValueFunc GetValue
	switch valueDataType {
	case arrow.FLOAT32:
		valuesList := valuesList.ListValues().(*array.Float32)
		getValueFunc = func(position int) float32 {
			return valuesList.Value(position)
		}
	case arrow.FLOAT64:
		valuesList := valuesList.ListValues().(*array.Float64)
		getValueFunc = func(position int) float32 {
			return (float32)(valuesList.Value(position))
		}
	default:
		msg := fmt.Sprintf("Invalid parquet struct for SparseFloatVector: value type must be float32 or float64 but actual type is '%s'", valuesListType.Elem().Name())
		return nil, 0, merr.WrapErrImportFailed(msg)
	}

	start, end := indicesList.ValueOffsets(row)
	start2, end2 := valuesList.ValueOffsets(row)
	rowLen := (int)(end - start)
	rowLenValues := (int)(end2 - start2)
	if rowLenValues != rowLen {
		msg := fmt.Sprintf("Invalid parquet struct for SparseFloatVector: number of elements of 'indices' and 'values' mismatched, '%d' vs '%d'", rowLen, rowLenValues)
		return nil, 0, merr.WrapErrImportFailed(msg)
	}

	rowIndices := make([]uint32, rowLen)
	rowValues := make([]float32, rowLen)
	for i := start; i < end; i++ {
		rowIndices[i-start] = getIndexFunc((int)(i))
		rowValues[i-start] = getValueFunc((int)(i))
	}

	// ensure the indices is sorted
	sortedIndices, sortedValues := typeutil.SortSparseFloatRow(rowIndices, rowValues)
	rowVec := typeutil.CreateSparseFloatRow(sortedIndices, sortedValues)
	if err := typeutil.ValidateSparseFloatRows(rowVec); err != nil {
		return nil, 0, err
	}

	maxDim := uint32(0)
	// set the maxDim as the last value of sortedIndices since it has been sorted
	if len(sortedIndices) > 0 {
		maxDim = sortedIndices[len(sortedIndices)-1]
	}
	return rowVec, maxDim, nil // rowVec could be an empty sparse
}

func parseSparseFloatVectorStructs(structs []map[string]arrow.Array) ([][]byte, uint32, error) {
	byteArr := make([][]byte, 0)
	maxDim := uint32(0)
	for _, st := range structs {
		indices, ok := st[sparseVectorIndice]
		if !ok {
			return nil, 0, merr.WrapErrImportFailed("Invalid parquet struct for SparseFloatVector: 'indices' or 'values' missed")
		}
		for i := 0; i < indices.Len(); i++ {
			rowVec, rowMaxDim, err := parseSparseFloatVectorStructRow(st, i)
			if err != nil {
				return byteArr, maxDim, err
			}
			if rowMaxDim > maxDim {
				maxDim = rowMaxDim
			}
			byteArr = append(byteArr, rowVec)
		}
	}
	return byteArr, maxDim, nil
}

func ReadSparseFloatVectorData(pcr *FieldReader, count int64) (any, error) {
	// read sparse vector from JSON-format string
	if pcr.sparseIsString {
		data, err := ReadStringData(pcr, count, false)
		if err != nil {
			return nil, err
		}
		if data == nil {
			return nil, nil
		}

		byteArr := make([][]byte, 0, count)
		maxDim := uint32(0)

		for _, str := range data.([]string) {
			rowVec, rowMaxIdx, err := parseSparseFloatRowVector(str)
			if err != nil {
				return nil, err
			}

			byteArr = append(byteArr, rowVec)
			if rowMaxIdx > maxDim {
				maxDim = rowMaxIdx
			}
		}

		return &storage.SparseFloatVectorFieldData{
			SparseFloatArray: schemapb.SparseFloatArray{
				Dim:      int64(maxDim),
				Contents: byteArr,
			},
		}, nil
	}

	// read sparse vector from parquet struct
	data, err := ReadStructData(pcr, count)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}

	byteArr, maxDim, err := parseSparseFloatVectorStructs(data)
	if err != nil {
		return nil, err
	}

	return &storage.SparseFloatVectorFieldData{
		SparseFloatArray: schemapb.SparseFloatArray{
			Dim:      int64(maxDim),
			Contents: byteArr,
		},
	}, nil
}

func ReadNullableSparseFloatVectorData(pcr *FieldReader, count int64) (any, []bool, error) {
	if pcr.sparseIsString {
		data, validData, err := ReadNullableStringData(pcr, count)
		if err != nil {
			return nil, nil, err
		}
		if data == nil {
			return nil, nil, nil
		}

		// Sparse storage: only store valid rows' data
		byteArr := make([][]byte, 0, count)
		maxDim := uint32(0)

		for i, str := range data.([]string) {
			if validData[i] {
				rowVec, rowMaxIdx, err := parseSparseFloatRowVector(str)
				if err != nil {
					return nil, nil, err
				}
				byteArr = append(byteArr, rowVec)
				if rowMaxIdx > maxDim {
					maxDim = rowMaxIdx
				}
			}
		}

		return &storage.SparseFloatVectorFieldData{
			SparseFloatArray: schemapb.SparseFloatArray{
				Dim:      int64(maxDim),
				Contents: byteArr,
			},
			ValidData: validData,
			Nullable:  true,
		}, validData, nil
	}

	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	if chunked == nil || len(chunked.Chunks()) == 0 {
		return nil, nil, nil
	}

	// Sparse storage: only store valid rows' data
	byteArr := make([][]byte, 0, count)
	validData := make([]bool, 0, count)
	maxDim := uint32(0)

	for _, chunk := range chunked.Chunks() {
		structReader, ok := chunk.(*array.Struct)
		if !ok {
			return nil, nil, WrapTypeErr(pcr.field, chunk.DataType().Name())
		}

		structType := structReader.DataType().(*arrow.StructType)
		st := make(map[string]arrow.Array)
		for k, field := range structType.Fields() {
			st[field.Name] = structReader.Field(k)
		}

		for i := 0; i < structReader.Len(); i++ {
			valid := !structReader.IsNull(i)
			validData = append(validData, valid)
			if !valid {
				continue
			}
			rowVec, rowMaxDim, err := parseSparseFloatVectorStructRow(st, i)
			if err != nil {
				return nil, nil, err
			}
			byteArr = append(byteArr, rowVec)
			if rowMaxDim > maxDim {
				maxDim = rowMaxDim
			}
		}
	}
	if len(validData) == 0 {
		return nil, nil, nil
	}

	return &storage.SparseFloatVectorFieldData{
		SparseFloatArray: schemapb.SparseFloatArray{
			Dim:      int64(maxDim),
			Contents: byteArr,
		},
		ValidData: validData,
		Nullable:  true,
	}, validData, nil
}

func checkVectorAlignWithDim(offsets []int32, dim int32) error {
	for i := 1; i < len(offsets); i++ {
		if offsets[i]-offsets[i-1] != dim {
			return merr.WrapErrParameterInvalidMsg("expected %d but got %d", dim, offsets[i]-offsets[i-1])
		}
	}
	return nil
}

func checkVectorAligned(offsets []int32, dim int, dataType schemapb.DataType) error {
	if len(offsets) < 1 {
		return merr.WrapErrParameterInvalidMsg("empty offsets")
	}
	switch dataType {
	case schemapb.DataType_BinaryVector:
		return checkVectorAlignWithDim(offsets, int32(dim/8))
	case schemapb.DataType_FloatVector:
		return checkVectorAlignWithDim(offsets, int32(dim))
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return checkVectorAlignWithDim(offsets, int32(dim*2))
	case schemapb.DataType_SparseFloatVector:
		// JSON format, skip alignment check
		return nil
	case schemapb.DataType_Int8Vector:
		return checkVectorAlignWithDim(offsets, int32(dim))
	default:
		return merr.WrapErrParameterInvalidMsg("unexpected vector data type %s", dataType.String())
	}
}

func ReadBoolArrayData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([][]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		if chunk.NullN() > 0 {
			// Array field is not nullable, but some arrays are null
			return nil, WrapNullRowErr(pcr.field)
		}
		listReader, err := newListLikeArray(chunk, pcr.field)
		if err != nil {
			return nil, err
		}
		err = readBoolListLikeData(pcr.field, listReader, func(arr []bool, valid bool) {
			data = append(data, arr)
		})
		if err != nil {
			return nil, err
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadNullableBoolArrayData(pcr *FieldReader, count int64) (any, []bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([][]bool, 0, count)
	validData := make([]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		if _, ok := chunk.(*array.Null); ok {
			// the chunk type may be *array.Null if the data in chunk is all null
			dataNums := chunk.Data().Len()
			validData = append(validData, make([]bool, dataNums)...)
			data = append(data, make([][]bool, dataNums)...)
		} else {
			listReader, err := newListLikeArray(chunk, pcr.field)
			if err != nil {
				return nil, nil, err
			}
			err = readBoolListLikeData(pcr.field, listReader, func(arr []bool, valid bool) {
				data = append(data, arr)
				validData = append(validData, valid)
			})
			if err != nil {
				return nil, nil, err
			}
		}
	}
	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrParameterInvalid(len(data), len(validData), "length of data is not equal to length of valid_data")
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	return data, validData, nil
}

func ReadIntegerOrFloatArrayData[T constraints.Integer | constraints.Float](pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([][]T, 0, count)

	for _, chunk := range chunked.Chunks() {
		if chunk.NullN() > 0 {
			// Array field is not nullable, but some arrays are null
			return nil, WrapNullRowErr(pcr.field)
		}
		listReader, err := newListLikeArray(chunk, pcr.field)
		if err != nil {
			return nil, err
		}
		dataType := pcr.field.GetDataType()
		if typeutil.IsVectorType(dataType) {
			if err = checkListLikeVectorAligned(listReader, pcr.dim, dataType); err != nil {
				return nil, merr.WrapErrImportFailedMsg("length of vector is not aligned: %s, data type: %s", err.Error(), dataType.String())
			}
		}
		if err = readIntegerOrFloatListLikeData(pcr.field, listReader, func(arr []T, valid bool) {
			data = append(data, arr)
		}); err != nil {
			return nil, err
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadNullableIntegerOrFloatArrayData[T constraints.Integer | constraints.Float](pcr *FieldReader, count int64) (any, []bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([][]T, 0, count)
	validData := make([]bool, 0, count)

	for _, chunk := range chunked.Chunks() {
		if _, ok := chunk.(*array.Null); ok {
			// the chunk type may be *array.Null if the data in chunk is all null
			dataNums := chunk.Data().Len()
			validData = append(validData, make([]bool, dataNums)...)
			data = append(data, make([][]T, dataNums)...)
		} else {
			listReader, err := newListLikeArray(chunk, pcr.field)
			if err != nil {
				return nil, nil, err
			}
			dataType := pcr.field.GetDataType()
			if typeutil.IsVectorType(dataType) {
				if err = checkListLikeVectorAligned(listReader, pcr.dim, dataType); err != nil {
					return nil, nil, merr.WrapErrImportFailedMsg("length of vector is not aligned: %s, data type: %s", err.Error(), dataType.String())
				}
			}
			if err = readIntegerOrFloatListLikeData(pcr.field, listReader, func(arr []T, valid bool) {
				data = append(data, arr)
				validData = append(validData, valid)
			}); err != nil {
				return nil, nil, err
			}
		}
	}
	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrParameterInvalid(len(data), len(validData), "length of data is not equal to length of valid_data")
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	return data, validData, nil
}

func ReadNullableFloatVectorData(pcr *FieldReader, count int64) (any, []bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([]float32, 0, int(count)*pcr.dim)
	validData := make([]bool, 0, count)

	for _, chunk := range chunked.Chunks() {
		if _, ok := chunk.(*array.Null); ok {
			// the chunk type may be *array.Null if the data in chunk is all null
			dataNums := chunk.Data().Len()
			validData = append(validData, make([]bool, dataNums)...)
			continue
		}
		listReader, err := newListLikeArray(chunk, pcr.field)
		if err != nil {
			return nil, nil, err
		}

		dataType := pcr.field.GetDataType()
		if err = checkNullableListLikeVectorAligned(listReader, pcr.dim, dataType); err != nil {
			return nil, nil, merr.WrapErrImportFailedMsg("length of vector is not aligned: %s, data type: %s", err.Error(), dataType.String())
		}

		valueReader := listReader.ListValues()
		rows := listReader.Len()

		// Sparse storage: only store valid rows' data
		float32Reader, ok := valueReader.(*array.Float32)
		if !ok {
			return nil, nil, WrapTypeErr(pcr.field, valueReader.DataType().Name())
		}
		for i := 0; i < rows; i++ {
			validData = append(validData, !listReader.IsNull(i))
			if !listReader.IsNull(i) {
				start, end := listReader.ValueOffsets(i)
				for j := start; j < end; j++ {
					if float32Reader.IsNull(int(j)) {
						return nil, nil, WrapNullElementErr(pcr.field)
					}
					data = append(data, float32Reader.Value(int(j)))
				}
			}
		}
	}
	if len(data) == 0 && len(validData) == 0 {
		return nil, nil, nil
	}
	return data, validData, typeutil.VerifyFloats32(data)
}

func isFP16BF16FloatArrowType(dataType arrow.DataType) bool {
	switch dataType.ID() {
	case arrow.LIST:
		elemType := dataType.(*arrow.ListType).Elem().ID()
		return elemType == arrow.FLOAT32 || elemType == arrow.FLOAT64
	case arrow.FIXED_SIZE_LIST:
		elemType := dataType.(*arrow.FixedSizeListType).Elem().ID()
		return elemType == arrow.FLOAT32 || elemType == arrow.FLOAT64
	default:
		return false
	}
}

func readFloatListLikeDataAsFloat32(field *schemapb.FieldSchema, listReader *listLikeArray, outputArray func(arr []float32, valid bool)) error {
	valueReader := listReader.ListValues()
	switch valueReader.DataType().ID() {
	case arrow.FLOAT32:
		float32Reader := valueReader.(*array.Float32)
		return getListLikeArrayData(listReader, func(i int) (float32, error) {
			if float32Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return float32Reader.Value(i), nil
		}, outputArray)
	case arrow.FLOAT64:
		float64Reader := valueReader.(*array.Float64)
		return getListLikeArrayData(listReader, func(i int) (float32, error) {
			if float64Reader.IsNull(i) {
				return 0, WrapNullElementErr(field)
			}
			return float32(float64Reader.Value(i)), nil
		}, outputArray)
	default:
		return WrapTypeErr(field, valueReader.DataType().Name())
	}
}

func ReadFP16BF16FloatVectorData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0, int(count)*pcr.dim*2)
	for _, chunk := range chunked.Chunks() {
		if chunk.NullN() > 0 {
			return nil, WrapNullRowErr(pcr.field)
		}
		listReader, err := newListLikeArray(chunk, pcr.field)
		if err != nil {
			return nil, err
		}
		if err = checkListLikeVectorAlignedWithExpected(listReader, int32(pcr.dim)); err != nil {
			return nil, merr.WrapErrImportFailedMsg("length of vector is not aligned: %s, data type: %s", err.Error(), pcr.field.GetDataType().String())
		}
		floatRows := make([][]float32, 0, int(count))
		if err = readFloatListLikeDataAsFloat32(pcr.field, listReader, func(arr []float32, valid bool) {
			floatRows = append(floatRows, arr)
		}); err != nil {
			return nil, err
		}
		converted, err := typeutil.ConvertFloat32ToFP16BF16Bytes(lo.Flatten(floatRows), pcr.field.GetDataType())
		if err != nil {
			return nil, err
		}
		data = append(data, converted...)
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadNullableFP16BF16FloatVectorData(pcr *FieldReader, count int64) (any, []bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([]byte, 0, int(count)*pcr.dim*2)
	validData := make([]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		if _, ok := chunk.(*array.Null); ok {
			rows := chunk.Data().Len()
			validData = append(validData, make([]bool, rows)...)
			continue
		}
		listReader, err := newListLikeArray(chunk, pcr.field)
		if err != nil {
			return nil, nil, err
		}
		if err = checkNullableListLikeVectorAlignedWithExpected(listReader, int32(pcr.dim)); err != nil {
			return nil, nil, merr.WrapErrImportFailedMsg("length of vector is not aligned: %s, data type: %s", err.Error(), pcr.field.GetDataType().String())
		}
		floatRows := make([][]float32, 0, int(count))
		if err = readFloatListLikeDataAsFloat32(pcr.field, listReader, func(arr []float32, valid bool) {
			validData = append(validData, valid)
			if valid {
				floatRows = append(floatRows, arr)
			}
		}); err != nil {
			return nil, nil, err
		}
		converted, err := typeutil.ConvertFloat32ToFP16BF16Bytes(lo.Flatten(floatRows), pcr.field.GetDataType())
		if err != nil {
			return nil, nil, err
		}
		data = append(data, converted...)
	}
	if len(data) == 0 && len(validData) == 0 {
		return nil, nil, nil
	}
	return data, validData, nil
}

func ReadNullableInt8VectorData(pcr *FieldReader, count int64) (any, []bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([]int8, 0, int(count)*pcr.dim)
	validData := make([]bool, 0, count)

	for _, chunk := range chunked.Chunks() {
		if _, ok := chunk.(*array.Null); ok {
			// the chunk type may be *array.Null if the data in chunk is all null
			dataNums := chunk.Data().Len()
			validData = append(validData, make([]bool, dataNums)...)
			continue
		}
		listReader, err := newListLikeArray(chunk, pcr.field)
		if err != nil {
			return nil, nil, err
		}

		dataType := pcr.field.GetDataType()
		if err = checkNullableListLikeVectorAligned(listReader, pcr.dim, dataType); err != nil {
			return nil, nil, merr.WrapErrImportFailedMsg("length of vector is not aligned: %s, data type: %s", err.Error(), dataType.String())
		}

		valueReader := listReader.ListValues()
		rows := listReader.Len()

		// Sparse storage: only store valid rows' data
		int8Reader, ok := valueReader.(*array.Int8)
		if !ok {
			return nil, nil, WrapTypeErr(pcr.field, valueReader.DataType().Name())
		}
		for i := 0; i < rows; i++ {
			validData = append(validData, !listReader.IsNull(i))
			if !listReader.IsNull(i) {
				start, end := listReader.ValueOffsets(i)
				for j := start; j < end; j++ {
					if int8Reader.IsNull(int(j)) {
						return nil, nil, WrapNullElementErr(pcr.field)
					}
					data = append(data, int8Reader.Value(int(j)))
				}
			}
		}
	}
	if len(data) == 0 && len(validData) == 0 {
		return nil, nil, nil
	}
	return data, validData, nil
}

func ReadStringArrayData(pcr *FieldReader, count int64) (any, error) {
	maxLength, err := parameterutil.GetMaxLength(pcr.field)
	if err != nil {
		return nil, err
	}
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([][]string, 0, count)
	for _, chunk := range chunked.Chunks() {
		if chunk.NullN() > 0 {
			// Array field is not nullable, but some arrays are null
			return nil, WrapNullRowErr(pcr.field)
		}
		listReader, err := newListLikeArray(chunk, pcr.field)
		if err != nil {
			return nil, err
		}
		err = readStringListLikeData(pcr.field, listReader, func(val string) error {
			return common.CheckValidString(val, maxLength, pcr.field)
		}, func(arr []string, valid bool) {
			data = append(data, arr)
		})
		if err != nil {
			return nil, err
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadNullableStringArrayData(pcr *FieldReader, count int64) (any, []bool, error) {
	maxLength, err := parameterutil.GetMaxLength(pcr.field)
	if err != nil {
		return nil, nil, err
	}
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([][]string, 0, count)
	validData := make([]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		if _, ok := chunk.(*array.Null); ok {
			// the chunk type may be *array.Null if the data in chunk is all null
			dataNums := chunk.Data().Len()
			validData = append(validData, make([]bool, dataNums)...)
			data = append(data, make([][]string, dataNums)...)
		} else {
			listReader, err := newListLikeArray(chunk, pcr.field)
			if err != nil {
				return nil, nil, err
			}
			err = readStringListLikeData(pcr.field, listReader, func(val string) error {
				return common.CheckValidString(val, maxLength, pcr.field)
			}, func(arr []string, valid bool) {
				data = append(data, arr)
				validData = append(validData, valid)
			})
			if err != nil {
				return nil, nil, err
			}
		}
	}

	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrImportFailed(
			fmt.Sprintf("length(%d) of data is not equal to length(%d) of valid_data for field '%s'",
				len(data), len(validData), pcr.field.GetName()))
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	return data, validData, nil
}

func ReadArrayData(pcr *FieldReader, count int64) (any, error) {
	data := make([]*schemapb.ScalarField, 0, count)
	maxCapacity, err := parameterutil.GetMaxCapacity(pcr.field)
	if err != nil {
		return nil, err
	}
	elementType := pcr.field.GetElementType()
	switch elementType {
	case schemapb.DataType_Bool:
		boolArray, err := ReadBoolArrayData(pcr, count)
		if err != nil {
			return nil, err
		}
		if boolArray == nil {
			return nil, nil
		}
		for _, elementArray := range boolArray.([][]bool) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: elementArray,
					},
				},
			})
		}
	case schemapb.DataType_Int8:
		int8Array, err := ReadIntegerOrFloatArrayData[int32](pcr, count)
		if err != nil {
			return nil, err
		}
		if int8Array == nil {
			return nil, nil
		}
		for _, elementArray := range int8Array.([][]int32) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: elementArray,
					},
				},
			})
		}
	case schemapb.DataType_Int16:
		int16Array, err := ReadIntegerOrFloatArrayData[int32](pcr, count)
		if err != nil {
			return nil, err
		}
		if int16Array == nil {
			return nil, nil
		}
		for _, elementArray := range int16Array.([][]int32) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: elementArray,
					},
				},
			})
		}
	case schemapb.DataType_Int32:
		int32Array, err := ReadIntegerOrFloatArrayData[int32](pcr, count)
		if err != nil {
			return nil, err
		}
		if int32Array == nil {
			return nil, nil
		}
		for _, elementArray := range int32Array.([][]int32) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: elementArray,
					},
				},
			})
		}
	case schemapb.DataType_Int64:
		int64Array, err := ReadIntegerOrFloatArrayData[int64](pcr, count)
		if err != nil {
			return nil, err
		}
		if int64Array == nil {
			return nil, nil
		}
		for _, elementArray := range int64Array.([][]int64) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: elementArray,
					},
				},
			})
		}
	case schemapb.DataType_Float:
		float32Array, err := ReadIntegerOrFloatArrayData[float32](pcr, count)
		if err != nil {
			return nil, err
		}
		if float32Array == nil {
			return nil, nil
		}
		for _, elementArray := range float32Array.([][]float32) {
			if err := typeutil.VerifyFloats32(elementArray); err != nil {
				return nil, merr.Wrap(err, "float32 verification failed")
			}
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: elementArray,
					},
				},
			})
		}
	case schemapb.DataType_Double:
		float64Array, err := ReadIntegerOrFloatArrayData[float64](pcr, count)
		if err != nil {
			return nil, err
		}
		if float64Array == nil {
			return nil, nil
		}
		for _, elementArray := range float64Array.([][]float64) {
			if err := typeutil.VerifyFloats64(elementArray); err != nil {
				return nil, merr.Wrap(err, "float64 verification failed")
			}
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: elementArray,
					},
				},
			})
		}
	case schemapb.DataType_Timestamptz:
		int64Array, err := ReadIntegerOrFloatArrayData[int64](pcr, count)
		if err != nil {
			return nil, err
		}
		if int64Array == nil {
			return nil, nil
		}
		for _, elementArray := range int64Array.([][]int64) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_TimestamptzData{
					TimestamptzData: &schemapb.TimestamptzArray{
						Data: elementArray,
					},
				},
			})
		}
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		stringArray, err := ReadStringArrayData(pcr, count)
		if err != nil {
			return nil, err
		}
		if stringArray == nil {
			return nil, nil
		}
		for _, elementArray := range stringArray.([][]string) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: elementArray,
					},
				},
			})
		}
	default:
		return nil, merr.WrapErrImportFailedMsg("unsupported data type '%s' for array field '%s'",
			elementType.String(), pcr.field.GetName())
	}
	return data, nil
}

func ReadNullableArrayData(pcr *FieldReader, count int64) (any, []bool, error) {
	data := make([]*schemapb.ScalarField, 0, count)
	maxCapacity, err := parameterutil.GetMaxCapacity(pcr.field)
	if err != nil {
		return nil, nil, err
	}
	elementType := pcr.field.GetElementType()
	switch elementType {
	case schemapb.DataType_Bool:
		boolArray, validData, err := ReadNullableBoolArrayData(pcr, count)
		if err != nil {
			return nil, nil, err
		}
		if boolArray == nil {
			return nil, nil, nil
		}
		for _, elementArray := range boolArray.([][]bool) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: elementArray,
					},
				},
			})
		}
		return data, validData, nil
	case schemapb.DataType_Int8:
		int8Array, validData, err := ReadNullableIntegerOrFloatArrayData[int32](pcr, count)
		if err != nil {
			return nil, nil, err
		}
		if int8Array == nil {
			return nil, nil, nil
		}
		for _, elementArray := range int8Array.([][]int32) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: elementArray,
					},
				},
			})
		}
		return data, validData, nil
	case schemapb.DataType_Int16:
		int16Array, validData, err := ReadNullableIntegerOrFloatArrayData[int32](pcr, count)
		if err != nil {
			return nil, nil, err
		}
		if int16Array == nil {
			return nil, nil, nil
		}
		for _, elementArray := range int16Array.([][]int32) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: elementArray,
					},
				},
			})
		}
		return data, validData, nil
	case schemapb.DataType_Int32:
		int32Array, validData, err := ReadNullableIntegerOrFloatArrayData[int32](pcr, count)
		if err != nil {
			return nil, nil, err
		}
		if int32Array == nil {
			return nil, nil, nil
		}
		for _, elementArray := range int32Array.([][]int32) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: elementArray,
					},
				},
			})
		}
		return data, validData, nil
	case schemapb.DataType_Int64:
		int64Array, validData, err := ReadNullableIntegerOrFloatArrayData[int64](pcr, count)
		if err != nil {
			return nil, nil, err
		}
		if int64Array == nil {
			return nil, nil, nil
		}
		for _, elementArray := range int64Array.([][]int64) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: elementArray,
					},
				},
			})
		}
		return data, validData, nil
	case schemapb.DataType_Float:
		float32Array, validData, err := ReadNullableIntegerOrFloatArrayData[float32](pcr, count)
		if err != nil {
			return nil, nil, err
		}
		if float32Array == nil {
			return nil, nil, nil
		}
		for _, elementArray := range float32Array.([][]float32) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: elementArray,
					},
				},
			})
		}
		return data, validData, nil
	case schemapb.DataType_Double:
		float64Array, validData, err := ReadNullableIntegerOrFloatArrayData[float64](pcr, count)
		if err != nil {
			return nil, nil, err
		}
		if float64Array == nil {
			return nil, nil, nil
		}
		for _, elementArray := range float64Array.([][]float64) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: elementArray,
					},
				},
			})
		}
		return data, validData, nil
	case schemapb.DataType_Timestamptz:
		int64Array, validData, err := ReadNullableIntegerOrFloatArrayData[int64](pcr, count)
		if err != nil {
			return nil, nil, err
		}
		if int64Array == nil {
			return nil, nil, nil
		}
		for _, elementArray := range int64Array.([][]int64) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_TimestamptzData{
					TimestamptzData: &schemapb.TimestamptzArray{
						Data: elementArray,
					},
				},
			})
		}
		return data, validData, nil
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		stringArray, validData, err := ReadNullableStringArrayData(pcr, count)
		if err != nil {
			return nil, nil, err
		}
		if stringArray == nil {
			return nil, nil, nil
		}
		for _, elementArray := range stringArray.([][]string) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity, pcr.field); err != nil {
				return nil, nil, err
			}
			data = append(data, &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: elementArray,
					},
				},
			})
		}
		return data, validData, nil
	default:
		return nil, nil, merr.WrapErrImportFailedMsg("unsupported data type '%s' for array field '%s'",
			elementType.String(), pcr.field.GetName())
	}
}

func ReadVectorArrayData(pcr *FieldReader, count int64) (any, error) {
	data, _, err := readVectorArrayData(pcr, count, false)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func ReadNullableVectorArrayData(pcr *FieldReader, count int64) (any, []bool, error) {
	return readVectorArrayData(pcr, count, true)
}

func readVectorArrayData(pcr *FieldReader, count int64, nullable bool) ([]*schemapb.VectorField, []bool, error) {
	data := make([]*schemapb.VectorField, 0, count)
	var validData []bool
	if nullable {
		validData = make([]bool, 0, count)
	}

	maxCapacity, err := parameterutil.GetMaxCapacity(pcr.field)
	if err != nil {
		return nil, nil, err
	}

	dim, err := typeutil.GetDim(pcr.field)
	if err != nil {
		return nil, nil, err
	}
	if _, err = vectorArrayBytesPerVector(pcr.field.GetElementType(), dim); err != nil {
		return nil, nil, err
	}

	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}

	if chunked == nil || chunked.Len() == 0 {
		return nil, nil, nil
	}

	for _, chunk := range chunked.Chunks() {
		switch reader := chunk.(type) {
		case *array.Null:
			if !nullable {
				return nil, nil, WrapNullRowErr(pcr.field)
			}
			for i := 0; i < reader.Len(); i++ {
				data = append(data, emptyVectorArrayRow(dim, pcr.field.GetElementType()))
				validData = append(validData, false)
			}

		case *array.List:
			if !nullable && reader.NullN() > 0 {
				return nil, nil, WrapNullRowErr(pcr.field)
			}

			for i := 0; i < reader.Len(); i++ {
				if reader.IsNull(i) {
					data = append(data, emptyVectorArrayRow(dim, pcr.field.GetElementType()))
					validData = append(validData, false)
					continue
				}
				start, end := reader.ValueOffsets(i)
				vectorCount := end - start

				if err = common.CheckArrayCapacity(int(vectorCount), maxCapacity, pcr.field); err != nil {
					return nil, nil, err
				}

				var rowData *schemapb.VectorField
				switch values := reader.ListValues().(type) {
				case *array.List:
					rowData, err = buildVectorArrayFieldFromList(pcr.field, dim, values, start, end)
				case *array.FixedSizeBinary:
					rowData, err = buildVectorArrayFieldFromFixedSizeBinary(pcr.field, dim, values, start, end)
				default:
					return nil, nil, WrapTypeErr(pcr.field, reader.ListValues().DataType().Name())
				}
				if err != nil {
					return nil, nil, err
				}

				data = append(data, rowData)
				if nullable {
					validData = append(validData, true)
				}
			}

		default:
			return nil, nil, WrapTypeErr(pcr.field, chunk.DataType().Name())
		}
	}

	if len(data) == 0 {
		return nil, nil, nil
	}
	return data, validData, nil
}

func emptyVectorArrayRow(dim int64, elementType schemapb.DataType) *schemapb.VectorField {
	vectorField := &schemapb.VectorField{Dim: dim}
	switch elementType {
	case schemapb.DataType_FloatVector:
		vectorField.Data = &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}}
	case schemapb.DataType_BinaryVector:
		vectorField.Data = &schemapb.VectorField_BinaryVector{BinaryVector: []byte{}}
	case schemapb.DataType_Float16Vector:
		vectorField.Data = &schemapb.VectorField_Float16Vector{Float16Vector: []byte{}}
	case schemapb.DataType_BFloat16Vector:
		vectorField.Data = &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: []byte{}}
	case schemapb.DataType_Int8Vector:
		vectorField.Data = &schemapb.VectorField_Int8Vector{Int8Vector: []byte{}}
	}
	return vectorField
}

func vectorArrayBytesPerVector(elementType schemapb.DataType, dim int64) (int, error) {
	switch elementType {
	case schemapb.DataType_FloatVector:
		return int(dim) * 4, nil
	case schemapb.DataType_BinaryVector:
		return int((dim + 7) / 8), nil
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return int(dim) * 2, nil
	case schemapb.DataType_Int8Vector:
		return int(dim), nil
	case schemapb.DataType_SparseFloatVector:
		return 0, merr.WrapErrImportFailed("ArrayOfVector with SparseFloatVector element type is not implemented yet")
	default:
		return 0, merr.WrapErrImportFailedMsg("unsupported ArrayOfVector element type: %v", elementType)
	}
}

func buildVectorArrayFieldFromFixedSizeBinary(field *schemapb.FieldSchema, dim int64, values *array.FixedSizeBinary, start, end int64) (*schemapb.VectorField, error) {
	bytesPerVector, err := vectorArrayBytesPerVector(field.GetElementType(), dim)
	if err != nil {
		return nil, err
	}
	actualByteSize := values.DataType().(*arrow.FixedSizeBinaryType).ByteWidth
	if actualByteSize != bytesPerVector {
		return nil, merr.WrapErrImportFailedMsg("vector byte size mismatch: expected %d, got %d for field '%s'",
			bytesPerVector, actualByteSize, field.GetName())
	}

	switch field.GetElementType() {
	case schemapb.DataType_FloatVector:
		floatData := make([]float32, 0, int(end-start)*int(dim))
		for vectorIndex := start; vectorIndex < end; vectorIndex++ {
			if values.IsNull(int(vectorIndex)) {
				return nil, WrapNullElementErr(field)
			}
			vectorFloats := arrow.Float32Traits.CastFromBytes(values.Value(int(vectorIndex)))
			floatData = append(floatData, vectorFloats...)
		}
		if err := typeutil.VerifyFloats32(floatData); err != nil {
			return nil, err
		}
		return &schemapb.VectorField{
			Dim: dim,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: floatData},
			},
		}, nil
	case schemapb.DataType_BinaryVector:
		binaryData := make([]byte, 0, int(end-start)*bytesPerVector)
		for vectorIndex := start; vectorIndex < end; vectorIndex++ {
			if values.IsNull(int(vectorIndex)) {
				return nil, WrapNullElementErr(field)
			}
			binaryData = append(binaryData, values.Value(int(vectorIndex))...)
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_BinaryVector{BinaryVector: binaryData},
		}, nil
	case schemapb.DataType_Float16Vector:
		float16Data := make([]byte, 0, int(end-start)*bytesPerVector)
		for vectorIndex := start; vectorIndex < end; vectorIndex++ {
			if values.IsNull(int(vectorIndex)) {
				return nil, WrapNullElementErr(field)
			}
			float16Data = append(float16Data, values.Value(int(vectorIndex))...)
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_Float16Vector{Float16Vector: float16Data},
		}, nil
	case schemapb.DataType_BFloat16Vector:
		bfloat16Data := make([]byte, 0, int(end-start)*bytesPerVector)
		for vectorIndex := start; vectorIndex < end; vectorIndex++ {
			if values.IsNull(int(vectorIndex)) {
				return nil, WrapNullElementErr(field)
			}
			bfloat16Data = append(bfloat16Data, values.Value(int(vectorIndex))...)
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: bfloat16Data},
		}, nil
	case schemapb.DataType_Int8Vector:
		int8Data := make([]byte, 0, int(end-start)*bytesPerVector)
		for vectorIndex := start; vectorIndex < end; vectorIndex++ {
			if values.IsNull(int(vectorIndex)) {
				return nil, WrapNullElementErr(field)
			}
			int8Data = append(int8Data, values.Value(int(vectorIndex))...)
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_Int8Vector{Int8Vector: int8Data},
		}, nil
	default:
		return nil, merr.WrapErrImportFailedMsg("unsupported ArrayOfVector element type: %v", field.GetElementType())
	}
}

func buildVectorArrayFieldFromList(field *schemapb.FieldSchema, dim int64, vectors *array.List, start, end int64) (*schemapb.VectorField, error) {
	switch field.GetElementType() {
	case schemapb.DataType_FloatVector:
		floatData := make([]float32, 0, int(end-start)*int(dim))
		appendFloat := func(pos int) error {
			switch values := vectors.ListValues().(type) {
			case *array.Float32:
				if values.IsNull(pos) {
					return WrapNullElementErr(field)
				}
				floatData = append(floatData, values.Value(pos))
			case *array.Float64:
				if values.IsNull(pos) {
					return WrapNullElementErr(field)
				}
				floatData = append(floatData, float32(values.Value(pos)))
			default:
				return WrapTypeErr(field, vectors.ListValues().DataType().Name())
			}
			return nil
		}
		for vectorIndex := start; vectorIndex < end; vectorIndex++ {
			vecStart, vecEnd, err := vectorArrayValueOffsets(field, dim, vectors, vectorIndex, int(dim))
			if err != nil {
				return nil, err
			}
			for pos := vecStart; pos < vecEnd; pos++ {
				if err := appendFloat(int(pos)); err != nil {
					return nil, err
				}
			}
		}
		if err := typeutil.VerifyFloats32(floatData); err != nil {
			return nil, err
		}
		return &schemapb.VectorField{
			Dim: dim,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: floatData},
			},
		}, nil
	case schemapb.DataType_BinaryVector:
		return buildByteVectorArrayFieldFromList(field, dim, vectors, start, end, int((dim+7)/8), func(data []byte) *schemapb.VectorField {
			return &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_BinaryVector{BinaryVector: data},
			}
		})
	case schemapb.DataType_Float16Vector:
		return buildByteVectorArrayFieldFromList(field, dim, vectors, start, end, int(dim)*2, func(data []byte) *schemapb.VectorField {
			return &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_Float16Vector{Float16Vector: data},
			}
		})
	case schemapb.DataType_BFloat16Vector:
		return buildByteVectorArrayFieldFromList(field, dim, vectors, start, end, int(dim)*2, func(data []byte) *schemapb.VectorField {
			return &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: data},
			}
		})
	case schemapb.DataType_Int8Vector:
		int8Values, ok := vectors.ListValues().(*array.Int8)
		if !ok {
			return nil, WrapTypeErr(field, vectors.ListValues().DataType().Name())
		}
		int8Data := make([]byte, 0, int(end-start)*int(dim))
		for vectorIndex := start; vectorIndex < end; vectorIndex++ {
			vecStart, vecEnd, err := vectorArrayValueOffsets(field, dim, vectors, vectorIndex, int(dim))
			if err != nil {
				return nil, err
			}
			for pos := vecStart; pos < vecEnd; pos++ {
				if int8Values.IsNull(int(pos)) {
					return nil, WrapNullElementErr(field)
				}
				int8Data = append(int8Data, byte(int8Values.Value(int(pos))))
			}
		}
		return &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_Int8Vector{Int8Vector: int8Data},
		}, nil
	case schemapb.DataType_SparseFloatVector:
		return nil, merr.WrapErrImportFailed("ArrayOfVector with SparseFloatVector element type is not implemented yet")
	default:
		return nil, merr.WrapErrImportFailedMsg("unsupported ArrayOfVector element type: %v", field.GetElementType())
	}
}

func vectorArrayValueOffsets(field *schemapb.FieldSchema, dim int64, vectors *array.List, vectorIndex int64, expected int) (int64, int64, error) {
	if vectors.IsNull(int(vectorIndex)) {
		return 0, 0, WrapNullElementErr(field)
	}
	start, end := vectors.ValueOffsets(int(vectorIndex))
	if int(end-start) != expected {
		return 0, 0, merr.WrapErrImportFailed(
			fmt.Sprintf("vector dimension mismatch for field '%s': position=%d, actual=%d, expected=%d",
				field.GetName(), vectorIndex, end-start, dim))
	}
	return start, end, nil
}

func buildByteVectorArrayFieldFromList(
	field *schemapb.FieldSchema,
	dim int64,
	vectors *array.List,
	start int64,
	end int64,
	expectedBytes int,
	build func([]byte) *schemapb.VectorField,
) (*schemapb.VectorField, error) {
	values, ok := vectors.ListValues().(*array.Uint8)
	if !ok {
		return nil, WrapTypeErr(field, vectors.ListValues().DataType().Name())
	}
	data := make([]byte, 0, int(end-start)*expectedBytes)
	for vectorIndex := start; vectorIndex < end; vectorIndex++ {
		vecStart, vecEnd, err := vectorArrayValueOffsets(field, dim, vectors, vectorIndex, expectedBytes)
		if err != nil {
			return nil, err
		}
		for pos := vecStart; pos < vecEnd; pos++ {
			if values.IsNull(int(pos)) {
				return nil, WrapNullElementErr(field)
			}
		}
		data = append(data, values.Uint8Values()[int(vecStart):int(vecEnd)]...)
	}
	return build(data), nil
}
