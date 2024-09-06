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
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/samber/lo"
	"golang.org/x/exp/constraints"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type FieldReader struct {
	columnIndex  int
	columnReader *pqarrow.ColumnReader

	dim   int
	field *schemapb.FieldSchema
}

func NewFieldReader(ctx context.Context, reader *pqarrow.FileReader, columnIndex int, field *schemapb.FieldSchema) (*FieldReader, error) {
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

	cr := &FieldReader{
		columnIndex:  columnIndex,
		columnReader: columnReader,
		dim:          int(dim),
		field:        field,
	}
	return cr, nil
}

func (c *FieldReader) Next(count int64) (any, any, error) {
	switch c.field.GetDataType() {
	case schemapb.DataType_Bool:
		if c.field.GetNullable() {
			return ReadNullableBoolData(c, count)
		}
		data, err := ReadBoolData(c, count)
		return data, nil, err
	case schemapb.DataType_Int8:
		if c.field.GetNullable() {
			return ReadNullableIntegerOrFloatData[int8](c, count)
		}
		data, err := ReadIntegerOrFloatData[int8](c, count)
		return data, nil, err
	case schemapb.DataType_Int16:
		if c.field.GetNullable() {
			return ReadNullableIntegerOrFloatData[int16](c, count)
		}
		data, err := ReadIntegerOrFloatData[int16](c, count)
		return data, nil, err
	case schemapb.DataType_Int32:
		if c.field.GetNullable() {
			return ReadNullableIntegerOrFloatData[int32](c, count)
		}
		data, err := ReadIntegerOrFloatData[int32](c, count)
		return data, nil, err
	case schemapb.DataType_Int64:
		if c.field.GetNullable() {
			return ReadNullableIntegerOrFloatData[int64](c, count)
		}
		data, err := ReadIntegerOrFloatData[int64](c, count)
		return data, nil, err
	case schemapb.DataType_Float:
		if c.field.GetNullable() {
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
		if c.field.GetNullable() {
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
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		if c.field.GetNullable() {
			return ReadNullableVarcharData(c, count)
		}
		data, err := ReadVarcharData(c, count)
		return data, nil, err
	case schemapb.DataType_JSON:
		if c.field.GetNullable() {
			return ReadNullableJSONData(c, count)
		}
		data, err := ReadJSONData(c, count)
		return data, nil, err
	case schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		if c.field.GetNullable() {
			return nil, nil, merr.WrapErrParameterInvalidMsg("not support nullable in vector")
		}
		data, err := ReadBinaryData(c, count)
		return data, nil, err
	case schemapb.DataType_FloatVector:
		if c.field.GetNullable() {
			return nil, nil, merr.WrapErrParameterInvalidMsg("not support nullable in vector")
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
			return nil, nil, merr.WrapErrParameterInvalidMsg("not support nullable in vector")
		}
		data, err := ReadSparseFloatVectorData(c, count)
		return data, nil, err
	case schemapb.DataType_Array:
		if c.field.GetNullable() {
			return ReadNullableArrayData(c, count)
		}
		data, err := ReadArrayData(c, count)
		return data, nil, err
	default:
		return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type '%s' for field '%s'",
			c.field.GetDataType().String(), c.field.GetName()))
	}
}

func (c *FieldReader) Close() {}

func ReadBoolData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		boolReader, ok := chunk.(*array.Boolean)
		if boolReader.NullN() > 0 {
			return nil, merr.WrapErrParameterInvalidMsg("not nullable, but has null value")
		}
		if !ok {
			return nil, WrapTypeErr("bool", chunk.DataType().Name(), pcr.field)
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
			return nil, nil, WrapTypeErr("bool", chunk.DataType().Name(), pcr.field)
		}
		validData = append(validData, bytesToBoolArray(dataNums, boolReader.NullBitmapBytes())...)

		for i := 0; i < dataNums; i++ {
			data = append(data, boolReader.Value(i))
		}
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrParameterInvalid(len(data), len(validData), "length of data is not equal to length of valid_data")
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
		switch chunk.DataType().ID() {
		case arrow.INT8:
			int8Reader := chunk.(*array.Int8)
			if int8Reader.NullN() > 0 {
				return nil, merr.WrapErrParameterInvalidMsg("not nullable, but has null value")
			}
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int8Reader.Value(i)))
			}
		case arrow.INT16:
			int16Reader := chunk.(*array.Int16)
			if int16Reader.NullN() > 0 {
				return nil, merr.WrapErrParameterInvalidMsg("not nullable, but has null value")
			}
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int16Reader.Value(i)))
			}
		case arrow.INT32:
			int32Reader := chunk.(*array.Int32)
			if int32Reader.NullN() > 0 {
				return nil, merr.WrapErrParameterInvalidMsg("not nullable, but has null value")
			}
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int32Reader.Value(i)))
			}
		case arrow.INT64:
			int64Reader := chunk.(*array.Int64)
			if int64Reader.NullN() > 0 {
				return nil, merr.WrapErrParameterInvalidMsg("not nullable, but has null value")
			}
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int64Reader.Value(i)))
			}
		case arrow.FLOAT32:
			float32Reader := chunk.(*array.Float32)
			if float32Reader.NullN() > 0 {
				return nil, merr.WrapErrParameterInvalidMsg("not nullable, but has null value")
			}
			for i := 0; i < dataNums; i++ {
				data = append(data, T(float32Reader.Value(i)))
			}
		case arrow.FLOAT64:
			float64Reader := chunk.(*array.Float64)
			if float64Reader.NullN() > 0 {
				return nil, merr.WrapErrParameterInvalidMsg("not nullable, but has null value")
			}
			for i := 0; i < dataNums; i++ {
				data = append(data, T(float64Reader.Value(i)))
			}
		default:
			return nil, WrapTypeErr("integer|float", chunk.DataType().Name(), pcr.field)
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
			validData = append(validData, bytesToBoolArray(dataNums, int8Reader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int8Reader.Value(i)))
			}
		case arrow.INT16:
			int16Reader := chunk.(*array.Int16)
			validData = append(validData, bytesToBoolArray(dataNums, int16Reader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int16Reader.Value(i)))
			}
		case arrow.INT32:
			int32Reader := chunk.(*array.Int32)
			validData = append(validData, bytesToBoolArray(dataNums, int32Reader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int32Reader.Value(i)))
			}
		case arrow.INT64:
			int64Reader := chunk.(*array.Int64)
			validData = append(validData, bytesToBoolArray(dataNums, int64Reader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(int64Reader.Value(i)))
			}
		case arrow.FLOAT32:
			float32Reader := chunk.(*array.Float32)
			validData = append(validData, bytesToBoolArray(dataNums, float32Reader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(float32Reader.Value(i)))
			}
		case arrow.FLOAT64:
			float64Reader := chunk.(*array.Float64)
			validData = append(validData, bytesToBoolArray(dataNums, float64Reader.NullBitmapBytes())...)
			for i := 0; i < dataNums; i++ {
				data = append(data, T(float64Reader.Value(i)))
			}
		default:
			return nil, nil, WrapTypeErr("integer|float", chunk.DataType().Name(), pcr.field)
		}
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrParameterInvalid(len(data), len(validData), "length of data is not equal to length of valid_data")
	}
	return data, validData, nil
}

func ReadStringData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]string, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		stringReader, ok := chunk.(*array.String)
		if stringReader.NullN() > 0 {
			return nil, merr.WrapErrParameterInvalidMsg("not nullable, but has null value")
		}
		if !ok {
			return nil, WrapTypeErr("string", chunk.DataType().Name(), pcr.field)
		}
		for i := 0; i < dataNums; i++ {
			data = append(data, stringReader.Value(i))
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadNullableStringData(pcr *FieldReader, count int64) (any, []bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([]string, 0, count)
	validData := make([]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		stringReader, ok := chunk.(*array.String)
		if !ok {
			return nil, nil, WrapTypeErr("string", chunk.DataType().Name(), pcr.field)
		}
		validData = append(validData, bytesToBoolArray(dataNums, stringReader.NullBitmapBytes())...)
		for i := 0; i < dataNums; i++ {
			if stringReader.IsNull(i) {
				data = append(data, "")
				continue
			}
			data = append(data, stringReader.ValueStr(i))
		}
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrParameterInvalid(len(data), len(validData), "length of data is not equal to length of valid_data")
	}
	return data, validData, nil
}

func ReadVarcharData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]string, 0, count)
	maxLength, err := parameterutil.GetMaxLength(pcr.field)
	if err != nil {
		return nil, err
	}
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		stringReader, ok := chunk.(*array.String)
		if stringReader.NullN() > 0 {
			return nil, merr.WrapErrParameterInvalidMsg("not nullable, but has null value")
		}
		if !ok {
			return nil, WrapTypeErr("string", chunk.DataType().Name(), pcr.field)
		}
		for i := 0; i < dataNums; i++ {
			if err = common.CheckVarcharLength(stringReader.Value(i), maxLength); err != nil {
				return nil, err
			}
			data = append(data, stringReader.Value(i))
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadNullableVarcharData(pcr *FieldReader, count int64) (any, []bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([]string, 0, count)
	maxLength, err := parameterutil.GetMaxLength(pcr.field)
	if err != nil {
		return nil, nil, err
	}
	validData := make([]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		stringReader, ok := chunk.(*array.String)
		if !ok {
			return nil, nil, WrapTypeErr("string", chunk.DataType().Name(), pcr.field)
		}
		validData = append(validData, bytesToBoolArray(dataNums, stringReader.NullBitmapBytes())...)
		for i := 0; i < dataNums; i++ {
			if stringReader.IsNull(i) {
				data = append(data, "")
				continue
			}
			if err = common.CheckVarcharLength(stringReader.Value(i), maxLength); err != nil {
				return nil, nil, err
			}
			data = append(data, stringReader.ValueStr(i))
		}
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrParameterInvalid(len(data), len(validData), "length of data is not equal to length of valid_data")
	}
	return data, validData, nil
}

func ReadJSONData(pcr *FieldReader, count int64) (any, error) {
	// JSON field read data from string array Parquet
	data, err := ReadStringData(pcr, count)
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
	data, validData, err := ReadNullableStringData(pcr, count)
	if err != nil {
		return nil, nil, err
	}
	if data == nil {
		return nil, nil, nil
	}
	byteArr := make([][]byte, 0)
	for i, str := range data.([]string) {
		if !validData[i] {
			byteArr = append(byteArr, []byte(nil))
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
		case arrow.LIST:
			listReader := chunk.(*array.List)
			if err = checkVectorAligned(listReader.Offsets(), pcr.dim, dataType); err != nil {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("length of vector is not aligned: %s, data type: %s", err.Error(), dataType.String()))
			}
			uint8Reader, ok := listReader.ListValues().(*array.Uint8)
			if !ok {
				return nil, WrapTypeErr("binary", listReader.ListValues().DataType().Name(), pcr.field)
			}
			data = append(data, uint8Reader.Uint8Values()...)
		default:
			return nil, WrapTypeErr("binary", chunk.DataType().Name(), pcr.field)
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadSparseFloatVectorData(pcr *FieldReader, count int64) (any, error) {
	data, err := ReadStringData(pcr, count)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	byteArr := make([][]byte, 0, count)
	maxDim := uint32(0)
	for _, str := range data.([]string) {
		rowVec, err := typeutil.CreateSparseFloatRowFromJSON([]byte(str))
		if err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("Invalid JSON string for SparseFloatVector: '%s', err = %v", str, err))
		}
		byteArr = append(byteArr, rowVec)
		elemCount := len(rowVec) / 8
		maxIdx := typeutil.SparseFloatRowIndexAt(rowVec, elemCount-1)
		if maxIdx+1 > maxDim {
			maxDim = maxIdx + 1
		}
	}
	return &storage.SparseFloatVectorFieldData{
		SparseFloatArray: schemapb.SparseFloatArray{
			Dim:      int64(maxDim),
			Contents: byteArr,
		},
	}, nil
}

func checkVectorAlignWithDim(offsets []int32, dim int32) error {
	for i := 1; i < len(offsets); i++ {
		if offsets[i]-offsets[i-1] != dim {
			return fmt.Errorf("expected %d but got %d", dim, offsets[i]-offsets[i-1])
		}
	}
	return nil
}

func checkVectorAligned(offsets []int32, dim int, dataType schemapb.DataType) error {
	if len(offsets) < 1 {
		return fmt.Errorf("empty offsets")
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
	default:
		return fmt.Errorf("unexpected vector data type %s", dataType.String())
	}
}

func ReadBoolArrayData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([][]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		listReader, ok := chunk.(*array.List)
		if !ok {
			return nil, WrapTypeErr("list", chunk.DataType().Name(), pcr.field)
		}
		boolReader, ok := listReader.ListValues().(*array.Boolean)
		if !ok {
			return nil, WrapTypeErr("boolArray", chunk.DataType().Name(), pcr.field)
		}
		offsets := listReader.Offsets()
		for i := 1; i < len(offsets); i++ {
			start, end := offsets[i-1], offsets[i]
			elementData := make([]bool, 0, end-start)
			for j := start; j < end; j++ {
				elementData = append(elementData, boolReader.Value(int(j)))
			}
			data = append(data, elementData)
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
		listReader, ok := chunk.(*array.List)
		if !ok {
			return nil, nil, WrapTypeErr("list", chunk.DataType().Name(), pcr.field)
		}
		boolReader, ok := listReader.ListValues().(*array.Boolean)
		if !ok {
			return nil, nil, WrapTypeErr("boolArray", chunk.DataType().Name(), pcr.field)
		}
		offsets := listReader.Offsets()
		for i := 1; i < len(offsets); i++ {
			start, end := offsets[i-1], offsets[i]
			elementData := make([]bool, 0, end-start)
			for j := start; j < end; j++ {
				elementData = append(elementData, boolReader.Value(int(j)))
			}
			data = append(data, elementData)
			elementDataValid := true
			if start == end {
				elementDataValid = false
			}
			validData = append(validData, elementDataValid)
		}
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrParameterInvalid(len(data), len(validData), "length of data is not equal to length of valid_data")
	}
	return data, validData, nil
}

func ReadIntegerOrFloatArrayData[T constraints.Integer | constraints.Float](pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([][]T, 0, count)

	getDataFunc := func(offsets []int32, getValue func(int) T) {
		for i := 1; i < len(offsets); i++ {
			start, end := offsets[i-1], offsets[i]
			elementData := make([]T, 0, end-start)
			for j := start; j < end; j++ {
				elementData = append(elementData, getValue(int(j)))
			}
			data = append(data, elementData)
		}
	}
	for _, chunk := range chunked.Chunks() {
		listReader, ok := chunk.(*array.List)
		if !ok {
			return nil, WrapTypeErr("list", chunk.DataType().Name(), pcr.field)
		}
		offsets := listReader.Offsets()
		dataType := pcr.field.GetDataType()
		if typeutil.IsVectorType(dataType) {
			if err = checkVectorAligned(offsets, pcr.dim, dataType); err != nil {
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("length of vector is not aligned: %s, data type: %s", err.Error(), dataType.String()))
			}
		}
		valueReader := listReader.ListValues()
		switch valueReader.DataType().ID() {
		case arrow.INT8:
			int8Reader := valueReader.(*array.Int8)
			getDataFunc(offsets, func(i int) T {
				return T(int8Reader.Value(i))
			})
		case arrow.INT16:
			int16Reader := valueReader.(*array.Int16)
			getDataFunc(offsets, func(i int) T {
				return T(int16Reader.Value(i))
			})
		case arrow.INT32:
			int32Reader := valueReader.(*array.Int32)
			getDataFunc(offsets, func(i int) T {
				return T(int32Reader.Value(i))
			})
		case arrow.INT64:
			int64Reader := valueReader.(*array.Int64)
			getDataFunc(offsets, func(i int) T {
				return T(int64Reader.Value(i))
			})
		case arrow.FLOAT32:
			float32Reader := valueReader.(*array.Float32)
			getDataFunc(offsets, func(i int) T {
				return T(float32Reader.Value(i))
			})
		case arrow.FLOAT64:
			float64Reader := valueReader.(*array.Float64)
			getDataFunc(offsets, func(i int) T {
				return T(float64Reader.Value(i))
			})
		default:
			return nil, WrapTypeErr("integerArray|floatArray", chunk.DataType().Name(), pcr.field)
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

	getDataFunc := func(offsets []int32, getValue func(int) T) {
		for i := 1; i < len(offsets); i++ {
			start, end := offsets[i-1], offsets[i]
			elementData := make([]T, 0, end-start)
			for j := start; j < end; j++ {
				elementData = append(elementData, getValue(int(j)))
			}
			data = append(data, elementData)
			elementDataValid := true
			if start == end {
				elementDataValid = false
			}
			validData = append(validData, elementDataValid)
		}
	}
	for _, chunk := range chunked.Chunks() {
		listReader, ok := chunk.(*array.List)
		if !ok {
			return nil, nil, WrapTypeErr("list", chunk.DataType().Name(), pcr.field)
		}
		offsets := listReader.Offsets()
		dataType := pcr.field.GetDataType()
		if typeutil.IsVectorType(dataType) {
			if err = checkVectorAligned(offsets, pcr.dim, dataType); err != nil {
				return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("length of vector is not aligned: %s, data type: %s", err.Error(), dataType.String()))
			}
		}
		valueReader := listReader.ListValues()
		switch valueReader.DataType().ID() {
		case arrow.INT8:
			int8Reader := valueReader.(*array.Int8)
			getDataFunc(offsets, func(i int) T {
				return T(int8Reader.Value(i))
			})
		case arrow.INT16:
			int16Reader := valueReader.(*array.Int16)
			getDataFunc(offsets, func(i int) T {
				return T(int16Reader.Value(i))
			})
		case arrow.INT32:
			int32Reader := valueReader.(*array.Int32)
			getDataFunc(offsets, func(i int) T {
				return T(int32Reader.Value(i))
			})
		case arrow.INT64:
			int64Reader := valueReader.(*array.Int64)
			getDataFunc(offsets, func(i int) T {
				return T(int64Reader.Value(i))
			})
		case arrow.FLOAT32:
			float32Reader := valueReader.(*array.Float32)
			getDataFunc(offsets, func(i int) T {
				return T(float32Reader.Value(i))
			})
		case arrow.FLOAT64:
			float64Reader := valueReader.(*array.Float64)
			getDataFunc(offsets, func(i int) T {
				return T(float64Reader.Value(i))
			})
		default:
			return nil, nil, WrapTypeErr("integerArray|floatArray", chunk.DataType().Name(), pcr.field)
		}
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrParameterInvalid(len(data), len(validData), "length of data is not equal to length of valid_data")
	}
	return data, validData, nil
}

func ReadStringArrayData(pcr *FieldReader, count int64) (any, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([][]string, 0, count)
	for _, chunk := range chunked.Chunks() {
		listReader, ok := chunk.(*array.List)
		if !ok {
			return nil, WrapTypeErr("list", chunk.DataType().Name(), pcr.field)
		}
		stringReader, ok := listReader.ListValues().(*array.String)
		if !ok {
			return nil, WrapTypeErr("stringArray", chunk.DataType().Name(), pcr.field)
		}
		offsets := listReader.Offsets()
		for i := 1; i < len(offsets); i++ {
			start, end := offsets[i-1], offsets[i]
			elementData := make([]string, 0, end-start)
			for j := start; j < end; j++ {
				elementData = append(elementData, stringReader.Value(int(j)))
			}
			data = append(data, elementData)
		}
	}
	if len(data) == 0 {
		return nil, nil
	}
	return data, nil
}

func ReadNullableStringArrayData(pcr *FieldReader, count int64) (any, []bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, nil, err
	}
	data := make([][]string, 0, count)
	validData := make([]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		listReader, ok := chunk.(*array.List)
		if !ok {
			return nil, nil, WrapTypeErr("list", chunk.DataType().Name(), pcr.field)
		}
		stringReader, ok := listReader.ListValues().(*array.String)
		if !ok {
			return nil, nil, WrapTypeErr("stringArray", chunk.DataType().Name(), pcr.field)
		}
		offsets := listReader.Offsets()
		for i := 1; i < len(offsets); i++ {
			start, end := offsets[i-1], offsets[i]
			elementData := make([]string, 0, end-start)
			for j := start; j < end; j++ {
				elementData = append(elementData, stringReader.Value(int(j)))
			}
			data = append(data, elementData)
			elementDataValid := true
			if start == end {
				elementDataValid = false
			}
			validData = append(validData, elementDataValid)
		}
	}
	if len(data) == 0 {
		return nil, nil, nil
	}
	if len(data) != len(validData) {
		return nil, nil, merr.WrapErrParameterInvalid(len(data), len(validData), "length of data is not equal to length of valid_data")
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		stringArray, err := ReadStringArrayData(pcr, count)
		if err != nil {
			return nil, err
		}
		if stringArray == nil {
			return nil, nil
		}
		for _, elementArray := range stringArray.([][]string) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type '%s' for array field '%s'",
			elementType.String(), pcr.field.GetName()))
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		stringArray, validData, err := ReadNullableStringArrayData(pcr, count)
		if err != nil {
			return nil, nil, err
		}
		if stringArray == nil {
			return nil, nil, nil
		}
		for _, elementArray := range stringArray.([][]string) {
			if err = common.CheckArrayCapacity(len(elementArray), maxCapacity); err != nil {
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
		return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("unsupported data type '%s' for array field '%s'",
			elementType.String(), pcr.field.GetName()))
	}
}
