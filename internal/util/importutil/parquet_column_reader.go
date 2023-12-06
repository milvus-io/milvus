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

package importutil

import (
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ParquetColumnReader struct {
	fieldName   string
	fieldID     int64
	columnIndex int
	// columnSchema  *parquet.SchemaElement
	dataType     schemapb.DataType
	elementType  schemapb.DataType
	columnReader *pqarrow.ColumnReader
	dimension    int
}

func ReadBoolData(pcr *ParquetColumnReader, count int64) ([]bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		chunkData := make([]bool, dataNums)
		boolReader, ok := chunk.(*array.Boolean)
		if !ok {
			log.Warn("the column data in parquet is not bool", zap.String("fieldName", pcr.fieldName), zap.String("actual type", chunk.DataType().Name()))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not bool of field: %s, but: %s", pcr.fieldName, chunk.DataType().Name()))
		}
		for i := 0; i < dataNums; i++ {
			chunkData[i] = boolReader.Value(i)
		}
		data = append(data, chunkData...)
	}
	return data, nil
}

func ReadIntegerOrFloatData[T constraints.Integer | constraints.Float](pcr *ParquetColumnReader, count int64) ([]T, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]T, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		chunkData := make([]T, dataNums)
		switch chunk.DataType().ID() {
		case arrow.INT8:
			int8Reader := chunk.(*array.Int8)
			for i := 0; i < dataNums; i++ {
				chunkData[i] = T(int8Reader.Value(i))
			}
		case arrow.INT16:
			int16Reader := chunk.(*array.Int16)
			for i := 0; i < dataNums; i++ {
				chunkData[i] = T(int16Reader.Value(i))
			}
		case arrow.INT32:
			int32Reader := chunk.(*array.Int32)
			for i := 0; i < dataNums; i++ {
				chunkData[i] = T(int32Reader.Value(i))
			}
		case arrow.INT64:
			int64Reader := chunk.(*array.Int64)
			for i := 0; i < dataNums; i++ {
				chunkData[i] = T(int64Reader.Value(i))
			}
		case arrow.FLOAT32:
			float32Reader := chunk.(*array.Float32)
			for i := 0; i < dataNums; i++ {
				chunkData[i] = T(float32Reader.Value(i))
			}
		case arrow.FLOAT64:
			float64Reader := chunk.(*array.Float64)
			for i := 0; i < dataNums; i++ {
				chunkData[i] = T(float64Reader.Value(i))
			}
		default:
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data type is not integer, neither float, but: %s", chunk.DataType().Name()))
		}
		data = append(data, chunkData...)
	}
	return data, nil
}

func ReadStringData(pcr *ParquetColumnReader, count int64) ([]string, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]string, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		chunkData := make([]string, dataNums)
		stringReader, ok := chunk.(*array.String)
		if !ok {
			log.Warn("the column data in parquet is not string", zap.String("fieldName", pcr.fieldName), zap.String("actual type", chunk.DataType().Name()))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not string of field: %s, but: %s", pcr.fieldName, chunk.DataType().Name()))
		}
		for i := 0; i < dataNums; i++ {
			chunkData[i] = stringReader.Value(i)
		}
		data = append(data, chunkData...)
	}
	return data, nil
}

func ReadBinaryData(pcr *ParquetColumnReader, count int64) ([]byte, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0, count)
	for _, chunk := range chunked.Chunks() {
		dataNums := chunk.Data().Len()
		switch chunk.DataType().ID() {
		case arrow.BINARY:
			binaryReader := chunk.(*array.Binary)
			for i := 0; i < dataNums; i++ {
				data = append(data, binaryReader.Value(i)...)
			}
		case arrow.LIST:
			listReader := chunk.(*array.List)
			if !checkVectorIsRegular(listReader.Offsets(), pcr.dimension, true) {
				log.Warn("Parquet parser: binary vector is irregular", zap.Int("dim", pcr.dimension), zap.Int32s("offsets", listReader.Offsets()))
				return nil, merr.WrapErrImportFailed("binary vector is irregular")
			}
			uint8Reader, ok := listReader.ListValues().(*array.Uint8)
			if !ok {
				log.Warn("the column element data of array in parquet is not binary", zap.String("fieldName", pcr.fieldName))
				return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not binary: %s", pcr.fieldName))
			}
			for i := 0; i < uint8Reader.Len(); i++ {
				data = append(data, uint8Reader.Value(i))
			}
		default:
			log.Warn("the column element data of array in parquet is not binary", zap.String("fieldName", pcr.fieldName), zap.String("actual data type", chunk.DataType().Name()))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column element data of array in parquet is not binary: %s, it's: %s", pcr.fieldName, chunk.DataType().Name()))
		}
	}
	return data, nil
}

func checkVectorIsRegular(offsets []int32, dim int, isBinary bool) bool {
	if len(offsets) < 1 {
		return false
	}
	if isBinary {
		dim = dim / 8
	}
	start := offsets[0]
	for i := 1; i < len(offsets); i++ {
		if offsets[i]-start != int32(dim) {
			return false
		}
		start = offsets[i]
	}
	return true
}

func ReadBoolArrayData(pcr *ParquetColumnReader, count int64) ([][]bool, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([][]bool, 0, count)
	for _, chunk := range chunked.Chunks() {
		listReader, ok := chunk.(*array.List)
		if !ok {
			log.Warn("the column data in parquet is not list", zap.String("fieldName", pcr.fieldName), zap.String("actual type", chunk.DataType().Name()))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not list of field: %s, but: %s", pcr.fieldName, chunk.DataType().Name()))
		}
		boolReader, ok := listReader.ListValues().(*array.Boolean)
		if !ok {
			log.Warn("the column data in parquet is not bool array", zap.String("fieldName", pcr.fieldName),
				zap.String("actual type", listReader.ListValues().DataType().Name()))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not bool array of field: %s， but: %s list", pcr.fieldName, listReader.ListValues().DataType().Name()))
		}
		offsets := listReader.Offsets()
		for i := 1; i < len(offsets); i++ {
			start, end := offsets[i-1], offsets[i]
			elementData := make([]bool, 0)
			for j := start; j < end; j++ {
				elementData = append(elementData, boolReader.Value(int(j)))
			}
			data = append(data, elementData)
		}
	}
	return data, nil
}

func ReadIntegerOrFloatArrayData[T constraints.Integer | constraints.Float](pcr *ParquetColumnReader, count int64) ([][]T, error) {
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
			log.Warn("the column data in parquet is not list", zap.String("fieldName", pcr.fieldName), zap.String("actual type", chunk.DataType().Name()))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not list of field: %s, but: %s", pcr.fieldName, chunk.DataType().Name()))
		}
		offsets := listReader.Offsets()
		if typeutil.IsVectorType(pcr.dataType) && !checkVectorIsRegular(offsets, pcr.dimension, pcr.dataType == schemapb.DataType_BinaryVector) {
			log.Warn("Parquet parser: float vector is irregular", zap.Int("dim", pcr.dimension), zap.Int32s("offsets", listReader.Offsets()))
			return nil, merr.WrapErrImportFailed("float vector is irregular")
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
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data type is not integer array, neither float array, but: %s", valueReader.DataType().Name()))
		}
	}
	return data, nil
}

func ReadStringArrayData(pcr *ParquetColumnReader, count int64) ([][]string, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([][]string, 0, count)
	for _, chunk := range chunked.Chunks() {
		listReader, ok := chunk.(*array.List)
		if !ok {
			log.Warn("the column data in parquet is not list", zap.String("fieldName", pcr.fieldName), zap.String("actual type", chunk.DataType().Name()))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not list of field: %s, but: %s", pcr.fieldName, chunk.DataType().Name()))
		}
		stringReader, ok := listReader.ListValues().(*array.String)
		if !ok {
			log.Warn("the column data in parquet is not string array", zap.String("fieldName", pcr.fieldName),
				zap.String("actual type", listReader.ListValues().DataType().Name()))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not string array of field: %s， but: %s list", pcr.fieldName, listReader.ListValues().DataType().Name()))
		}
		offsets := listReader.Offsets()
		for i := 1; i < len(offsets); i++ {
			start, end := offsets[i-1], offsets[i]
			elementData := make([]string, 0)
			for j := start; j < end; j++ {
				elementData = append(elementData, stringReader.Value(int(j)))
			}
			data = append(data, elementData)
		}
	}
	return data, nil
}
