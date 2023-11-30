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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
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

func ReadData[T any](pcr *ParquetColumnReader, count int64, getDataFunc func(chunk arrow.Array) ([]T, error)) ([]T, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	data := make([]T, 0, count)
	for _, chunk := range chunked.Chunks() {
		chunkData, err := getDataFunc(chunk)
		if err != nil {
			return nil, err
		}
		data = append(data, chunkData...)
	}
	return data, nil
}

func ReadArrayData[T any](pcr *ParquetColumnReader, count int64, getArrayData func(offsets []int32, array arrow.Array) ([][]T, error)) ([][]T, error) {
	chunked, err := pcr.columnReader.NextBatch(count)
	if err != nil {
		return nil, err
	}
	arrayData := make([][]T, 0, count)
	for _, chunk := range chunked.Chunks() {
		listReader, ok := chunk.(*array.List)
		if !ok {
			log.Warn("the column data in parquet is not array", zap.String("fieldName", pcr.fieldName))
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("the column data in parquet is not array of field: %s", pcr.fieldName))
		}
		offsets := listReader.Offsets()
		chunkData, err := getArrayData(offsets, listReader.ListValues())
		if err != nil {
			return nil, err
		}
		arrayData = append(arrayData, chunkData...)
	}
	return arrayData, nil
}
