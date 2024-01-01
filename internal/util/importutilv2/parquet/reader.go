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
	"fmt"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

type Reader struct {
	schema     *schemapb.CollectionSchema
	fileReader *pqarrow.FileReader
	crs        map[int64]*ColumnReader // fieldID -> ColumnReader
}

func NewReader(schema *schemapb.CollectionSchema, cmReader storage.FileReader) (*Reader, error) {
	reader, err := file.NewParquetReader(cmReader, file.WithReadProps(&parquet.ReaderProperties{
		BufferSize:            32 * 1024 * 1024, // TODO: dyh, make if configurable
		BufferedStreamEnabled: true,
	}))
	if err != nil {
		return nil, err
	}
	log.Info("create file reader done", zap.Int("row group num", reader.NumRowGroups()),
		zap.Int64("num rows", reader.NumRows()))

	fileReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		log.Warn("create arrow parquet file reader failed", zap.Error(err))
		return nil, err
	}

	crs, err := CreateColumnReaders(fileReader, schema)
	if err != nil {
		return nil, err
	}
	return &Reader{
		schema:     schema,
		fileReader: fileReader,
		crs:        crs,
	}, nil
}

func (r *Reader) Next(count int64) (*storage.InsertData, error) {
	insertData, err := storage.NewInsertData(r.schema)
	if err != nil {
		return nil, err
	}
	for fieldID, cr := range r.crs {
		fieldData, err := cr.Next(count)
		if err != nil {
			return nil, err
		}
		insertData.Data[fieldID] = fieldData
		fmt.Println("dyh debug, ColumnReader Next", ", rowNum:", fieldData.RowNum(), ", count:", count)
	}
	fmt.Println("dyh debug, Reader Next", ", rowNum:", insertData)
	return insertData, nil
}

func (r *Reader) Close() {
	for _, cr := range r.crs {
		cr.Close()
	}
}
