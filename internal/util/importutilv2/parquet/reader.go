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
	"io"

	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type reader struct {
	ctx    context.Context
	cm     storage.ChunkManager
	schema *schemapb.CollectionSchema

	path string
	r    *file.Reader

	fileSize   *atomic.Int64
	bufferSize int
	count      int64

	frs map[int64]*FieldReader // fieldID -> FieldReader
}

func NewReader(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema, path string, bufferSize int) (*reader, error) {
	cmReader, err := cm.Reader(ctx, path)
	if err != nil {
		return nil, err
	}
	r, err := file.NewParquetReader(cmReader, file.WithReadProps(&parquet.ReaderProperties{
		BufferSize:            int64(bufferSize),
		BufferedStreamEnabled: true,
	}))
	if err != nil {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("new parquet reader failed, err=%v", err))
	}
	log.Info("create parquet reader done", zap.Int("row group num", r.NumRowGroups()),
		zap.Int64("num rows", r.NumRows()))

	fileReader, err := pqarrow.NewFileReader(r, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("new parquet file reader failed, err=%v", err))
	}

	crs, err := CreateFieldReaders(ctx, fileReader, schema)
	if err != nil {
		return nil, err
	}
	count, err := common.EstimateReadCountPerBatch(bufferSize, schema)
	if err != nil {
		return nil, err
	}
	return &reader{
		ctx:        ctx,
		cm:         cm,
		schema:     schema,
		fileSize:   atomic.NewInt64(0),
		path:       path,
		r:          r,
		bufferSize: bufferSize,
		count:      count,
		frs:        crs,
	}, nil
}

func (r *reader) Read() (*storage.InsertData, error) {
	insertData, err := storage.NewInsertData(r.schema)
	if err != nil {
		return nil, err
	}
OUTER:
	for {
		for fieldID, cr := range r.frs {
			data, validData, err := cr.Next(r.count)
			if err != nil {
				return nil, err
			}
			if data == nil {
				break OUTER
			}
			err = insertData.Data[fieldID].AppendRows(data, validData)
			if err != nil {
				return nil, err
			}
		}
		if insertData.GetMemorySize() >= r.bufferSize {
			break
		}
	}
	for fieldID := range r.frs {
		if insertData.Data[fieldID].RowNum() == 0 {
			return nil, io.EOF
		}
	}
	err = common.FillDynamicData(insertData, r.schema)
	if err != nil {
		return nil, err
	}
	return insertData, nil
}

func (r *reader) Size() (int64, error) {
	if size := r.fileSize.Load(); size != 0 {
		return size, nil
	}
	size, err := r.cm.Size(r.ctx, r.path)
	if err != nil {
		return 0, err
	}
	r.fileSize.Store(size)
	return size, nil
}

func (r *reader) Close() {
	for _, cr := range r.frs {
		cr.Close()
	}
	err := r.r.Close()
	if err != nil {
		log.Warn("close parquet reader failed", zap.Error(err))
	}
}
