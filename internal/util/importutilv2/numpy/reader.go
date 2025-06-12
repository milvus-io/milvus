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

package numpy

import (
	"context"
	"io"

	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
)

type reader struct {
	ctx    context.Context
	cm     storage.ChunkManager
	cmrs   map[int64]storage.FileReader
	schema *schemapb.CollectionSchema

	fileSize *atomic.Int64
	paths    []string

	count int64
	frs   map[int64]*FieldReader // fieldID -> FieldReader
}

func NewReader(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema, paths []string, bufferSize int) (*reader, error) {
	fields := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	count, err := common.EstimateReadCountPerBatch(bufferSize, schema)
	if err != nil {
		return nil, err
	}
	crs := make(map[int64]*FieldReader)
	readers, err := CreateReaders(ctx, cm, schema, paths)
	if err != nil {
		return nil, err
	}

	for fieldID, r := range readers {
		cr, err := NewFieldReader(r, fields[fieldID])
		if err != nil {
			return nil, err
		}
		crs[fieldID] = cr
	}

	return &reader{
		ctx:      ctx,
		cm:       cm,
		cmrs:     readers,
		schema:   schema,
		fileSize: atomic.NewInt64(0),
		paths:    paths,
		count:    count,
		frs:      crs,
	}, nil
}

func (r *reader) Read() (*storage.InsertData, error) {
	insertData, err := storage.NewInsertData(r.schema)
	if err != nil {
		return nil, err
	}
	for fieldID, cr := range r.frs {
		data, validData, err := cr.Next(r.count)
		if err != nil {
			return nil, err
		}
		if data == nil {
			return nil, io.EOF
		}
		err = insertData.Data[fieldID].AppendRows(data, validData)
		if err != nil {
			return nil, err
		}
	}
	return insertData, nil
}

func (r *reader) Size() (int64, error) {
	if size := r.fileSize.Load(); size != 0 {
		return size, nil
	}
	size, err := storage.GetFilesSize(r.ctx, r.paths, r.cm)
	if err != nil {
		return 0, err
	}
	r.fileSize.Store(size)
	return size, nil
}

func (r *reader) Close() {
	for _, cmr := range r.cmrs {
		cmr.Close()
	}
}
