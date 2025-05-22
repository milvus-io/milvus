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
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
	for _, fieldSchema := range schema.Fields {
		if fieldSchema.GetNullable() {
			return nil, merr.WrapErrParameterInvalidMsg(fmt.Sprintf("not support bulk insert numpy files in field(%s) which set nullable == true", fieldSchema.GetName()))
		}
	}
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
		var data any
		data, err = cr.Next(r.count)
		if err != nil {
			return nil, err
		}
		if data == nil {
			return nil, io.EOF
		}
		err = insertData.Data[fieldID].AppendRows(data, nil)
		if err != nil {
			return nil, err
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

func CreateReaders(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema, paths []string) (map[int64]storage.FileReader, error) {
	readers := make(map[int64]storage.FileReader)
	nameToPath := lo.SliceToMap(paths, func(path string) (string, string) {
		nameWithExt := filepath.Base(path)
		name := strings.TrimSuffix(nameWithExt, filepath.Ext(nameWithExt))
		return name, path
	})
	for _, field := range schema.GetFields() {
		path, hasPath := nameToPath[field.GetName()]
		if field.GetIsPrimaryKey() && field.GetAutoID() {
			if hasPath {
				return nil, merr.WrapErrImportFailed(
					fmt.Sprintf("the primary key '%s' is auto-generated, no need to provide", field.GetName()))
			}
			continue
		}
		if field.GetIsFunctionOutput() {
			if hasPath {
				return nil, merr.WrapErrImportFailed(
					fmt.Sprintf("field %s is Function output, should not be provided. Provided files: %v", field.GetName(), lo.Values(nameToPath)))
			}
			continue
		}
		if !hasPath {
			if field.GetIsDynamic() {
				continue
			}
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("no file for field: %s, files: %v", field.GetName(), lo.Values(nameToPath)))
		}
		reader, err := cm.Reader(ctx, path)
		if err != nil {
			return nil, merr.WrapErrImportFailed(
				fmt.Sprintf("failed to read the file '%s', error: %s", path, err.Error()))
		}
		readers[field.GetFieldID()] = reader
	}
	return readers, nil
}
