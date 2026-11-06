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

	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	pkgcommon "github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

// ValidateSchema rejects a NumPy import that cannot fill the collection: struct
// rows it cannot carry, a provided auto-id or function-output field, an
// unsupported type, or a required field with no file.
func ValidateSchema(schema *schemapb.CollectionSchema, paths []string) error {
	for _, structField := range schema.GetStructArrayFields() {
		if !structField.GetNullable() {
			return merr.WrapErrImportFailedMsg(
				"numpy import cannot fill struct field '%s', which is not nullable: numpy files cannot carry struct data; use JSON, CSV or Parquet files",
				structField.GetName())
		}
	}
	if err := checkStructFilePaths(schema, paths); err != nil {
		return err
	}

	sourced, all := resolveFieldPaths(schema, paths)
	nameToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) string {
		return field.GetName()
	})
	for name := range sourced {
		field := nameToField[name]

		// auto-id field must not provided
		if typeutil.IsAutoPKField(field) {
			return merr.WrapErrImportFailed(
				fmt.Sprintf("the primary key '%s' is auto-generated, no need to provide", field.GetName()))
		}
		// validate function output field
		if field.GetIsFunctionOutput() {
			if typeutil.IsBM25FunctionOutputField(field, schema) {
				return merr.WrapErrImportFailed(
					fmt.Sprintf("not allowed to provide data for BM25 function output field '%s'", field.GetName()))
			}
			if !pkgcommon.GetCollectionAllowInsertNonBM25FunctionOutputs(schema.GetProperties()) {
				return merr.WrapErrImportFailed(
					fmt.Sprintf("not allowed to provide data for function output field '%s', "+
						"set collection property '%s' to enable", field.GetName(), pkgcommon.CollectionAllowInsertNonBM25FunctionOutputs))
			}
		}
		// report an error if user provided the files for unsupported fields
		if err := checkUnsupportedDataType(field.GetDataType()); err != nil {
			return err
		}
	}

	// this loop is for "are there any fields not provided?"
	for _, field := range nameToField {
		// auto-id field, function output field already checked
		// dynamic field, nullable field, default value field, not provided or provided both ok
		if typeutil.IsAutoPKField(field) || field.GetIsDynamic() || field.GetIsFunctionOutput() ||
			field.GetNullable() || field.GetDefaultValue() != nil {
			continue
		}
		// report an error if the collection contains an unsupported field even user doesn't provide a numpy file for the field
		if err := checkUnsupportedDataType(field.GetDataType()); err != nil {
			return err
		}
		// the other field must be provided
		if _, ok := sourced[field.GetName()]; !ok {
			return merr.WrapErrImportFailed(
				fmt.Sprintf("no file for field: %s, files: %v", field.GetName(), lo.Values(all)))
		}
	}
	return nil
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
	timezone := common.GetSchemaTimezone(schema)
	for fieldID, r := range readers {
		cr, err := NewFieldReader(r, fields[fieldID], timezone)
		if err != nil {
			for _, opened := range readers {
				_ = opened.Close()
			}
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
	insertData, err := storage.NewInsertDataWithFunctionOutputField(r.schema)
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
	common.RemoveUnpopulatedFunctionOutputFields(r.schema, insertData)
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
