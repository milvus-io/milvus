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

package binlog

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type reader struct {
	ctx            context.Context
	cm             storage.ChunkManager
	storageConfig  *indexpb.StorageConfig
	schema         *schemapb.CollectionSchema
	storageVersion int64
	importEz       string

	fileSize   *atomic.Int64
	bufferSize int
	deleteData map[any]typeutil.Timestamp // pk2ts
	insertLogs map[int64][]string         // fieldID (or fieldGroupID if storage v2) -> binlogs

	filters []Filter
	dr      storage.DeserializeReader[*storage.Value]
}

func NewReader(ctx context.Context,
	cm storage.ChunkManager,
	schema *schemapb.CollectionSchema,
	storageConfig *indexpb.StorageConfig,
	storageVersion int64,
	paths []string,
	tsStart,
	tsEnd uint64,
	bufferSize int,
	importEz string,
) (*reader, error) {
	systemFieldsAbsent := true
	for _, field := range schema.Fields {
		if field.GetFieldID() < 100 {
			systemFieldsAbsent = false
			break
		}
	}
	if systemFieldsAbsent {
		schema = typeutil.AppendSystemFields(schema)
	}
	r := &reader{
		ctx:            ctx,
		cm:             cm,
		schema:         schema,
		storageVersion: storageVersion,
		fileSize:       atomic.NewInt64(0),
		bufferSize:     bufferSize,
		storageConfig:  storageConfig,
		importEz:       importEz,
	}
	err := r.init(paths, tsStart, tsEnd)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *reader) init(paths []string, tsStart, tsEnd uint64) error {
	if tsStart != 0 || tsEnd != math.MaxUint64 {
		r.filters = append(r.filters, FilterWithTimeRange(tsStart, tsEnd))
	}
	if len(paths) == 0 {
		return merr.WrapErrImportFailed("no insert binlogs to import")
	}
	// the "paths" has one or two paths, the first is the binlog path of a segment
	// the other is optional, is the delta path of a segment
	if len(paths) > 2 {
		return merr.WrapErrImportFailed(fmt.Sprintf("too many input paths for binlog import. "+
			"Valid paths length should be one or two, but got paths:%s", paths))
	}
	insertLogs, err := listInsertLogs(r.ctx, r.cm, paths[0])
	if err != nil {
		return err
	}

	validInsertLogs, cloneschema, err := verify(r.schema, r.storageVersion, insertLogs)
	if err != nil {
		return err
	}
	binlogs := createFieldBinlogList(validInsertLogs)
	r.insertLogs = validInsertLogs
	r.schema = cloneschema

	validIDs := lo.Keys(r.insertLogs)
	log.Info("create binlog reader for these fields", zap.Any("validIDs", validIDs))

	rwOptions := []storage.RwOption{
		storage.WithVersion(r.storageVersion),
		storage.WithBufferSize(32 * 1024 * 1024),
		storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
			return r.cm.MultiRead(ctx, paths)
		}),
		storage.WithStorageConfig(r.storageConfig),
	}

	if len(r.importEz) > 0 {
		ezID, err := hookutil.GetEzIDByImportEzk(r.importEz)
		if err != nil {
			return err
		}
		pluginContext, err := hookutil.GetCPluginContextByEzID(ezID)
		if err != nil {
			return err
		}
		rwOptions = append(rwOptions, storage.WithPluginContext(pluginContext))
	}

	rr, err := storage.NewBinlogRecordReader(r.ctx, binlogs, r.schema, rwOptions...)
	if err != nil {
		return err
	}

	r.dr = storage.NewDeserializeReader(rr, func(record storage.Record, v []*storage.Value) error {
		return storage.ValueDeserializerWithSchema(record, v, r.schema, true)
	})

	if len(paths) < 2 {
		return nil
	}
	deltaLogs, _, err := storage.ListAllChunkWithPrefix(context.Background(), r.cm, paths[1], true)
	if err != nil {
		return err
	}
	if len(deltaLogs) == 0 {
		return nil
	}
	r.deleteData, err = r.readDelete(deltaLogs, tsStart, tsEnd)
	if err != nil {
		return err
	}

	log.Ctx(context.TODO()).Info("read delete done",
		zap.String("collection", r.schema.GetName()),
		zap.Int("deleteRows", len(r.deleteData)),
	)

	deleteFilter, err := FilterWithDelete(r)
	if err != nil {
		return err
	}
	r.filters = append(r.filters, deleteFilter)
	return nil
}

func (r *reader) readDelete(deltaLogs []string, tsStart, tsEnd uint64) (map[any]typeutil.Timestamp, error) {
	v1opts := []storage.RwOption{
		storage.WithVersion(storage.StorageV1),
		storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
			return r.cm.MultiRead(ctx, paths)
		}),
	}
	v2opts := []storage.RwOption{
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(r.storageConfig),
	}

	deleteData := make(map[any]typeutil.Timestamp)

	readInternal := func(path string, opts []storage.RwOption) (map[any]typeutil.Timestamp, error) {
		tempData := make(map[any]typeutil.Timestamp)
		pkField, err := typeutil.GetPrimaryFieldSchema(r.schema)
		if err != nil {
			return nil, err
		}
		reader, err := storage.NewDeltalogReader(pkField.DataType, []string{path}, opts...)
		if err != nil {
			return nil, err
		}
		defer reader.Close()

		for {
			rec, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Error("compose delete wrong, failed to read deltalogs", zap.Error(err))
				return nil, err
			}

			for i := 0; i < rec.Len(); i++ {
				ts := typeutil.Timestamp(rec.Column(1).(*array.Int64).Value(i))
				if ts < tsStart || ts > tsEnd {
					continue
				}
				var pk any
				switch pkField.DataType {
				case schemapb.DataType_Int64:
					pk = rec.Column(0).(*array.Int64).Value(i)
				case schemapb.DataType_VarChar:
					pk = rec.Column(0).(*array.String).Value(i)
				}
				if tsExisting, ok := tempData[pk]; ok && tsExisting > ts {
					// skip if existing entry is newer
					continue
				}
				tempData[pk] = ts
			}
		}
		return tempData, nil
	}

	for _, path := range deltaLogs {
		// try v1 first
		tempData, errv1 := readInternal(path, v1opts)
		if errv1 != nil {
			// try v2 if v1 failed
			tempData, errv2 := readInternal(path, v2opts)
			if errv2 != nil {
				return nil, errv2
			}
			// Merge v2 results into deleteData
			for pk, ts := range tempData {
				if tsExisting, ok := deleteData[pk]; ok && tsExisting > ts {
					continue
				}
				deleteData[pk] = ts
			}
		} else {
			// Merge v1 results into deleteData
			for pk, ts := range tempData {
				if tsExisting, ok := deleteData[pk]; ok && tsExisting > ts {
					continue
				}
				deleteData[pk] = ts
			}
		}
	}
	return deleteData, nil
}

func (r *reader) Read() (*storage.InsertData, error) {
	insertData, err := storage.NewInsertDataWithFunctionOutputField(r.schema)
	if err != nil {
		return nil, err
	}
	rowNum := 0
	for {
		v, err := r.dr.NextValue()
		if err == io.EOF {
			if insertData.GetRowNum() == 0 {
				return nil, io.EOF
			}
			break
		}
		if err != nil {
			return nil, err
		}
		allFields := typeutil.GetAllFieldSchemas(r.schema)
		// convert record to fieldData
		for _, field := range allFields {
			fieldData := insertData.Data[field.GetFieldID()]
			if fieldData == nil {
				fieldData, err = storage.NewFieldData(field.GetDataType(), field, 1024)
				if err != nil {
					return nil, err
				}
				insertData.Data[field.GetFieldID()] = fieldData
			}

			err := fieldData.AppendRow((*v).Value.(map[int64]any)[field.GetFieldID()])
			if err != nil {
				return nil, err
			}
			rowNum++
		}
		if rowNum%100 == 0 && // Prevent frequent memory check
			insertData.GetMemorySize() >= r.bufferSize {
			break
		}
	}
	insertData, err = r.filter(insertData)
	if err != nil {
		return nil, err
	}
	return insertData, nil
}

func (r *reader) filter(insertData *storage.InsertData) (*storage.InsertData, error) {
	if len(r.filters) == 0 {
		return insertData, nil
	}
	masks := make(map[int]struct{}, 0)
OUTER:
	for i := 0; i < insertData.GetRowNum(); i++ {
		row := insertData.GetRow(i)
		for _, f := range r.filters {
			if !f(row) {
				masks[i] = struct{}{}
				continue OUTER
			}
		}
	}
	if len(masks) == 0 { // no data will undergo filtration, return directly
		return insertData, nil
	}
	result, err := storage.NewInsertDataWithFunctionOutputField(r.schema)
	if err != nil {
		return nil, err
	}
	for i := 0; i < insertData.GetRowNum(); i++ {
		if _, ok := masks[i]; ok {
			continue
		}
		row := insertData.GetRow(i)
		err = result.Append(row)
		if err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to append row, err=%s", err.Error()))
		}
	}
	return result, nil
}

func (r *reader) Size() (int64, error) {
	if size := r.fileSize.Load(); size != 0 {
		return size, nil
	}
	size, err := storage.GetFilesSize(r.ctx, lo.Flatten(lo.Values(r.insertLogs)), r.cm)
	if err != nil {
		return 0, err
	}
	r.fileSize.Store(size)
	return size, nil
}

func (r *reader) Close() {}
