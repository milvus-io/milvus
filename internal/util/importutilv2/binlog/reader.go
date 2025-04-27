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
	"time"

	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type reader struct {
	ctx    context.Context
	cm     storage.ChunkManager
	schema *schemapb.CollectionSchema

	fileSize   *atomic.Int64
	deleteData map[any]typeutil.Timestamp // pk2ts
	insertLogs map[int64][]string         // fieldID -> binlogs

	readIdx int
	filters []Filter
}

func NewReader(ctx context.Context,
	cm storage.ChunkManager,
	schema *schemapb.CollectionSchema,
	paths []string,
	tsStart,
	tsEnd uint64,
) (*reader, error) {
	schema = typeutil.AppendSystemFields(schema)
	r := &reader{
		ctx:      ctx,
		cm:       cm,
		schema:   schema,
		fileSize: atomic.NewInt64(0),
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
	if len(paths) > 2 {
		return merr.WrapErrImportFailed(fmt.Sprintf("too many input paths for binlog import. "+
			"Valid paths length should be one or two, but got paths:%s", paths))
	}
	insertLogs, err := listInsertLogs(r.ctx, r.cm, paths[0])
	if err != nil {
		return err
	}
	err = verify(r.schema, insertLogs)
	if err != nil {
		return err
	}
	r.insertLogs = insertLogs

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
	deleteData := make(map[any]typeutil.Timestamp)
	for _, path := range deltaLogs {
		reader, err := newBinlogReader(r.ctx, r.cm, path)
		if err != nil {
			return nil, err
		}
		// no need to read nulls in DeleteEventType
		rowsSet, _, err := readData(reader, storage.DeleteEventType)
		if err != nil {
			return nil, err
		}
		for _, rows := range rowsSet {
			for _, row := range rows.([]string) {
				dl := &storage.DeleteLog{}
				err = dl.Parse(row)
				if err != nil {
					return nil, err
				}
				if dl.Ts >= tsStart && dl.Ts <= tsEnd {
					pk := dl.Pk.GetValue()
					if ts, ok := deleteData[pk]; !ok || ts < dl.Ts {
						deleteData[pk] = dl.Ts
					}
				}
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
	if r.readIdx == len(r.insertLogs[0]) {
		// In the binlog import scenario, all data may be filtered out
		// due to time range or deletions. Therefore, we use io.EOF as
		// the indicator of the read end, instead of InsertData with 0 rows.
		return nil, io.EOF
	}
	for fieldID, binlogs := range r.insertLogs {
		field := typeutil.GetField(r.schema, fieldID)
		if field == nil {
			return nil, merr.WrapErrFieldNotFound(fieldID)
		}
		path := binlogs[r.readIdx]
		fr, err := newFieldReader(r.ctx, r.cm, field, path)
		if err != nil {
			return nil, err
		}
		fieldData, err := fr.Next()
		if err != nil {
			fr.Close()
			return nil, err
		}
		fr.Close()
		insertData.Data[field.GetFieldID()] = fieldData
	}
	insertData, err = r.filter(insertData)
	if err != nil {
		return nil, err
	}
	r.readIdx++
	return insertData, nil
}

func (r *reader) filter(insertData *storage.InsertData) (*storage.InsertData, error) {
	if len(r.filters) == 0 {
		return insertData, nil
	}
	start := time.Now()
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
	log.Ctx(context.TODO()).Info("filter data done",
		zap.String("collection", r.schema.GetName()),
		zap.Int("deleteRows", len(r.deleteData)),
		zap.Int("originRows", insertData.GetRowNum()),
		zap.Int("resultRows", result.GetRowNum()),
		zap.Duration("dur", time.Since(start)),
	)
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
