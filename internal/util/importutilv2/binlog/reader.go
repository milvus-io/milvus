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
	"encoding/json"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type reader struct {
	cm     storage.ChunkManager
	schema *schemapb.CollectionSchema

	delData    *storage.DeleteData
	insertLogs []*datapb.FieldBinlog
	readIdx    int

	filters []Filter

	tsBegin uint64
	tsEnd   uint64
}

func (r *reader) Init(paths []string) error {
	insertLogs, deltaLogs, err := r.ListBinlogs(paths)
	if err != nil {
		return err
	}
	r.delData, err = r.ReadDelta(deltaLogs)
	if err != nil {
		return err
	}
	r.insertLogs = insertLogs
	isDeleted, err := FilterWithDelete(r)
	if err != nil {
		return err
	}
	r.filters = []Filter{
		isDeleted,
		FilterWithTimerange(r),
	}
	return nil
}

func (r *reader) ReadDelta(deltaLogs *datapb.FieldBinlog) (*storage.DeleteData, error) {
	deleteData := storage.NewDeleteData(nil, nil)
	for _, binlog := range deltaLogs.GetBinlogs() {
		path := binlog.GetLogPath()
		reader, err := newBinlogReader(r.cm, path)
		if err != nil {
			return nil, err
		}
		data, err := readData(reader, storage.DeleteEventType)
		if err != nil {
			return nil, err
		}
		for _, d := range data {
			dl := &storage.DeleteLog{}
			err = json.Unmarshal([]byte(d.(string)), dl)
			if err != nil {
				return nil, err
			}
			if dl.Ts >= r.tsBegin && dl.Ts <= r.tsEnd {
				deleteData.Append(dl.Pk, dl.Ts)
			}
		}
	}
	return deleteData, nil
}

func (r *reader) ListBinlogs(paths []string) ([]*datapb.FieldBinlog, *datapb.FieldBinlog, error) {
	if len(paths) < 1 {
		return nil, nil, merr.WrapErrImportFailed("no insert binlogs to import")
	}
	_, _, err := r.cm.ListWithPrefix(context.Background(), paths[0], true)
	if err != nil {
		return nil, nil, err
	}
	// TODO: parse logPaths to fieldBinlog
	if len(paths) < 2 {
		return nil, nil, nil
	}
	_, _, err = r.cm.ListWithPrefix(context.Background(), paths[1], true)
	if err != nil {
		return nil, nil, err
	}
	return nil, nil, nil
}

func (r *reader) Next(count int64) (*storage.InsertData, error) {
	insertData, err := storage.NewInsertData(r.schema)
	if err != nil {
		return nil, err
	}
	if r.readIdx == len(r.insertLogs[0].GetBinlogs()) {
		return nil, nil
	}
	for _, fieldBinlog := range r.insertLogs {
		field := typeutil.GetField(r.schema, fieldBinlog.GetFieldID())
		if field == nil {
			return nil, merr.WrapErrFieldNotFound(fieldBinlog.GetFieldID())
		}
		path := fieldBinlog.GetBinlogs()[r.readIdx].GetLogPath()
		cr, err := NewColumnReader(r.cm, field, path)
		if err != nil {
			return nil, err
		}
		fieldData, err := cr.Next(count)
		if err != nil {
			return nil, err
		}
		insertData.Data[field.GetFieldID()] = fieldData
	}

	res, err := storage.NewInsertData(r.schema)
	if err != nil {
		return nil, err
	}
	for i := 0; i < insertData.GetRowNum(); i++ {
		row := insertData.GetRow(i)
		for _, filter := range r.filters {
			if !filter(row) {
				continue
			}
		}
		err = res.Append(row)
		if err != nil {
			return nil, err
		}
	}
	r.readIdx++
	return res, nil
}
