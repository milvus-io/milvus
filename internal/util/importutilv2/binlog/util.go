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
	"path"
	"sort"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func readData(reader *storage.BinlogReader, et storage.EventTypeCode) ([]any, [][]bool, error) {
	rowsSet := make([]any, 0)
	validDataRowsSet := make([][]bool, 0)
	for {
		event, err := reader.NextEventReader()
		if err != nil {
			return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to iterate events reader, error: %v", err))
		}
		if event == nil {
			break // end of the file
		}
		if event.TypeCode != et {
			return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("wrong binlog type, expect:%s, actual:%s",
				et.String(), event.TypeCode.String()))
		}
		rows, validDataRows, _, err := event.PayloadReaderInterface.GetDataFromPayload()
		if err != nil {
			return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read data, error: %v", err))
		}
		rowsSet = append(rowsSet, rows)
		validDataRowsSet = append(validDataRowsSet, validDataRows)
	}
	return rowsSet, validDataRowsSet, nil
}

func newBinlogReader(ctx context.Context, cm storage.ChunkManager, path string) (*storage.BinlogReader, error) {
	bytes, err := cm.Read(ctx, path) // TODO: dyh, checks if the error is a retryable error
	if err != nil {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to open binlog %s", path))
	}
	var reader *storage.BinlogReader
	reader, err = storage.NewBinlogReader(bytes)
	if err != nil {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to create reader, binlog:%s, error:%v", path, err))
	}
	return reader, nil
}

func listInsertLogs(ctx context.Context, cm storage.ChunkManager, insertPrefix string) (map[int64][]string, error) {
	insertLogs := make(map[int64][]string)
	var walkErr error
	if err := cm.WalkWithPrefix(ctx, insertPrefix, true, func(insertLog *storage.ChunkObjectInfo) bool {
		fieldPath := path.Dir(insertLog.FilePath)
		fieldStrID := path.Base(fieldPath)
		fieldID, err := strconv.ParseInt(fieldStrID, 10, 64)
		if err != nil {
			walkErr = merr.WrapErrImportFailed(fmt.Sprintf("failed to parse field id from log, error: %v", err))
			return false
		}
		insertLogs[fieldID] = append(insertLogs[fieldID], insertLog.FilePath)
		return true
	}); err != nil {
		return nil, err
	}
	if walkErr != nil {
		return nil, walkErr
	}

	for _, v := range insertLogs {
		sort.Strings(v)
	}
	return insertLogs, nil
}

func verify(schema *schemapb.CollectionSchema, insertLogs map[int64][]string) error {
	// 1. check schema fields
	for _, field := range schema.GetFields() {
		if _, ok := insertLogs[field.GetFieldID()]; !ok {
			return merr.WrapErrImportFailed(fmt.Sprintf("no binlog for field:%s", field.GetName()))
		}
	}
	// 2. check system fields (ts and rowID)
	if _, ok := insertLogs[common.RowIDField]; !ok {
		return merr.WrapErrImportFailed("no binlog for RowID field")
	}
	if _, ok := insertLogs[common.TimeStampField]; !ok {
		return merr.WrapErrImportFailed("no binlog for TimestampField")
	}
	// 3. check file count
	for fieldID, logs := range insertLogs {
		if len(logs) != len(insertLogs[common.RowIDField]) {
			return merr.WrapErrImportFailed(fmt.Sprintf("misaligned binlog count, field%d:%d, field%d:%d",
				fieldID, len(logs), common.RowIDField, len(insertLogs[common.RowIDField])))
		}
	}
	// for Function output field, we do not re-run the Function when restoring from a backup.
	return nil
}
