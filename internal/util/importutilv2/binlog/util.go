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

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

// read delete data only
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

func createFieldBinlogList(insertLogs map[int64][]string) []*datapb.FieldBinlog {
	binlogFields := make([]*datapb.FieldBinlog, 0)
	for fieldID, logs := range insertLogs {
		binlog := &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: lo.Map(logs, func(path string, _ int) *datapb.Binlog {
				return &datapb.Binlog{
					LogPath: path,
				}
			}),
		}
		binlogFields = append(binlogFields, binlog)
	}
	return binlogFields
}

func verify(schema *schemapb.CollectionSchema, storageVersion int64, insertLogs map[int64][]string) (map[int64][]string, *schemapb.CollectionSchema, error) {
	// check system fields (ts and rowID)
	if storageVersion == storage.StorageV2 {
		if _, ok := insertLogs[storagecommon.DefaultShortColumnGroupID]; !ok {
			return nil, nil, merr.WrapErrImportFailed("no binlog for system fields")
		}
	} else {
		// storage v1
		if _, ok := insertLogs[common.RowIDField]; !ok {
			return nil, nil, merr.WrapErrImportFailed("no binlog for RowID field")
		}
		if _, ok := insertLogs[common.TimeStampField]; !ok {
			return nil, nil, merr.WrapErrImportFailed("no binlog for Timestamp field")
		}
	}

	// check binlog file count, must be equal for all fields
	for fieldID, logs := range insertLogs {
		if len(logs) != len(insertLogs[common.RowIDField]) {
			return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("misaligned binlog count, field%d:%d, field%d:%d",
				fieldID, len(logs), common.RowIDField, len(insertLogs[common.RowIDField])))
		}
	}

	allFields := typeutil.GetAllFieldSchemas(schema)
	if storageVersion == storage.StorageV2 {
		for _, field := range allFields {
			if typeutil.IsVectorType(field.GetDataType()) {
				if _, ok := insertLogs[field.GetFieldID()]; !ok {
					// vector field must be provided
					return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("no binlog for field:%s", field.GetName()))
				}
			}
		}
		return insertLogs, schema, nil
	}

	// Goal: support import binlog files to a different schema collection.
	//
	// What we know:
	// - The milvus-backup tool knows the original collection's schema, it has the ability to create
	//   a new collection from the old schema, it also supports restore data into an existing collection
	//   which could be different schema.
	// - The binlog field ids are parsed from the storage path, here we don't know the original field name.
	//   The binlogs are mapped by fieldID, which could lead to type error later.
	//
	// How to do:
	// - Always import pk field into the target collection no matter it is auto-id or not.
	// - Returns an error if the target collection has a Function output field, but the source collection has no this field.
	// - If both collections have the function output field, we do not re-run the Function when restoring from a backup.
	// - For nullable/defaultValue fields, the binlog is optional.
	// - For dynamic field, the binlog is optional.
	// - For other fields, if a field is not in the target collection's schema, just skip the binlog of this field.
	// - If the target collection's schema contains a field(not nullable/defaultValue) that the source collection
	// doesn't have, reports an error

	// Here we copy the schema for reading part of collection's data. The storage.NewBinlogRecordReader() requires
	// a schema and the schema must be consistent with the binglog files([]*datapb.FieldBinlog)
	cloneSchema := typeutil.Clone(schema)
	cloneSchema.Fields = []*schemapb.FieldSchema{}                       // the Fields will be reset according to the validInsertLogs
	cloneSchema.StructArrayFields = []*schemapb.StructArrayFieldSchema{} // the StructArrayFields will be reset according to the validInsertLogs
	cloneSchema.EnableDynamicField = false                               // this flag will be reset

	// this loop will reset the cloneSchema.Fields and return validInsertLogs
	validInsertLogs := make(map[int64][]string)

	for _, field := range schema.GetFields() {
		id := field.GetFieldID()
		logs, ok := insertLogs[id]
		if !ok {
			// dynamic field, nullable field, default value field, optional, no need to check
			// they will be filled by AppendNullableDefaultFieldsData
			if field.GetIsDynamic() || field.GetNullable() || field.GetDefaultValue() != nil {
				continue
			}
			// primary key is required
			// function output field is also required
			// the other field must be provided
			return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("no binlog for field:%s", field.GetName()))
		} else {
			// these binlogs are intend to be imported
			validInsertLogs[id] = logs

			// reset the cloneSchema.Fields
			cloneSchema.Fields = append(cloneSchema.Fields, field)
			if field.IsDynamic {
				cloneSchema.EnableDynamicField = true
			}
		}
	}

	for _, structArrayField := range schema.GetStructArrayFields() {
		for _, field := range structArrayField.GetFields() {
			id := field.GetFieldID()
			logs, ok := insertLogs[id]
			if !ok {
				return nil, nil, merr.WrapErrImportFailed(fmt.Sprintf("no binlog for struct field:%s", field.GetName()))
			}

			validInsertLogs[id] = logs
		}
		cloneSchema.StructArrayFields = append(cloneSchema.StructArrayFields, structArrayField)
	}

	return validInsertLogs, cloneSchema, nil
}
