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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type columnReader struct {
	reader      *storage.BinlogReader
	fieldSchema *schemapb.FieldSchema
}

func NewColumnReader(cm storage.ChunkManager, fieldSchema *schemapb.FieldSchema, path string) (importutilv2.ColumnReader, error) {
	reader, err := newBinlogReader(cm, path)
	if err != nil {
		return nil, err
	}
	return &columnReader{
		reader:      reader,
		fieldSchema: fieldSchema,
	}, nil
}

func (r *columnReader) Next(_ int64) (storage.FieldData, error) {
	fieldData, err := storage.NewFieldData(r.fieldSchema.GetDataType(), r.fieldSchema)
	if err != nil {
		return nil, err
	}
	result, err := readData(r.reader, storage.InsertEventType)
	if err != nil {
		return nil, err
	}
	for _, v := range result {
		err = fieldData.AppendRow(v)
		if err != nil {
			return nil, err
		}
	}
	return fieldData, nil
}

func readData(reader *storage.BinlogReader, et storage.EventTypeCode) ([]any, error) {
	result := make([]any, 0)
	for {
		event, err := reader.NextEventReader()
		if err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to iterate events reader, error: %v", err))
		}
		if event == nil {
			break // end of the file
		}
		if event.TypeCode != et {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("wrong binlog type, expect:%s, actual:%s",
				et.String(), event.TypeCode.String()))
		}
		data, _, err := event.PayloadReaderInterface.GetDataFromPayload()
		if err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to read data, error: %v", err))
		}
		result = append(result, data.([]any)...)
	}
	return result, nil
}

func newBinlogReader(cm storage.ChunkManager, path string) (*storage.BinlogReader, error) {
	bytes, err := cm.Read(context.TODO(), path) // TODO: fix context
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
