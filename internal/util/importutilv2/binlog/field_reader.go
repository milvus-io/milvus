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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

type fieldReader struct {
	reader      *storage.BinlogReader
	fieldSchema *schemapb.FieldSchema
}

func newFieldReader(ctx context.Context, cm storage.ChunkManager, fieldSchema *schemapb.FieldSchema, path string) (*fieldReader, error) {
	reader, err := newBinlogReader(ctx, cm, path)
	if err != nil {
		return nil, err
	}
	return &fieldReader{
		reader:      reader,
		fieldSchema: fieldSchema,
	}, nil
}

func (r *fieldReader) Next() (storage.FieldData, error) {
	fieldData, err := storage.NewFieldData(r.fieldSchema.GetDataType(), r.fieldSchema, 0)
	if err != nil {
		return nil, err
	}
	rowsSet, err := readData(r.reader, storage.InsertEventType)
	if err != nil {
		return nil, err
	}
	for _, rows := range rowsSet {
		err = fieldData.AppendRows(rows)
		if err != nil {
			return nil, err
		}
	}
	return fieldData, nil
}

func (r *fieldReader) Close() {
	r.reader.Close()
}
