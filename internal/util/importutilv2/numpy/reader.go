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
	"io"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

type Reader struct {
	schema     *schemapb.CollectionSchema
	bufferSize int
	frs        map[int64]*FieldReader // fieldID -> FieldReader
}

func NewReader(schema *schemapb.CollectionSchema, readers map[int64]io.Reader, bufferSize int) (*Reader, error) {
	fields := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	crs := make(map[int64]*FieldReader)
	for fieldID, r := range readers {
		cr, err := NewFieldReader(r, fields[fieldID])
		if err != nil {
			return nil, err
		}
		crs[fieldID] = cr
	}
	return &Reader{
		schema:     schema,
		bufferSize: bufferSize,
		frs:        crs,
	}, nil
}

func (r *Reader) Read() (*storage.InsertData, error) {
	insertData, err := storage.NewInsertData(r.schema)
	if err != nil {
		return nil, err
	}
OUTER:
	for {
		for fieldID, cr := range r.frs {
			data, err := cr.Next(1)
			if err != nil {
				return nil, err
			}
			if data == nil {
				break OUTER
			}
			err = insertData.Data[fieldID].AppendRows(data)
			if err != nil {
				return nil, err
			}
		}
		if insertData.GetMemorySize() >= r.bufferSize {
			break
		}
	}

	for fieldID := range r.frs {
		if insertData.Data[fieldID].RowNum() == 0 {
			return nil, nil
		}
	}
	return insertData, nil
}

func (r *Reader) Close() {
	for _, cr := range r.frs {
		cr.Close()
	}
}
