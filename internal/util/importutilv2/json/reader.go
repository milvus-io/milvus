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

package json

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	RowRootNode = "rows"
)

type Row = map[storage.FieldID]any

type reader struct {
	dec    *json.Decoder
	schema *schemapb.CollectionSchema

	bufferSize  int
	count       int64
	isOldFormat bool

	parser RowParser
}

func NewReader(r io.Reader, schema *schemapb.CollectionSchema, bufferSize int) (*reader, error) {
	var err error
	count, err := readCountPerBatch(bufferSize, schema)
	if err != nil {
		return nil, err
	}
	reader := &reader{
		dec:        json.NewDecoder(r),
		schema:     schema,
		bufferSize: bufferSize,
		count:      count,
	}
	reader.parser, err = NewRowParser(schema)
	if err != nil {
		return nil, err
	}
	err = reader.Init()
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (j *reader) Init() error {
	// Treat number value as a string instead of a float64.
	// By default, json lib treat all number values as float64,
	// but if an int64 value has more than 15 digits,
	// the value would be incorrect after converting from float64.
	j.dec.UseNumber()
	t, err := j.dec.Token()
	if err != nil {
		return merr.WrapErrImportFailed(fmt.Sprintf("init failed, failed to decode JSON, error: %v", err))
	}
	if t != json.Delim('{') && t != json.Delim('[') {
		return merr.WrapErrImportFailed("invalid JSON format, the content should be started with '{' or '['")
	}
	j.isOldFormat = t == json.Delim('{')
	return nil
}

func (j *reader) Read() (*storage.InsertData, error) {
	insertData, err := storage.NewInsertData(j.schema)
	if err != nil {
		return nil, err
	}
	if !j.dec.More() {
		return nil, nil
	}
	if j.isOldFormat {
		// read the key
		t, err := j.dec.Token()
		if err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to decode the JSON file, error: %v", err))
		}
		key := t.(string)
		keyLower := strings.ToLower(key)
		// the root key should be RowRootNode
		if keyLower != RowRootNode {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("invalid JSON format, the root key should be '%s', but get '%s'", RowRootNode, key))
		}

		// started by '['
		t, err = j.dec.Token()
		if err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to decode the JSON file, error: %v", err))
		}

		if t != json.Delim('[') {
			return nil, merr.WrapErrImportFailed("invalid JSON format, rows list should begin with '['")
		}
	}
	var cnt int64 = 0
	for j.dec.More() {
		var value any
		if err = j.dec.Decode(&value); err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to parse row, error: %v", err))
		}
		row, err := j.parser.Parse(value)
		if err != nil {
			return nil, err
		}
		err = insertData.Append(row)
		if err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to append row, err=%s", err.Error()))
		}
		cnt++
		if cnt >= j.count {
			cnt = 0
			if insertData.GetMemorySize() >= j.bufferSize {
				break
			}
		}
	}

	if !j.dec.More() {
		t, err := j.dec.Token()
		if err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to decode JSON, error: %v", err))
		}
		if t != json.Delim(']') {
			return nil, merr.WrapErrImportFailed("invalid JSON format, rows list should end with ']'")
		}
	}

	return insertData, nil
}

func (j *reader) Close() {}

func readCountPerBatch(bufferSize int, schema *schemapb.CollectionSchema) (int64, error) {
	sizePerRecord, err := typeutil.EstimateMaxSizePerRecord(schema)
	if err != nil {
		return 0, err
	}
	rowCount := int64(bufferSize) / int64(sizePerRecord)
	if rowCount >= 1000 {
		return 1000, nil
	}
	return rowCount, nil
}
