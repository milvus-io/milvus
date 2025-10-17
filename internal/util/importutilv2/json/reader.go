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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

const (
	RowRootNode = "rows"
)

type Row = map[storage.FieldID]any

type reader struct {
	ctx    context.Context
	cm     storage.ChunkManager
	cmr    storage.FileReader
	schema *schemapb.CollectionSchema

	fileSize *atomic.Int64
	filePath string
	dec      *json.Decoder

	bufferSize    int
	count         int64
	isOldFormat   bool
	isLinesFormat bool

	parser RowParser
}

func newReader(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema, path string, bufferSize int, isLinesFormat bool) (*reader, error) {
	r, err := cm.Reader(ctx, path)
	if err != nil {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("read json file failed, path=%s, err=%s", path, err.Error()))
	}
	count, err := common.EstimateReadCountPerBatch(bufferSize, schema)
	if err != nil {
		return nil, err
	}
	reader := &reader{
		ctx:           ctx,
		cm:            cm,
		cmr:           r,
		schema:        schema,
		fileSize:      atomic.NewInt64(0),
		filePath:      path,
		dec:           json.NewDecoder(r),
		bufferSize:    bufferSize,
		count:         count,
		isLinesFormat: isLinesFormat,
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

func NewReader(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema, path string, bufferSize int) (*reader, error) {
	reader, err := newReader(ctx, cm, schema, path, bufferSize, false)
	return reader, err
}

func NewLinesReader(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema, path string, bufferSize int) (*reader, error) {
	reader, err := newReader(ctx, cm, schema, path, bufferSize, true)
	return reader, err
}

func (j *reader) Init() error {
	// Treat number value as a string instead of a float64.
	// By default, json lib treat all number values as float64,
	// but if an int64 value has more than 15 digits,
	// the value would be incorrect after converting from float64.
	j.dec.UseNumber()

	// JSONLines format, each line is a JSON dict like this:
	// a.jsonl
	//	{"a": 1}
	//  {"b": 2}
	if j.isLinesFormat {
		return nil
	}

	// Normal JSON format, accept two formats:
	// old_format.json
	//	{
	//		"rows": [
	//			{"a": 1},
	//			{"b": 2}
	//		]
	//	}
	//
	// list_format.json
	//  [
	//		{"a": 1},
	//		{"b": 2}
	//  ]
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
		return nil, io.EOF
	}

	if j.isLinesFormat {
		if err = j.readJSONLines(insertData); err != nil {
			return nil, err
		}
	} else {
		if err = j.readNormalJSON(insertData); err != nil {
			return nil, err
		}
	}

	return insertData, nil
}

func (j *reader) readNormalJSON(insertData *storage.InsertData) error {
	// In old versions(2.2/2.3), milvus requires users to create the JSON begins with a root key "rows"
	// old_format.json
	//	{
	//		"rows": [
	//			{"a": 1},
	//			{"b": 2}
	//		]
	//	}
	// this section is to detect the old format
	if j.isOldFormat {
		// read the key
		t, err := j.dec.Token()
		if err != nil {
			return merr.WrapErrImportFailed(fmt.Sprintf("failed to decode the JSON file, error: %v", err))
		}
		key := t.(string)
		keyLower := strings.ToLower(key)
		// the root key should be RowRootNode
		if keyLower != RowRootNode {
			return merr.WrapErrImportFailed(fmt.Sprintf("invalid JSON format, the root key should be '%s', but get '%s'", RowRootNode, key))
		}

		// started by '['
		t, err = j.dec.Token()
		if err != nil {
			return merr.WrapErrImportFailed(fmt.Sprintf("failed to decode the JSON file, error: %v", err))
		}

		if t != json.Delim('[') {
			return merr.WrapErrImportFailed("invalid JSON format, rows list should begin with '['")
		}
		j.isOldFormat = false
	}

	// Read the content under the key "rows"(or inside the list) line by line
	if err := j.readJSONLines(insertData); err != nil {
		return err
	}

	// Verify the content is ended with "]"
	if !j.dec.More() {
		t, err := j.dec.Token()
		if err != nil {
			return merr.WrapErrImportFailed(fmt.Sprintf("failed to decode JSON, error: %v", err))
		}
		if t != json.Delim(']') {
			return merr.WrapErrImportFailed("invalid JSON format, rows list should end with ']'")
		}
	}

	return nil
}

func (j *reader) readJSONLines(insertData *storage.InsertData) error {
	// Read line by line, the json.Decoder.Decode() can ignore the comma at the end of each line,
	// both of the two formats will get the same result:
	// 1. lines ending with a comma:
	//	{"a": 1},
	//	{"b": 2},
	//	{"c": 3}
	// 2. lines ending without comma:
	//	{"a": 1}
	//	{"b": 2}
	//	{"c": 3}
	var cnt int64 = 0
	for j.dec.More() {
		var value any
		if err := j.dec.Decode(&value); err != nil {
			return merr.WrapErrImportFailed(fmt.Sprintf("failed to decode row, error: %v", err))
		}

		row, err := j.parser.Parse(value)
		if err != nil {
			return merr.WrapErrImportFailed(fmt.Sprintf("failed to parse row, error: %v", err))
		}
		err = insertData.Append(row)
		if err != nil {
			return merr.WrapErrImportFailed(fmt.Sprintf("failed to append row, err=%s", err.Error()))
		}
		cnt++
		if cnt >= j.count {
			cnt = 0
			if insertData.GetMemorySize() >= j.bufferSize {
				break
			}
		}
	}
	return nil
}

func (j *reader) Size() (int64, error) {
	if size := j.fileSize.Load(); size != 0 {
		return size, nil
	}
	size, err := j.cm.Size(j.ctx, j.filePath)
	if err != nil {
		return 0, err
	}
	j.fileSize.Store(size)
	return size, nil
}

func (j *reader) Close() {
	if j.cmr != nil {
		j.cmr.Close()
	}
}
