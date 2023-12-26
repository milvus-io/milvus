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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type Row = map[storage.FieldID]any

type reader struct {
	dec    *json.Decoder
	schema *schemapb.CollectionSchema

	validator Validator
	parser    RowParser
}

func NewReader(r io.Reader, schema *schemapb.CollectionSchema) (*reader, error) {
	reader := &reader{
		dec:    json.NewDecoder(r),
		schema: schema,
	}
	var err error
	reader.validator, err = NewValidator(schema)
	if err != nil {
		return nil, err
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
		return merr.WrapErrImportFailed(fmt.Sprintf("failed to decode JSON, error: %v", err))
	}
	if t != json.Delim('{') && t != json.Delim('[') {
		return merr.WrapErrImportFailed("invalid JSON format, the content should be started with '{' or '['")
	}
	_ = j.dec.More()
	return nil
}

func (j *reader) Next(count int64) (*storage.InsertData, error) {
	insertData, err := storage.NewInsertData(j.schema)
	if err != nil {
		return nil, err
	}
	for j.dec.More() && count > 0 {
		var value any
		if err = j.dec.Decode(&value); err != nil {
			return nil, merr.WrapErrImportFailed(fmt.Sprintf("failed to parse row, error: %v", err))
		}
		row, err := j.validator.Validate(value)
		if err != nil {
			return nil, err
		}
		row, err = j.parser.Parse(row)
		if err != nil {
			return nil, err
		}
		err = insertData.Append(row)
		if err != nil {
			return nil, err
		}
		count--
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
