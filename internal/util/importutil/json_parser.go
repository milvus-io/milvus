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

package importutil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

const (
	// root field of row-based json format
	RowRootNode = "rows"
)

type IOReader struct {
	r        io.Reader
	fileSize int64
}

type JSONParser struct {
	ctx                context.Context  // for canceling parse process
	bufRowCount        int              // max rows in a buffer
	fields             map[string]int64 // fields need to be parsed
	name2FieldID       map[string]storage.FieldID
	updateProgressFunc func(percent int64) // update working progress percent value
}

// NewJSONParser helper function to create a JSONParser
func NewJSONParser(ctx context.Context, collectionSchema *schemapb.CollectionSchema, updateProgressFunc func(percent int64)) *JSONParser {
	fields := make(map[string]int64)
	name2FieldID := make(map[string]storage.FieldID)
	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]
		// RowIDField and TimeStampField is internal field, no need to parse
		if schema.GetFieldID() == common.RowIDField || schema.GetFieldID() == common.TimeStampField {
			continue
		}
		// if primary key field is auto-gernerated, no need to parse
		if schema.GetAutoID() {
			continue
		}

		fields[schema.GetName()] = 0
		name2FieldID[schema.GetName()] = schema.GetFieldID()
	}

	parser := &JSONParser{
		ctx:                ctx,
		bufRowCount:        1024,
		fields:             fields,
		name2FieldID:       name2FieldID,
		updateProgressFunc: updateProgressFunc,
	}
	adjustBufSize(parser, collectionSchema)

	return parser
}

func adjustBufSize(parser *JSONParser, collectionSchema *schemapb.CollectionSchema) {
	sizePerRecord, _ := typeutil.EstimateSizePerRecord(collectionSchema)
	if sizePerRecord <= 0 {
		return
	}

	// for high dimensional vector, the bufSize is a small value, read few rows each time
	// for low dimensional vector, the bufSize is a large value, read more rows each time
	bufRowCount := parser.bufRowCount
	for {
		if bufRowCount*sizePerRecord > SingleBlockSize {
			bufRowCount--
		} else {
			break
		}
	}

	// at least one row per buffer
	if bufRowCount <= 0 {
		bufRowCount = 1
	}

	log.Info("JSON parser: reset bufRowCount", zap.Int("sizePerRecord", sizePerRecord), zap.Int("bufRowCount", bufRowCount))
	parser.bufRowCount = bufRowCount
}

func (p *JSONParser) verifyRow(raw interface{}) (map[storage.FieldID]interface{}, error) {
	stringMap, ok := raw.(map[string]interface{})
	if !ok {
		log.Error("JSON parser: invalid JSON format, each row should be a key-value map")
		return nil, errors.New("invalid JSON format, each row should be a key-value map")
	}

	row := make(map[storage.FieldID]interface{})
	for k, v := range stringMap {
		// if user provided redundant field, return error
		fieldID, ok := p.name2FieldID[k]
		if !ok {
			log.Error("JSON parser: the field is not defined in collection schema", zap.String("fieldName", k))
			return nil, fmt.Errorf("the field '%s' is not defined in collection schema", k)
		}
		row[fieldID] = v
	}

	// some fields not provided?
	if len(row) != len(p.name2FieldID) {
		for k, v := range p.name2FieldID {
			_, ok := row[v]
			if !ok {
				log.Error("JSON parser: a field value is missed", zap.String("fieldName", k))
				return nil, fmt.Errorf("value of field '%s' is missed", k)
			}
		}
	}

	return row, nil
}

func (p *JSONParser) ParseRows(reader *IOReader, handler JSONRowHandler) error {
	if handler == nil || reader == nil {
		log.Error("JSON parse handler is nil")
		return errors.New("JSON parse handler is nil")
	}

	dec := json.NewDecoder(reader.r)

	oldPercent := int64(0)
	updateProgress := func() {
		if p.updateProgressFunc != nil && reader.fileSize > 0 {
			percent := (dec.InputOffset() * ProgressValueForPersist) / reader.fileSize
			if percent > oldPercent { // avoid too many log
				log.Debug("JSON parser: working progress", zap.Int64("offset", dec.InputOffset()),
					zap.Int64("fileSize", reader.fileSize), zap.Int64("percent", percent))
			}
			oldPercent = percent
			p.updateProgressFunc(percent)
		}
	}

	// treat number value as a string instead of a float64.
	// by default, json lib treat all number values as float64, but if an int64 value
	// has more than 15 digits, the value would be incorrect after converting from float64
	dec.UseNumber()
	t, err := dec.Token()
	if err != nil {
		log.Error("JSON parser: failed to decode the JSON file", zap.Error(err))
		return fmt.Errorf("failed to decode the JSON file, error: %w", err)
	}
	if t != json.Delim('{') {
		log.Error("JSON parser: invalid JSON format, the content should be started with'{'")
		return errors.New("invalid JSON format, the content should be started with'{'")
	}

	// read the first level
	isEmpty := true
	for dec.More() {
		// read the key
		t, err := dec.Token()
		if err != nil {
			log.Error("JSON parser: failed to decode the JSON file", zap.Error(err))
			return fmt.Errorf("failed to decode the JSON file, error: %w", err)
		}
		key := t.(string)
		keyLower := strings.ToLower(key)
		// the root key should be RowRootNode
		if keyLower != RowRootNode {
			log.Error("JSON parser: invalid JSON format, the root key is not found", zap.String("RowRootNode", RowRootNode), zap.String("key", key))
			return fmt.Errorf("invalid JSON format, the root key should be '%s', but get '%s'", RowRootNode, key)
		}

		// started by '['
		t, err = dec.Token()
		if err != nil {
			log.Error("JSON parser: failed to decode the JSON file", zap.Error(err))
			return fmt.Errorf("failed to decode the JSON file, error: %w", err)
		}

		if t != json.Delim('[') {
			log.Error("JSON parser: invalid JSON format, rows list should begin with '['")
			return errors.New("invalid JSON format, rows list should begin with '['")
		}

		// read buffer
		buf := make([]map[storage.FieldID]interface{}, 0, p.bufRowCount)
		for dec.More() {
			var value interface{}
			if err := dec.Decode(&value); err != nil {
				log.Error("JSON parser: failed to parse row value", zap.Error(err))
				return fmt.Errorf("failed to parse row value, error: %w", err)
			}

			row, err := p.verifyRow(value)
			if err != nil {
				return err
			}

			updateProgress()

			buf = append(buf, row)
			if len(buf) >= p.bufRowCount {
				isEmpty = false
				if err = handler.Handle(buf); err != nil {
					log.Error("JSON parser: failed to convert row value to entity", zap.Error(err))
					return fmt.Errorf("failed to convert row value to entity, error: %w", err)
				}

				// clear the buffer
				buf = make([]map[storage.FieldID]interface{}, 0, p.bufRowCount)
			}
		}

		// some rows in buffer not parsed, parse them
		if len(buf) > 0 {
			isEmpty = false
			if err = handler.Handle(buf); err != nil {
				log.Error("JSON parser: failed to convert row value to entity", zap.Error(err))
				return fmt.Errorf("failed to convert row value to entity, error: %w", err)
			}
		}

		// end by ']'
		t, err = dec.Token()
		if err != nil {
			log.Error("JSON parser: failed to decode the JSON file", zap.Error(err))
			return fmt.Errorf("failed to decode the JSON file, error: %w", err)
		}

		if t != json.Delim(']') {
			log.Error("JSON parser: invalid JSON format, rows list should end with a ']'")
			return errors.New("invalid JSON format, rows list should end with a ']'")
		}

		// outside context might be canceled(service stop, or future enhancement for canceling import task)
		if isCanceled(p.ctx) {
			log.Error("JSON parser: import task was canceled")
			return errors.New("import task was canceled")
		}

		// this break means we require the first node must be RowRootNode
		// once the RowRootNode is parsed, just finish
		break
	}

	if isEmpty {
		log.Error("JSON parser: row count is 0")
		return errors.New("row count is 0")
	}

	updateProgress()

	// send nil to notify the handler all have done
	return handler.Handle(nil)
}
