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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
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
	ctx                context.Context     // for canceling parse process
	collectionInfo     *CollectionInfo     // collection details including schema
	bufRowCount        int                 // max rows in a buffer
	updateProgressFunc func(percent int64) // update working progress percent value
}

// NewJSONParser helper function to create a JSONParser
func NewJSONParser(ctx context.Context, collectionInfo *CollectionInfo, updateProgressFunc func(percent int64)) *JSONParser {
	parser := &JSONParser{
		ctx:                ctx,
		collectionInfo:     collectionInfo,
		bufRowCount:        1024,
		updateProgressFunc: updateProgressFunc,
	}
	adjustBufSize(parser, collectionInfo.Schema)

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

func (p *JSONParser) combineDynamicRow(dynamicValues map[string]interface{}, row map[storage.FieldID]interface{}) error {
	if p.collectionInfo.DynamicField == nil {
		return nil
	}

	dynamicFieldID := p.collectionInfo.DynamicField.GetFieldID()
	// combine the dynamic field value
	// valid input:
	// case 1: {"id": 1, "vector": [], "x": 8, "$meta": "{\"y\": 8}"} ==>> {"id": 1, "vector": [], "$meta": "{\"y\": 8, \"x\": 8}"}
	// case 2: {"id": 1, "vector": [], "x": 8, "$meta": {}} ==>> {"id": 1, "vector": [], "$meta": {\"x\": 8}}
	// case 3: {"id": 1, "vector": [], "$meta": "{\"x\": 8}"}
	// case 4: {"id": 1, "vector": [], "$meta": {"x": 8}}
	// case 5: {"id": 1, "vector": [], "$meta": {}}
	// case 6: {"id": 1, "vector": [], "x": 8} ==>> {"id": 1, "vector": [], "$meta": "{\"x\": 8}"}
	// case 7: {"id": 1, "vector": []}
	obj, ok := row[dynamicFieldID]
	if ok {
		if len(dynamicValues) > 0 {
			if value, is := obj.(string); is {
				// case 1
				mp := make(map[string]interface{})
				err := json.Unmarshal([]byte(value), &mp)
				if err != nil {
					// invalid input
					return errors.New("illegal value for dynamic field, not a JSON format string")
				}

				maps.Copy(dynamicValues, mp)
			} else if mp, is := obj.(map[string]interface{}); is {
				// case 2
				maps.Copy(dynamicValues, mp)
			} else {
				// invalid input
				return errors.New("illegal value for dynamic field, not a JSON object")
			}
			row[dynamicFieldID] = dynamicValues
		}
		// else case 3/4/5
	} else {
		if len(dynamicValues) > 0 {
			// case 6
			row[dynamicFieldID] = dynamicValues
		} else {
			// case 7
			row[dynamicFieldID] = "{}"
		}
	}

	return nil
}

func (p *JSONParser) verifyRow(raw interface{}) (map[storage.FieldID]interface{}, error) {
	stringMap, ok := raw.(map[string]interface{})
	if !ok {
		log.Warn("JSON parser: invalid JSON format, each row should be a key-value map")
		return nil, errors.New("invalid JSON format, each row should be a key-value map")
	}

	dynamicValues := make(map[string]interface{})
	row := make(map[storage.FieldID]interface{})
	// some fields redundant?
	for k, v := range stringMap {
		fieldID, ok := p.collectionInfo.Name2FieldID[k]
		if (fieldID == p.collectionInfo.PrimaryKey.GetFieldID()) && p.collectionInfo.PrimaryKey.GetAutoID() {
			// primary key is auto-id, no need to provide
			log.Warn("JSON parser: the primary key is auto-generated, no need to provide", zap.String("fieldName", k))
			return nil, fmt.Errorf("the primary key '%s' is auto-generated, no need to provide", k)
		}

		if ok {
			row[fieldID] = v
		} else if p.collectionInfo.DynamicField != nil {
			// has dynamic field. put redundant pair to dynamicValues
			dynamicValues[k] = v
		} else {
			// no dynamic field. if user provided redundant field, return error
			log.Warn("JSON parser: the field is not defined in collection schema", zap.String("fieldName", k))
			return nil, fmt.Errorf("the field '%s' is not defined in collection schema", k)
		}
	}

	// some fields not provided?
	if len(row) != len(p.collectionInfo.Name2FieldID) {
		for k, v := range p.collectionInfo.Name2FieldID {
			if (p.collectionInfo.DynamicField != nil) && (v == p.collectionInfo.DynamicField.GetFieldID()) {
				// ignore dyanmic field, user don't have to provide values for dynamic field
				continue
			}

			if v == p.collectionInfo.PrimaryKey.GetFieldID() && p.collectionInfo.PrimaryKey.GetAutoID() {
				// ignore auto-generaed primary key
				continue
			}

			_, ok := row[v]
			if !ok {
				// not auto-id primary key, no dynamic field,  must provide value
				log.Warn("JSON parser: a field value is missed", zap.String("fieldName", k))
				return nil, fmt.Errorf("value of field '%s' is missed", k)
			}
		}
	}

	// combine the redundant pairs into dunamic field(if has)
	err := p.combineDynamicRow(dynamicValues, row)
	if err != nil {
		log.Warn("JSON parser: failed to combine dynamic values", zap.Error(err))
		return nil, err
	}

	return row, err
}

func (p *JSONParser) ParseRows(reader *IOReader, handler JSONRowHandler) error {
	if handler == nil || reader == nil {
		log.Warn("JSON parse handler is nil")
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
		log.Warn("JSON parser: failed to decode the JSON file", zap.Error(err))
		return fmt.Errorf("failed to decode the JSON file, error: %w", err)
	}
	if t != json.Delim('{') {
		log.Warn("JSON parser: invalid JSON format, the content should be started with'{'")
		return errors.New("invalid JSON format, the content should be started with'{'")
	}

	// read the first level
	isEmpty := true
	for dec.More() {
		// read the key
		t, err := dec.Token()
		if err != nil {
			log.Warn("JSON parser: failed to decode the JSON file", zap.Error(err))
			return fmt.Errorf("failed to decode the JSON file, error: %w", err)
		}
		key := t.(string)
		keyLower := strings.ToLower(key)
		// the root key should be RowRootNode
		if keyLower != RowRootNode {
			log.Warn("JSON parser: invalid JSON format, the root key is not found", zap.String("RowRootNode", RowRootNode), zap.String("key", key))
			return fmt.Errorf("invalid JSON format, the root key should be '%s', but get '%s'", RowRootNode, key)
		}

		// started by '['
		t, err = dec.Token()
		if err != nil {
			log.Warn("JSON parser: failed to decode the JSON file", zap.Error(err))
			return fmt.Errorf("failed to decode the JSON file, error: %w", err)
		}

		if t != json.Delim('[') {
			log.Warn("JSON parser: invalid JSON format, rows list should begin with '['")
			return errors.New("invalid JSON format, rows list should begin with '['")
		}

		// read buffer
		buf := make([]map[storage.FieldID]interface{}, 0, p.bufRowCount)
		for dec.More() {
			var value interface{}
			if err := dec.Decode(&value); err != nil {
				log.Warn("JSON parser: failed to parse row value", zap.Error(err))
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
					log.Warn("JSON parser: failed to convert row value to entity", zap.Error(err))
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
				log.Warn("JSON parser: failed to convert row value to entity", zap.Error(err))
				return fmt.Errorf("failed to convert row value to entity, error: %w", err)
			}
		}

		// end by ']'
		t, err = dec.Token()
		if err != nil {
			log.Warn("JSON parser: failed to decode the JSON file", zap.Error(err))
			return fmt.Errorf("failed to decode the JSON file, error: %w", err)
		}

		if t != json.Delim(']') {
			log.Warn("JSON parser: invalid JSON format, rows list should end with a ']'")
			return errors.New("invalid JSON format, rows list should end with a ']'")
		}

		// outside context might be canceled(service stop, or future enhancement for canceling import task)
		if isCanceled(p.ctx) {
			log.Warn("JSON parser: import task was canceled")
			return errors.New("import task was canceled")
		}

		// this break means we require the first node must be RowRootNode
		// once the RowRootNode is parsed, just finish
		break
	}

	// empty file is allowed, don't return error
	if isEmpty {
		log.Info("JSON parser: row count is 0")
		return nil
	}

	updateProgress()

	// send nil to notify the handler all have done
	return handler.Handle(nil)
}
