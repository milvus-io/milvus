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
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

const (
	// root field of row-based json format
	RowRootNode = "rows"
	// minimal size of a buffer
	MinBufferSize = 1024
	// split file into batches no more than this count
	MaxBatchCount = 16
)

type JSONParser struct {
	ctx          context.Context  // for canceling parse process
	bufSize      int64            // max rows in a buffer
	fields       map[string]int64 // fields need to be parsed
	name2FieldID map[string]storage.FieldID
}

// NewJSONParser helper function to create a JSONParser
func NewJSONParser(ctx context.Context, collectionSchema *schemapb.CollectionSchema) *JSONParser {
	fields := make(map[string]int64)
	name2FieldID := make(map[string]storage.FieldID)
	for i := 0; i < len(collectionSchema.Fields); i++ {
		schema := collectionSchema.Fields[i]
		fields[schema.GetName()] = 0
		name2FieldID[schema.GetName()] = schema.GetFieldID()
	}

	parser := &JSONParser{
		ctx:          ctx,
		bufSize:      MinBufferSize,
		fields:       fields,
		name2FieldID: name2FieldID,
	}
	adjustBufSize(parser, collectionSchema)

	return parser
}

func adjustBufSize(parser *JSONParser, collectionSchema *schemapb.CollectionSchema) {
	sizePerRecord, _ := typeutil.EstimateSizePerRecord(collectionSchema)
	if sizePerRecord <= 0 {
		return
	}

	// split the file into no more than MaxBatchCount batches to parse
	// for high dimensional vector, the bufSize is a small value, read few rows each time
	// for low dimensional vector, the bufSize is a large value, read more rows each time
	maxRows := MaxFileSize / sizePerRecord
	bufSize := maxRows / MaxBatchCount

	// bufSize should not be less than MinBufferSize
	if bufSize < MinBufferSize {
		bufSize = MinBufferSize
	}

	log.Info("JSON parser: reset bufSize", zap.Int("sizePerRecord", sizePerRecord), zap.Int("bufSize", bufSize))
	parser.bufSize = int64(bufSize)
}

func (p *JSONParser) ParseRows(r io.Reader, handler JSONRowHandler) error {
	if handler == nil {
		log.Error("JSON parse handler is nil")
		return errors.New("JSON parse handler is nil")
	}

	dec := json.NewDecoder(r)

	t, err := dec.Token()
	if err != nil {
		log.Error("JSON parser: row count is 0")
		return errors.New("JSON parser: row count is 0")
	}
	if t != json.Delim('{') {
		log.Error("JSON parser: invalid JSON format, the content should be started with'{'")
		return errors.New("JSON parser: invalid JSON format, the content should be started with'{'")
	}

	// read the first level
	isEmpty := true
	for dec.More() {
		// read the key
		t, err := dec.Token()
		if err != nil {
			log.Error("JSON parser: read json token error", zap.Error(err))
			return fmt.Errorf("JSON parser: read json token error: %v", err)
		}
		key := t.(string)
		keyLower := strings.ToLower(key)

		// the root key should be RowRootNode
		if keyLower != RowRootNode {
			log.Error("JSON parser: invalid row-based JSON format, the key is not found", zap.String("key", key))
			return fmt.Errorf("JSON parser: invalid row-based JSON format, the key %s is not found", key)
		}

		// started by '['
		t, err = dec.Token()
		if err != nil {
			log.Error("JSON parser: read json token error", zap.Error(err))
			return fmt.Errorf("JSON parser: read json token error: %v", err)
		}

		if t != json.Delim('[') {
			log.Error("JSON parser: invalid row-based JSON format, rows list should begin with '['")
			return errors.New("JSON parser: invalid row-based JSON format, rows list should begin with '['")
		}

		// read buffer
		buf := make([]map[storage.FieldID]interface{}, 0, MinBufferSize)
		for dec.More() {
			var value interface{}
			if err := dec.Decode(&value); err != nil {
				log.Error("JSON parser: decode json value error", zap.Error(err))
				return fmt.Errorf("JSON parser: decode json value error: %v", err)
			}

			switch value.(type) {
			case map[string]interface{}:
				break
			default:
				log.Error("JSON parser: invalid JSON format, each row should be a key-value map")
				return errors.New("JSON parser: invalid JSON format, each row should be a key-value map")
			}

			row := make(map[storage.FieldID]interface{})
			stringMap := value.(map[string]interface{})
			for k, v := range stringMap {
				// if user provided redundant field, return error
				fieldID, ok := p.name2FieldID[k]
				if !ok {
					log.Error("JSON parser: the field is not defined in collection schema", zap.String("fieldName", k))
					return fmt.Errorf("JSON parser: the field '%s' is not defined in collection schema", k)
				}
				row[fieldID] = v
			}

			buf = append(buf, row)
			if len(buf) >= int(p.bufSize) {
				isEmpty = false
				if err = handler.Handle(buf); err != nil {
					log.Error("JSON parser: parse values error", zap.Error(err))
					return fmt.Errorf("JSON parser: parse values error: %v", err)
				}

				// clear the buffer
				buf = make([]map[storage.FieldID]interface{}, 0, MinBufferSize)
			}
		}

		// some rows in buffer not parsed, parse them
		if len(buf) > 0 {
			isEmpty = false
			if err = handler.Handle(buf); err != nil {
				log.Error("JSON parser: parse values error", zap.Error(err))
				return fmt.Errorf("JSON parser: parse values error: %v", err)
			}
		}

		// end by ']'
		t, err = dec.Token()
		if err != nil {
			log.Error("JSON parser: read json token error", zap.Error(err))
			return fmt.Errorf("JSON parser: read json token error: %v", err)
		}

		if t != json.Delim(']') {
			log.Error("JSON parser: invalid column-based JSON format, rows list should end with a ']'")
			return errors.New("JSON parser: invalid column-based JSON format, rows list should end with a ']'")
		}

		// outside context might be canceled(service stop, or future enhancement for canceling import task)
		if isCanceled(p.ctx) {
			log.Error("JSON parser: import task was canceled")
			return errors.New("JSON parser: import task was canceled")
		}

		// this break means we require the first node must be RowRootNode
		// once the RowRootNode is parsed, just finish
		break
	}

	if isEmpty {
		log.Error("JSON parser: row count is 0")
		return errors.New("JSON parser: row count is 0")
	}

	// send nil to notify the handler all have done
	return handler.Handle(nil)
}
