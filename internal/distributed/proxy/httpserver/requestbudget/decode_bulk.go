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

package requestbudget

import (
	"bufio"
	"context"
	"encoding/json"
	"io"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MaxJSONUnitBytes bounds one non-bulk JSON value or one row of bulk data.
// The capacity limit is separate from the elapsed-time request budget.
const MaxJSONUnitBytes = 4 << 20

// MaxJSONBodyBytes is a provisional draft guard. Benchmark and compatibility
// evidence are required before this value can be treated as a production default.
const MaxJSONBodyBytes = 256 << 20

// DecodeBulkJSON frames a REST object without retaining its whole body. The
// data array is delivered one bounded row at a time; the returned object has a
// small data placeholder so the existing Gin metadata validation still runs.
// The callback may retain raw rows for schema-sensitive conversion.
func DecodeBulkJSON(ctx context.Context, body io.Reader, maxUnitBytes, maxBodyBytes int, onRow func([]byte) error) ([]byte, error) {
	if maxUnitBytes <= 0 || maxBodyBytes <= 0 || onRow == nil {
		return nil, merr.WrapErrServiceInternalMsg("invalid REST JSON decoder policy")
	}
	scanner := &bulkJSONScanner{
		ctx:     ctx,
		reader:  bufio.NewReaderSize(body, 4096),
		maxBody: int64(maxBodyBytes),
		maxUnit: maxUnitBytes,
	}
	return scanner.decode(onRow)
}

type bulkJSONScanner struct {
	ctx     context.Context
	reader  *bufio.Reader
	read    int64
	maxBody int64
	maxUnit int
}

func (s *bulkJSONScanner) nextByte() (byte, error) {
	if s.read&4095 == 0 {
		if err := s.ctx.Err(); err != nil {
			return 0, err
		}
	}
	value, err := s.reader.ReadByte()
	if err != nil {
		return 0, err
	}
	s.read++
	if s.read > s.maxBody {
		return 0, merr.WrapErrParameterTooLarge("REST JSON request body")
	}
	return value, nil
}

func (s *bulkJSONScanner) nextNonSpace() (byte, error) {
	for {
		value, err := s.nextByte()
		if err != nil {
			return 0, err
		}
		switch value {
		case ' ', '\n', '\r', '\t':
		default:
			return value, nil
		}
	}
}

func (s *bulkJSONScanner) requiredNonSpace() (byte, error) {
	value, err := s.nextNonSpace()
	if err == io.EOF {
		return 0, merr.WrapErrParameterInvalidMsg("incomplete REST JSON request body")
	}
	return value, err
}

func (s *bulkJSONScanner) decode(onRow func([]byte) error) ([]byte, error) {
	first, err := s.requiredNonSpace()
	if err != nil {
		return nil, err
	}
	if first != '{' {
		return nil, merr.WrapErrParameterInvalidMsg("REST JSON request body must be an object")
	}
	metadata := []byte{'{'}
	fields := 0
	dataFound := false
	afterComma := false
	for {
		first, err = s.requiredNonSpace()
		if err != nil {
			return nil, err
		}
		if first == '}' {
			if afterComma {
				return nil, merr.WrapErrParameterInvalidMsg("REST JSON object has a trailing comma")
			}
			break
		}
		if first != '"' {
			return nil, merr.WrapErrParameterInvalidMsg("REST JSON object field must have a string key")
		}
		keyRaw, err := s.frameValue(first)
		if err != nil {
			return nil, err
		}
		var key string
		if err := json.Unmarshal(keyRaw, &key); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("invalid REST JSON object key")
		}
		colon, err := s.requiredNonSpace()
		if err != nil {
			return nil, err
		}
		if colon != ':' {
			return nil, merr.WrapErrParameterInvalidMsg("REST JSON object field is missing ':'")
		}
		first, err = s.requiredNonSpace()
		if err != nil {
			return nil, err
		}
		if fields != 0 {
			metadata = append(metadata, ',')
		}
		fields++
		afterComma = false
		metadata = append(metadata, keyRaw...)
		metadata = append(metadata, ':')
		if key == "data" {
			if dataFound {
				return nil, merr.WrapErrParameterInvalidMsg("REST JSON object has duplicate data fields")
			}
			dataFound = true
			if first != '[' {
				return nil, merr.WrapErrParameterInvalidMsg("REST JSON data must be an array")
			}
			rows, err := s.decodeRows(onRow)
			if err != nil {
				return nil, err
			}
			if rows == 0 {
				metadata = append(metadata, "[]"...)
			} else {
				metadata = append(metadata, "[{}]"...)
			}
		} else {
			value, err := s.frameValue(first)
			if err != nil {
				return nil, err
			}
			metadata = append(metadata, value...)
		}
		if len(metadata)+1 > s.maxUnit {
			return nil, merr.WrapErrParameterTooLarge("REST JSON metadata")
		}
		separator, err := s.requiredNonSpace()
		if err != nil {
			return nil, err
		}
		if separator == '}' {
			break
		}
		if separator != ',' {
			return nil, merr.WrapErrParameterInvalidMsg("REST JSON object field must end with ',' or '}'")
		}
		afterComma = true
	}
	metadata = append(metadata, '}')
	if !json.Valid(metadata) {
		return nil, merr.WrapErrParameterInvalidMsg("invalid REST JSON request metadata")
	}
	if trailing, err := s.nextNonSpace(); err != io.EOF {
		if err != nil {
			return nil, err
		}
		_ = trailing
		return nil, merr.WrapErrParameterInvalidMsg("REST JSON request has trailing content")
	}
	return metadata, s.ctx.Err()
}

func (s *bulkJSONScanner) decodeRows(onRow func([]byte) error) (int, error) {
	rows := 0
	afterComma := false
	for {
		first, err := s.requiredNonSpace()
		if err != nil {
			return 0, err
		}
		if first == ']' {
			if afterComma {
				return 0, merr.WrapErrParameterInvalidMsg("REST JSON data array has a trailing comma")
			}
			return rows, nil
		}
		row, err := s.frameValue(first)
		if err != nil {
			return 0, err
		}
		if !json.Valid(row) {
			return 0, merr.WrapErrParameterInvalidMsg("invalid REST JSON data row")
		}
		if err := s.ctx.Err(); err != nil {
			return 0, err
		}
		if err := onRow(row); err != nil {
			return 0, err
		}
		if err := s.ctx.Err(); err != nil {
			return 0, err
		}
		rows++
		afterComma = false
		separator, err := s.requiredNonSpace()
		if err != nil {
			return 0, err
		}
		if separator == ']' {
			return rows, nil
		}
		if separator != ',' {
			return 0, merr.WrapErrParameterInvalidMsg("REST JSON data row must end with ',' or ']'")
		}
		afterComma = true
	}
}

func (s *bulkJSONScanner) frameValue(first byte) ([]byte, error) {
	value := make([]byte, 0, 256)
	appendByte := func(next byte) error {
		if len(value) >= s.maxUnit {
			return merr.WrapErrParameterTooLarge("REST JSON value")
		}
		value = append(value, next)
		return nil
	}
	if err := appendByte(first); err != nil {
		return nil, err
	}
	if first == '"' {
		escaped := false
		for {
			next, err := s.nextByte()
			if err != nil {
				return nil, s.valueReadError(err)
			}
			if err := appendByte(next); err != nil {
				return nil, err
			}
			if escaped {
				escaped = false
			} else if next == '\\' {
				escaped = true
			} else if next == '"' {
				return value, nil
			}
		}
	}
	if first == '{' || first == '[' {
		stack := []byte{'}'}
		if first == '[' {
			stack[0] = ']'
		}
		inString := false
		escaped := false
		for len(stack) != 0 {
			next, err := s.nextByte()
			if err != nil {
				return nil, s.valueReadError(err)
			}
			if err := appendByte(next); err != nil {
				return nil, err
			}
			if inString {
				if escaped {
					escaped = false
				} else if next == '\\' {
					escaped = true
				} else if next == '"' {
					inString = false
				}
				continue
			}
			switch next {
			case '"':
				inString = true
			case '{':
				stack = append(stack, '}')
			case '[':
				stack = append(stack, ']')
			case '}', ']':
				if next != stack[len(stack)-1] {
					return nil, merr.WrapErrParameterInvalidMsg("invalid REST JSON value nesting")
				}
				stack = stack[:len(stack)-1]
			}
		}
		return value, nil
	}
	for {
		next, err := s.reader.Peek(1)
		if err == io.EOF {
			return value, nil
		}
		if err != nil {
			return nil, err
		}
		switch next[0] {
		case ',', '}', ']', ' ', '\n', '\r', '\t':
			return value, nil
		}
		byteValue, err := s.nextByte()
		if err != nil {
			return nil, err
		}
		if err := appendByte(byteValue); err != nil {
			return nil, err
		}
	}
}

func (s *bulkJSONScanner) valueReadError(err error) error {
	if err == io.EOF {
		return merr.WrapErrParameterInvalidMsg("incomplete REST JSON value")
	}
	return err
}
