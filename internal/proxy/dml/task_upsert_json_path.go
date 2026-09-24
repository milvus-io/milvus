// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dml

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/fieldvalidator"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type jsonPathSegment struct {
	key   string
	index int
	isKey bool
}

// Keys use JSON string literals, so ["1"] and [1] are distinct targets.
func parseJSONReplacePath(path string) ([]jsonPathSegment, error) {
	var segments []jsonPathSegment
	for len(path) > 0 {
		if path[0] != '[' || len(path) < 3 || len(segments) >= 64 {
			return nil, merr.WrapErrParameterInvalidMsg("invalid JSON replacement path (maximum depth 64)")
		}
		path = path[1:]
		segment := jsonPathSegment{}
		if path[0] == '"' {
			decoder := json.NewDecoder(strings.NewReader(path))
			if err := decoder.Decode(&segment.key); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("invalid JSON path key")
			}
			if !validJSONPathKey(path[:decoder.InputOffset()]) {
				return nil, merr.WrapErrParameterInvalidMsg("JSON path key must contain valid Unicode")
			}
			segment.isKey = true
			path = path[decoder.InputOffset():]
		} else {
			end := strings.IndexByte(path, ']')
			if end < 1 {
				return nil, merr.WrapErrParameterInvalidMsg("invalid JSON path index")
			}
			index := path[:end]
			for _, ch := range index {
				if ch < '0' || ch > '9' {
					return nil, merr.WrapErrParameterInvalidMsg("JSON path indexes must be non-negative integers")
				}
			}
			value, err := strconv.Atoi(index)
			if err != nil || len(index) > 1 && index[0] == '0' {
				return nil, merr.WrapErrParameterInvalidMsg("invalid JSON path index")
			}
			segment.index = value
			path = path[end:]
		}
		if len(path) == 0 || path[0] != ']' {
			return nil, merr.WrapErrParameterInvalidMsg("JSON path segment must end with ]")
		}
		path = path[1:]
		segments = append(segments, segment)
	}
	if len(segments) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("JSON replacement path is required")
	}
	return segments, nil
}

// encoding/json substitutes U+FFFD for lone surrogates. A path must reject
// them instead of silently targeting a different object's key.
func validJSONPathKey(literal string) bool {
	if !utf8.ValidString(literal) {
		return false
	}
	for i := 1; i < len(literal)-1; i++ {
		if literal[i] != '\\' {
			continue
		}
		i++
		if literal[i] != 'u' {
			continue
		}
		code, _ := strconv.ParseUint(literal[i+1:i+5], 16, 16) // JSON syntax was already validated.
		i += 4
		if code >= 0xdc00 && code <= 0xdfff {
			return false
		}
		if code >= 0xd800 && code <= 0xdbff {
			if i+6 >= len(literal) || literal[i+1:i+3] != `\u` {
				return false
			}
			low, _ := strconv.ParseUint(literal[i+3:i+7], 16, 16)
			if low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}

func validateJSONReplaceOperand(field *schemapb.FieldData, rows int) error {
	if field.GetType() != schemapb.DataType_JSON || field.GetScalars().GetJsonData() == nil || len(field.GetScalars().GetJsonData().GetData()) != rows {
		return merr.WrapErrParameterInvalidMsg("JSON PATH_REPLACE requires one JSON value per entity")
	}
	if err := validateNonNullOperandRows(field, rows); err != nil {
		return err
	}
	for _, value := range field.GetScalars().GetJsonData().GetData() {
		if !json.Valid(value) || !utf8.Valid(value) {
			return merr.WrapErrParameterInvalidMsg("invalid JSON replacement value")
		}
	}
	return nil
}

// jsonPathOutput checks every write before bytes.Buffer can grow. One output is
// shared by the whole row, so duplicate keys and nested matches consume the same
// budget without constructing intermediate replacement documents.
type jsonPathOutput struct {
	buffer    bytes.Buffer
	maxLength int64
}

func (out *jsonPathOutput) Write(value []byte) (int, error) {
	if int64(len(value)) > out.maxLength-int64(out.buffer.Len()) {
		return 0, merr.WrapErrParameterInvalidMsg("JSON PATH_REPLACE result exceeds max length (%d)", out.maxLength)
	}
	return out.buffer.Write(value)
}

func (out *jsonPathOutput) writeByte(value byte) error {
	_, err := out.Write([]byte{value})
	return err
}

func replaceJSONPath(value json.RawMessage, path []jsonPathSegment, replacement json.RawMessage, maxLength int64) (json.RawMessage, error) {
	if !json.Valid(value) || !json.Valid(replacement) {
		return nil, merr.WrapErrServiceInternalMsg("malformed JSON passed to path replacement")
	}
	output := &jsonPathOutput{maxLength: maxLength}
	if err := writeJSONValue(output, value, path, replacement); err != nil {
		return nil, err
	}
	return output.buffer.Bytes(), nil
}

// writeJSONValue traverses validated JSON without converting objects to maps.
// Every duplicate key matches, including intermediate keys. Preserve all members
// and update every matching branch; any branch error discards the entire result.
// RawMessage preserves numbers and untouched values without HTML re-encoding.
func writeJSONValue(output *jsonPathOutput, value json.RawMessage, path []jsonPathSegment, replacement json.RawMessage) error {
	if len(path) == 0 {
		_, err := output.Write(replacement)
		return err
	}
	value = bytes.TrimSpace(value)
	segment := path[0]
	if segment.isKey && value[0] != '{' {
		return merr.WrapErrParameterInvalidMsg("JSON path requires an existing object parent")
	}
	if !segment.isKey && value[0] != '[' {
		return merr.WrapErrParameterInvalidMsg("JSON path requires an existing array parent")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	_, _ = decoder.Token() // Opening delimiter; the complete value is validated.
	if err := output.writeByte(value[0]); err != nil {
		return err
	}
	found := false
	count := 0
	for decoder.More() {
		match := count == segment.index
		if count > 0 {
			if err := output.writeByte(','); err != nil {
				return err
			}
		}
		if segment.isKey {
			start := decoder.InputOffset()
			token, err := decoder.Token()
			if err != nil {
				return merr.WrapErrServiceInternalErr(err, "decode JSON member key")
			}
			key := token.(string) // Object keys in validated JSON are strings.
			match = key == segment.key
			// Token also consumes the preceding comma and whitespace. Copy the
			// original key literal: re-encoding can expand U+2028/U+2029 even
			// with SetEscapeHTML(false), making an otherwise valid update too large.
			if _, err := output.Write(bytes.TrimLeft(value[start:decoder.InputOffset()], ", \t\r\n")); err != nil {
				return err
			}
			if err := output.writeByte(':'); err != nil {
				return err
			}
		}
		var child json.RawMessage
		if err := decoder.Decode(&child); err != nil {
			return merr.WrapErrServiceInternalErr(err, "decode JSON member value")
		}
		if match {
			found = true
			if err := writeJSONValue(output, child, path[1:], replacement); err != nil {
				return err
			}
		} else if _, err := output.Write(child); err != nil {
			return err
		}
		count++
	}
	if !found {
		if !segment.isKey {
			return merr.WrapErrParameterInvalidMsg("JSON path index is out of range")
		}
		if len(path) > 1 {
			return merr.WrapErrParameterInvalidMsg("JSON intermediate path key is missing")
		}
		if count > 0 {
			if err := output.writeByte(','); err != nil {
				return err
			}
		}
		// Only a missing key needs encoding; existing keys retain their bytes.
		encoder := json.NewEncoder(output)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(segment.key); err != nil {
			return err
		}
		// Reuse Encode's trailing LF as the colon: this byte was already
		// checked by Write, and no temporary byte consumes the output budget.
		output.buffer.Bytes()[output.buffer.Len()-1] = ':'
		if _, err := output.Write(replacement); err != nil {
			return err
		}
	}
	return output.writeByte(value[len(value)-1])
}

func materializeJSONPathReplace(dst, old, operand *schemapb.FieldData, path []jsonPathSegment, dataIndices, rowIndices, operandIndices []int64) error {
	if old.GetType() != schemapb.DataType_JSON || old.GetScalars().GetJsonData() == nil || len(dataIndices) != len(rowIndices) || len(dataIndices) != len(operandIndices) {
		return merr.WrapErrServiceInternalMsg("malformed retrieved JSON field or row mapping")
	}
	values := make([][]byte, len(dataIndices))
	valid := typeutil.GetFieldDataValidData(old)
	maxLength := paramtable.Get().CommonCfg.JSONMaxLength.GetAsInt64()
	for i, index := range dataIndices {
		row := rowIndices[i]
		if len(valid) != 0 {
			if row < 0 || row >= int64(len(valid)) {
				return merr.WrapErrServiceInternalMsg("malformed retrieved JSON validity")
			}
			if !valid[row] {
				return merr.WrapErrParameterInvalidMsg("PATH_REPLACE cannot target null parent JSON field")
			}
		}
		if index < 0 || index >= int64(len(old.GetScalars().GetJsonData().GetData())) || operandIndices[i] < 0 || operandIndices[i] >= int64(len(operand.GetScalars().GetJsonData().GetData())) {
			return merr.WrapErrServiceInternalMsg("JSON PATH_REPLACE row mapping is out of range")
		}
		value := old.GetScalars().GetJsonData().GetData()[index]
		if !json.Valid(value) || !utf8.Valid(value) {
			return merr.WrapErrServiceInternalMsg("malformed retrieved JSON value")
		}
		updated, err := replaceJSONPath(value, path, operand.GetScalars().GetJsonData().GetData()[operandIndices[i]], maxLength)
		if err != nil {
			return merr.Wrapf(err, "PATH_REPLACE field %q row %d", old.GetFieldName(), i)
		}
		if err := fieldvalidator.CheckJSONDepth(old.GetFieldName(), updated); err != nil {
			return merr.Wrapf(err, "PATH_REPLACE row %d", i)
		}
		values[i] = updated
	}
	// Do not publish even a partial destination until every row passes validation.
	typeutil.AppendFieldDataByColumn(dst, old, dataIndices)
	dst.GetScalars().GetJsonData().Data = values
	return nil
}
