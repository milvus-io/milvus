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

package transformio

import (
	"encoding/json"
	"math"
	"unicode/utf8"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A struct is a response payload object, not a collection field schema. Encode
// it with JSON's public wire type so SDKs can decode semantic highlight/model
// payloads while Arrow owns their temporary nested representation.
func encodeJSONOutput(values arrow.Array, row int) ([]byte, error) {
	value, err := jsonOutputValue(values, row)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, merr.WrapErrFunctionFailed(err, "cannot encode transform JSON output")
	}
	return data, nil
}

func validJSONOutput(data []byte) bool {
	return utf8.Valid(data) && json.Valid(data)
}

func jsonOutputValue(values arrow.Array, row int) (any, error) {
	if row < 0 || row >= values.Len() {
		return nil, merr.WrapErrFunctionFailedMsg("transform JSON output index is out of bounds")
	}
	if values.IsNull(row) {
		return nil, nil
	}
	if integer, ok := integerValue(values, row); ok {
		return integer, nil
	}
	if number, ok := floatValue(values, row); ok {
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return nil, merr.WrapErrFunctionFailedMsg("transform JSON output cannot contain a non-finite number")
		}
		return number, nil
	}
	switch v := values.(type) {
	case *array.Boolean:
		return v.Value(row), nil
	case *array.Uint8:
		return v.Value(row), nil
	case *array.Uint16:
		return v.Value(row), nil
	case *array.Uint32:
		return v.Value(row), nil
	case *array.Uint64:
		return v.Value(row), nil
	case *array.String:
		return jsonOutputString(v.Value(row))
	case *array.LargeString:
		return jsonOutputString(v.Value(row))
	case *array.Binary:
		return v.Value(row), nil // JSON's standard base64 string encoding.
	case *array.LargeBinary:
		return v.Value(row), nil
	case *array.FixedSizeBinary:
		return v.Value(row), nil
	case *array.Struct:
		fields := v.DataType().(*arrow.StructType).Fields()
		object := make(map[string]any, len(fields))
		for column, field := range fields {
			if !utf8.ValidString(field.Name) {
				return nil, merr.WrapErrFunctionFailedMsg("transform JSON object field name is not valid UTF-8")
			}
			if _, duplicate := object[field.Name]; duplicate {
				return nil, merr.WrapErrFunctionFailedMsg("transform JSON object has duplicate field %q", field.Name)
			}
			child, err := jsonOutputValue(v.Field(column), row)
			if err != nil {
				return nil, merr.Wrapf(err, "transform JSON object field %q", field.Name)
			}
			object[field.Name] = child
		}
		return object, nil
	case *array.List, *array.LargeList, *array.FixedSizeList:
		start, end, child, ok := listValues(values, row)
		if !ok || start < 0 || end < start || end > int64(child.Len()) {
			return nil, merr.WrapErrFunctionFailedMsg("transform JSON list offsets are invalid")
		}
		result := make([]any, end-start)
		for index := start; index < end; index++ {
			value, err := jsonOutputValue(child, int(index))
			if err != nil {
				return nil, err
			}
			result[index-start] = value
		}
		return result, nil
	default:
		return nil, merr.WrapErrFunctionFailedMsg("unsupported transform JSON Arrow type %s", values.DataType())
	}
}

func jsonOutputString(value string) (string, error) {
	if !utf8.ValidString(value) {
		return "", merr.WrapErrFunctionFailedMsg("transform JSON string is not valid UTF-8")
	}
	return value, nil
}
