/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package types

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ParamReader reads typed FunctionChain params from schemapb.FunctionParamValue.
// It is shared by FunctionExpr factories and typed Operator factories.
type ParamReader struct {
	scope  string
	params map[string]*schemapb.FunctionParamValue
}

// NewParamReader creates a reader for typed FunctionChain params. The scope is
// used in error messages, typically the function or operator name.
func NewParamReader(scope string, params map[string]*schemapb.FunctionParamValue) ParamReader {
	return ParamReader{scope: scope, params: params}
}

func (r ParamReader) get(key string, required bool) (*schemapb.FunctionParamValue, bool, error) {
	value, ok := r.params[key]
	if !ok {
		if required {
			return nil, false, merr.WrapErrParameterInvalidMsg("%s: missing required parameter %q", r.scope, key)
		}
		return nil, false, nil
	}
	if value == nil || value.GetValue() == nil {
		return nil, false, merr.WrapErrParameterInvalidMsg("%s: parameter %q is unset", r.scope, key)
	}
	return value, true, nil
}

func (r ParamReader) String(key string, required bool) (string, error) {
	value, ok, err := r.get(key, required)
	if err != nil || !ok {
		return "", err
	}
	v, ok := value.GetValue().(*schemapb.FunctionParamValue_StringValue)
	if !ok {
		return "", merr.WrapErrParameterInvalidMsg("%s: parameter %q must be a string", r.scope, key)
	}
	return v.StringValue, nil
}

func (r ParamReader) Int64(key string, required bool, defaultVal int64) (int64, error) {
	value, ok, err := r.get(key, required)
	if err != nil {
		return 0, err
	}
	if !ok {
		return defaultVal, nil
	}
	switch v := value.GetValue().(type) {
	case *schemapb.FunctionParamValue_Int64Value:
		return v.Int64Value, nil
	case *schemapb.FunctionParamValue_DoubleValue:
		if float64(int64(v.DoubleValue)) != v.DoubleValue {
			return 0, merr.WrapErrParameterInvalidMsg("%s: parameter %q must be an integer, got %f", r.scope, key, v.DoubleValue)
		}
		return int64(v.DoubleValue), nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("%s: parameter %q must be an integer", r.scope, key)
	}
}

func (r ParamReader) Float64(key string, required bool, defaultVal float64) (float64, error) {
	value, ok, err := r.get(key, required)
	if err != nil {
		return 0, err
	}
	if !ok {
		return defaultVal, nil
	}
	switch v := value.GetValue().(type) {
	case *schemapb.FunctionParamValue_DoubleValue:
		return v.DoubleValue, nil
	case *schemapb.FunctionParamValue_Int64Value:
		return float64(v.Int64Value), nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("%s: parameter %q must be a number", r.scope, key)
	}
}

func (r ParamReader) Bool(key string, required bool, defaultVal bool) (bool, error) {
	value, ok, err := r.get(key, required)
	if err != nil {
		return false, err
	}
	if !ok {
		return defaultVal, nil
	}
	v, ok := value.GetValue().(*schemapb.FunctionParamValue_BoolValue)
	if !ok {
		return false, merr.WrapErrParameterInvalidMsg("%s: parameter %q must be a bool", r.scope, key)
	}
	return v.BoolValue, nil
}

func (r ParamReader) StringSlice(key string, required bool) ([]string, error) {
	value, ok, err := r.get(key, required)
	if err != nil || !ok {
		return nil, err
	}
	arrayValue, ok := value.GetValue().(*schemapb.FunctionParamValue_ArrayValue)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("%s: parameter %q must be a string array", r.scope, key)
	}
	values := arrayValue.ArrayValue.GetValues()
	result := make([]string, len(values))
	for i, item := range values {
		v, ok := item.GetValue().(*schemapb.FunctionParamValue_StringValue)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("%s: parameter %q[%d] must be a string", r.scope, key, i)
		}
		result[i] = v.StringValue
	}
	return result, nil
}

func (r ParamReader) Float64Slice(key string, required bool) ([]float64, error) {
	value, ok, err := r.get(key, required)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	arrayValue, ok := value.GetValue().(*schemapb.FunctionParamValue_ArrayValue)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("%s: parameter %q must be a number array", r.scope, key)
	}
	values := arrayValue.ArrayValue.GetValues()
	result := make([]float64, len(values))
	for i, item := range values {
		switch v := item.GetValue().(type) {
		case *schemapb.FunctionParamValue_DoubleValue:
			result[i] = v.DoubleValue
		case *schemapb.FunctionParamValue_Int64Value:
			result[i] = float64(v.Int64Value)
		default:
			return nil, merr.WrapErrParameterInvalidMsg("%s: parameter %q[%d] must be a number", r.scope, key, i)
		}
	}
	return result, nil
}

func (r ParamReader) KeyValuePairs() ([]*commonpb.KeyValuePair, error) {
	result := make([]*commonpb.KeyValuePair, 0, len(r.params))
	for key, value := range r.params {
		strValue, err := r.ParamValueToString(key, value)
		if err != nil {
			return nil, err
		}
		result = append(result, &commonpb.KeyValuePair{Key: key, Value: strValue})
	}
	return result, nil
}

func (r ParamReader) ParamValueToString(key string, value *schemapb.FunctionParamValue) (string, error) {
	if value == nil || value.GetValue() == nil {
		return "", merr.WrapErrParameterInvalidMsg("%s: parameter %q is unset", r.scope, key)
	}
	switch v := value.GetValue().(type) {
	case *schemapb.FunctionParamValue_StringValue:
		return v.StringValue, nil
	case *schemapb.FunctionParamValue_Int64Value:
		return strconv.FormatInt(v.Int64Value, 10), nil
	case *schemapb.FunctionParamValue_DoubleValue:
		return strconv.FormatFloat(v.DoubleValue, 'f', -1, 64), nil
	case *schemapb.FunctionParamValue_BoolValue:
		return strconv.FormatBool(v.BoolValue), nil
	default:
		return "", merr.WrapErrParameterInvalidMsg("%s: parameter %q must be scalar string/number/bool", r.scope, key)
	}
}
