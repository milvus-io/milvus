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

package expr

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// =============================================================================
// BaseExpr
// =============================================================================

// BaseExpr is a base struct that can be embedded in expression implementations.
// It provides common functionality for all expression types.
type BaseExpr struct {
	name          string               // function name
	supportStages typeutil.Set[string] // supported stages
}

// NewBaseExpr creates a new BaseExpr with the given function name.
func NewBaseExpr(name string, supportStages []string) *BaseExpr {
	return &BaseExpr{
		name:          name,
		supportStages: typeutil.NewSet(supportStages...),
	}
}

// Name returns the function name.
func (b *BaseExpr) Name() string {
	return b.name
}

// IsRunnable returns whether the function is runnable in the given stage.
func (b *BaseExpr) IsRunnable(stage string) bool {
	if b.supportStages == nil || b.supportStages.Len() == 0 {
		return true
	}
	return b.supportStages.Contain(stage)
}

// =============================================================================
// Parameter Parsing Functions
// =============================================================================

// GetStringParam extracts a string parameter from the params map.
// funcName is used for error message prefixing.
func GetStringParam(params map[string]interface{}, funcName, key string, required bool) (string, error) {
	val, ok := params[key]
	if !ok {
		if required {
			return "", fmt.Errorf("%s: missing required parameter %q", funcName, key)
		}
		return "", nil
	}

	strVal, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("%s: parameter %q must be a string, got %T", funcName, key, val)
	}
	return strVal, nil
}

// GetFloat64Param extracts a float64 parameter from the params map.
// funcName is used for error message prefixing.
func GetFloat64Param(params map[string]interface{}, funcName, key string, required bool, defaultVal float64) (float64, error) {
	val, ok := params[key]
	if !ok {
		if required {
			return 0, fmt.Errorf("%s: missing required parameter %q", funcName, key)
		}
		return defaultVal, nil
	}

	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("%s: parameter %q must be a number, got %T", funcName, key, val)
	}
}

// ParseStringSliceParam extracts a string slice parameter from the params map.
// funcName is used for error message prefixing.
func ParseStringSliceParam(params map[string]interface{}, funcName, key string) ([]string, error) {
	val, ok := params[key]
	if !ok {
		return nil, fmt.Errorf("%s: missing required parameter %q", funcName, key)
	}

	switch v := val.(type) {
	case []string:
		return v, nil
	case []interface{}:
		result := make([]string, len(v))
		for i, item := range v {
			str, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%s: parameter %q[%d] must be a string, got %T", funcName, key, i, item)
			}
			result[i] = str
		}
		return result, nil
	default:
		return nil, fmt.Errorf("%s: parameter %q must be a string array, got %T", funcName, key, val)
	}
}

// ParseFloat64SliceParam extracts a float64 slice parameter from the params map.
// funcName is used for error message prefixing.
// Returns nil if the parameter is not present (optional parameter).
func ParseFloat64SliceParam(params map[string]interface{}, funcName, key string) ([]float64, error) {
	val, ok := params[key]
	if !ok {
		return nil, nil // optional, return nil if not present
	}

	switch v := val.(type) {
	case []float64:
		return v, nil
	case []interface{}:
		result := make([]float64, len(v))
		for i, item := range v {
			switch num := item.(type) {
			case float64:
				result[i] = num
			case float32:
				result[i] = float64(num)
			case int:
				result[i] = float64(num)
			case int64:
				result[i] = float64(num)
			case int32:
				result[i] = float64(num)
			default:
				return nil, fmt.Errorf("%s: parameter %q[%d] must be a number, got %T", funcName, key, i, item)
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("%s: parameter %q must be a number array, got %T", funcName, key, val)
	}
}

// =============================================================================
// Numeric Value Extraction Functions
// =============================================================================

// GetNumericValue extracts a float64 value from a numeric Arrow array at the given index.
// This function supports Int8, Int16, Int32, Int64, Float32, and Float64 types.
func GetNumericValue(arr arrow.Array, idx int) (float64, error) {
	switch a := arr.(type) {
	case *array.Int8:
		return float64(a.Value(idx)), nil
	case *array.Int16:
		return float64(a.Value(idx)), nil
	case *array.Int32:
		return float64(a.Value(idx)), nil
	case *array.Int64:
		return float64(a.Value(idx)), nil
	case *array.Float32:
		return float64(a.Value(idx)), nil
	case *array.Float64:
		return a.Value(idx), nil
	default:
		return 0, fmt.Errorf("unsupported input column type %T, expected numeric type", arr)
	}
}
