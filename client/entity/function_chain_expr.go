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

package entity

import (
	"math"
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

func columnArgs(cols []ColumnRef) []any {
	args := make([]any, len(cols))
	for i, col := range cols {
		args[i] = col
	}
	return args
}

// NewNumCombineExpr combines at least two numeric columns. Weighted mode
// requires WithParam("weights", []float64{...}) matching the column count.
func NewNumCombineExpr(mode string, cols ...ColumnRef) *FunctionChainExpr {
	return newBuiltinFunctionChainExpr("num_combine", columnArgs(cols)...).WithParam("mode", mode)
}

// NewDecayExpr defaults to offset=0 and decay=0.5. Override with WithParam.
func NewDecayExpr(value ColumnRef, function string, origin, scale float64) *FunctionChainExpr {
	return newBuiltinFunctionChainExpr("decay", value).WithParam("function", function).
		WithParam("origin", origin).WithParam("scale", scale).WithParam("offset", 0).WithParam("decay", 0.5)
}

func NewRoundDecimalExpr(value ColumnRef, decimal int) *FunctionChainExpr {
	return newBuiltinFunctionChainExpr("round_decimal", value).WithParam("decimal", decimal)
}

// NewRerankModelExpr runs on the server at L2. Configure provider/model options
// with WithParam; credentials are resolved by the server.
func NewRerankModelExpr(value ColumnRef, queries ...string) *FunctionChainExpr {
	return newBuiltinFunctionChainExpr("rerank_model", value).WithParam("queries", slices.Clone(queries))
}

// NewPyUDFExpr references a registered wheel FileResource. The server must
// enable the Python worker. Use MapOutputs for multiple output columns.
func NewPyUDFExpr(resourceName string, params map[string]any, inputs ...ColumnRef) *FunctionChainExpr {
	return newBuiltinFunctionChainExpr("py_udf", columnArgs(inputs)...).
		WithParam("resource_name", resourceName).WithParam("udf_params", params)
}

// validateBuiltinExpr validates only schema-independent public contracts.
// Resource availability, column types and provider configuration are server-owned.
func validateBuiltinExpr(pb *schemapb.FunctionChainExpr) error {
	name := pb.GetName()
	if !slices.Contains([]string{"num_combine", "decay", "round_decimal", "rerank_model", "xgboost", "py_udf"}, name) {
		return nil
	}
	for _, arg := range pb.Args {
		if arg.GetColumn() == nil {
			return merr.WrapErrParameterInvalidMsg("%s requires column arguments", name)
		}
	}
	if len(pb.Args) == 0 {
		return merr.WrapErrParameterInvalidMsg("%s requires input columns", name)
	}
	params := pb.Params
	switch name {
	case "num_combine":
		mode := params["mode"].GetStringValue()
		if len(pb.Args) < 2 || !slices.Contains([]string{"sum", "multiply", "min", "max", "avg", "weighted"}, mode) {
			return merr.WrapErrParameterInvalidMsg("num_combine requires at least two columns and a supported mode")
		}
		weights, hasWeights := params["weights"]
		if mode == "weighted" {
			if !hasWeights || weights.GetArrayValue() == nil || len(weights.GetArrayValue().Values) != len(pb.Args) {
				return merr.WrapErrParameterInvalidMsg("num_combine weights must match the column count")
			}
			for _, weight := range weights.GetArrayValue().Values {
				if _, ok := chainNumber(weight); !ok {
					return merr.WrapErrParameterInvalidMsg("num_combine weights must be finite numbers")
				}
			}
		} else if hasWeights {
			return merr.WrapErrParameterInvalidMsg("num_combine weights require weighted mode")
		}
	case "decay":
		if len(pb.Args) != 1 || !slices.Contains([]string{"gauss", "exp", "linear"}, params["function"].GetStringValue()) {
			return merr.WrapErrParameterInvalidMsg("decay requires one column and gauss, exp or linear function")
		}
		for _, key := range []string{"origin", "scale", "offset", "decay"} {
			if _, exists := params[key]; !exists && (key == "offset" || key == "decay") {
				continue
			}
			if _, ok := chainNumber(params[key]); !ok {
				return merr.WrapErrParameterInvalidMsg("decay %s must be a finite number", key)
			}
		}
	case "round_decimal":
		decimal, ok := params["decimal"].GetValue().(*schemapb.FunctionParamValue_Int64Value)
		if len(pb.Args) != 1 || !ok || decimal.Int64Value < 0 || decimal.Int64Value > 6 {
			return merr.WrapErrParameterInvalidMsg("round_decimal requires one column and decimal in [0, 6]")
		}
	case "rerank_model":
		queries := params["queries"].GetArrayValue().GetValues()
		if len(pb.Args) != 1 || len(queries) == 0 {
			return merr.WrapErrParameterInvalidMsg("rerank_model requires one column and non-empty queries")
		}
		for _, query := range queries {
			if query.GetStringValue() == "" {
				return merr.WrapErrParameterInvalidMsg("rerank_model queries must be non-empty strings")
			}
		}
	case "xgboost":
		output := params["output"].GetStringValue()
		if params["model_resource"].GetStringValue() == "" || !slices.Contains([]string{"", "default", "raw"}, output) {
			return merr.WrapErrParameterInvalidMsg("xgboost requires model_resource and output default or raw")
		}
	case "py_udf":
		if params["resource_name"].GetStringValue() == "" {
			return merr.WrapErrParameterInvalidMsg("py_udf requires resource_name")
		}
		if p, ok := params["udf_params"]; ok && p.GetObjectValue() == nil {
			return merr.WrapErrParameterInvalidMsg("py_udf udf_params must be an object")
		}
	}
	return nil
}

func chainNumber(p *schemapb.FunctionParamValue) (float64, bool) {
	var value float64
	switch v := p.GetValue().(type) {
	case *schemapb.FunctionParamValue_Int64Value:
		value = float64(v.Int64Value)
	case *schemapb.FunctionParamValue_DoubleValue:
		value = v.DoubleValue
	default:
		return 0, false
	}
	return value, !math.IsNaN(value) && !math.IsInf(value, 0)
}

// NewXGBoostExpr scores ordered feature columns using a UBJ FileResource.
// It supports L0, L1 and L2. WithParam("output", "raw") selects raw margins.
func NewXGBoostExpr(modelResource string, features ...ColumnRef) *FunctionChainExpr {
	return newBuiltinFunctionChainExpr("xgboost", columnArgs(features)...).
		WithParam("model_resource", modelResource).WithParam("output", "default")
}

// Generic expressions intentionally defer function-specific semantics to the
// server, matching PyMilvus FunctionChainExpr and permitting future parameters.
func newBuiltinFunctionChainExpr(name string, args ...any) *FunctionChainExpr {
	expr := NewFunctionChainExpr(name, args...)
	expr.validateBuiltin = true
	return expr
}
