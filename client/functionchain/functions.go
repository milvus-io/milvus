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

package functionchain

import (
	"fmt"
	"reflect"
)

func helper(name string, cols ...ColumnRef) *Expr {
	args := make([]any, len(cols))
	for i, c := range cols {
		args[i] = c
	}
	e := NewExpr(name, args...)
	e.helper = true
	return e
}

// NumCombine defaults to sum. Set mode and weights with WithParam.
func NumCombine(cols ...ColumnRef) *Expr {
	return helper("num_combine", cols...).WithParam("mode", "sum")
}

// Decay defaults to offset=0 and decay=0.5. WithParam overrides these defaults.
func Decay(value ColumnRef, function string, origin, scale any) *Expr {
	return helper("decay", value).WithParam("function", function).WithParam("origin", origin).WithParam("scale", scale).WithParam("offset", 0).WithParam("decay", 0.5)
}

func RoundDecimal(value ColumnRef, decimal int) *Expr {
	return helper("round_decimal", value).WithParam("decimal", decimal)
}

// XGBoost references a server-side UBJ FileResource; feature order is preserved.
// Set output="raw" with WithParam to request raw margin scores.
func XGBoost(modelResource string, features ...ColumnRef) *Expr {
	return helper("xgboost", features...).WithParam("model_resource", modelResource).WithParam("output", "default")
}

// RerankModel accepts provider-specific options through WithParam.
func RerankModel(value ColumnRef, queries []string) *Expr {
	return helper("rerank_model", value).WithParam("queries", queries)
}

func numeric(v any) bool {
	if v == nil {
		return false
	}
	switch reflect.TypeOf(v).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func oneOf(v any, values ...string) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	for _, x := range values {
		if s == x {
			return true
		}
	}
	return false
}

func (e *Expr) validateHelper() error {
	for _, a := range e.Args {
		c, ok := a.(ColumnRef)
		if !ok || c.Name == "" {
			return fmt.Errorf("%s arguments must be non-empty column references", e.Name)
		}
	}
	bad := func(s string) error { return fmt.Errorf("%s: %s", e.Name, s) }
	switch e.Name {
	case "num_combine":
		if len(e.Args) < 2 {
			return bad("requires at least two columns")
		}
		mode := e.Params["mode"]
		if !oneOf(mode, "sum", "multiply", "max", "min", "avg", "weighted") {
			return bad("unsupported mode")
		}
		weights, has := e.Params["weights"]
		if mode == "weighted" && !has {
			return bad("weighted mode requires weights")
		}
		if has {
			if mode != "weighted" {
				return bad("weights require weighted mode")
			}
			v := reflect.ValueOf(weights)
			if !v.IsValid() || (v.Kind() != reflect.Slice && v.Kind() != reflect.Array) || v.Len() != len(e.Args) {
				return bad("weights must match column count")
			}
			for i := 0; i < v.Len(); i++ {
				if !numeric(v.Index(i).Interface()) {
					return bad("weights must be numeric")
				}
			}
		}
	case "decay":
		if len(e.Args) != 1 {
			return bad("requires one column")
		}
		if !oneOf(e.Params["function"], "gauss", "exp", "linear") {
			return bad("unsupported function")
		}
		for _, k := range []string{"origin", "scale", "offset", "decay"} {
			if !numeric(e.Params[k]) {
				return bad(k + " must be numeric")
			}
		}
	case "round_decimal":
		if len(e.Args) != 1 {
			return bad("requires one column")
		}
		d, ok := e.Params["decimal"].(int)
		if !ok || d < 0 || d > 6 {
			return bad("decimal must be an integer in [0,6]")
		}
	case "xgboost":
		if len(e.Args) == 0 {
			return bad("requires at least one feature")
		}
		s, ok := e.Params["model_resource"].(string)
		if !ok || s == "" {
			return bad("model_resource must not be empty")
		}
		if !oneOf(e.Params["output"], "default", "raw") {
			return bad("unsupported output")
		}
	case "rerank_model":
		if len(e.Args) != 1 {
			return bad("requires one column")
		}
		q, ok := e.Params["queries"].([]string)
		if !ok || len(q) == 0 {
			return bad("queries must be a non-empty string slice")
		}
		for _, s := range q {
			if s == "" {
				return bad("queries must not contain empty strings")
			}
		}
	}
	return nil
}
