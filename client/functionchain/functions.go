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
	"math"
	"reflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
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

// BoostScore produces an L0 boost score with no public column arguments.
// Set filter (string) and random_score (an object with optional field and int64
// seed entries) using WithParam. An omitted filter matches all candidates.
// Non-matching rows produce NULL; combine with $score using NumCombine and
// WithParam("null_policy", "skip") to preserve their original scores.
func BoostScore(weight float64) *Expr {
	return helper("boost_score").WithParam("weight", weight)
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
	case "boost_score":
		if len(e.Args) != 0 {
			return bad("does not accept public arguments")
		}
		if !numeric(e.Params["weight"]) {
			return bad("weight must be numeric")
		}
		weight := reflect.ValueOf(e.Params["weight"]).Convert(reflect.TypeOf(float64(0))).Float()
		if math.IsNaN(weight) || math.IsInf(weight, 0) || math.Abs(weight) > math.MaxFloat32 {
			return bad("weight must be a finite Float32 value")
		}
		if filter, exists := e.Params["filter"]; exists {
			if _, ok := filter.(string); !ok {
				return bad("filter must be a string")
			}
		}
		if random, exists := e.Params["random_score"]; exists {
			value, err := paramValue(random, 0)
			if err != nil {
				return bad("random_score: " + err.Error())
			}
			object := value.GetObjectValue()
			if object == nil {
				return bad("random_score must be an object")
			}
			for key, value := range object.Fields {
				switch key {
				case "field":
					if _, ok := value.Value.(*schemapb.FunctionParamValue_StringValue); !ok {
						return bad("random_score.field must be a string")
					}
				case "seed":
					if _, ok := value.Value.(*schemapb.FunctionParamValue_Int64Value); !ok {
						return bad("random_score.seed must be an int64")
					}
				default:
					return bad("unknown random_score parameter: " + key)
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
