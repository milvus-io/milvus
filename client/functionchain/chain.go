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

// Package functionchain builds Milvus function chains. Builders are mutable and
// are not safe for concurrent mutation. Validation happens in ProtoMessage or
// when the containing search/query request is built.
package functionchain

import (
	"fmt"
	"math"
	"reflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// Stage identifies where a chain executes.
type Stage = schemapb.FunctionChainStage

const (
	Unspecified = schemapb.FunctionChainStage_FunctionChainStageUnspecified
	Ingestion   = schemapb.FunctionChainStage_FunctionChainStageIngestion
	PreProcess  = schemapb.FunctionChainStage_FunctionChainStagePreProcess
	L0Rerank    = schemapb.FunctionChainStage_FunctionChainStageL0Rerank
	L1Rerank    = schemapb.FunctionChainStage_FunctionChainStageL1Rerank
	L2Rerank    = schemapb.FunctionChainStage_FunctionChainStageL2Rerank
	PostProcess = schemapb.FunctionChainStage_FunctionChainStagePostProcess
	QueryScan   = schemapb.FunctionChainStage_FunctionChainStageQueryScan
)

// ColumnRef distinguishes a field reference from a string literal.
type ColumnRef struct{ Name string }

func Col(name string) ColumnRef { return ColumnRef{Name: name} }

// Expr is a function invocation. Args accept ColumnRef or supported literals;
// Params accept literals only. NewExpr supports custom server functions.
type Expr struct {
	Name   string
	Args   []any
	Params map[string]any
	helper bool
}

func NewExpr(name string, args ...any) *Expr {
	return &Expr{Name: name, Args: args, Params: map[string]any{}}
}

func (e *Expr) WithParam(key string, value any) *Expr {
	if e.Params == nil {
		e.Params = map[string]any{}
	}
	e.Params[key] = value
	return e
}

func (e *Expr) ProtoMessage() (*schemapb.FunctionChainExpr, error) {
	if e == nil || e.Name == "" {
		return nil, fmt.Errorf("function expression name must not be empty")
	}
	if e.helper {
		if err := e.validateHelper(); err != nil {
			return nil, err
		}
	}
	params, err := paramMap(e.Params, 0)
	if err != nil {
		return nil, err
	}
	// PyMilvus normalizes num_combine weights to floating-point values.
	if e.helper && e.Name == "num_combine" {
		if weights, ok := e.Params["weights"]; ok {
			values := reflect.ValueOf(weights)
			normalized := make([]float64, values.Len())
			for i := range normalized {
				v := reflect.ValueOf(values.Index(i).Interface())
				normalized[i] = v.Convert(reflect.TypeOf(float64(0))).Float()
			}
			params["weights"], err = paramValue(normalized, 0)
			if err != nil {
				return nil, err
			}
		}
	}
	p := &schemapb.FunctionChainExpr{Name: e.Name, Params: params}
	for _, a := range e.Args {
		arg := &schemapb.FunctionChainExprArg{}
		if col, ok := a.(ColumnRef); ok {
			if col.Name == "" {
				return nil, fmt.Errorf("column name must not be empty")
			}
			arg.Arg = &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: col.Name}}
		} else {
			v, err := paramValue(a, 0)
			if err != nil {
				return nil, err
			}
			arg.Arg = &schemapb.FunctionChainExprArg_Literal{Literal: v}
		}
		p.Args = append(p.Args, arg)
	}
	return p, nil
}

// Op is an extensible pipeline operation.
type Op struct {
	Op              string
	Expr            *Expr
	Inputs, Outputs []string
	Params          map[string]any
}

func (o Op) ProtoMessage() (*schemapb.FunctionChainOp, error) {
	if o.Op == "" {
		return nil, fmt.Errorf("operation name must not be empty")
	}
	params, err := paramMap(o.Params, 0)
	if err != nil {
		return nil, err
	}
	p := &schemapb.FunctionChainOp{Op: o.Op, Inputs: append([]string(nil), o.Inputs...), Outputs: append([]string(nil), o.Outputs...), Params: params}
	if o.Expr != nil {
		p.Expr, err = o.Expr.ProtoMessage()
	}
	return p, err
}

// FunctionChain composes operations in insertion order.
type FunctionChain struct {
	stage         Stage
	name          string
	ops           []Op
	outputColumns []string
	err           error
}

func NewFunctionChain(stage Stage) *FunctionChain            { return &FunctionChain{stage: stage} }
func (c *FunctionChain) WithName(name string) *FunctionChain { c.name = name; return c }
func (c *FunctionChain) WithOutputColumns(columns ...string) *FunctionChain {
	c.outputColumns = append([]string(nil), columns...)
	return c
}

// OutputColumns returns a copy of the exported column names.
func (c *FunctionChain) OutputColumns() []string { return append([]string(nil), c.outputColumns...) }

// Ops returns a copy of the operation list. Expression and parameter values
// retain their builder references and must not be mutated concurrently.
func (c *FunctionChain) Ops() []Op                   { return append([]Op(nil), c.ops...) }
func (c *FunctionChain) Name() string                { return c.name }
func (c *FunctionChain) Stage() Stage                { return c.stage }
func (c *FunctionChain) Append(op Op) *FunctionChain { c.ops = append(c.ops, op); return c }
func (c *FunctionChain) fail(err error) *FunctionChain {
	if c.err == nil {
		c.err = err
	}
	return c
}

func (c *FunctionChain) Map(output string, expr *Expr) *FunctionChain {
	if output == "" || expr == nil {
		return c.fail(fmt.Errorf("map requires a non-empty output and an expression"))
	}
	return c.Append(Op{Op: "map", Expr: expr, Outputs: []string{output}})
}

// Sort defaults to descending order. SortBy specifies direction and a tie-break key.
func (c *FunctionChain) Sort(by any) *FunctionChain { return c.SortBy(by, true, nil) }

func (c *FunctionChain) SortBy(by any, desc bool, tieBreak any) *FunctionChain {
	name, err := columnName(by)
	if err != nil {
		return c.fail(err)
	}
	op := Op{Op: "sort", Inputs: []string{name}, Params: map[string]any{"column": name, "desc": desc}}
	if tieBreak != nil {
		name, err := columnName(tieBreak)
		if err != nil {
			return c.fail(err)
		}
		op.Inputs = append(op.Inputs, name)
		op.Params["tie_break_col"] = name
	}
	return c.Append(op)
}

func (c *FunctionChain) Limit(limit int, offset ...int) *FunctionChain {
	n := 0
	if len(offset) > 1 {
		return c.fail(fmt.Errorf("limit accepts at most one offset"))
	}
	if len(offset) == 1 {
		n = offset[0]
	}
	if limit <= 0 || n < 0 {
		return c.fail(fmt.Errorf("limit must be positive and offset non-negative"))
	}
	return c.Append(Op{Op: "limit", Params: map[string]any{"limit": limit, "offset": n}})
}

// MergeOptions preserves the distinction between omitted and explicit zero/false.
type MergeOptions struct {
	Weights   []float64
	NormScore *bool
	K         *int
}

func (c *FunctionChain) Merge(strategy string, options ...MergeOptions) *FunctionChain {
	if len(options) > 1 {
		return c.fail(fmt.Errorf("merge accepts at most one options value"))
	}
	o := MergeOptions{}
	if len(options) == 1 {
		o = options[0]
	}
	p := map[string]any{"strategy": strategy}
	if strategy == "rrf" && o.K == nil {
		p["k"] = 60
	}
	if (strategy == "weighted" || strategy == "max" || strategy == "sum" || strategy == "avg") && o.NormScore == nil {
		p["norm_score"] = true
	}
	if o.K != nil {
		p["k"] = *o.K
	}
	if o.NormScore != nil {
		p["norm_score"] = *o.NormScore
	}
	if o.Weights != nil {
		p["weights"] = o.Weights
	}
	return c.Append(Op{Op: "merge", Params: p})
}

// Grep forwards rg-style arguments unchanged. Its default output is "matches".
func (c *FunctionChain) Grep(value any, arguments string, output ...string) *FunctionChain {
	if c.stage != QueryScan {
		return c.fail(fmt.Errorf("grep requires QueryScan stage"))
	}
	name, err := columnName(value)
	if err != nil {
		return c.fail(err)
	}
	out := "matches"
	if len(output) > 1 {
		return c.fail(fmt.Errorf("grep accepts at most one output"))
	}
	if len(output) == 1 {
		out = output[0]
	}
	if out == "" {
		return c.fail(fmt.Errorf("grep output must not be empty"))
	}
	return c.Append(Op{Op: "grep", Inputs: []string{name}, Outputs: []string{out}, Params: map[string]any{"arguments": arguments}})
}

func (c *FunctionChain) ProtoMessage() (*schemapb.FunctionChain, error) {
	if c == nil {
		return nil, fmt.Errorf("function chain must not be nil")
	}
	if c.err != nil {
		return nil, c.err
	}
	if _, ok := schemapb.FunctionChainStage_name[int32(c.stage)]; !ok {
		return nil, fmt.Errorf("unknown function chain stage: %d", c.stage)
	}
	if len(c.outputColumns) > 0 && (c.stage != QueryScan || c.name == "") {
		return nil, fmt.Errorf("output columns require a named QueryScan chain")
	}
	seen := map[string]bool{}
	for _, s := range c.outputColumns {
		if s == "" || s == "_original_idx" || seen[s] {
			return nil, fmt.Errorf("invalid or duplicate output column: %q", s)
		}
		seen[s] = true
	}
	p := &schemapb.FunctionChain{Name: c.name, Stage: c.stage, OutputColumns: append([]string(nil), c.outputColumns...)}
	for i, o := range c.ops {
		op, err := o.ProtoMessage()
		if err != nil {
			return nil, fmt.Errorf("operation %d: %w", i, err)
		}
		p.Ops = append(p.Ops, op)
	}
	return p, nil
}

func columnName(v any) (string, error) {
	switch x := v.(type) {
	case string:
		if x != "" {
			return x, nil
		}
	case ColumnRef:
		if x.Name != "" {
			return x.Name, nil
		}
	}
	return "", fmt.Errorf("expected a non-empty column name or ColumnRef")
}

func paramMap(m map[string]any, depth int) (map[string]*schemapb.FunctionParamValue, error) {
	p := make(map[string]*schemapb.FunctionParamValue, len(m))
	for k, v := range m {
		if k == "" {
			return nil, fmt.Errorf("parameter name must not be empty")
		}
		value, err := paramValue(v, depth+1)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", k, err)
		}
		p[k] = value
	}
	return p, nil
}

func paramValue(value any, depth int) (*schemapb.FunctionParamValue, error) {
	if depth > 100 {
		return nil, fmt.Errorf("parameter nesting exceeds 100 levels (possibly cyclic)")
	}
	if value == nil {
		return nil, fmt.Errorf("nil function parameter is unsupported")
	}
	p := &schemapb.FunctionParamValue{}
	if b, ok := value.([]byte); ok {
		p.Value = &schemapb.FunctionParamValue_BytesValue{BytesValue: append([]byte(nil), b...)}
		return p, nil
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Bool:
		p.Value = &schemapb.FunctionParamValue_BoolValue{BoolValue: v.Bool()}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		p.Value = &schemapb.FunctionParamValue_Int64Value{Int64Value: v.Int()}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v.Uint() > math.MaxInt64 {
			return nil, fmt.Errorf("integer overflows int64")
		}
		p.Value = &schemapb.FunctionParamValue_Int64Value{Int64Value: int64(v.Uint())}
	case reflect.Float32, reflect.Float64:
		p.Value = &schemapb.FunctionParamValue_DoubleValue{DoubleValue: v.Float()}
	case reflect.String:
		p.Value = &schemapb.FunctionParamValue_StringValue{StringValue: v.String()}
	case reflect.Slice, reflect.Array:
		a := &schemapb.FunctionParamArray{}
		for i := 0; i < v.Len(); i++ {
			x, err := paramValue(v.Index(i).Interface(), depth+1)
			if err != nil {
				return nil, err
			}
			a.Values = append(a.Values, x)
		}
		p.Value = &schemapb.FunctionParamValue_ArrayValue{ArrayValue: a}
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("parameter object keys must be strings")
		}
		m := map[string]any{}
		it := v.MapRange()
		for it.Next() {
			m[it.Key().String()] = it.Value().Interface()
		}
		fields, err := paramMap(m, depth)
		if err != nil {
			return nil, err
		}
		p.Value = &schemapb.FunctionParamValue_ObjectValue{ObjectValue: &schemapb.FunctionParamObject{Fields: fields}}
	default:
		return nil, fmt.Errorf("unsupported function parameter type: %T", value)
	}
	return p, nil
}
