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
	"bytes"
	"math"
	"reflect"
	"slices"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

// FunctionChainStage identifies the server execution boundary. Reserved stages
// are representable, but each request API validates the stages it supports.
type FunctionChainStage = schemapb.FunctionChainStage

const (
	FunctionChainStageUnspecified = schemapb.FunctionChainStage_FunctionChainStageUnspecified
	FunctionChainStageIngestion   = schemapb.FunctionChainStage_FunctionChainStageIngestion
	FunctionChainStagePreProcess  = schemapb.FunctionChainStage_FunctionChainStagePreProcess
	FunctionChainStageL0Rerank    = schemapb.FunctionChainStage_FunctionChainStageL0Rerank
	FunctionChainStageL1Rerank    = schemapb.FunctionChainStage_FunctionChainStageL1Rerank
	FunctionChainStageL2Rerank    = schemapb.FunctionChainStage_FunctionChainStageL2Rerank
	FunctionChainStagePostProcess = schemapb.FunctionChainStage_FunctionChainStagePostProcess
	FunctionChainStageQueryScan   = schemapb.FunctionChainStage_FunctionChainStageQueryScan
)

const functionChainInputTypes = "$input_data_types"

// ColumnRef distinguishes a column from a literal string. JSON paths require
// an explicit Bool, Int64, Double or VarChar hint; ordinary columns omit it.
type ColumnRef struct {
	Name     string
	DataType FieldType
}

func Col(name string) ColumnRef { return ColumnRef{Name: name} }

// WithDataType returns a typed copy of the reference.
func (c ColumnRef) WithDataType(dataType FieldType) ColumnRef {
	c.DataType = dataType
	return c
}

func (c ColumnRef) validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return merr.WrapErrParameterInvalidMsg("function chain column name must not be empty")
	}
	if c.DataType != 0 {
		switch c.DataType {
		case FieldTypeBool, FieldTypeInt64, FieldTypeDouble, FieldTypeVarChar:
		default:
			return merr.WrapErrParameterInvalidMsg("unsupported function chain input data type: %s", c.DataType)
		}
		if strings.HasPrefix(c.Name, "$") && !strings.HasPrefix(c.Name, "$meta[") {
			return merr.WrapErrParameterInvalidMsg("system column %q does not accept a data type hint", c.Name)
		}
	}
	return nil
}

// FunctionChainExpr is a typed function invocation. Strings in Args are
// literals; use Col to reference fields. The server validates function semantics.
type FunctionChainExpr struct {
	Name            string
	Args            []any
	Params          map[string]any
	validateBuiltin bool
}

func NewFunctionChainExpr(name string, args ...any) *FunctionChainExpr {
	return &FunctionChainExpr{Name: name, Args: slices.Clone(args), Params: make(map[string]any)}
}

func (e *FunctionChainExpr) WithParam(name string, value any) *FunctionChainExpr {
	if e.Params == nil {
		e.Params = make(map[string]any)
	}
	e.Params[name] = value
	return e
}

func (e *FunctionChainExpr) ProtoMessage() (*schemapb.FunctionChainExpr, error) {
	if e == nil || strings.TrimSpace(e.Name) == "" {
		return nil, merr.WrapErrParameterInvalidMsg("function chain expression name must not be empty")
	}
	params, err := functionChainParams(e.Params, 0)
	if err != nil {
		return nil, err
	}
	pb := &schemapb.FunctionChainExpr{Name: e.Name, Params: params}
	for _, arg := range e.Args {
		if col, ok := arg.(ColumnRef); ok {
			if err := col.validate(); err != nil {
				return nil, err
			}
			pb.Args = append(pb.Args, &schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: col.Name}}})
		} else {
			value, err := functionChainParam(arg, 0)
			if err != nil {
				return nil, err
			}
			pb.Args = append(pb.Args, &schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Literal{Literal: value}})
		}
	}
	if e.validateBuiltin {
		if err := validateBuiltinExpr(pb); err != nil {
			return nil, err
		}
	}
	return pb, nil
}

// FunctionChainOp is an extensible operator. Typed input hints are encoded in
// Params on the wire, in column occurrence order (including repeated columns).
type FunctionChainOp struct {
	Op      string
	Expr    *FunctionChainExpr
	Inputs  []ColumnRef
	Outputs []string
	Params  map[string]any
}

func (op *FunctionChainOp) ProtoMessage() (*schemapb.FunctionChainOp, error) {
	if op == nil || strings.TrimSpace(op.Op) == "" {
		return nil, merr.WrapErrParameterInvalidMsg("function chain operator must not be nil or unnamed")
	}
	params, err := functionChainParams(op.Params, 0)
	if err != nil {
		return nil, err
	}
	pb := &schemapb.FunctionChainOp{Op: op.Op, Outputs: slices.Clone(op.Outputs), Params: params}
	for _, output := range op.Outputs {
		if strings.TrimSpace(output) == "" {
			return nil, merr.WrapErrParameterInvalidMsg("function chain output name must not be empty")
		}
	}
	refs := op.Inputs
	for _, input := range op.Inputs {
		pb.Inputs = append(pb.Inputs, input.Name)
	}
	if op.Expr != nil {
		pb.Expr, err = op.Expr.ProtoMessage()
		if err != nil {
			return nil, err
		}
		refs = nil
		for _, arg := range op.Expr.Args {
			if col, ok := arg.(ColumnRef); ok {
				refs = append(refs, col)
			}
		}
	}
	hints := make([]int64, len(refs))
	hasHints := false
	seen := make(map[string]FieldType)
	for i, col := range refs {
		if err := col.validate(); err != nil {
			return nil, err
		}
		if previous, ok := seen[col.Name]; ok && previous != col.DataType {
			return nil, merr.WrapErrParameterInvalidMsg("conflicting data type hints for column %q", col.Name)
		}
		seen[col.Name] = col.DataType
		hints[i] = int64(col.DataType)
		hasHints = hasHints || col.DataType != 0
	}
	if _, exists := params[functionChainInputTypes]; exists && hasHints {
		return nil, merr.WrapErrParameterInvalidMsg("explicit %s conflicts with typed column references", functionChainInputTypes)
	}
	if hasHints {
		pb.Params[functionChainInputTypes], err = functionChainParam(hints, 0)
		if err != nil {
			return nil, err
		}
	}
	return pb, nil
}

// FunctionChain is a mutable builder. ProtoMessage returns an independent
// snapshot and validates serialization; request options validate placement.
// Do not mutate a builder concurrently with request construction.
type FunctionChain struct {
	Name          string
	Stage         FunctionChainStage
	Ops           []*FunctionChainOp
	OutputColumns []string
}

func NewFunctionChain(stage FunctionChainStage) *FunctionChain { return &FunctionChain{Stage: stage} }
func (c *FunctionChain) WithName(name string) *FunctionChain   { c.Name = name; return c }
func (c *FunctionChain) WithOutputColumns(names ...string) *FunctionChain {
	c.OutputColumns = slices.Clone(names)
	return c
}

func (c *FunctionChain) AddOp(op *FunctionChainOp) *FunctionChain {
	c.Ops = append(c.Ops, op)
	return c
}

func (c *FunctionChain) Map(output string, expr *FunctionChainExpr) *FunctionChain {
	return c.MapOutputs(expr, output)
}

// MapOutputs supports functions such as py_udf that produce several columns.
func (c *FunctionChain) MapOutputs(expr *FunctionChainExpr, outputs ...string) *FunctionChain {
	return c.AddOp(&FunctionChainOp{Op: "map", Expr: expr, Outputs: slices.Clone(outputs)})
}

// Sort orders candidates by a column. An optional tie-break column stabilizes ties.
func (c *FunctionChain) Sort(by ColumnRef, desc bool, tieBreak ...ColumnRef) *FunctionChain {
	inputs := append([]ColumnRef{by}, tieBreak...)
	params := map[string]any{"column": by.Name, "desc": desc}
	if len(tieBreak) == 1 {
		params["tie_break_col"] = tieBreak[0].Name
	}
	return c.AddOp(&FunctionChainOp{Op: "sort", Inputs: inputs, Params: params})
}

// Limit trims each query's candidates. L0 does not support Limit; L1 limits
// each worker independently, while L2 limits the globally merged results.
func (c *FunctionChain) Limit(limit int64, offset int64) *FunctionChain {
	return c.AddOp(&FunctionChainOp{Op: "limit", Params: map[string]any{"limit": limit, "offset": offset}})
}

// Merge fuses HybridSearch inputs. Defaults match PyMilvus: k=60 for rrf,
// norm_score=true otherwise. params may override defaults and supply weights.
func (c *FunctionChain) Merge(strategy string, params map[string]any) *FunctionChain {
	merged := map[string]any{"strategy": strategy}
	if strategy == "rrf" {
		merged["k"] = int64(60)
	} else {
		merged["norm_score"] = true
	}
	for k, v := range params {
		merged[k] = v
	}
	return c.AddOp(&FunctionChainOp{Op: "merge", Params: merged})
}

// Grep selects Query candidates with an rg-style argument string. Matching
// syntax is validated by the server. Export output via WithOutputColumns.
func (c *FunctionChain) Grep(input ColumnRef, arguments, output string) *FunctionChain {
	return c.AddOp(&FunctionChainOp{Op: "grep", Inputs: []ColumnRef{input}, Outputs: []string{output}, Params: map[string]any{"arguments": arguments}})
}

func (c *FunctionChain) ProtoMessage() (*schemapb.FunctionChain, error) {
	if c == nil {
		return nil, merr.WrapErrParameterInvalidMsg("function chain must not be nil")
	}
	if _, ok := schemapb.FunctionChainStage_name[int32(c.Stage)]; !ok {
		return nil, merr.WrapErrParameterInvalidMsg("unknown function chain stage: %d", c.Stage)
	}
	if len(c.OutputColumns) > 0 && (c.Stage != FunctionChainStageQueryScan || strings.TrimSpace(c.Name) == "") {
		return nil, merr.WrapErrParameterInvalidMsg("output_columns requires a named QueryScan chain")
	}
	seen := make(map[string]bool)
	for _, name := range c.OutputColumns {
		if strings.TrimSpace(name) == "" || name == "_original_idx" || seen[name] {
			return nil, merr.WrapErrParameterInvalidMsg("invalid or duplicate output column %q", name)
		}
		seen[name] = true
	}
	pb := &schemapb.FunctionChain{Name: c.Name, Stage: c.Stage, OutputColumns: slices.Clone(c.OutputColumns)}
	for _, op := range c.Ops {
		p, err := op.ProtoMessage()
		if err != nil {
			return nil, err
		}
		switch p.Op {
		case "map":
			if p.Expr == nil || len(p.Outputs) == 0 {
				return nil, merr.WrapErrParameterInvalidMsg("map requires an expression and output columns")
			}
		case "sort":
			if len(p.Inputs) < 1 || len(p.Inputs) > 2 {
				return nil, merr.WrapErrParameterInvalidMsg("sort requires one column and at most one tie-break column")
			}
		case "limit":
			limit, ok := p.Params["limit"].GetValue().(*schemapb.FunctionParamValue_Int64Value)
			offset, offsetOK := p.Params["offset"].GetValue().(*schemapb.FunctionParamValue_Int64Value)
			if !ok || limit.Int64Value <= 0 || (p.Params["offset"] != nil && (!offsetOK || offset.Int64Value < 0)) {
				return nil, merr.WrapErrParameterInvalidMsg("limit must be positive and offset must be non-negative integers")
			}
		}
		pb.Ops = append(pb.Ops, p)
	}
	return pb, nil
}

// Recursive values are bounded to reject cycles and pathological nesting before
// protobuf marshaling. Unlike JSON encoding, this preserves int64 and raw bytes.
func functionChainParam(value any, depth int) (*schemapb.FunctionParamValue, error) {
	if depth > 64 {
		return nil, merr.WrapErrParameterInvalidMsg("function chain parameter nesting exceeds 64 levels (possibly cyclic)")
	}
	if value == nil {
		return nil, merr.WrapErrParameterInvalidMsg("function chain parameters do not support nil")
	}
	if data, ok := value.([]byte); ok {
		return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BytesValue{BytesValue: bytes.Clone(data)}}, nil
	}
	rv := reflect.ValueOf(value)
	pb := &schemapb.FunctionParamValue{}
	switch rv.Kind() {
	case reflect.Bool:
		pb.Value = &schemapb.FunctionParamValue_BoolValue{BoolValue: rv.Bool()}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		pb.Value = &schemapb.FunctionParamValue_Int64Value{Int64Value: rv.Int()}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if rv.Uint() > math.MaxInt64 {
			return nil, merr.WrapErrParameterInvalidMsg("function chain integer overflows int64")
		}
		pb.Value = &schemapb.FunctionParamValue_Int64Value{Int64Value: int64(rv.Uint())}
	case reflect.Float32, reflect.Float64:
		pb.Value = &schemapb.FunctionParamValue_DoubleValue{DoubleValue: rv.Float()}
	case reflect.String:
		pb.Value = &schemapb.FunctionParamValue_StringValue{StringValue: rv.String()}
	case reflect.Array, reflect.Slice:
		array := &schemapb.FunctionParamArray{}
		for i := 0; i < rv.Len(); i++ {
			item, err := functionChainParam(rv.Index(i).Interface(), depth+1)
			if err != nil {
				return nil, err
			}
			array.Values = append(array.Values, item)
		}
		pb.Value = &schemapb.FunctionParamValue_ArrayValue{ArrayValue: array}
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, merr.WrapErrParameterInvalidMsg("function chain object keys must be strings")
		}
		fields := make(map[string]*schemapb.FunctionParamValue, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			key := iter.Key().String()
			if key == "" {
				return nil, merr.WrapErrParameterInvalidMsg("function chain parameter names must not be empty")
			}
			item, err := functionChainParam(iter.Value().Interface(), depth+1)
			if err != nil {
				return nil, err
			}
			fields[key] = item
		}
		pb.Value = &schemapb.FunctionParamValue_ObjectValue{ObjectValue: &schemapb.FunctionParamObject{Fields: fields}}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported function chain parameter type: %T", value)
	}
	return pb, nil
}

func functionChainParams(params map[string]any, depth int) (map[string]*schemapb.FunctionParamValue, error) {
	value, err := functionChainParam(params, depth)
	if err != nil {
		return nil, err
	}
	return value.GetObjectValue().GetFields(), nil
}
