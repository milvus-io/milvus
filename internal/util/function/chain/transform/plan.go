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

package transform

import (
	"reflect"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	OpSelect  = "select"
	OpExplode = "explode"
	OpFlatMap = "flat_map"
)

// ExplodeOptions controls one-level list expansion. Empty/null lists produce
// zero rows unless Outer is true, in which case they produce one null element.
// Position names an optional nullable zero-based element ordinal column; it is
// null for a row produced by Outer. Parent columns are inherited by child rows.
type ExplodeOptions struct {
	Outer    bool
	Position string
}

// PreInsert builds an ingestion plan. It does not attach or execute it.
func PreInsert(name string, ops ...*schemapb.FunctionChainOp) (*schemapb.FunctionChain, error) {
	return newPlan(name, schemapb.FunctionChainStage_FunctionChainStageIngestion, ops)
}

// PreProcess builds a plan preserving input row identity and order.
func PreProcess(name string, ops ...*schemapb.FunctionChainOp) (*schemapb.FunctionChain, error) {
	return newPlan(name, schemapb.FunctionChainStage_FunctionChainStagePreProcess, ops)
}

// PostProcess builds a plan preserving input row identity and order.
func PostProcess(name string, ops ...*schemapb.FunctionChainOp) (*schemapb.FunctionChain, error) {
	return newPlan(name, schemapb.FunctionChainStage_FunctionChainStagePostProcess, ops)
}

func newPlan(name string, stage schemapb.FunctionChainStage, ops []*schemapb.FunctionChainOp) (*schemapb.FunctionChain, error) {
	plan := &schemapb.FunctionChain{Name: name, Stage: stage, Ops: ops}
	if err := ValidatePlan(plan); err != nil {
		return nil, err
	}
	return proto.Clone(plan).(*schemapb.FunctionChain), nil
}

// Select selects columns in input order and optionally renames them. Empty
// outputs means same-name projection. Runtime-produced struct paths are input
// references resolved by the runtime. Identity/lineage is out of band
// and cannot be removed by selecting columns.
func Select(inputs, outputs []string) (*schemapb.FunctionChainOp, error) {
	if len(outputs) == 0 {
		outputs = inputs
	}
	op := &schemapb.FunctionChainOp{
		Op: OpSelect, Inputs: append([]string(nil), inputs...), Outputs: append([]string(nil), outputs...),
	}
	if err := validateOp(op); err != nil {
		return nil, err
	}
	return op, nil
}

// Explode expands one existing List<T> or List<Struct> column. Each child
// inherits its parent columns; output replaces a same-name column. The executor
// preserves parent order and element order and enforces list types.
func Explode(input, output string, opts ExplodeOptions) (*schemapb.FunctionChainOp, error) {
	op := expansionOp(OpExplode, output, opts)
	op.Inputs = []string{input}
	if err := validateOp(op); err != nil {
		return nil, err
	}
	return op, nil
}

// FlatMap evaluates expr, then expands its single List<T>/List<Struct> output.
// The list's outer row count must equal the input row count; the offsets retain
// the parent-child relationship. It has map(output, expr) followed by
// explode(output, output) semantics, without executing either operation here.
func FlatMap(output string, expr *schemapb.FunctionChainExpr, opts ExplodeOptions) (*schemapb.FunctionChainOp, error) {
	op := expansionOp(OpFlatMap, output, opts)
	op.Expr = expr
	if err := validateOp(op); err != nil {
		return nil, err
	}
	return proto.Clone(op).(*schemapb.FunctionChainOp), nil
}

func expansionOp(opType, output string, opts ExplodeOptions) *schemapb.FunctionChainOp {
	op := &schemapb.FunctionChainOp{
		Op: opType, Outputs: []string{output},
		Params: map[string]*schemapb.FunctionParamValue{
			"outer": {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: opts.Outer}},
		},
	}
	if opts.Position != "" {
		op.Outputs = append(op.Outputs, opts.Position)
	}
	return op
}

// ValidatePlan validates only the v1 structural contract. It does not
// imply that the stage, operators or named expressions can execute on a server.
// Ingestion accepts map/filter/select/explode/flat_map; Pre/PostProcess accept
// map/select. Schema resolution and function stage/type checks are adapter-owned.
func ValidatePlan(plan *schemapb.FunctionChain) error {
	if plan == nil {
		return merr.WrapErrParameterMissingMsg("transform: function chain is required")
	}
	if err := validateStage(plan.Stage); err != nil {
		return err
	}
	if len(plan.Ops) == 0 {
		return merr.WrapErrParameterMissingMsg("transform: at least one operation is required")
	}
	for i, op := range plan.Ops {
		if err := validateOp(op); err != nil {
			return merr.Wrapf(err, "transform op[%d]", i)
		}
		if plan.Stage != schemapb.FunctionChainStage_FunctionChainStageIngestion && op.Op != "map" && op.Op != OpSelect {
			return merr.WrapErrParameterInvalidMsg("transform op[%d]: %s does not preserve rows for stage %s", i, op.Op, plan.Stage)
		}
	}
	return nil
}

func validateStage(stage schemapb.FunctionChainStage) error {
	switch stage {
	case schemapb.FunctionChainStage_FunctionChainStageIngestion,
		schemapb.FunctionChainStage_FunctionChainStagePreProcess,
		schemapb.FunctionChainStage_FunctionChainStagePostProcess:
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("transform: unsupported stage %s", stage)
	}
}

func validateOp(op *schemapb.FunctionChainOp) error {
	if op == nil {
		return merr.WrapErrParameterMissingMsg("operation is required")
	}
	if err := validateNames(op.Inputs, false); err != nil {
		return merr.Wrap(err, "operation inputs")
	}
	if err := validateNames(op.Outputs, true); err != nil {
		return merr.Wrap(err, "operation outputs")
	}
	if op.Op != OpSelect {
		for _, output := range op.Outputs {
			if strings.HasPrefix(strings.TrimSpace(output), "$") {
				return merr.WrapErrParameterInvalidMsg("transform cannot write reserved system column %q", output)
			}
		}
	}
	switch op.Op {
	case OpSelect:
		if op.Expr != nil || len(op.Inputs) == 0 || len(op.Outputs) != 0 && len(op.Inputs) != len(op.Outputs) {
			return merr.WrapErrParameterInvalidMsg("select requires non-empty inputs, optional equally sized outputs, and no expression")
		}
		for key, value := range op.Params {
			if key != "$input_data_types" {
				return merr.WrapErrParameterInvalidMsg("select: unsupported parameter %q", key)
			}
			if err := validateTypeHintCount(value, len(op.Inputs)); err != nil {
				return merr.Wrap(err, "select input data types")
			}
		}
		if len(op.Outputs) == 0 {
			if err := validateNames(op.Inputs, true); err != nil {
				return merr.Wrap(err, "select implicit outputs")
			}
		}
		for i, output := range op.Outputs {
			if strings.HasPrefix(strings.TrimSpace(output), "$") && output != op.Inputs[i] {
				return merr.WrapErrParameterInvalidMsg("select cannot rename a value into reserved system column %q", output)
			}
		}
	case OpExplode, OpFlatMap:
		if len(op.Outputs) < 1 || len(op.Outputs) > 2 {
			return merr.WrapErrParameterInvalidMsg("%s requires element output and optional position output", op.Op)
		}
		if op.Op == OpFlatMap {
			if err := validateExprOp(op); err != nil {
				return err
			}
		}
		for key, value := range op.Params {
			if op.Op == OpFlatMap && key == "$input_data_types" {
				if err := validateInputDataTypes(value, op.Expr.Args); err != nil {
					return merr.Wrap(err, "input data types")
				}
				continue
			}
			if key != "outer" || value == nil {
				return merr.WrapErrParameterInvalidMsg("%s: unsupported parameter %q", op.Op, key)
			}
			if err := validateValue(value, 0); err != nil {
				return merr.Wrap(err, "expansion outer parameter")
			}
			if _, ok := value.Value.(*schemapb.FunctionParamValue_BoolValue); !ok {
				return merr.WrapErrParameterInvalidMsg("%s: outer must be bool", op.Op)
			}
		}
		if op.Op == OpExplode {
			if op.Expr != nil || len(op.Inputs) != 1 {
				return merr.WrapErrParameterInvalidMsg("explode requires one input and no expression")
			}
		}
	case "map", "filter":
		if err := validateExprOp(op); err != nil {
			return err
		}
		if op.Op == "map" && len(op.Outputs) == 0 || op.Op == "filter" && len(op.Outputs) != 0 {
			return merr.WrapErrParameterInvalidMsg("%s has invalid output count", op.Op)
		}
		for key, value := range op.Params {
			if key != "$input_data_types" {
				return merr.WrapErrParameterInvalidMsg("%s: unsupported parameter %q", op.Op, key)
			}
			if err := validateInputDataTypes(value, op.Expr.Args); err != nil {
				return merr.Wrap(err, "input data types")
			}
		}
	case "aggregate", "reduce":
		return merr.WrapErrParameterInvalidMsg("%s is reserved and not supported by the v1 transform contract", op.Op)
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported transform operation %q", op.Op)
	}
	return nil
}

func validateNames(names []string, unique bool) error {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		normalized := strings.TrimSpace(name)
		if normalized == "" {
			return merr.WrapErrParameterInvalidMsg("column name must not be empty")
		}
		if _, found := seen[normalized]; unique && found {
			return merr.WrapErrParameterInvalidMsg("duplicate output column %q", name)
		}
		seen[normalized] = struct{}{}
	}
	return nil
}

func validateExprOp(op *schemapb.FunctionChainOp) error {
	if op.Expr == nil {
		return merr.WrapErrParameterMissingMsg("%s requires an expression", op.Op)
	}
	if len(op.Inputs) != 0 {
		return merr.WrapErrParameterInvalidMsg("%s reads expression arguments; explicit inputs are not allowed", op.Op)
	}
	if strings.TrimSpace(op.Expr.Name) == "" {
		return merr.WrapErrParameterInvalidMsg("expression name must not be empty")
	}
	columns := 0
	for _, arg := range op.Expr.Args {
		if arg == nil || isNilPointer(arg.Arg) {
			return merr.WrapErrParameterInvalidMsg("expression argument must not be nil")
		}
		switch value := arg.Arg.(type) {
		case *schemapb.FunctionChainExprArg_Column:
			if value.Column == nil || strings.TrimSpace(value.Column.Name) == "" {
				return merr.WrapErrParameterInvalidMsg("expression column must not be empty")
			}
			columns++
		case *schemapb.FunctionChainExprArg_Literal:
			if err := validateValue(value.Literal, 0); err != nil {
				return merr.Wrap(err, "expression literal")
			}
		default:
			return merr.WrapErrParameterInvalidMsg("expression argument is unset")
		}
	}
	if columns == 0 {
		return merr.WrapErrParameterMissingMsg("%s requires at least one expression input column", op.Op)
	}
	for key, value := range op.Expr.Params {
		if strings.TrimSpace(key) == "" {
			return merr.WrapErrParameterInvalidMsg("expression parameter name must not be empty")
		}
		if err := validateValue(value, 0); err != nil {
			return merr.Wrapf(err, "expression parameter %q", key)
		}
	}
	return nil
}

func validateValue(value *schemapb.FunctionParamValue, depth int) error {
	if value == nil || isNilPointer(value.Value) || depth > 64 {
		return merr.WrapErrParameterInvalidMsg("parameter is nil or exceeds 64 nested levels")
	}
	switch v := value.Value.(type) {
	case *schemapb.FunctionParamValue_BoolValue, *schemapb.FunctionParamValue_Int64Value,
		*schemapb.FunctionParamValue_DoubleValue, *schemapb.FunctionParamValue_StringValue,
		*schemapb.FunctionParamValue_BytesValue:
		return nil
	case *schemapb.FunctionParamValue_ArrayValue:
		if v.ArrayValue != nil {
			for _, item := range v.ArrayValue.Values {
				if err := validateValue(item, depth+1); err != nil {
					return err
				}
			}
			return nil
		}
	case *schemapb.FunctionParamValue_ObjectValue:
		if v.ObjectValue != nil {
			for key, item := range v.ObjectValue.Fields {
				if strings.TrimSpace(key) == "" {
					return merr.WrapErrParameterInvalidMsg("object parameter name must not be empty")
				}
				if err := validateValue(item, depth+1); err != nil {
					return err
				}
			}
			return nil
		}
	}
	return merr.WrapErrParameterInvalidMsg("parameter value is unset")
}

func validateInputDataTypes(value *schemapb.FunctionParamValue, args []*schemapb.FunctionChainExprArg) error {
	columns := 0
	for _, arg := range args {
		if arg.GetColumn() != nil {
			columns++
		}
	}
	return validateTypeHintCount(value, columns)
}

func validateTypeHintCount(value *schemapb.FunctionParamValue, columns int) error {
	if err := validateValue(value, 0); err != nil {
		return err
	}
	array := value.GetArrayValue()
	if array == nil || len(array.Values) != columns {
		return merr.WrapErrParameterInvalidMsg("type hints must be an array aligned with expression column occurrences")
	}
	for _, item := range array.Values {
		v, ok := item.Value.(*schemapb.FunctionParamValue_Int64Value)
		if !ok || int64(int32(v.Int64Value)) != v.Int64Value {
			return merr.WrapErrParameterInvalidMsg("type hints must contain data type enum integers")
		}
		if _, ok := schemapb.DataType_name[int32(v.Int64Value)]; !ok {
			return merr.WrapErrParameterInvalidMsg("unknown type hint %d", v.Int64Value)
		}
	}
	return nil
}

func isNilPointer(value any) bool {
	return value == nil || reflect.ValueOf(value).Kind() == reflect.Ptr && reflect.ValueOf(value).IsNil()
}
