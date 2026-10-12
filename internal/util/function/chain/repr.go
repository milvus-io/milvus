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

package chain

import (
	"strings"

	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ChainRepr is the internal representation of a FuncChain.
type ChainRepr struct {
	Name        string
	Stage       string
	Operators   []OperatorRepr
	Info        ChainReprInfo
	TableResult bool
}

// OperatorRepr is the internal representation of an Operator.
type OperatorRepr struct {
	Type           string
	Params         map[string]*schemapb.FunctionParamValue
	Function       *FunctionRepr // for map/filter operators that evaluate an expression
	Inputs         []string      // input column names, preserving occurrence order
	InputDataTypes []schemapb.DataType
	Outputs        []string // output column names
	TableResult    bool     // inherited from ChainRepr, never read from operator params
}

// FunctionRepr is the internal representation of a FunctionExpr.
type FunctionRepr struct {
	Name   string
	Params map[string]*schemapb.FunctionParamValue
	Args   []*schemapb.FunctionChainExprArg
}

// ChainReprInfo contains context-independent structural information derived from a ChainRepr.
// RequiredInputs are names read before any previous op produces them. Callers decide
// whether those names are schema fields, request payload fields, or runtime/system values.
type ChainReprInfo struct {
	RequiredInputs []string
	WrittenNames   []string
	Ops            []OperatorReprInfo
}

// OperatorReprInfo contains normalized input/output information for one operator.
type OperatorReprInfo struct {
	Type       string
	ReadNames  []string
	WriteNames []string
}

// ParseFuncChainProto creates a FuncChain from the public FunctionChain proto.
// It uses an empty FunctionBuildContext. Use FuncChainFromReprWithContext when
// constructing functions that require runtime-only context, such as model rerank.
func ParseFuncChainProto(pb *schemapb.FunctionChain, alloc memory.Allocator) (*FuncChain, error) {
	repr, err := ProtoChainToRepr(pb)
	if err != nil {
		return nil, err
	}
	return funcChainFromRepr(repr, alloc, types.FunctionBuildContext{})
}

// ProtoChainToRepr converts the public FunctionChain proto to the internal representation.
func ProtoChainToRepr(pb *schemapb.FunctionChain) (*ChainRepr, error) {
	if pb == nil {
		return nil, merr.WrapErrParameterInvalidMsg("function chain proto is nil")
	}

	stage, err := ProtoStageToReprStage(pb.GetStage())
	if err != nil {
		return nil, err
	}

	repr := &ChainRepr{
		Name:        pb.GetName(),
		Stage:       stage,
		TableResult: pb.GetPostProcessOptions().GetResultMode() == schemapb.FunctionChainResultMode_FunctionChainResultModeTable,
		Operators:   make([]OperatorRepr, 0, len(pb.GetOps())),
	}

	for i, opPB := range pb.GetOps() {
		opRepr, err := ProtoOpToRepr(opPB)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("op[%d]: %v", i, err)
		}
		opRepr.TableResult = repr.TableResult
		repr.Operators = append(repr.Operators, *opRepr)
	}

	if err := repr.RefreshInfo(); err != nil {
		return nil, err
	}

	return repr, nil
}

// ProtoOpToRepr converts a public FunctionChainOp proto to the internal operator representation.
func ProtoOpToRepr(pb *schemapb.FunctionChainOp) (*OperatorRepr, error) {
	if pb == nil {
		return nil, merr.WrapErrParameterInvalidMsg("op proto is nil")
	}

	opType := strings.TrimSpace(pb.GetOp())
	if opType == "" {
		return nil, merr.WrapErrParameterInvalidMsg("op name is empty")
	}

	inputs, err := normalizeReprNames(pb.GetInputs(), "input")
	if err != nil {
		return nil, err
	}
	outputs, err := normalizeReprNames(pb.GetOutputs(), "output")
	if err != nil {
		return nil, err
	}

	repr := &OperatorRepr{
		Type:    opType,
		Params:  cloneFunctionParamMap(pb.GetParams()),
		Inputs:  inputs,
		Outputs: outputs,
	}

	if pb.GetExpr() != nil {
		expr, exprInputs, err := ProtoExprToRepr(pb.GetExpr())
		if err != nil {
			return nil, err
		}
		repr.Function = expr
		repr.Inputs = exprInputs
		if opType == types.OpTypeReduce {
			keys, err := types.NewParamReader("reduce", repr.Params).StringSlice("keys", false)
			if err != nil {
				return nil, err
			}
			repr.Inputs = append(append([]string(nil), keys...), exprInputs...)
		}
	}
	if opType == types.OpTypeSelect && len(repr.Outputs) == 0 {
		repr.Outputs = append([]string(nil), repr.Inputs...)
	}

	inputDataTypes, err := parseInputDataTypes(repr.Params, len(repr.Inputs))
	if err != nil {
		return nil, err
	}
	repr.InputDataTypes = inputDataTypes
	delete(repr.Params, types.InputDataTypesParam)

	return repr, nil
}

// ProtoExprToRepr converts a public FunctionChainExpr proto to the internal function representation.
// It also returns the column references in expr args, preserving occurrence order.
func ProtoExprToRepr(pb *schemapb.FunctionChainExpr) (*FunctionRepr, []string, error) {
	if pb == nil {
		return nil, nil, nil
	}

	name := strings.TrimSpace(pb.GetName())
	if name == "" {
		return nil, nil, merr.WrapErrParameterInvalidMsg("expr name is empty")
	}

	inputs, err := ProtoExprArgsToInputs(pb.GetArgs())
	if err != nil {
		return nil, nil, err
	}

	return &FunctionRepr{
		Name:   name,
		Params: pb.GetParams(),
		Args:   pb.GetArgs(),
	}, inputs, nil
}

// ProtoExprArgsToInputs extracts column references from public FunctionChainExpr args.
func ProtoExprArgsToInputs(args []*schemapb.FunctionChainExprArg) ([]string, error) {
	inputs := make([]string, 0, len(args))

	for i, arg := range args {
		input, err := FunctionChainExprArgInput(arg)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("expr arg[%d]: %v", i, err)
		}
		if input == "" {
			continue
		}
		inputs = append(inputs, input)
	}

	return inputs, nil
}

// FunctionChainExprArgInput extracts the input column name from an arg, if it is a column reference.
func FunctionChainExprArgInput(arg *schemapb.FunctionChainExprArg) (string, error) {
	if arg == nil {
		return "", merr.WrapErrParameterInvalidMsg("function chain expr arg is nil")
	}

	switch v := arg.GetArg().(type) {
	case *schemapb.FunctionChainExprArg_Column:
		name := strings.TrimSpace(v.Column.GetName())
		if name == "" {
			return "", merr.WrapErrParameterInvalidMsg("column name is empty")
		}
		return name, nil
	case *schemapb.FunctionChainExprArg_Literal:
		if v.Literal == nil {
			return "", merr.WrapErrParameterInvalidMsg("literal: function param value is nil")
		}
		return "", nil
	default:
		return "", merr.WrapErrParameterInvalidMsg("function chain expr arg is unset")
	}
}

// ProtoStageToReprStage converts public FunctionChainStage enum to chain runtime stage string.
func ProtoStageToReprStage(stage schemapb.FunctionChainStage) (string, error) {
	switch stage {
	case schemapb.FunctionChainStage_FunctionChainStageIngestion:
		return types.StageIngestion, nil
	case schemapb.FunctionChainStage_FunctionChainStagePreProcess:
		return types.StagePreProcess, nil
	case schemapb.FunctionChainStage_FunctionChainStageL0Rerank:
		return types.StageL0Rerank, nil
	case schemapb.FunctionChainStage_FunctionChainStageL1Rerank:
		return types.StageL1Rerank, nil
	case schemapb.FunctionChainStage_FunctionChainStageL2Rerank:
		return types.StageL2Rerank, nil
	case schemapb.FunctionChainStage_FunctionChainStagePostProcess:
		return types.StagePostProcess, nil
	default:
		return "", merr.WrapErrParameterInvalidMsg("unsupported function chain stage: %s", stage.String())
	}
}

// RefreshInfo rebuilds context-independent dependency information from a ChainRepr.
// It does not classify names as schema fields, payload fields, or system variables.
func (repr *ChainRepr) RefreshInfo() error {
	if repr == nil {
		return nil
	}

	info := ChainReprInfo{
		RequiredInputs: make([]string, 0),
		WrittenNames:   make([]string, 0),
		Ops:            make([]OperatorReprInfo, 0, len(repr.Operators)),
	}
	produced := make(map[string]struct{})
	scopeLocked := false
	seenRequiredInputs := make(map[string]struct{})
	seenWrittenNames := make(map[string]struct{})

	for i, op := range repr.Operators {
		opType := strings.TrimSpace(op.Type)
		if opType == "" {
			return merr.WrapErrParameterInvalidMsg("op[%d] name is empty", i)
		}

		inputs, err := normalizeReprNames(op.Inputs, "input")
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("op[%d]: %v", i, err)
		}
		outputs, err := normalizeReprNames(op.Outputs, "output")
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("op[%d]: %v", i, err)
		}
		if opType == types.OpTypeSelect && len(outputs) == 0 {
			outputs = append([]string(nil), inputs...)
		}
		hints := op.InputDataTypes
		if hints != nil && len(hints) != len(inputs) {
			return merr.WrapErrParameterInvalidMsg("op[%d]: input data types count %d does not match input count %d", i, len(hints), len(inputs))
		}
		// Legacy sort/group_by parameters also name real column dependencies.
		// Normalize them before requery planning, pruning and runtime checks.
		if opType == types.OpTypeSort || opType == types.OpTypeGroupBy {
			normalized := op
			normalized.Type, normalized.Inputs, normalized.Outputs = opType, inputs, outputs
			normalized.TableResult = repr.TableResult
			factory, _ := GetOperatorFactory(opType)
			built, err := factory(&normalized, types.FunctionBuildContext{})
			if err != nil {
				return merr.Wrapf(err, "op[%d]", i)
			}
			actual := built.Inputs()
			actualHints := make([]schemapb.DataType, len(actual))
			for at, name := range actual {
				for before, explicit := range inputs {
					if explicit != name || hints == nil || hints[before] == schemapb.DataType_None {
						continue
					}
					if actualHints[at] != schemapb.DataType_None && actualHints[at] != hints[before] {
						return merr.WrapErrParameterInvalidMsg("op[%d]: conflicting type hints for input %q", i, name)
					}
					actualHints[at] = hints[before]
				}
			}
			inputs, hints = actual, actualHints
		}

		repr.Operators[i].Type = opType
		repr.Operators[i].Inputs = inputs
		repr.Operators[i].Outputs = outputs
		repr.Operators[i].TableResult = repr.TableResult
		if hints == nil {
			repr.Operators[i].InputDataTypes = make([]schemapb.DataType, len(inputs))
		} else {
			repr.Operators[i].InputDataTypes = hints
		}

		_, ok := GetOperatorFactory(opType)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("op[%d]: unknown operator type: %s", i, opType)
		}
		info.Ops = append(info.Ops, OperatorReprInfo{
			Type:       opType,
			ReadNames:  append([]string(nil), inputs...),
			WriteNames: append([]string(nil), outputs...),
		})

		for _, input := range inputs {
			if producedColumn(produced, input) {
				continue
			}
			if scopeLocked && (repr.TableResult || !IsFunctionChainSystemName(input)) {
				return merr.WrapErrParameterInvalidMsg("op[%d] input %q is unavailable after select", i, input)
			}
			if _, ok := seenRequiredInputs[input]; !ok {
				seenRequiredInputs[input] = struct{}{}
				info.RequiredInputs = append(info.RequiredInputs, input)
			}
		}

		if reprOpResetsColumnScope(opType) {
			produced = make(map[string]struct{}, len(outputs))
			scopeLocked = true
		}
		for _, output := range outputs {
			produced[output] = struct{}{}
			if _, ok := seenWrittenNames[output]; !ok {
				seenWrittenNames[output] = struct{}{}
				info.WrittenNames = append(info.WrittenNames, output)
			}
		}
	}

	repr.Info = info
	return nil
}

func producedColumn(produced map[string]struct{}, name string) bool {
	if _, ok := produced[name]; ok {
		return true
	}
	root := ColumnReferenceRoot(name)
	if root != name {
		_, ok := produced[root]
		return ok
	}
	return false
}

// FuncChainFromRepr creates a FuncChain from a ChainRepr.
// Stage is required and validates that all functions support the stage.
// alloc must not be nil.
// It uses an empty FunctionBuildContext. Use FuncChainFromReprWithContext when
// constructing functions that require runtime-only context, such as model rerank.
func FuncChainFromRepr(repr *ChainRepr, alloc memory.Allocator) (*FuncChain, error) {
	return funcChainFromRepr(repr, alloc, types.FunctionBuildContext{})
}

// FuncChainFromReprWithContext creates a FuncChain from a ChainRepr with runtime-only context.
func FuncChainFromReprWithContext(repr *ChainRepr, alloc memory.Allocator, buildCtx types.FunctionBuildContext) (*FuncChain, error) {
	return funcChainFromRepr(repr, alloc, buildCtx)
}

// funcChainFromRepr creates a FuncChain from a ChainRepr.
func funcChainFromRepr(repr *ChainRepr, alloc memory.Allocator, buildCtx types.FunctionBuildContext) (*FuncChain, error) {
	if alloc == nil {
		return nil, merr.WrapErrServiceInternal("alloc is nil")
	}

	if repr.Stage == "" {
		return nil, merr.WrapErrParameterMissingMsg("stage is required")
	}

	chain := NewFuncChainWithAllocator(alloc)
	if repr.Name != "" {
		chain.SetName(repr.Name)
	}
	chain.SetStage(repr.Stage)
	chain.SetTableResult(repr.TableResult)

	for i, opRepr := range repr.Operators {
		opRepr.TableResult = repr.TableResult
		op, err := operatorFromReprWithContext(&opRepr, buildCtx)
		if err != nil {
			return nil, merr.Wrapf(err, "operator[%d]", i)
		}
		chain.Add(op)
	}

	// Validate the chain (including stage compatibility)
	if err := chain.Validate(); err != nil {
		return nil, err
	}

	return chain, nil
}

// operatorFromRepr creates an Operator from an OperatorRepr.
func operatorFromRepr(repr *OperatorRepr) (Operator, error) {
	return operatorFromReprWithContext(repr, types.FunctionBuildContext{})
}

func operatorFromReprWithContext(repr *OperatorRepr, buildCtx types.FunctionBuildContext) (Operator, error) {
	factory, ok := GetOperatorFactory(repr.Type)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("unknown operator type: %s", repr.Type)
	}
	op, err := factory(repr, buildCtx)
	if err != nil {
		return nil, err
	}
	if len(repr.InputDataTypes) > 0 {
		if len(repr.InputDataTypes) != len(op.Inputs()) {
			return nil, merr.WrapErrParameterInvalidMsg("operator %s input type hints do not match inputs", repr.Type)
		}
		if typed, ok := op.(interface{ SetInputDataTypes([]schemapb.DataType) }); ok {
			typed.SetInputDataTypes(repr.InputDataTypes)
		}
	}
	return op, nil
}

// FunctionFromRepr creates a FunctionExpr from a FunctionRepr.
// It uses an empty FunctionBuildContext. Use FunctionFromReprWithContext when
// constructing functions that require runtime-only context, such as model rerank.
func FunctionFromRepr(repr *FunctionRepr) (types.FunctionExpr, error) {
	return FunctionFromReprWithContext(repr, types.FunctionBuildContext{})
}

// FunctionFromReprWithContext creates a FunctionExpr from a FunctionRepr and build context.
func FunctionFromReprWithContext(repr *FunctionRepr, buildCtx types.FunctionBuildContext) (types.FunctionExpr, error) {
	if repr == nil {
		return nil, merr.WrapErrParameterInvalidMsg("function repr is nil")
	}
	if repr.Name == "" {
		return nil, merr.WrapErrParameterMissingMsg("function name is required")
	}

	fn, err := types.CreateFunction(buildCtx, types.FunctionConfig{
		Name:   repr.Name,
		Params: repr.Params,
		Args:   repr.Args,
	})
	if err != nil {
		return nil, err
	}
	if validator, ok := fn.(types.FunctionArgValidator); ok {
		if err := validator.ValidateArgs(repr.Args); err != nil {
			return nil, err
		}
	}
	return fn, nil
}

func normalizeReprNames(names []string, label string) ([]string, error) {
	result := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, merr.WrapErrParameterInvalidMsg("%s name is empty", label)
		}
		result = append(result, name)
	}
	return result, nil
}

func cloneFunctionParamMap(params map[string]*schemapb.FunctionParamValue) map[string]*schemapb.FunctionParamValue {
	if params == nil {
		return nil
	}
	cloned := make(map[string]*schemapb.FunctionParamValue, len(params))
	for key, value := range params {
		cloned[key] = value
	}
	return cloned
}

func parseInputDataTypes(params map[string]*schemapb.FunctionParamValue, inputCount int) ([]schemapb.DataType, error) {
	result := make([]schemapb.DataType, inputCount)
	value, ok := params[types.InputDataTypesParam]
	if !ok {
		return result, nil
	}
	if value == nil || value.GetArrayValue() == nil {
		return nil, merr.WrapErrParameterInvalidMsg(
			"%s must be an array of data type enum values", types.InputDataTypesParam)
	}

	values := value.GetArrayValue().GetValues()
	if len(values) != inputCount {
		return nil, merr.WrapErrParameterInvalidMsg(
			"%s count %d does not match input count %d",
			types.InputDataTypesParam, len(values), inputCount)
	}
	for i, item := range values {
		if item == nil {
			return nil, merr.WrapErrParameterInvalidMsg(
				"%s[%d] is nil", types.InputDataTypesParam, i)
		}
		intValue, ok := item.GetValue().(*schemapb.FunctionParamValue_Int64Value)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg(
				"%s[%d] must be an int64 data type enum value", types.InputDataTypesParam, i)
		}
		if int64(int32(intValue.Int64Value)) != intValue.Int64Value {
			return nil, merr.WrapErrParameterInvalidMsg(
				"%s[%d] has unknown data type value %d",
				types.InputDataTypesParam, i, intValue.Int64Value)
		}
		if _, ok := schemapb.DataType_name[int32(intValue.Int64Value)]; !ok {
			return nil, merr.WrapErrParameterInvalidMsg(
				"%s[%d] has unknown data type value %d",
				types.InputDataTypesParam, i, intValue.Int64Value)
		}
		result[i] = schemapb.DataType(intValue.Int64Value)
	}
	return result, nil
}

// IsFunctionChainSystemName reports whether name belongs to the function-chain
// system namespace. The physical dynamic JSON root and its paths are schema
// inputs, not system columns, despite using the '$' prefix.
func IsFunctionChainSystemName(name string) bool {
	return strings.HasPrefix(name, "$") && !isExplicitMetaInput(name)
}

func reprOpResetsColumnScope(name string) bool {
	return name == types.OpTypeSelect || name == types.OpTypeAggregate || name == types.OpTypeReduce
}
