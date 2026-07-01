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
	Name      string
	Stage     string
	Operators []OperatorRepr
	Info      ChainReprInfo
}

// OperatorRepr is the internal representation of an Operator.
type OperatorRepr struct {
	Type     string
	Params   map[string]*schemapb.FunctionParamValue
	Function *FunctionRepr // for map/filter operators that evaluate an expression
	Inputs   []string      // input column names
	Outputs  []string      // output column names
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
		Name:      pb.GetName(),
		Stage:     stage,
		Operators: make([]OperatorRepr, 0, len(pb.GetOps())),
	}

	for i, opPB := range pb.GetOps() {
		opRepr, err := ProtoOpToRepr(opPB)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("op[%d]: %v", i, err)
		}
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
		Params:  pb.GetParams(),
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
	}

	return repr, nil
}

// ProtoExprToRepr converts a public FunctionChainExpr proto to the internal function representation.
// It also returns the column references in expr args, preserving first-seen order.
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
	seenInputs := make(map[string]struct{})

	for i, arg := range args {
		input, err := FunctionChainExprArgInput(arg)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("expr arg[%d]: %v", i, err)
		}
		if input == "" {
			continue
		}
		if _, ok := seenInputs[input]; ok {
			continue
		}
		seenInputs[input] = struct{}{}
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

		repr.Operators[i].Type = opType
		repr.Operators[i].Inputs = inputs
		repr.Operators[i].Outputs = outputs

		info.Ops = append(info.Ops, OperatorReprInfo{
			Type:       opType,
			ReadNames:  append([]string(nil), inputs...),
			WriteNames: append([]string(nil), outputs...),
		})

		for _, input := range inputs {
			if _, ok := produced[input]; ok {
				continue
			}
			if _, ok := seenRequiredInputs[input]; !ok {
				seenRequiredInputs[input] = struct{}{}
				info.RequiredInputs = append(info.RequiredInputs, input)
			}
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

	for i, opRepr := range repr.Operators {
		op, err := operatorFromReprWithContext(&opRepr, buildCtx)
		if err != nil {
			return nil, merr.WrapErrServiceInternalMsg("operator[%d]: %v", i, err)
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
	switch repr.Type {
	case types.OpTypeMap:
		return NewMapOpFromReprWithContext(repr, buildCtx)
	case types.OpTypeFilter:
		return NewFilterOpFromReprWithContext(repr, buildCtx)
	}

	factory, ok := GetOperatorFactory(repr.Type)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("unknown operator type: %s", repr.Type)
	}
	return factory(repr)
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

// IsFunctionChainSystemName reports whether name uses the function-chain system-name prefix.
// It does not validate whether the current caller or stage may provide that name.
func IsFunctionChainSystemName(name string) bool {
	return strings.HasPrefix(name, "$")
}
