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

package chain

import (
	"errors"
	"math"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/grep"
	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func init() {
	MustRegisterOperator(types.OpTypeGrep, NewGrepOpFromReprWithContext)
}

// GrepOp selects rows and emits matching details. It consumes only offsets;
// the public text-field input is a native dependency resolved during preparation.
// Each instance binds one borrowed segment and snapshots its Go parameters.
// Native compilation and mutable execution state are local to that segment
// call. No Close is required; the caller keeps the segment pinned.
type GrepOp struct {
	BaseOp
	segment native.Segment
	request native.ScanRequest
}

func (*GrepOp) Name() string                 { return "Grep" }
func (*GrepOp) String() string               { return "Grep" }
func (*GrepOp) IsRunnable(stage string) bool { return stage == types.StageQueryScan }

// validateGrepRepr does not parse rg syntax or compile regexes. It is shared by
// schema-aware static planning and execution-node operator construction.
func validateGrepRepr(repr *OperatorRepr) (string, error) {
	if repr == nil || repr.Type != types.OpTypeGrep || repr.Function != nil {
		return "", merr.WrapErrParameterInvalidMsg("grep requires an independent operator without an expression")
	}
	if len(repr.Inputs) != 1 || len(repr.Outputs) != 1 {
		return "", merr.WrapErrParameterInvalidMsg("grep requires exactly one input and one output")
	}
	for _, name := range []string{repr.Inputs[0], repr.Outputs[0]} {
		if name == "" || strings.TrimSpace(name) != name || strings.HasPrefix(name, "$") {
			return "", merr.WrapErrParameterInvalidMsg("invalid grep field name %q", name)
		}
	}
	if repr.Inputs[0] == repr.Outputs[0] {
		return "", merr.WrapErrParameterInvalidMsg("grep output must not overwrite its input field")
	}
	arguments, ok := repr.Params["arguments"].GetValue().(*schemapb.FunctionParamValue_StringValue)
	if !ok || arguments == nil {
		return "", merr.WrapErrParameterInvalidMsg("grep arguments must be a string")
	}
	if len(repr.Params) != 1 {
		return "", merr.WrapErrParameterInvalidMsg("grep accepts only the arguments parameter; use Query limit")
	}
	return arguments.StringValue, nil
}

func NewGrepOpFromReprWithContext(repr *OperatorRepr, buildCtx types.FunctionBuildContext) (Operator, error) {
	arguments, err := validateGrepRepr(repr)
	if err != nil {
		return nil, err
	}
	binding := buildCtx.QueryScan
	if binding == nil {
		return nil, merr.WrapErrServiceInternalMsg("grep preparation requires a QueryScan binding")
	}
	if binding.Segment == nil {
		return nil, merr.WrapErrServiceInternalMsg("grep preparation requires a segment binding")
	}
	limit := binding.QueryLimit
	if limit == typeutil.Unlimited {
		// Native uses a positive cap; MaxInt64 cannot truncate a Go input array.
		limit = math.MaxInt64
	} else if limit <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("invalid Query limit in grep execution context")
	}
	maxResultBytes := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	if maxResultBytes <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("grep execution budgets must be positive")
	}
	config := &paramtable.Get().FunctionCfg
	compileLimits := native.CompileLimits{
		MaxRegexBytes:    uint64(config.GrepMaxRegexBytes.GetAsInt64()),
		MaxDFACacheBytes: uint64(config.GrepMaxDFACacheBytes.GetAsInt64()),
	}
	spec, err := grep.Parse(arguments, config.GrepMaxArgumentBytes.GetAsInt64())
	if err != nil {
		if errors.Is(err, merr.ErrParameterTooLarge) {
			return nil, merr.Wrap(err, "prepare grep arguments (function.grep.maxArgumentBytes)")
		}
		return nil, merr.Wrap(err, "prepare grep arguments")
	}
	executionLimits := native.ExecutionLimits{
		BatchSize:      config.GrepBatchSize.GetAsInt64(),
		MaxResultBytes: uint64(maxResultBytes),
	}
	return &GrepOp{
		BaseOp:  BaseOp{inputs: []string{types.SegOffsetFieldName}, outputs: append([]string(nil), repr.Outputs...)},
		segment: binding.Segment,
		request: native.ScanRequest{
			FieldID: binding.FieldID, Spec: *spec, Limit: limit,
			CompileLimits: compileLimits, ExecutionLimits: executionLimits,
		},
	}, nil
}

// Execute relies on factory/chain validation and ExecuteOperator's frame checks.
// ScanGrep validates native results before returning them.
func (o *GrepOp) Execute(ctx *types.FuncContext, input *DataFrame) (*OperatorResult, error) {
	if input.NumChunks() != 1 {
		return nil, merr.WrapErrServiceInternalMsg("grep requires one segment and one logical query chunk")
	}
	columns, err := o.ReadInputColumns("grep", input)
	if err != nil {
		return nil, err
	}
	offsets, ok := columns[0].Chunk(0).(*array.Int64)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("grep offsets must be Int64")
	}
	result, err := o.segment.ScanGrep(ctx.Context(), offsets, o.request)
	defer result.Release()
	if err != nil {
		return nil, merr.Wrap(err, "execute grep runner")
	}
	return &OperatorResult{
		Selection: &RowSelection{Chunks: []ChunkSelection{RowIndices(result.Positions)}},
		Columns: []NamedColumn{{
			Field: arrow.Field{Name: o.outputs[0], Type: native.GrepDetailsType()},
			Data:  arrow.NewChunked(native.GrepDetailsType(), []arrow.Array{result.Details}),
		}},
	}, nil
}

// grepOutputFields declares the operator's final Arrow result independently of
// segment binding, regex compilation and whether execution returns any rows.
func grepOutputFields(repr *OperatorRepr) ([]arrow.Field, error) {
	fields := make([]arrow.Field, 0, len(repr.Outputs))
	for _, name := range repr.Outputs {
		fields = append(fields, arrow.Field{Name: name, Type: native.GrepDetailsType()})
	}
	return fields, nil
}
